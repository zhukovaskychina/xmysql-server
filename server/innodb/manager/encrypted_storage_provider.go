package manager

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// EncryptedStorageProvider composes page encryption with an existing durable
// provider. Only explicitly configured spaces are encrypted; all other spaces
// retain the underlying provider's behavior. The wrapper deliberately fails
// closed when a configured space has no loaded key.
type EncryptedStorageProvider struct {
	inner      basic.StorageProvider
	encryption *EncryptionManager

	mu              sync.RWMutex
	ioMu            sync.RWMutex
	encryptedSpaces map[uint32]struct{}
}

// NewEncryptedStorageProvider creates a provider that encrypts the listed
// tablespaces. Space membership can be changed with SetSpaceEncrypted.
func NewEncryptedStorageProvider(inner basic.StorageProvider, encryption *EncryptionManager, encryptedSpaces ...uint32) (*EncryptedStorageProvider, error) {
	if inner == nil {
		return nil, fmt.Errorf("storage provider is required")
	}
	if encryption == nil {
		return nil, fmt.Errorf("encryption manager is required")
	}
	spaces := make(map[uint32]struct{}, len(encryptedSpaces))
	for _, spaceID := range encryptedSpaces {
		spaces[spaceID] = struct{}{}
	}
	return &EncryptedStorageProvider{inner: inner, encryption: encryption, encryptedSpaces: spaces}, nil
}

// NewEncryptedStorageProviderFromKeyring restores encryption policy from the
// key IDs already loaded by EncryptionManager. This is the restart-safe
// constructor for a provider whose keyring is authoritative.
func NewEncryptedStorageProviderFromKeyring(inner basic.StorageProvider, encryption *EncryptionManager) (*EncryptedStorageProvider, error) {
	if encryption == nil {
		return nil, fmt.Errorf("encryption manager is required")
	}
	return NewEncryptedStorageProvider(inner, encryption, encryption.SpaceIDs()...)
}

// SetSpaceEncrypted changes encryption policy for one tablespace. Existing
// pages are not rewritten by this method; callers must migrate or rotate
// existing pages explicitly before changing policy.
func (p *EncryptedStorageProvider) SetSpaceEncrypted(spaceID uint32, encrypted bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.setSpaceEncryptedLocked(spaceID, encrypted)
}

func (p *EncryptedStorageProvider) setSpaceEncryptedLocked(spaceID uint32, encrypted bool) {
	if encrypted {
		p.encryptedSpaces[spaceID] = struct{}{}
	} else {
		delete(p.encryptedSpaces, spaceID)
	}
}

// EncryptSpace converts an existing plaintext tablespace to the configured
// page-encryption policy. It snapshots the old pages first so a failed write
// can restore the plaintext representation and disable the policy again.
func (p *EncryptedStorageProvider) EncryptSpace(spaceID uint32) error {
	if p.isSpaceEncrypted(spaceID) {
		return nil
	}
	p.ioMu.Lock()
	defer p.ioMu.Unlock()
	if p.encryption.GetKey(spaceID) == nil {
		return ErrKeyNotFound
	}
	info, err := p.inner.GetSpaceInfo(spaceID)
	if err != nil || info == nil {
		if err != nil {
			return err
		}
		return fmt.Errorf("space %d information is unavailable", spaceID)
	}
	type pageSnapshot struct {
		pageNo uint32
		plain  []byte
	}
	snapshots := make([]pageSnapshot, 0, info.TotalPages)
	for pageNo := uint32(0); pageNo < uint32(info.TotalPages); pageNo++ {
		plain, readErr := p.inner.ReadPage(spaceID, pageNo)
		if readErr != nil {
			if isMissingPageError(readErr) {
				continue
			}
			return fmt.Errorf("read page %d for encryption: %w", pageNo, readErr)
		}
		snapshots = append(snapshots, pageSnapshot{pageNo: pageNo, plain: append([]byte(nil), plain...)})
	}
	p.mu.Lock()
	p.setSpaceEncryptedLocked(spaceID, true)
	p.mu.Unlock()
	rollback := func(cause error) error {
		p.mu.Lock()
		p.setSpaceEncryptedLocked(spaceID, false)
		p.mu.Unlock()
		for _, snapshot := range snapshots {
			if restoreErr := p.inner.WritePage(spaceID, snapshot.pageNo, snapshot.plain); restoreErr != nil {
				cause = fmt.Errorf("%w; rollback page %d: %v", cause, snapshot.pageNo, restoreErr)
			}
		}
		_ = p.inner.Sync(spaceID)
		return cause
	}
	for _, snapshot := range snapshots {
		ciphertext, encryptErr := p.encryption.EncryptPageFixed(spaceID, snapshot.pageNo, snapshot.plain)
		if encryptErr != nil {
			return rollback(encryptErr)
		}
		if writeErr := p.inner.WritePage(spaceID, snapshot.pageNo, ciphertext); writeErr != nil {
			return rollback(writeErr)
		}
	}
	if err := p.inner.Sync(spaceID); err != nil {
		return rollback(err)
	}
	return nil
}

func (p *EncryptedStorageProvider) isSpaceEncrypted(spaceID uint32) bool {
	p.mu.RLock()
	_, ok := p.encryptedSpaces[spaceID]
	p.mu.RUnlock()
	return ok
}

func (p *EncryptedStorageProvider) requireKey(spaceID uint32) error {
	if !p.isSpaceEncrypted(spaceID) {
		return nil
	}
	if p.encryption.GetKey(spaceID) == nil {
		return ErrKeyNotFound
	}
	return nil
}

func (p *EncryptedStorageProvider) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	p.ioMu.RLock()
	defer p.ioMu.RUnlock()
	data, err := p.inner.ReadPage(spaceID, pageNo)
	if err != nil || !p.isSpaceEncrypted(spaceID) {
		return data, err
	}
	if err := p.requireKey(spaceID); err != nil {
		return nil, err
	}
	return p.encryption.DecryptPageFixed(spaceID, pageNo, data)
}

func (p *EncryptedStorageProvider) WritePage(spaceID, pageNo uint32, data []byte) error {
	p.ioMu.RLock()
	defer p.ioMu.RUnlock()
	if !p.isSpaceEncrypted(spaceID) {
		return p.inner.WritePage(spaceID, pageNo, data)
	}
	if err := p.requireKey(spaceID); err != nil {
		return err
	}
	ciphertext, err := p.encryption.EncryptPageFixed(spaceID, pageNo, data)
	if err != nil {
		return err
	}
	return p.inner.WritePage(spaceID, pageNo, ciphertext)
}

// ReencryptSpace rotates the configured tablespace key and rewrites every
// existing page under the new key. The old ciphertext is retained until all
// new writes succeed; a write or sync failure restores both the old key and
// the old page bytes before returning the original error.
func (p *EncryptedStorageProvider) ReencryptSpace(spaceID uint32) error {
	if !p.isSpaceEncrypted(spaceID) {
		return fmt.Errorf("space %d is not configured for encryption", spaceID)
	}
	p.ioMu.Lock()
	defer p.ioMu.Unlock()
	if err := p.requireKey(spaceID); err != nil {
		return err
	}
	info, err := p.inner.GetSpaceInfo(spaceID)
	if err != nil || info == nil {
		if err != nil {
			return err
		}
		return fmt.Errorf("space %d information is unavailable", spaceID)
	}
	if info.TotalPages > uint64(^uint32(0)) {
		return fmt.Errorf("space %d has too many pages to rotate", spaceID)
	}

	type pageSnapshot struct {
		pageNo     uint32
		ciphertext []byte
		plaintext  []byte
	}
	snapshots := make([]pageSnapshot, 0, info.TotalPages)
	for pageNo := uint32(0); pageNo < uint32(info.TotalPages); pageNo++ {
		ciphertext, readErr := p.inner.ReadPage(spaceID, pageNo)
		if readErr != nil {
			if isMissingPageError(readErr) {
				continue
			}
			return fmt.Errorf("read page %d for rotation: %w", pageNo, readErr)
		}
		plaintext, decryptErr := p.encryption.DecryptPageFixed(spaceID, pageNo, ciphertext)
		if decryptErr != nil {
			return fmt.Errorf("decrypt page %d for rotation: %w", pageNo, decryptErr)
		}
		snapshots = append(snapshots, pageSnapshot{
			pageNo:     pageNo,
			ciphertext: append([]byte(nil), ciphertext...),
			plaintext:  plaintext,
		})
	}
	oldKey := p.encryption.GetKey(spaceID)
	if err := p.encryption.RotateKey(spaceID); err != nil {
		return err
	}
	rollback := func(cause error) error {
		p.encryption.restoreKey(oldKey)
		for _, snapshot := range snapshots {
			if restoreErr := p.inner.WritePage(spaceID, snapshot.pageNo, snapshot.ciphertext); restoreErr != nil {
				cause = fmt.Errorf("%w; rollback page %d: %v", cause, snapshot.pageNo, restoreErr)
			}
		}
		_ = p.inner.Sync(spaceID)
		return cause
	}
	for _, snapshot := range snapshots {
		ciphertext, encryptErr := p.encryption.EncryptPageFixed(spaceID, snapshot.pageNo, snapshot.plaintext)
		if encryptErr != nil {
			return rollback(encryptErr)
		}
		if writeErr := p.inner.WritePage(spaceID, snapshot.pageNo, ciphertext); writeErr != nil {
			return rollback(writeErr)
		}
	}
	if err := p.inner.Sync(spaceID); err != nil {
		return rollback(err)
	}
	return nil
}

func isMissingPageError(err error) bool {
	return err != nil && (errors.Is(err, io.EOF) || strings.Contains(strings.ToLower(err.Error()), "eof"))
}

func (p *EncryptedStorageProvider) AllocatePage(spaceID uint32) (uint32, error) {
	return p.inner.AllocatePage(spaceID)
}
func (p *EncryptedStorageProvider) FreePage(spaceID, pageNo uint32) error {
	return p.inner.FreePage(spaceID, pageNo)
}
func (p *EncryptedStorageProvider) CreateSpace(name string, pageSize uint32) (uint32, error) {
	return p.inner.CreateSpace(name, pageSize)
}
func (p *EncryptedStorageProvider) OpenSpace(spaceID uint32) error {
	return p.inner.OpenSpace(spaceID)
}
func (p *EncryptedStorageProvider) CloseSpace(spaceID uint32) error {
	return p.inner.CloseSpace(spaceID)
}
func (p *EncryptedStorageProvider) DeleteSpace(spaceID uint32) error {
	return p.inner.DeleteSpace(spaceID)
}
func (p *EncryptedStorageProvider) GetSpaceInfo(spaceID uint32) (*basic.SpaceInfo, error) {
	return p.inner.GetSpaceInfo(spaceID)
}
func (p *EncryptedStorageProvider) ListSpaces() ([]basic.SpaceInfo, error) {
	return p.inner.ListSpaces()
}
func (p *EncryptedStorageProvider) BeginTransaction() (uint64, error) {
	return p.inner.BeginTransaction()
}
func (p *EncryptedStorageProvider) CommitTransaction(txID uint64) error {
	return p.inner.CommitTransaction(txID)
}
func (p *EncryptedStorageProvider) RollbackTransaction(txID uint64) error {
	return p.inner.RollbackTransaction(txID)
}
func (p *EncryptedStorageProvider) Sync(spaceID uint32) error {
	return p.inner.Sync(spaceID)
}
func (p *EncryptedStorageProvider) Close() error { return p.inner.Close() }
