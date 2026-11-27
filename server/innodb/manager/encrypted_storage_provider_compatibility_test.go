package manager

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

func TestEncryptedStorageProviderRoundTripsFixedSizePages(t *testing.T) {
	const (
		spaceID = 41
		pageNo  = 7
	)
	inner := newEncryptedProviderTestStorage()
	em := NewEncryptionManager([]byte("provider-master-key"), EncryptionSettings{Method: ENCRYPTION_METHOD_AES})
	if _, err := em.CreateKey(spaceID); err != nil {
		t.Fatalf("CreateKey() error = %v", err)
	}
	provider, err := NewEncryptedStorageProvider(inner, em, spaceID)
	if err != nil {
		t.Fatalf("NewEncryptedStorageProvider() error = %v", err)
	}

	plaintext := bytes.Repeat([]byte("page-data-"), 32)
	if err := provider.WritePage(spaceID, pageNo, plaintext); err != nil {
		t.Fatalf("WritePage() error = %v", err)
	}
	raw := inner.pages[[2]uint32{spaceID, pageNo}]
	if bytes.Equal(raw, plaintext) {
		t.Fatal("underlying provider received plaintext")
	}
	if len(raw) != len(plaintext) {
		t.Fatalf("encrypted page length = %d, want %d", len(raw), len(plaintext))
	}

	got, err := provider.ReadPage(spaceID, pageNo)
	if err != nil {
		t.Fatalf("ReadPage() error = %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("ReadPage() = %x, want %x", got, plaintext)
	}
}

func TestEncryptedStorageProviderFailsClosedForConfiguredSpaceWithoutKey(t *testing.T) {
	inner := newEncryptedProviderTestStorage()
	em := NewEncryptionManager([]byte("provider-master-key"), EncryptionSettings{Method: ENCRYPTION_METHOD_AES})
	provider, err := NewEncryptedStorageProvider(inner, em, 52)
	if err != nil {
		t.Fatalf("NewEncryptedStorageProvider() error = %v", err)
	}
	if err := provider.WritePage(52, 1, bytes.Repeat([]byte{'x'}, 32)); err != ErrKeyNotFound {
		t.Fatalf("WritePage() error = %v, want %v", err, ErrKeyNotFound)
	}
}

func TestEncryptedStorageProviderDelegatesUnencryptedSpace(t *testing.T) {
	inner := newEncryptedProviderTestStorage()
	em := NewEncryptionManager([]byte("provider-master-key"), EncryptionSettings{Method: ENCRYPTION_METHOD_AES})
	provider, err := NewEncryptedStorageProvider(inner, em)
	if err != nil {
		t.Fatalf("NewEncryptedStorageProvider() error = %v", err)
	}
	plaintext := bytes.Repeat([]byte{'p'}, 32)
	if err := provider.WritePage(53, 2, plaintext); err != nil {
		t.Fatalf("WritePage() error = %v", err)
	}
	if !bytes.Equal(inner.pages[[2]uint32{53, 2}], plaintext) {
		t.Fatal("unencrypted space was unexpectedly transformed")
	}
}

func TestEncryptedStorageProviderRestartsFromKeyringAndReencryptsExistingPages(t *testing.T) {
	const spaceID = 61
	inner := newEncryptedProviderTestStorage()
	inner.pageCount = 3
	masterKey := []byte("restartable-provider-master")
	writer := NewEncryptionManager(masterKey, EncryptionSettings{Method: ENCRYPTION_METHOD_AES})
	if _, err := writer.CreateKey(spaceID); err != nil {
		t.Fatalf("CreateKey() error = %v", err)
	}
	provider, err := NewEncryptedStorageProvider(inner, writer, spaceID)
	if err != nil {
		t.Fatalf("NewEncryptedStorageProvider() error = %v", err)
	}
	for pageNo := uint32(0); pageNo < inner.pageCount; pageNo++ {
		if err := provider.WritePage(spaceID, pageNo, bytes.Repeat([]byte{byte(pageNo + 1)}, 32)); err != nil {
			t.Fatalf("WritePage(%d) error = %v", pageNo, err)
		}
	}

	keyringPath := t.TempDir() + "/keyring"
	if err := writer.SaveKeyring(keyringPath); err != nil {
		t.Fatalf("SaveKeyring() error = %v", err)
	}
	reader := NewEncryptionManager(masterKey, EncryptionSettings{Method: ENCRYPTION_METHOD_AES})
	if err := reader.LoadKeyring(keyringPath); err != nil {
		t.Fatalf("LoadKeyring() error = %v", err)
	}
	restarted, err := NewEncryptedStorageProviderFromKeyring(inner, reader)
	if err != nil {
		t.Fatalf("restarted provider error = %v", err)
	}
	before := append([]byte(nil), inner.pages[[2]uint32{spaceID, 0}]...)
	if err := restarted.ReencryptSpace(spaceID); err != nil {
		t.Fatalf("ReencryptSpace() error = %v", err)
	}
	after := inner.pages[[2]uint32{spaceID, 0}]
	if bytes.Equal(before, after) {
		t.Fatal("key rotation did not change persisted ciphertext")
	}
	for pageNo := uint32(0); pageNo < inner.pageCount; pageNo++ {
		got, err := restarted.ReadPage(spaceID, pageNo)
		if err != nil {
			t.Fatalf("ReadPage(%d) after rotation error = %v", pageNo, err)
		}
		want := bytes.Repeat([]byte{byte(pageNo + 1)}, 32)
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadPage(%d) after rotation = %x, want %x", pageNo, got, want)
		}
	}
}

type encryptedProviderTestStorage struct {
	mu        sync.Mutex
	pages     map[[2]uint32][]byte
	pageCount uint32
}

func newEncryptedProviderTestStorage() *encryptedProviderTestStorage {
	return &encryptedProviderTestStorage{pages: make(map[[2]uint32][]byte)}
}

func (s *encryptedProviderTestStorage) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.pages[[2]uint32{spaceID, pageNo}]
	if !ok {
		return nil, fmt.Errorf("page not found")
	}
	return append([]byte(nil), data...), nil
}

func (s *encryptedProviderTestStorage) WritePage(spaceID, pageNo uint32, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pages[[2]uint32{spaceID, pageNo}] = append([]byte(nil), data...)
	return nil
}

func (s *encryptedProviderTestStorage) AllocatePage(uint32) (uint32, error) { return 0, nil }
func (s *encryptedProviderTestStorage) FreePage(uint32, uint32) error       { return nil }
func (s *encryptedProviderTestStorage) CreateSpace(string, uint32) (uint32, error) {
	return 0, nil
}
func (s *encryptedProviderTestStorage) OpenSpace(uint32) error   { return nil }
func (s *encryptedProviderTestStorage) CloseSpace(uint32) error  { return nil }
func (s *encryptedProviderTestStorage) DeleteSpace(uint32) error { return nil }
func (s *encryptedProviderTestStorage) GetSpaceInfo(uint32) (*basic.SpaceInfo, error) {
	return &basic.SpaceInfo{TotalPages: uint64(s.pageCount), PageSize: 32}, nil
}
func (s *encryptedProviderTestStorage) ListSpaces() ([]basic.SpaceInfo, error) { return nil, nil }
func (s *encryptedProviderTestStorage) BeginTransaction() (uint64, error)      { return 0, nil }
func (s *encryptedProviderTestStorage) CommitTransaction(uint64) error         { return nil }
func (s *encryptedProviderTestStorage) RollbackTransaction(uint64) error       { return nil }
func (s *encryptedProviderTestStorage) Sync(uint32) error                      { return nil }
func (s *encryptedProviderTestStorage) Close() error                           { return nil }
