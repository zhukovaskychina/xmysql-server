package manager

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestStorageManagerUsesEncryptedProviderWhenMasterKeyConfigured(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	cfg.InnodbBufferPoolSize = 1 << 20
	cfg.InnodbEncryption.MasterKey = "storage-manager-master-key"

	sm := NewStorageManager(cfg)
	if sm == nil {
		t.Fatal("NewStorageManager() returned nil")
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = sm.Close()
		}
	})
	if sm.encryptionManager == nil {
		t.Fatal("master key did not create an encryption manager")
	}
	if sm.encryptedProvider == nil {
		t.Fatal("master key did not wrap the storage provider")
	}
	handle, err := sm.CreateTablespace("encrypted_tablespace")
	if err != nil {
		t.Fatalf("CreateTablespace() error = %v", err)
	}
	if sm.encryptionManager.GetKey(handle.SpaceID) == nil {
		t.Fatalf("CreateTablespace() did not create a tablespace key for %d", handle.SpaceID)
	}
	if !sm.encryptedProvider.isSpaceEncrypted(handle.SpaceID) {
		t.Fatalf("CreateTablespace() did not enable page encryption for %d", handle.SpaceID)
	}
	page := bytes.Repeat([]byte("managed-page-"), 1261)
	page = page[:16384]
	if err := sm.encryptedProvider.WritePage(handle.SpaceID, 0, page); err != nil {
		t.Fatalf("encrypted provider WritePage() error = %v", err)
	}
	rawPage, err := sm.encryptedProvider.inner.ReadPage(handle.SpaceID, 0)
	if err != nil {
		t.Fatalf("raw LoadPageByPageNumber() error = %v", err)
	}
	if bytes.Equal(rawPage, page) {
		t.Fatal("StorageManager encrypted provider persisted plaintext")
	}
	decoded, err := sm.encryptedProvider.ReadPage(handle.SpaceID, 0)
	if err != nil || !bytes.Equal(decoded, page) {
		t.Fatalf("encrypted provider read-back = (%v, %v), want original page", decoded, err)
	}
	directSpace, err := sm.GetSpaceManager().GetSpace(handle.SpaceID)
	if err != nil {
		t.Fatalf("GetSpaceManager().GetSpace() error = %v", err)
	}
	if err := directSpace.FlushToDisk(1, page); err != nil {
		t.Fatalf("direct Space FlushToDisk() error = %v", err)
	}
	directDecoded, err := directSpace.LoadPageByPageNumber(1)
	if err != nil || !bytes.Equal(directDecoded, page) {
		t.Fatalf("direct Space read-back = (%v, %v), want original page", directDecoded, err)
	}
	rawDirect, err := sm.encryptedProvider.inner.ReadPage(handle.SpaceID, 1)
	if err != nil {
		t.Fatalf("raw direct page read error = %v", err)
	}
	if bytes.Equal(rawDirect, page) {
		t.Fatal("direct Space path persisted plaintext")
	}
	if err := sm.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	closed = true
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "encryption.keyring")); err != nil {
		t.Fatalf("Close() did not persist the keyring: %v", err)
	}
}

func TestStorageManagerReloadsEncryptedTablespaceAfterRestart(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	cfg.InnodbBufferPoolSize = 1 << 20
	cfg.InnodbEncryption.MasterKey = "storage-manager-restart-key"

	first := NewStorageManager(cfg)
	handle, err := first.CreateTablespace("restart_encrypted")
	if err != nil {
		t.Fatalf("first CreateTablespace() error = %v", err)
	}
	page := bytes.Repeat([]byte{'r'}, 16384)
	firstSpace, err := first.GetSpaceManager().GetSpace(handle.SpaceID)
	if err != nil {
		t.Fatalf("first GetSpace() error = %v", err)
	}
	pageNo, err := firstSpace.(interface{ AllocatePage() (uint32, error) }).AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage() error = %v", err)
	}
	if err := first.encryptedProvider.WritePage(handle.SpaceID, pageNo, page); err != nil {
		t.Fatalf("first WritePage() error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	second := NewStorageManager(cfg)
	if second == nil || second.encryptedProvider == nil {
		t.Fatal("restart did not initialize encrypted provider")
	}
	defer second.Close()
	space, err := second.GetSpaceManager().GetSpace(handle.SpaceID)
	if err != nil {
		t.Fatalf("restarted GetSpace() error = %v", err)
	}
	got, err := space.LoadPageByPageNumber(pageNo)
	if err != nil {
		t.Fatalf("restarted LoadPageByPageNumber() error = %v", err)
	}
	if !bytes.Equal(got, page) {
		t.Fatalf("restarted page = %x, want original page", got)
	}
}

func TestStorageManagerMigratesPreKeyringPlaintextTablespace(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	cfg.InnodbBufferPoolSize = 1 << 20

	plainManager := NewSpaceManager(cfg.DataDir)
	spaceID, err := plainManager.CreateTableSpace("pre_keyring")
	if err != nil {
		t.Fatalf("create plaintext tablespace: %v", err)
	}
	plainSpace, err := plainManager.GetSpace(spaceID)
	if err != nil {
		t.Fatalf("get plaintext tablespace: %v", err)
	}
	pageNo, err := plainSpace.(interface{ AllocatePage() (uint32, error) }).AllocatePage()
	if err != nil {
		t.Fatalf("allocate plaintext page: %v", err)
	}
	page := bytes.Repeat([]byte{'p'}, 16384)
	if err := plainSpace.FlushToDisk(pageNo, page); err != nil {
		t.Fatalf("write plaintext page: %v", err)
	}
	if err := plainManager.Close(); err != nil {
		t.Fatalf("close plaintext manager: %v", err)
	}

	cfg.InnodbEncryption.MasterKey = "migration-master-key"
	sm := NewStorageManager(cfg)
	if sm == nil || sm.encryptedProvider == nil {
		t.Fatal("encrypted storage manager was not initialized")
	}
	defer sm.Close()
	if sm.encryptionManager.GetKey(spaceID) == nil {
		t.Fatalf("migration did not provision a key for space %d", spaceID)
	}
	if !sm.encryptedProvider.isSpaceEncrypted(spaceID) {
		t.Fatalf("migration did not enable encryption for space %d", spaceID)
	}
	raw, err := sm.encryptedProvider.inner.ReadPage(spaceID, pageNo)
	if err != nil {
		t.Fatalf("read migrated raw page: %v", err)
	}
	if bytes.Equal(raw, page) {
		t.Fatal("pre-keyring plaintext page remained unencrypted")
	}
	decoded, err := sm.encryptedProvider.ReadPage(spaceID, pageNo)
	if err != nil || !bytes.Equal(decoded, page) {
		t.Fatalf("read migrated page = (%v, %v), want original page", decoded, err)
	}
}
