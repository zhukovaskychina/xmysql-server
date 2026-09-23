package manager

import (
	"bytes"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestStorageManagerPersistsCompressionPolicyAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	cfg := conf.NewCfg()
	cfg.DataDir = dataDir
	cfg.InnodbDataDir = dataDir
	cfg.InnodbBufferPoolSize = 16 * 1024 * 1024
	cfg.InnodbPageSize = 16384
	cfg.InnodbCompression.Enabled = true
	cfg.InnodbCompression.AllSpaces = false

	first := NewStorageManager(cfg)
	if first == nil || first.compressedProvider == nil {
		t.Fatal("first manager did not initialize compression")
	}
	handle, err := first.CreateTablespace("persisted_compression")
	if err != nil {
		t.Fatalf("CreateTablespace() error = %v", err)
	}
	spaceID := handle.SpaceID
	if err := first.SetSpaceCompressed(spaceID, true); err != nil {
		t.Fatalf("SetSpaceCompressed() error = %v", err)
	}
	page := bytes.Repeat([]byte{'p'}, 16384)
	space, err := first.GetSpaceManager().GetTableSpace(spaceID)
	if err != nil {
		t.Fatalf("GetTableSpace() error = %v", err)
	}
	space.FlushToDisk(0, page)
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	second := NewStorageManager(cfg)
	if second == nil || second.compressedProvider == nil {
		t.Fatal("second manager did not initialize compression")
	}
	defer second.Close()
	info, err := second.GetSpaceInfo(spaceID)
	if err != nil {
		t.Fatalf("GetSpaceInfo() after restart error = %v", err)
	}
	if !info.IsCompressed {
		t.Fatal("explicit compression policy was not restored after restart")
	}
	space, err = second.GetSpaceManager().GetTableSpace(spaceID)
	if err != nil {
		t.Fatalf("GetTableSpace() after restart error = %v", err)
	}
	got, err := space.LoadPageByPageNumber(0)
	if err != nil {
		t.Fatalf("LoadPageByPageNumber() after restart error = %v", err)
	}
	if !bytes.Equal(got, page) {
		t.Fatalf("page after restart = %q, want original page", got)
	}
}

func TestStorageManagerWiresConfiguredCompressionThroughDirectSpaceAccess(t *testing.T) {
	dataDir := t.TempDir()
	cfg := conf.NewCfg()
	cfg.DataDir = dataDir
	cfg.InnodbDataDir = dataDir
	cfg.InnodbBufferPoolSize = 16 * 1024 * 1024
	cfg.InnodbPageSize = 16384
	cfg.InnodbCompression.Enabled = true
	cfg.InnodbCompression.Method = "zlib"
	cfg.InnodbCompression.Level = 1
	cfg.InnodbCompression.AllSpaces = true

	sm := NewStorageManager(cfg)
	if sm == nil {
		t.Fatal("NewStorageManager() returned nil")
	}
	defer sm.Close()
	if sm.compressedProvider == nil || sm.compressionManager == nil {
		t.Fatal("configured compression was not wired into StorageManager")
	}

	spaceID, err := sm.spaceMgr.CreateTableSpace("compressed_direct")
	if err != nil {
		t.Fatalf("CreateTableSpace() error = %v", err)
	}
	space, err := sm.spaceMgr.GetTableSpace(spaceID)
	if err != nil {
		t.Fatalf("GetTableSpace() error = %v", err)
	}
	page := bytes.Repeat([]byte{'x'}, 16384)
	space.FlushToDisk(0, page)
	got, err := space.LoadPageByPageNumber(0)
	if err != nil {
		t.Fatalf("LoadPageByPageNumber() error = %v", err)
	}
	if !bytes.Equal(got, page) {
		t.Fatalf("round trip page = %q, want %q", got, page)
	}
	info, err := sm.GetSpaceInfo(spaceID)
	if err != nil {
		t.Fatalf("GetSpaceInfo() error = %v", err)
	}
	if !info.IsCompressed {
		t.Fatal("configured tablespace must report IsCompressed")
	}
}

func TestStorageManagerComposesCompressionBeforeEncryption(t *testing.T) {
	dataDir := t.TempDir()
	cfg := conf.NewCfg()
	cfg.DataDir = dataDir
	cfg.InnodbDataDir = dataDir
	cfg.InnodbBufferPoolSize = 16 * 1024 * 1024
	cfg.InnodbPageSize = 16384
	cfg.InnodbEncryption.MasterKey = "compression-order-master-key"
	cfg.InnodbCompression.Enabled = true
	cfg.InnodbCompression.AllSpaces = true

	sm := NewStorageManager(cfg)
	if sm == nil {
		t.Fatal("NewStorageManager() returned nil")
	}
	defer sm.Close()
	if sm.compressedProvider == nil || sm.encryptedProvider == nil {
		t.Fatal("compression and encryption providers must both be wired")
	}

	spaceID, err := sm.spaceMgr.CreateTableSpace("compressed_encrypted")
	if err != nil {
		t.Fatalf("CreateTableSpace() error = %v", err)
	}
	space, err := sm.spaceMgr.GetTableSpace(spaceID)
	if err != nil {
		t.Fatalf("GetTableSpace() error = %v", err)
	}
	page := bytes.Repeat([]byte{'z'}, 16384)
	space.FlushToDisk(0, page)
	got, err := space.LoadPageByPageNumber(0)
	if err != nil {
		t.Fatalf("LoadPageByPageNumber() error = %v", err)
	}
	if !bytes.Equal(got, page) {
		t.Fatal("compression-before-encryption round trip changed page bytes")
	}
	raw, err := sm.encryptedProvider.inner.ReadPage(spaceID, 0)
	if err != nil {
		t.Fatalf("raw encrypted page read error = %v", err)
	}
	if bytes.Equal(raw, page) {
		t.Fatal("underlying page must not contain plaintext")
	}
}
