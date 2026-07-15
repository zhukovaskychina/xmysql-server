package manager

import (
	"path/filepath"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestQueryMySQLUserDoesNotSynthesizeRootWhenPrimaryIndexMissing(t *testing.T) {
	storage := &StorageManager{
		tablespaces: map[string]*TablespaceHandle{
			"mysql/user": {SpaceID: 1},
		},
	}

	user, err := storage.QueryMySQLUser("root", "localhost")
	if err == nil {
		t.Fatalf("QueryMySQLUser() returned synthetic user %#v, want error", user)
	}
}

func TestQueryMySQLUserReadsInitializedRootFromBTree(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewStorageManager(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        filepath.Join(dataDir, "innodb"),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
	t.Cleanup(func() {
		_ = storage.Close()
	})

	user, err := storage.QueryMySQLUser("root", "localhost")
	if err != nil {
		t.Fatalf("QueryMySQLUser(root@localhost) error = %v", err)
	}
	if user.User != "root" || user.Host != "localhost" {
		t.Fatalf("QueryMySQLUser(root@localhost) = %#v", user)
	}
	if user.AuthenticationString == "" {
		t.Fatalf("QueryMySQLUser(root@localhost) returned empty password hash")
	}
	if user.SelectPriv != "Y" || user.SuperPriv != "Y" {
		t.Fatalf("QueryMySQLUser(root@localhost) privileges = SELECT:%s SUPER:%s, want Y/Y", user.SelectPriv, user.SuperPriv)
	}
}
