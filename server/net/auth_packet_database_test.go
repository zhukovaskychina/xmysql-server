package net

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func TestReadClientAuthDatabaseDoesNotConsumePluginWithoutConnectWithDB(t *testing.T) {
	payload := []byte("mysql_native_password\x00")
	database, next, err := readClientAuthDatabase(payload, 0, common.CLIENT_PLUGIN_AUTH)
	if err != nil {
		t.Fatalf("readClientAuthDatabase returned error: %v", err)
	}
	if database != "" {
		t.Fatalf("database = %q, want empty database", database)
	}
	if next != 0 {
		t.Fatalf("next = %d, want unchanged offset 0", next)
	}
}

func TestReadClientAuthDatabaseReadsEmptyDefaultDatabase(t *testing.T) {
	payload := []byte{0, 'm', 'y', 's', 'q', 'l', '_', 'n', 'a', 't', 'i', 'v', 'e', '_', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd', 0}
	database, next, err := readClientAuthDatabase(payload, 0, common.CLIENT_CONNECT_WITH_DB|common.CLIENT_PLUGIN_AUTH)
	if err != nil {
		t.Fatalf("readClientAuthDatabase returned error: %v", err)
	}
	if database != "" {
		t.Fatalf("database = %q, want empty database", database)
	}
	if next != 1 {
		t.Fatalf("next = %d, want 1", next)
	}
}
