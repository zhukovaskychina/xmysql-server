package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func TestSelectExecutorMysqlUserRequiresStorageManager(t *testing.T) {
	executor := &SelectExecutor{
		schemaName: "mysql",
		tableName:  "user",
	}

	err := executor.executeUserTableQuery(context.Background())
	if err == nil {
		t.Fatalf("executeUserTableQuery() created default mysql.user data without storage manager")
	}
	if !strings.Contains(err.Error(), "storage manager") {
		t.Fatalf("executeUserTableQuery() error = %v, want storage manager error", err)
	}
}

func TestSelectExecutorMysqlUserDoesNotSynthesizeDefaultRoot(t *testing.T) {
	executor := &SelectExecutor{
		schemaName:      "mysql",
		tableName:       "user",
		storageManager:  &manager.StorageManager{},
		whereConditions: []string{"User = 'root'", "Host = 'localhost'"},
	}

	err := executor.executeUserTableQuery(context.Background())
	if err == nil {
		t.Fatalf("executeUserTableQuery() synthesized default root user")
	}
	if !strings.Contains(err.Error(), "mysql.user") {
		t.Fatalf("executeUserTableQuery() error = %v, want mysql.user error", err)
	}
}
