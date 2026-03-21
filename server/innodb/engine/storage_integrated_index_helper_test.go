package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func TestStorageIntegratedDMLExecutorHandleIndexErrorUsesStructuredErrors(t *testing.T) {
	dml := &StorageIntegratedDMLExecutor{}

	tests := []struct {
		name       string
		err        error
		wantPrefix string
	}{
		{
			name:       "structured duplicate key",
			err:        basic.ErrDuplicateKey,
			wantPrefix: "索引键重复",
		},
		{
			name:       "wrapped structured not found",
			err:        fmt.Errorf("%w: %v", manager.ErrIndexNotFound, errors.New("lookup failed")),
			wantPrefix: "索引键未找到",
		},
		{
			name:       "plain text duplicate should not be classified",
			err:        errors.New("duplicate key without structured type"),
			wantPrefix: "索引键重复",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := dml.handleIndexError(tt.err, "INSERT", "idx_users_name", "alice")
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.HasPrefix(err.Error(), tt.wantPrefix) {
				t.Fatalf("expected error prefix %q, got %q", tt.wantPrefix, err.Error())
			}
		})
	}
}
