package engine

import (
	"errors"
	"fmt"
	"reflect"
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
			wantPrefix: "索引操作失败",
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

func TestStorageIntegratedDMLExecutorBuildIndexKeyUsesAllCompositeColumns(t *testing.T) {
	dml := &StorageIntegratedDMLExecutor{}
	index := &manager.Index{
		Name: "idx_tenant_email",
		Columns: []manager.Column{
			{Name: "tenant_id", Nullable: false, Position: 1},
			{Name: "email", Nullable: false, Position: 2},
		},
	}

	key, err := dml.buildIndexKey(&InsertRowData{
		ColumnValues: map[string]interface{}{
			"tenant_id": int64(42),
			"email":     "a@example.com",
			"name":      "alice",
		},
	}, index, nil)
	if err != nil {
		t.Fatalf("buildIndexKey returned error: %v", err)
	}

	want := []interface{}{int64(42), "a@example.com"}
	if !reflect.DeepEqual(key, want) {
		t.Fatalf("expected composite key %#v, got %#v", want, key)
	}
}

func TestStorageIntegratedDMLExecutorBuildIndexKeyFromUpdateExpressionsKeepsOldCompositeParts(t *testing.T) {
	dml := &StorageIntegratedDMLExecutor{}
	index := &manager.Index{
		Name: "idx_tenant_email",
		Columns: []manager.Column{
			{Name: "tenant_id", Nullable: false, Position: 1},
			{Name: "email", Nullable: false, Position: 2},
		},
	}

	key, err := dml.buildIndexKeyFromUpdateExpressions(
		map[string]interface{}{
			"tenant_id": int64(42),
			"email":     "old@example.com",
		},
		[]*UpdateExpression{{ColumnName: "email", NewValue: "new@example.com"}},
		index,
		nil,
	)
	if err != nil {
		t.Fatalf("buildIndexKeyFromUpdateExpressions returned error: %v", err)
	}

	want := []interface{}{int64(42), "new@example.com"}
	if !reflect.DeepEqual(key, want) {
		t.Fatalf("expected composite key %#v, got %#v", want, key)
	}
}
