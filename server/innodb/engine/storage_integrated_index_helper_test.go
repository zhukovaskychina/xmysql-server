package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
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

func TestStorageIntegratedIndexKeyBuildersResolveColumnNamesCaseInsensitively(t *testing.T) {
	dml := &StorageIntegratedDMLExecutor{}
	single := &manager.Index{
		Name:    "idx_email",
		Columns: []manager.Column{{Name: "Email", Nullable: false}},
	}
	key, err := dml.buildIndexKey(&InsertRowData{ColumnValues: map[string]interface{}{
		"EMAIL": "a@example.com",
	}}, single, nil)
	if err != nil || key != "a@example.com" {
		t.Fatalf("single-column index key = %#v, %v; want a@example.com", key, err)
	}
	oldKey, err := dml.buildIndexKeyFromOldValues(map[string]interface{}{
		"email": "old@example.com",
	}, single, nil)
	if err != nil || oldKey != "old@example.com" {
		t.Fatalf("old single-column index key = %#v, %v; want old@example.com", oldKey, err)
	}
	updatedKey, err := dml.buildIndexKeyFromUpdateExpressions(
		map[string]interface{}{"email": "old@example.com"},
		[]*UpdateExpression{{ColumnName: "EMAIL", NewValue: "new@example.com"}},
		single,
		nil,
	)
	if err != nil || updatedKey != "new@example.com" {
		t.Fatalf("updated single-column index key = %#v, %v; want new@example.com", updatedKey, err)
	}

	composite := &manager.Index{
		Name: "idx_tenant_email",
		Columns: []manager.Column{
			{Name: "Tenant_ID", Nullable: false},
			{Name: "Email", Nullable: false},
		},
	}
	compositeKey, err := dml.buildIndexKey(&InsertRowData{ColumnValues: map[string]interface{}{
		"tenant_id": int64(42),
		"EMAIL":     "a@example.com",
	}}, composite, nil)
	if err != nil {
		t.Fatalf("composite index key returned error: %v", err)
	}
	if !reflect.DeepEqual(compositeKey, []interface{}{int64(42), "a@example.com"}) {
		t.Fatalf("composite index key = %#v, want [42 a@example.com]", compositeKey)
	}
}

func TestStorageIntegratedPrimaryKeyBuilderResolvesColumnNamesCaseInsensitively(t *testing.T) {
	table := &metadata.TableMeta{
		Name:       "users",
		PrimaryKey: []string{"UserID"},
		Columns:    []*metadata.ColumnMeta{{Name: "UserID", Type: metadata.TypeInt, IsPrimary: true}},
	}

	key, ok, err := buildPrimaryKeyIfAvailable(map[string]interface{}{"userid": int64(7)}, table)
	if err != nil {
		t.Fatalf("buildPrimaryKeyIfAvailable() error = %v", err)
	}
	if !ok {
		t.Fatal("buildPrimaryKeyIfAvailable() reported no primary key")
	}
	if string(key) != "\x00\x00\x00\x01"+"7" {
		t.Fatalf("primary key bytes = %v, want length-prefixed 7", key)
	}
}

func TestStorageIntegratedIndexMaintenanceMatchesColumnNamesCaseInsensitively(t *testing.T) {
	dml := &StorageIntegratedDMLExecutor{}
	index := &manager.Index{
		Name:    "idx_email",
		Columns: []manager.Column{{Name: "Email"}},
	}
	if !dml.indexNeedsUpdateForExpressions(index, []*UpdateExpression{{ColumnName: "EMAIL", NewValue: "new@example.com"}}) {
		t.Fatal("index maintenance should detect a case-insensitive column update")
	}
	if !dml.indexAffectedByColumns(index, []string{"email"}) {
		t.Fatal("index maintenance should detect a case-insensitive affected column")
	}
}

func TestStorageIntegratedIndexHelpersRejectMissingIndexManager(t *testing.T) {
	var dml *StorageIntegratedDMLExecutor
	index := &manager.Index{Name: "idx", IsUnique: true}

	tests := []struct {
		name string
		call func() error
	}{
		{name: "insert", call: func() error { return dml.insertIndexEntry(1, "k", 1) }},
		{name: "delete", call: func() error { return dml.deleteIndexEntry(1, "k") }},
		{name: "update", call: func() error { return dml.updateIndexEntry(1, "old", "new", 1) }},
		{name: "unique", call: func() error { return dml.checkIndexKeyUniqueness(1, "k", index) }},
		{name: "batch insert", call: func() error { return dml.batchInsertIndexEntries(1, nil) }},
		{name: "batch delete", call: func() error { return dml.batchDeleteIndexEntries(1, nil) }},
		{name: "rebuild", call: func() error { return dml.rebuildIndexForTable(1) }},
		{name: "consistency", call: func() error { return dml.checkIndexConsistency(1) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); err == nil {
				t.Fatal("expected missing index manager error")
			}
		})
	}
}

func TestUniqueIndexLookupTreatsPageZeroAsDuplicate(t *testing.T) {
	index := &manager.Index{Name: "uk_email", IsUnique: true}

	err := checkUniqueIndexKey("alice@example.com", index, func() (uint32, int, error) {
		return 0, 3, nil
	})
	if err == nil || !strings.Contains(err.Error(), "唯一索引约束违反") {
		t.Fatalf("page-zero match should violate unique index, got %v", err)
	}
}

func TestUniqueIndexLookupIgnoresOnlyMissingKey(t *testing.T) {
	index := &manager.Index{Name: "uk_email", IsUnique: true}

	if err := checkUniqueIndexKey("alice@example.com", index, func() (uint32, int, error) {
		return 0, 0, basic.ErrKeyNotFound
	}); err != nil {
		t.Fatalf("missing key should allow insert, got %v", err)
	}
}

func TestUniqueIndexLookupPropagatesSearchFailure(t *testing.T) {
	index := &manager.Index{Name: "uk_email", IsUnique: true}
	searchErr := errors.New("storage read failed")

	err := checkUniqueIndexKey("alice@example.com", index, func() (uint32, int, error) {
		return 0, 0, searchErr
	})
	if err == nil || !errors.Is(err, searchErr) {
		t.Fatalf("search failure should be propagated, got %v", err)
	}
}

func TestUniqueSecondaryIndexAllowsNullableNullKey(t *testing.T) {
	dml := &StorageIntegratedDMLExecutor{}
	index := &manager.Index{
		Name:     "uk_email",
		IsUnique: true,
		Columns:  []manager.Column{{Name: "email", Nullable: true}},
	}
	if err := dml.validateIndexKey(nil, index); err != nil {
		t.Fatalf("nullable unique secondary key should allow NULL, got %v", err)
	}
	called := false
	if err := checkUniqueIndexKey(nil, index, func() (uint32, int, error) {
		called = true
		return 0, 0, errors.New("NULL key should not be searched")
	}); err != nil {
		t.Fatalf("nullable unique secondary NULL should not violate uniqueness, got %v", err)
	}
	if called {
		t.Fatal("NULL unique secondary key should bypass duplicate lookup")
	}
}

func TestUniqueNonNullableAndPrimaryIndexRejectNullKey(t *testing.T) {
	dml := &StorageIntegratedDMLExecutor{}
	for _, index := range []*manager.Index{
		{Name: "uk_email", IsUnique: true, Columns: []manager.Column{{Name: "email", Nullable: false}}},
		{Name: "PRIMARY", IsUnique: true, IsPrimary: true, Columns: []manager.Column{{Name: "id", Nullable: true}}},
	} {
		if err := dml.validateIndexKey(nil, index); err == nil {
			t.Fatalf("index %s should reject NULL key", index.Name)
		}
	}
}

func TestUniqueConflictLookupResolvesColumnNamesCaseInsensitively(t *testing.T) {
	tableMeta := &metadata.TableMeta{
		Columns: []*metadata.ColumnMeta{{Name: "Email", IsUnique: true}},
	}
	conflicts, err := rowConflictsWithInsert(
		map[string]interface{}{"email": "alice@example.com"},
		map[string]interface{}{"EMAIL": "alice@example.com"},
		tableMeta,
		[]string{"Email"},
	)
	if err != nil {
		t.Fatalf("rowConflictsWithInsert() error = %v", err)
	}
	if !conflicts {
		t.Fatal("unique conflict lookup should be case-insensitive")
	}
}

func TestCompositeUniqueIndexConflictsUseTableIndexMetadata(t *testing.T) {
	tableMeta := &metadata.TableMeta{
		Columns: []*metadata.ColumnMeta{
			{Name: "Tenant_ID"},
			{Name: "Email"},
		},
		Indices: []metadata.IndexMeta{{
			Name:    "uq_tenant_email",
			Columns: []string{"Tenant_ID", "Email"},
			Unique:  true,
		}},
	}

	conflicts, err := rowConflictsWithInsert(
		map[string]interface{}{"tenant_id": int64(7), "email": "alice@example.com"},
		map[string]interface{}{"TENANT_ID": int64(7), "EMAIL": "alice@example.com"},
		tableMeta,
		nil,
	)
	if err != nil {
		t.Fatalf("rowConflictsWithInsert() error = %v", err)
	}
	if !conflicts {
		t.Fatal("matching composite unique index values should conflict")
	}

	conflicts, err = rowConflictsWithInsert(
		map[string]interface{}{"tenant_id": int64(7), "email": "alice@example.com"},
		map[string]interface{}{"tenant_id": int64(8), "email": "alice@example.com"},
		tableMeta,
		nil,
	)
	if err != nil {
		t.Fatalf("rowConflictsWithInsert() error = %v", err)
	}
	if conflicts {
		t.Fatal("different composite unique prefix values should not conflict")
	}

	conflicts, err = rowConflictsWithInsert(
		map[string]interface{}{"tenant_id": int64(7), "email": nil},
		map[string]interface{}{"tenant_id": int64(7), "email": nil},
		tableMeta,
		nil,
	)
	if err != nil {
		t.Fatalf("rowConflictsWithInsert() NULL error = %v", err)
	}
	if conflicts {
		t.Fatal("NULL in a composite unique key should not conflict")
	}
}

func TestCompositeUniqueIndexRejectsDuplicateBatchRows(t *testing.T) {
	tableMeta := &metadata.TableMeta{
		Columns: []*metadata.ColumnMeta{{Name: "tenant_id"}, {Name: "email"}},
		Indices: []metadata.IndexMeta{{Name: "uq_tenant_email", Columns: []string{"tenant_id", "email"}, Unique: true}},
	}
	dml := &StorageIntegratedDMLExecutor{uniqueChecks: true}
	rows := []*InsertRowData{
		{ColumnValues: map[string]interface{}{"tenant_id": int64(7), "email": "alice@example.com"}},
		{ColumnValues: map[string]interface{}{"tenant_id": int64(7), "email": "alice@example.com"}},
	}
	if err := dml.validateUniqueConstraints(context.Background(), rows, tableMeta, nil, nil); err == nil {
		t.Fatal("duplicate composite unique batch rows should be rejected")
	}
}
