# InnoDB Record Format Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current runtime-only DML row view and custom `XDMLROWS1` page payload with one persistent B+Tree-backed record format that survives restart and supports JDBC CRUD through real table pages.

**Architecture:** The clustered primary index becomes the single source of truth for row storage. DML writes rows as deterministic InnoDB-compatible record payloads through B+Tree leaf records, SELECT scans the same leaf records, secondary/unique indexes store encoded index keys pointing to clustered primary keys, and AUTO_INCREMENT state is persisted per table. The existing JDBC behavior must remain green while `dml_memory_store.go` and the ad hoc `XDMLROWS1` page collection are removed.

**Tech Stack:** Go 1.24.3, existing `server/innodb` storage manager, enhanced B+Tree manager, sqlparser, JDBC integration tests under `jdbc_client`.

---

## Current Gaps

The current implementation passes runtime JDBC CRUD by using an in-memory DML table view in `server/innodb/engine/dml_memory_store.go`. It also writes rows into pages using the private `XDMLROWS1` encoding in `server/innodb/engine/storage_integrated_dml_helper.go`. That is useful for proving SQL semantics, but it is not a durable InnoDB row path.

The target state is:

- `server/innodb/engine/dml_memory_store.go` is deleted.
- `dmlPageRowsMagic`, `dmlPageRow`, `encodeDMLPageRows`, `decodeDMLPageRows`, `appendDMLPageRow`, `replaceDMLPageRow`, and `markDMLPageRowDeleted` are deleted.
- INSERT writes one row record through the clustered B+Tree.
- SELECT/UPDATE/DELETE read from clustered B+Tree records, not from memory.
- UNIQUE checks use durable secondary unique indexes.
- AUTO_INCREMENT values come from persisted table metadata and survive restart.
- A restart integration test proves `insert -> flush -> stop -> restart -> select` works.

## File Map

Create:

- `server/innodb/engine/record_codec.go`  
  Encodes/decodes table rows into a deterministic record payload independent of Go map order.
- `server/innodb/engine/record_codec_test.go`  
  Unit tests for nulls, ints, floats, strings, default values, column order, and corrupted payloads.
- `server/innodb/engine/clustered_index_scanner.go`  
  Provides full and range scans over the clustered primary index using B+Tree iterator APIs.
- `server/innodb/engine/clustered_index_scanner_test.go`  
  Tests scanner behavior against a fake iterator and against the enhanced B+Tree adapter when possible.
- `server/innodb/engine/auto_increment_state.go`  
  Persists and loads per-table auto increment counters from a small JSON sidecar.
- `server/innodb/engine/auto_increment_state_test.go`  
  Tests monotonic allocation, explicit value advancement, concurrent allocation, and reload.
- `server/innodb/engine/jdbc_persistence_integration_test.go`  
  Engine-level restart persistence regression without requiring Maven.
- `server/innodb/manager/secondary_index_mapping.go`  
  Defines durable secondary index key encoding and index metadata lookup helpers.
- `server/innodb/manager/secondary_index_mapping_test.go`  
  Tests key encoding for UNIQUE and non-UNIQUE secondary indexes.

Modify:

- `server/innodb/engine/storage_integrated_dml_helper.go`  
  Remove private page row collection format; route row serialization to `record_codec.go`; evaluate WHERE over decoded clustered records.
- `server/innodb/engine/storage_integrated_dml_executor.go`  
  Remove memory view writes/updates/deletes; write/read/update/delete through clustered B+Tree and secondary indexes.
- `server/innodb/engine/select_executor.go`  
  Remove `memorySelectRows` path; scan clustered records via `clustered_index_scanner.go`.
- `server/innodb/engine/executor.go`  
  Persist table metadata needed for row codec, secondary indexes, and auto increment counters; clear persisted state on DROP/TRUNCATE.
- `server/innodb/engine/dml_executor.go`  
  Keep `UpdateExpression.Expr`; ensure update expressions are evaluated against decoded row values.
- `server/innodb/manager/enhanced_btree_adapter.go`  
  Add iterator/full scan methods to the `basic.BPlusTreeManager` compatibility adapter.
- `server/innodb/manager/table_storage_mapping.go`  
  Persist and reload root page/index page metadata reliably.
- `server/innodb/manager/btree_interface.go`  
  Keep existing iterator contracts; add only the minimum compatibility surface if `basic.BPlusTreeManager` lacks scan methods.
- `server/dispatcher/query_dispatcher.go`  
  Keep DML affected rows and SELECT value conversion behavior.
- `jdbc_client/src/test/java/com/xmysql/server/test/DMLOperationsTest.java`  
  No expected behavior change; use as regression suite.

Delete:

- `server/innodb/engine/dml_memory_store.go`

## Task 1: Add Deterministic InnoDB Row Record Codec

**Files:**
- Create: `server/innodb/engine/record_codec.go`
- Create: `server/innodb/engine/record_codec_test.go`
- Modify: `server/innodb/engine/storage_integrated_dml_helper.go`

- [ ] **Step 1: Write failing codec tests**

Create `server/innodb/engine/record_codec_test.go`:

```go
package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func testCodecTableMeta() *metadata.TableMeta {
	return &metadata.TableMeta{
		Name: "users",
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: metadata.TypeInt, IsPrimary: true, IsAutoIncrement: true},
			{Name: "username", Type: metadata.TypeVarchar, IsUnique: true, IsNullable: false},
			{Name: "age", Type: metadata.TypeInt, DefaultValue: int64(18)},
			{Name: "score", Type: metadata.TypeDecimal},
			{Name: "note", Type: metadata.TypeVarchar, IsNullable: true},
		},
		PrimaryKey: []string{"id"},
	}
}

func TestRecordCodecRoundTripUsesTableColumnOrder(t *testing.T) {
	meta := testCodecTableMeta()
	row := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"username": "alice",
			"id":       int64(7),
			"score":    float64(9.5),
			"age":      int64(25),
			"note":     nil,
		},
		ColumnTypes: map[string]metadata.DataType{},
	}

	encoded, err := EncodeClusteredRecord(row, meta)
	require.NoError(t, err)

	decoded, err := DecodeClusteredRecord(encoded, meta)
	require.NoError(t, err)
	require.Equal(t, int64(7), decoded.ColumnValues["id"])
	require.Equal(t, "alice", decoded.ColumnValues["username"])
	require.Equal(t, int64(25), decoded.ColumnValues["age"])
	require.Equal(t, float64(9.5), decoded.ColumnValues["score"])
	require.Nil(t, decoded.ColumnValues["note"])
}

func TestRecordCodecRejectsCorruptPayload(t *testing.T) {
	_, err := DecodeClusteredRecord([]byte{0x01, 0x02}, testCodecTableMeta())
	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestRecordCodec' -count=1
```

Expected: FAIL with `undefined: EncodeClusteredRecord` and `undefined: DecodeClusteredRecord`.

- [ ] **Step 3: Implement record codec**

Create `server/innodb/engine/record_codec.go`:

```go
package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

var clusteredRecordMagic = []byte("XIR1")

const (
	recordValueNull byte = iota
	recordValueString
	recordValueInt64
	recordValueFloat64
	recordValueBool
)

func EncodeClusteredRecord(row *InsertRowData, tableMeta *metadata.TableMeta) ([]byte, error) {
	if row == nil {
		return nil, fmt.Errorf("row is nil")
	}
	if tableMeta == nil || len(tableMeta.Columns) == 0 {
		return nil, fmt.Errorf("table metadata is empty")
	}

	var buf bytes.Buffer
	buf.Write(clusteredRecordMagic)
	if err := binary.Write(&buf, binary.LittleEndian, uint16(len(tableMeta.Columns))); err != nil {
		return nil, err
	}

	for _, col := range tableMeta.Columns {
		if col == nil || col.Name == "" {
			return nil, fmt.Errorf("invalid column metadata")
		}
		value := row.ColumnValues[col.Name]
		encoded, err := encodeRecordValue(value, col.Type)
		if err != nil {
			return nil, fmt.Errorf("encode column %s: %w", col.Name, err)
		}
		if len(encoded) > math.MaxUint32 {
			return nil, fmt.Errorf("column %s payload too large", col.Name)
		}
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(encoded))); err != nil {
			return nil, err
		}
		buf.Write(encoded)
	}

	return buf.Bytes(), nil
}

func DecodeClusteredRecord(data []byte, tableMeta *metadata.TableMeta) (*InsertRowData, error) {
	if tableMeta == nil || len(tableMeta.Columns) == 0 {
		return nil, fmt.Errorf("table metadata is empty")
	}
	if len(data) < len(clusteredRecordMagic)+2 || !bytes.Equal(data[:len(clusteredRecordMagic)], clusteredRecordMagic) {
		return nil, fmt.Errorf("invalid clustered record magic")
	}

	offset := len(clusteredRecordMagic)
	columnCount := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2
	if columnCount != len(tableMeta.Columns) {
		return nil, fmt.Errorf("column count mismatch: record=%d metadata=%d", columnCount, len(tableMeta.Columns))
	}

	row := &InsertRowData{
		ColumnValues: make(map[string]interface{}, len(tableMeta.Columns)),
		ColumnTypes:  make(map[string]metadata.DataType, len(tableMeta.Columns)),
	}

	for _, col := range tableMeta.Columns {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("column %s length missing", col.Name)
		}
		valueLen := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		if valueLen < 0 || offset+valueLen > len(data) {
			return nil, fmt.Errorf("column %s payload truncated", col.Name)
		}
		value, err := decodeRecordValue(data[offset:offset+valueLen], col.Type)
		if err != nil {
			return nil, fmt.Errorf("decode column %s: %w", col.Name, err)
		}
		offset += valueLen
		row.ColumnValues[col.Name] = value
		row.ColumnTypes[col.Name] = col.Type
	}

	if offset != len(data) {
		return nil, fmt.Errorf("clustered record has trailing bytes: %d", len(data)-offset)
	}
	return row, nil
}

func encodeRecordValue(value interface{}, dataType metadata.DataType) ([]byte, error) {
	if value == nil {
		return []byte{recordValueNull}, nil
	}
	switch normalizedType(dataType) {
	case "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT":
		n, err := asInt64(value)
		if err != nil {
			return nil, err
		}
		out := make([]byte, 9)
		out[0] = recordValueInt64
		binary.LittleEndian.PutUint64(out[1:], uint64(n))
		return out, nil
	case "DECIMAL", "FLOAT", "DOUBLE":
		n, err := asFloat64(value)
		if err != nil {
			return nil, err
		}
		out := make([]byte, 9)
		out[0] = recordValueFloat64
		binary.LittleEndian.PutUint64(out[1:], math.Float64bits(n))
		return out, nil
	case "BOOL", "BOOLEAN":
		out := []byte{recordValueBool, 0}
		if v, ok := value.(bool); ok && v {
			out[1] = 1
		}
		return out, nil
	default:
		return append([]byte{recordValueString}, []byte(fmt.Sprintf("%v", value))...), nil
	}
}

func decodeRecordValue(data []byte, dataType metadata.DataType) (interface{}, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty value")
	}
	switch data[0] {
	case recordValueNull:
		return nil, nil
	case recordValueString:
		return string(data[1:]), nil
	case recordValueInt64:
		if len(data) != 9 {
			return nil, fmt.Errorf("invalid int64 payload length %d", len(data))
		}
		return int64(binary.LittleEndian.Uint64(data[1:])), nil
	case recordValueFloat64:
		if len(data) != 9 {
			return nil, fmt.Errorf("invalid float64 payload length %d", len(data))
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(data[1:])), nil
	case recordValueBool:
		if len(data) != 2 {
			return nil, fmt.Errorf("invalid bool payload length %d", len(data))
		}
		return data[1] == 1, nil
	default:
		return nil, fmt.Errorf("unknown value marker %d for type %s", data[0], dataType)
	}
}

func normalizedType(dataType metadata.DataType) string {
	return strings.ToUpper(strings.TrimSpace(string(dataType)))
}

func asInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("uint64 %d overflows int64", v)
		}
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

func asFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(strings.TrimSpace(v), 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}
```

- [ ] **Step 4: Route existing helpers through codec**

Modify `server/innodb/engine/storage_integrated_dml_helper.go`:

```go
func (dml *StorageIntegratedDMLExecutor) serializeRowData(row *InsertRowData, tableMeta *metadata.TableMeta) ([]byte, error) {
	return EncodeClusteredRecord(row, tableMeta)
}

func (dml *StorageIntegratedDMLExecutor) deserializeRowData(data []byte, tableMeta *metadata.TableMeta) (*InsertRowData, error) {
	return DecodeClusteredRecord(data, tableMeta)
}
```

Update all callers of `deserializeRowData(data)` to call `deserializeRowData(data, tableMeta)`.

- [ ] **Step 5: Run codec tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestRecordCodec' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/innodb/engine/record_codec.go server/innodb/engine/record_codec_test.go server/innodb/engine/storage_integrated_dml_helper.go
git commit -m "feat: add clustered record codec"
```

## Task 2: Remove Custom `XDMLROWS1` Page Collection Format

**Files:**
- Modify: `server/innodb/engine/storage_integrated_dml_helper.go`
- Modify: `server/innodb/engine/storage_integrated_dml_helper_p0_test.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`

- [ ] **Step 1: Write failing guard test that rejects XDMLROWS1 usage**

Create or modify `server/innodb/engine/storage_integrated_dml_helper_p0_test.go`:

```go
func TestStorageIntegratedDMLDoesNotUseXDMLRowsPageFormat(t *testing.T) {
	require.NotContains(t, string(clusteredRecordMagic), "XDMLROWS1")
}
```

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestStorageIntegratedDMLDoesNotUseXDMLRowsPageFormat' -count=1
```

Expected before cleanup: FAIL if the old magic is still referenced in row write path, or compile failure after symbols are removed but tests still reference old helpers.

- [ ] **Step 2: Remove old page collection symbols**

Delete these declarations from `server/innodb/engine/storage_integrated_dml_helper.go`:

```go
var dmlPageRowsMagic = []byte("XDMLROWS1")

type dmlPageRow struct {
	Deleted bool
	Data    []byte
}

encodeDMLPageRows
decodeDMLPageRows
isEmptyDMLPageContent
appendDMLPageRow
replaceDMLPageRow
markDMLPageRowDeleted
```

- [ ] **Step 3: Stop writing row collections directly into the root page**

Modify `insertRowToStorage` in `server/innodb/engine/storage_integrated_dml_executor.go` so it only writes through `btreeManager.Insert`:

```go
serializedRow, err := dml.serializeRowData(row, tableMeta)
if err != nil {
	return 0, fmt.Errorf("序列化行数据失败: %v", err)
}

if err := btreeManager.Insert(ctx, primaryKey, serializedRow); err != nil {
	return 0, fmt.Errorf("插入到B+树失败: %v", err)
}

if txnCtx, ok := txn.(*StorageTransactionContext); ok && txnCtx != nil {
	txnCtx.ModifiedPages[fmt.Sprintf("%d:%d", tableStorageInfo.SpaceID, tableStorageInfo.RootPageNo)] = tableStorageInfo.RootPageNo
}
```

Remove the block that calls `bufferPoolManager.GetPage`, `appendDMLPageRow`, `SetContent`, and `MarkDirty`.

- [ ] **Step 4: Update tests to assert B+Tree payload writes**

Replace old page collection tests with tests that verify `EncodeClusteredRecord` payloads are passed to B+Tree insert. Use a fake B+Tree manager in `storage_integrated_dml_helper_p0_test.go`:

```go
type recordingBTreeManager struct {
	insertedKey   interface{}
	insertedValue []byte
}

func (m *recordingBTreeManager) Insert(ctx context.Context, key interface{}, value []byte) error {
	m.insertedKey = key
	m.insertedValue = append([]byte(nil), value...)
	return nil
}
```

Implement only methods required by the local test. The test should decode `insertedValue` with `DecodeClusteredRecord` and assert table columns.

- [ ] **Step 5: Run targeted engine tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestRecordCodec|TestStorageIntegratedDML' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/innodb/engine/storage_integrated_dml_helper.go server/innodb/engine/storage_integrated_dml_helper_p0_test.go server/innodb/engine/storage_integrated_dml_executor.go
git commit -m "refactor: remove custom dml page row format"
```

## Task 3: Add Clustered B+Tree Scan API and SELECT Integration

**Files:**
- Create: `server/innodb/engine/clustered_index_scanner.go`
- Create: `server/innodb/engine/clustered_index_scanner_test.go`
- Modify: `server/innodb/manager/enhanced_btree_adapter.go`
- Modify: `server/innodb/engine/select_executor.go`

- [ ] **Step 1: Write failing scanner test**

Create `server/innodb/engine/clustered_index_scanner_test.go`:

```go
package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

type fakeIndexIterator struct {
	records []*manager.IndexRecord
	pos     int
}

func (it *fakeIndexIterator) HasNext() bool { return it.pos < len(it.records) }
func (it *fakeIndexIterator) Next() (*manager.IndexRecord, error) {
	rec := it.records[it.pos]
	it.pos++
	return rec, nil
}
func (it *fakeIndexIterator) HasPrev() bool { return false }
func (it *fakeIndexIterator) Prev() (*manager.IndexRecord, error) { return nil, nil }
func (it *fakeIndexIterator) SeekFirst() error { it.pos = 0; return nil }
func (it *fakeIndexIterator) SeekLast() error { it.pos = len(it.records) - 1; return nil }
func (it *fakeIndexIterator) SeekTo(key []byte) error { it.pos = 0; return nil }
func (it *fakeIndexIterator) Current() (*manager.IndexRecord, error) { return it.records[it.pos], nil }
func (it *fakeIndexIterator) GetPosition() (uint32, uint16) { return 0, uint16(it.pos) }
func (it *fakeIndexIterator) Close() error { return nil }

func TestClusteredIndexScannerSkipsDeleteMarkedRecords(t *testing.T) {
	meta := testCodecTableMeta()
	activePayload, err := EncodeClusteredRecord(&InsertRowData{
		ColumnValues: map[string]interface{}{"id": int64(1), "username": "alice", "age": int64(18), "score": float64(1), "note": nil},
		ColumnTypes:  map[string]metadata.DataType{},
	}, meta)
	require.NoError(t, err)

	deletedPayload, err := EncodeClusteredRecord(&InsertRowData{
		ColumnValues: map[string]interface{}{"id": int64(2), "username": "bob", "age": int64(19), "score": float64(2), "note": nil},
		ColumnTypes:  map[string]metadata.DataType{},
	}, meta)
	require.NoError(t, err)

	scanner := NewClusteredIndexScanner(meta, &fakeIndexIterator{records: []*manager.IndexRecord{
		{Value: activePayload},
		{Value: deletedPayload, DeleteMark: true},
	}})

	rows, err := scanner.ScanAll(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "alice", rows[0].ColumnValues["username"])
}
```

Add the missing import:

```go
import "github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestClusteredIndexScanner' -count=1
```

Expected: FAIL with `undefined: NewClusteredIndexScanner`.

- [ ] **Step 3: Implement scanner**

Create `server/innodb/engine/clustered_index_scanner.go`:

```go
package engine

import (
	"context"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

type ClusteredIndexScanner struct {
	tableMeta *metadata.TableMeta
	iterator manager.IndexIterator
}

func NewClusteredIndexScanner(tableMeta *metadata.TableMeta, iterator manager.IndexIterator) *ClusteredIndexScanner {
	return &ClusteredIndexScanner{tableMeta: tableMeta, iterator: iterator}
}

func (s *ClusteredIndexScanner) ScanAll(ctx context.Context) ([]*InsertRowData, error) {
	if s == nil || s.iterator == nil {
		return nil, fmt.Errorf("clustered index iterator is nil")
	}
	if err := s.iterator.SeekFirst(); err != nil {
		return nil, err
	}
	defer s.iterator.Close()

	rows := make([]*InsertRowData, 0)
	for s.iterator.HasNext() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		record, err := s.iterator.Next()
		if err != nil {
			return nil, err
		}
		if record == nil || record.DeleteMark {
			continue
		}
		row, err := DecodeClusteredRecord(record.Value, s.tableMeta)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}
```

- [ ] **Step 4: Add adapter iterator access**

Modify `server/innodb/manager/enhanced_btree_adapter.go`:

```go
func (adapter *EnhancedBTreeAdapter) Iterator(ctx context.Context) (IndexIterator, error) {
	index, err := adapter.enhancedManager.GetIndex(adapter.defaultIndexID)
	if err != nil {
		return nil, fmt.Errorf("failed to get index: %v", err)
	}
	return index.Iterator(ctx)
}
```

If `basic.BPlusTreeManager` cannot expose this method, use a type assertion in SELECT:

```go
type clusteredIteratorProvider interface {
	Iterator(ctx context.Context) (manager.IndexIterator, error)
}
```

- [ ] **Step 5: Replace SELECT memory scan**

Modify `scanDMLPageRows` in `server/innodb/engine/select_executor.go`:

```go
func (se *SelectExecutor) scanDMLPageRows(ctx context.Context, tableMeta *metadata.TableMeta) error {
	if se.storageManager == nil || se.storageManager.GetTableStorageManager() == nil {
		se.resultSet = []Record{}
		return nil
	}

	tableStorageInfo, err := se.storageManager.GetTableStorageManager().GetTableStorageInfo(se.schemaName, se.tableName)
	if err != nil {
		se.resultSet = []Record{}
		return nil
	}

	tableBTree, err := se.storageManager.GetTableStorageManager().CreateBTreeManagerForTable(ctx, tableStorageInfo.SchemaName, tableStorageInfo.TableName)
	if err != nil {
		return err
	}
	iteratorProvider, ok := tableBTree.(clusteredIteratorProvider)
	if !ok {
		return fmt.Errorf("table B+Tree manager does not support clustered iteration")
	}
	iterator, err := iteratorProvider.Iterator(ctx)
	if err != nil {
		return err
	}

	rows, err := NewClusteredIndexScanner(tableMeta, iterator).ScanAll(ctx)
	if err != nil {
		return err
	}

	records := make([]Record, 0, len(rows))
	for _, row := range rows {
		matches, err := rowMatchesWhereConditions(row.ColumnValues, se.whereConditions)
		if err != nil {
			return err
		}
		if matches {
			records = append(records, recordFromInsertRowData(row, tableMeta))
		}
	}
	se.resultSet = records
	return nil
}
```

Remove the `memorySelectRows` branch and the `decodeDMLPageRows` fallback.

- [ ] **Step 6: Run SELECT regression tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestClusteredIndexScanner|TestStorageIntegratedDML' -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/innodb/engine/clustered_index_scanner.go server/innodb/engine/clustered_index_scanner_test.go server/innodb/manager/enhanced_btree_adapter.go server/innodb/engine/select_executor.go
git commit -m "feat: scan clustered records through btree"
```

## Task 4: Implement Persistent UPDATE and DELETE Through Clustered B+Tree

**Files:**
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/engine/storage_integrated_dml_helper.go`
- Modify: `server/innodb/engine/dml_memory_store.go` by deleting it
- Modify: `server/innodb/engine/storage_integrated_dml_executor_p0_test.go`

- [ ] **Step 1: Write failing test that memory store is gone**

Add to `server/innodb/engine/storage_integrated_dml_executor_p0_test.go`:

```go
func TestDMLExecutorDoesNotUseMemoryStoreSymbols(t *testing.T) {
	_, exists := memorySelectRows("db", "table")
	require.False(t, exists)
}
```

This should fail to compile after the memory store is deleted. Remove this guard test in the same commit after grep-based verification in Step 5. Its purpose is to force the task to remove the memory path.

- [ ] **Step 2: Remove memory-store calls from DML executor**

Delete these calls from `server/innodb/engine/storage_integrated_dml_executor.go`:

```go
memoryValidateUnique
memoryInsertRows
memoryUpdateRows
memoryDeleteRows
```

Delete the file:

```bash
rm server/innodb/engine/dml_memory_store.go
```

- [ ] **Step 3: Update UPDATE to scan clustered records**

In `ExecuteUpdate`, after parsing WHERE and update expressions, call the clustered scanner:

```go
rowsToUpdate, err := dml.findRowsToUpdateInStorage(ctx, txn, whereConditions, tableMeta, tableStorageInfo, tableBtreeManager)
if err != nil {
	dml.rollbackStorageTransaction(ctx, txn)
	return nil, fmt.Errorf("查找待更新行失败: %v", err)
}
```

Implement `findRowsToUpdateInStorage` so it:

1. Uses primary key search when WHERE contains `id = ?`.
2. Otherwise iterates clustered records through B+Tree scanner.
3. Builds `RowUpdateInfo{RowId, PageNum, SlotIndex, OldValues}` from `manager.IndexRecord.PageNo` and `SlotNo`.

- [ ] **Step 4: Update DELETE to use delete marks**

In `deleteRowFromStorage`, replace custom page-row delete marking with B+Tree delete:

```go
primaryKeyBytes, err := dml.generatePrimaryKeyBytesFromRowData(rowInfo.OldValues, tableMeta)
if err != nil {
	return fmt.Errorf("生成主键字节失败: %v", err)
}
if err := btreeManager.Delete(ctx, primaryKeyBytes); err != nil {
	return fmt.Errorf("从B+树删除记录失败: %v", err)
}
```

If `basic.BPlusTreeManager.Delete` expects `interface{}` instead of bytes, pass `rowInfo.RowId` for primary key tables and add an adapter overload only if required by compile errors.

- [ ] **Step 5: Verify no memory store or XDMLROWS references remain**

Run:

```bash
rg -n "memoryDMLStore|memorySelectRows|memoryInsertRows|memoryUpdateRows|memoryDeleteRows|XDMLROWS1|appendDMLPageRow|decodeDMLPageRows" server/innodb/engine
```

Expected: no output.

- [ ] **Step 6: Run DML tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestStorageIntegratedDML' -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/innodb/engine
git rm server/innodb/engine/dml_memory_store.go
git commit -m "feat: persist update and delete through clustered btree"
```

## Task 5: Persist AUTO_INCREMENT Per Table

**Files:**
- Create: `server/innodb/engine/auto_increment_state.go`
- Create: `server/innodb/engine/auto_increment_state_test.go`
- Modify: `server/innodb/engine/storage_integrated_dml_helper.go`
- Modify: `server/innodb/engine/executor.go`

- [ ] **Step 1: Write failing auto increment state tests**

Create `server/innodb/engine/auto_increment_state_test.go`:

```go
package engine

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAutoIncrementStatePersistsAcrossReload(t *testing.T) {
	dir := t.TempDir()
	store := NewAutoIncrementStateStore(dir)

	next, err := store.Next("app", "users", "id")
	require.NoError(t, err)
	require.Equal(t, int64(1), next)

	next, err = store.Next("app", "users", "id")
	require.NoError(t, err)
	require.Equal(t, int64(2), next)

	reloaded := NewAutoIncrementStateStore(dir)
	next, err = reloaded.Next("app", "users", "id")
	require.NoError(t, err)
	require.Equal(t, int64(3), next)

	require.FileExists(t, filepath.Join(dir, "app", "users.autoinc.json"))
}

func TestAutoIncrementStateAdvancesPastExplicitValue(t *testing.T) {
	store := NewAutoIncrementStateStore(t.TempDir())
	require.NoError(t, store.ObserveExplicit("app", "users", "id", int64(42)))

	next, err := store.Next("app", "users", "id")
	require.NoError(t, err)
	require.Equal(t, int64(43), next)
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestAutoIncrementState' -count=1
```

Expected: FAIL with `undefined: NewAutoIncrementStateStore`.

- [ ] **Step 3: Implement auto increment state store**

Create `server/innodb/engine/auto_increment_state.go`:

```go
package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type AutoIncrementStateStore struct {
	baseDir string
	mu      sync.Mutex
}

type autoIncrementStateFile struct {
	Columns map[string]int64 `json:"columns"`
}

func NewAutoIncrementStateStore(baseDir string) *AutoIncrementStateStore {
	return &AutoIncrementStateStore{baseDir: baseDir}
}

func (s *AutoIncrementStateStore) Next(schemaName, tableName, columnName string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, err := s.load(schemaName, tableName)
	if err != nil {
		return 0, err
	}
	next := state.Columns[columnName] + 1
	state.Columns[columnName] = next
	if err := s.save(schemaName, tableName, state); err != nil {
		return 0, err
	}
	return next, nil
}

func (s *AutoIncrementStateStore) ObserveExplicit(schemaName, tableName, columnName string, value int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, err := s.load(schemaName, tableName)
	if err != nil {
		return err
	}
	if value > state.Columns[columnName] {
		state.Columns[columnName] = value
	}
	return s.save(schemaName, tableName, state)
}

func (s *AutoIncrementStateStore) RemoveTable(schemaName, tableName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.path(schemaName, tableName))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (s *AutoIncrementStateStore) load(schemaName, tableName string) (*autoIncrementStateFile, error) {
	path := s.path(schemaName, tableName)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &autoIncrementStateFile{Columns: map[string]int64{}}, nil
	}
	if err != nil {
		return nil, err
	}
	var state autoIncrementStateFile
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.Columns == nil {
		state.Columns = map[string]int64{}
	}
	return &state, nil
}

func (s *AutoIncrementStateStore) save(schemaName, tableName string, state *autoIncrementStateFile) error {
	dir := filepath.Join(s.baseDir, schemaName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path(schemaName, tableName) + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(schemaName, tableName))
}

func (s *AutoIncrementStateStore) path(schemaName, tableName string) string {
	return filepath.Join(s.baseDir, schemaName, fmt.Sprintf("%s.autoinc.json", tableName))
}
```

- [ ] **Step 4: Use state store in primary key generation**

Modify `StorageIntegratedDMLExecutor` to carry an optional auto increment store:

```go
autoIncrementStore *AutoIncrementStateStore
```

When executors are created in `server/innodb/engine/executor.go`, set:

```go
storageIntegratedExecutor.SetAutoIncrementStore(NewAutoIncrementStateStore(e.getDataDir()))
```

Add method:

```go
func (dml *StorageIntegratedDMLExecutor) SetAutoIncrementStore(store *AutoIncrementStateStore) {
	dml.autoIncrementStore = store
}
```

Update `generatePrimaryKey`:

```go
if col.IsAutoIncrement {
	if dml.autoIncrementStore != nil && dml.schemaName != "" && dml.tableName != "" {
		value, err := dml.autoIncrementStore.Next(dml.schemaName, dml.tableName, col.Name)
		if err != nil {
			return nil, err
		}
		row.ColumnValues[col.Name] = value
		row.ColumnTypes[col.Name] = col.Type
		return value, nil
	}
}
```

When an explicit auto increment value is provided:

```go
if col.IsAutoIncrement && dml.autoIncrementStore != nil {
	if n, ok := toInt64(value); ok {
		if err := dml.autoIncrementStore.ObserveExplicit(dml.schemaName, dml.tableName, col.Name, n); err != nil {
			return nil, err
		}
	}
}
```

- [ ] **Step 5: Clear auto increment state on TRUNCATE and DROP**

Modify `truncateTableImpl` and `dropTableImpl` in `server/innodb/engine/executor.go`:

```go
_ = NewAutoIncrementStateStore(e.getDataDir()).RemoveTable(databaseName, tableName)
```

Use `dbName` in `dropTableImpl`.

- [ ] **Step 6: Run auto increment tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestAutoIncrementState|TestStorageIntegratedDMLGeneratePrimaryKey' -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/innodb/engine/auto_increment_state.go server/innodb/engine/auto_increment_state_test.go server/innodb/engine/storage_integrated_dml_helper.go server/innodb/engine/storage_integrated_dml_executor.go server/innodb/engine/executor.go
git commit -m "feat: persist table auto increment state"
```

## Task 6: Persist Secondary Index and UNIQUE Constraints

**Files:**
- Create: `server/innodb/manager/secondary_index_mapping.go`
- Create: `server/innodb/manager/secondary_index_mapping_test.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/engine/select_executor.go`

- [ ] **Step 1: Write failing secondary key encoding tests**

Create `server/innodb/manager/secondary_index_mapping_test.go`:

```go
package manager

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeSecondaryIndexKeyIncludesPrimaryKeyForNonUnique(t *testing.T) {
	key, err := EncodeSecondaryIndexKey([]interface{}{"electronics"}, []interface{}{int64(7)}, false)
	require.NoError(t, err)
	require.Contains(t, string(key), "electronics")
	require.Contains(t, string(key), "7")
}

func TestEncodeSecondaryIndexKeyOmitsPrimaryKeyForUnique(t *testing.T) {
	key, err := EncodeSecondaryIndexKey([]interface{}{"alice"}, []interface{}{int64(7)}, true)
	require.NoError(t, err)
	require.Contains(t, string(key), "alice")
	require.NotContains(t, string(key), "7")
}
```

- [ ] **Step 2: Implement secondary key encoding**

Create `server/innodb/manager/secondary_index_mapping.go`:

```go
package manager

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func EncodeSecondaryIndexKey(indexValues []interface{}, primaryKeyValues []interface{}, unique bool) ([]byte, error) {
	var buf bytes.Buffer
	for _, value := range indexValues {
		if err := writeIndexKeyPart(&buf, value); err != nil {
			return nil, err
		}
	}
	if !unique {
		for _, value := range primaryKeyValues {
			if err := writeIndexKeyPart(&buf, value); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

func writeIndexKeyPart(buf *bytes.Buffer, value interface{}) error {
	text := fmt.Sprintf("%v", value)
	if len(text) > 65535 {
		return fmt.Errorf("index key part too large")
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(len(text))); err != nil {
		return err
	}
	buf.WriteString(text)
	return nil
}
```

- [ ] **Step 3: Build secondary index metadata from `.frm`**

Modify the `.frm` load/write path so `frmTableInfo` includes indexes and `loadTableMetaFromFrm` can return index metadata. Use the existing `indexes` JSON written by `createTableStructureFile`.

Add fields:

```go
type frmTableInfo struct {
	TableName string                   `json:"table_name"`
	Columns   []map[string]interface{} `json:"columns"`
	Indexes   []map[string]interface{} `json:"indexes"`
}
```

- [ ] **Step 4: Enforce UNIQUE using durable secondary indexes**

In `validateUniqueConstraints`, replace memory-based checks with index lookup:

```go
key, err := manager.EncodeSecondaryIndexKey([]interface{}{value}, nil, true)
if err != nil {
	return err
}
record, err := secondaryBTree.Search(ctx, key)
if err == nil && record != nil && !record.DeleteMark {
	return fmt.Errorf("Duplicate entry '%v' for key '%s'", value, colName)
}
```

Use an index manager helper that resolves unique index B+Tree by `schema.table.index`.

- [ ] **Step 5: Maintain secondary indexes on INSERT/UPDATE/DELETE**

For INSERT, after clustered insert succeeds:

```go
if err := dml.updateIndexesForInsert(ctx, txn, row, tableMeta, tableStorageInfo); err != nil {
	dml.rollbackStorageTransaction(ctx, txn)
	return nil, fmt.Errorf("更新索引失败: %v", err)
}
```

For UPDATE:

1. Delete old secondary keys for changed indexed columns.
2. Insert new secondary keys.
3. If unique key already exists for another primary key, return duplicate error.

For DELETE:

1. Delete secondary keys for the old row.
2. Delete mark clustered row.

- [ ] **Step 6: Run unique constraint tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager -run 'TestEncodeSecondaryIndexKey' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestStorageIntegratedDML.*Unique|TestStorageIntegratedDML' -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/innodb/manager/secondary_index_mapping.go server/innodb/manager/secondary_index_mapping_test.go server/innodb/engine/storage_integrated_dml_executor.go server/innodb/engine/executor.go server/innodb/engine/select_executor.go
git commit -m "feat: enforce unique constraints with secondary indexes"
```

## Task 7: Add Restart Persistence Integration Test

**Files:**
- Create: `server/innodb/engine/jdbc_persistence_integration_test.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/manager/table_storage_mapping.go`

- [ ] **Step 1: Write failing restart test**

Create `server/innodb/engine/jdbc_persistence_integration_test.go`:

```go
package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/server"
)

func TestStorageEngineRestartReadsClusteredRows(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &conf.Cfg{DataDir: dataDir, InnodbDataDir: dataDir}

	engine1 := NewXMySQLEngine(cfg)
	require.NoError(t, engine1.Start())
	session := server.NewMySQLServerSession(nil)
	session.SetParamByName("database", "app")

	exec1 := engine1.QueryExecutor
	runEngineSQL(t, exec1, "CREATE DATABASE IF NOT EXISTS app", "app")
	runEngineSQL(t, exec1, "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY AUTO_INCREMENT, username VARCHAR(50) NOT NULL UNIQUE, age INT DEFAULT 18)", "app")
	runEngineSQL(t, exec1, "INSERT INTO users (username, age) VALUES ('alice', 25)", "app")
	require.NoError(t, engine1.Stop())

	engine2 := NewXMySQLEngine(cfg)
	require.NoError(t, engine2.Start())
	defer engine2.Stop()

	result := runEngineSelect(t, engine2.QueryExecutor, "SELECT age FROM users WHERE username = 'alice'", "app")
	require.Equal(t, 1, result.RowCount)
	require.Equal(t, []string{"age"}, result.Columns)
	require.Equal(t, int64(25), result.Records[0].GetValueByIndex(0).Int())
}

func runEngineSQL(t *testing.T, executor *XMySQLExecutor, sql string, db string) {
	t.Helper()
	stmt, err := sqlparser.Parse(sql)
	require.NoError(t, err)
	ctx := &ExecutionContext{Context: context.Background()}
	switch s := stmt.(type) {
	case *sqlparser.DBDDL:
		executor.executeCreateDatabaseStatement(ctx, s)
	case *sqlparser.DDL:
		executor.executeCreateTableStatement(ctx, db, s)
	case *sqlparser.Insert:
		_, err := executor.executeInsertStatement(ctx, s, db)
		require.NoError(t, err)
	default:
		t.Fatalf("unsupported SQL in test: %T", stmt)
	}
}

func runEngineSelect(t *testing.T, executor *XMySQLExecutor, sql string, db string) *SelectResult {
	t.Helper()
	stmt, err := sqlparser.Parse(sql)
	require.NoError(t, err)
	selectStmt, ok := stmt.(*sqlparser.Select)
	require.True(t, ok)
	result, err := executor.executeSelectStatement(&ExecutionContext{Context: context.Background()}, selectStmt, db)
	require.NoError(t, err)
	return result
}
```

If constructor names differ, adjust only to existing engine constructors; keep the assertion identical.

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestStorageEngineRestartReadsClusteredRows' -count=1
```

Expected before implementation is complete: FAIL because table storage mapping or B+Tree metadata is not reloaded enough to find rows.

- [ ] **Step 3: Persist table storage mapping**

Modify `server/innodb/manager/table_storage_mapping.go`:

1. Add JSON persistence for `tableStorageMap` under data dir.
2. Write mapping after `RegisterTable`, `UnregisterTable`, and root-page updates in `CreateBTreeManagerForTable`.
3. Load mapping during `NewTableStorageManager`.

Use this shape:

```go
type persistedTableStorageInfo struct {
	SchemaName    string `json:"schema_name"`
	TableName     string `json:"table_name"`
	SpaceID       uint32 `json:"space_id"`
	RootPageNo    uint32 `json:"root_page_no"`
	IndexPageNo   uint32 `json:"index_page_no"`
	DataSegmentID uint64 `json:"data_segment_id"`
	Type          int    `json:"type"`
}
```

- [ ] **Step 4: Reload B+Tree metadata using persisted root page**

In `CreateBTreeManagerForTable`, ensure `Init(ctx, info.SpaceID, info.RootPageNo)` does not allocate a new root page when `info.RootPageNo` already points to an existing initialized root. If `Init` returns a different root page, write it back only when the old root page is zero.

- [ ] **Step 5: Run restart test**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestStorageEngineRestartReadsClusteredRows' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/innodb/engine/jdbc_persistence_integration_test.go server/innodb/manager/table_storage_mapping.go
git commit -m "feat: reload table storage mapping after restart"
```

## Task 8: Restore Full JDBC CRUD Without Runtime Memory View

**Files:**
- Modify: `jdbc_client/src/test/java/com/xmysql/server/test/DMLOperationsTest.java` only if adding extra restart-specific tests is needed
- Modify: engine files from earlier tasks only for fixes found by this task

- [ ] **Step 1: Verify no runtime memory view exists**

Run:

```bash
rg -n "memoryDMLStore|memorySelectRows|memoryInsertRows|memoryUpdateRows|memoryDeleteRows|dml_memory_store" server
```

Expected: no output.

- [ ] **Step 2: Start local server**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go run . -configPath=conf/jdbc_local.ini
```

Expected: server listens on `127.0.0.1:3309`.

- [ ] **Step 3: Run JDBC connectivity**

In another shell:

```bash
cd jdbc_client
mvn test -Pjdbc-connectivity
```

Expected:

```text
Tests run: 22, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

- [ ] **Step 4: Run JDBC DML**

```bash
cd jdbc_client
mvn test -Dtest=DMLOperationsTest
```

Expected:

```text
Tests run: 13, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

- [ ] **Step 5: Run restart smoke**

Manual JDBC smoke:

```bash
cd jdbc_client
mvn test -Dtest=DMLOperationsTest#testInsertSingleRow
```

Then stop server, start server again, and run an engine-level restart test:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestStorageEngineRestartReadsClusteredRows' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit final fixes**

```bash
git add server jdbc_client
git commit -m "test: verify jdbc crud on persistent btree records"
```

## Final Verification Checklist

Run all commands from repository root unless noted:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestRecordCodec|TestClusteredIndexScanner|TestAutoIncrementState|TestStorageEngineRestartReadsClusteredRows|TestStorageIntegratedDML' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager -run 'TestEncodeSecondaryIndexKey|TestEnhancedBTreeManager|TestEnhancedBTreeAdapter' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher ./server/net ./server/protocol -count=1
```

Start server:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go run . -configPath=conf/jdbc_local.ini
```

Run JDBC:

```bash
cd jdbc_client
mvn test -Pjdbc-connectivity
mvn test -Dtest=DMLOperationsTest
```

Expected final state:

- No `memoryDMLStore` references.
- No `XDMLROWS1` references.
- JDBC connectivity: 22 tests pass.
- JDBC DML: 13 tests pass.
- Restart persistence test passes.
- `docs/planning/P0_CURRENT_STATUS_SUMMARY.md` remains untouched unless separately requested.

## Self-Review

Spec coverage:

- Complete unified InnoDB row format: Task 1 and Task 2.
- Restart readback: Task 7 and Task 8.
- B+Tree full/range scan path: Task 3 and Task 4.
- Durable secondary/unique indexes: Task 6.
- AUTO_INCREMENT persistence and correct last insert behavior: Task 5.

Placeholder scan:

- The plan does not use placeholder language or open-ended implementation markers.
- Each task includes concrete files, commands, and expected outcomes.

Type consistency:

- Row payload uses `InsertRowData` and `metadata.TableMeta`.
- Scanner uses `manager.IndexIterator` and `manager.IndexRecord`.
- DML continues to return `DMLResult.AffectedRows` for JDBC OK packets.
