package engine

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

type clusteredIndexFullScanner interface {
	FullScan(ctx context.Context) ([]basic.Row, error)
}

type clusteredIndexStreamingScanner interface {
	FullScanEach(ctx context.Context, visit func(basic.Row) error) error
}

type ClusteredIndexScanner struct {
	btree        basic.BPlusTreeManager
	tableMeta    *metadata.TableMeta
	rowsExamined int64
}

type clusteredScannedRow struct {
	data       *InsertRowData
	storageKey interface{}
	lookupKey  interface{}
	pageNumber uint32
}

func NewClusteredIndexScanner(btree basic.BPlusTreeManager, tableMeta *metadata.TableMeta) *ClusteredIndexScanner {
	return &ClusteredIndexScanner{btree: btree, tableMeta: tableMeta}
}

// RowsExamined returns the number of valid clustered records inspected during
// the most recent scan, including records rejected by the scan predicate.
func (s *ClusteredIndexScanner) RowsExamined() int64 {
	if s == nil {
		return 0
	}
	return s.rowsExamined
}

func (s *ClusteredIndexScanner) Scan(ctx context.Context, whereConditions []string) ([]*InsertRowData, error) {
	scannedRows, err := s.ScanWithStorageKeys(ctx, whereConditions)
	if err != nil {
		return nil, err
	}

	rows := make([]*InsertRowData, 0, len(scannedRows))
	for _, scannedRow := range scannedRows {
		rows = append(rows, scannedRow.data)
	}
	return rows, nil
}

// ScanProjected scans clustered records and returns only the requested logical
// columns. Callers that provide WHERE conditions must include every predicate
// dependency in requiredColumns; the predicate is evaluated on that projected
// row before it is returned.
func (s *ClusteredIndexScanner) ScanProjected(ctx context.Context, whereConditions, requiredColumns []string) ([]*InsertRowData, error) {
	scannedRows, err := s.scanWithProjection(ctx, whereConditions, requiredColumns)
	if err != nil {
		return nil, err
	}

	rows := make([]*InsertRowData, 0, len(scannedRows))
	for _, scannedRow := range scannedRows {
		rows = append(rows, scannedRow.data)
	}
	return rows, nil
}

func (s *ClusteredIndexScanner) ScanWithStorageKeys(ctx context.Context, whereConditions []string) ([]clusteredScannedRow, error) {
	return s.scanWithProjection(ctx, whereConditions, nil)
}

// FindByKey reads a single clustered row through the B+Tree range interface.
// It is used by database-managed derived-data directories to re-read a
// candidate row by its owning table key instead of rescanning that table.
// The returned row is decoded from the current storage record; cached derived
// metadata must still validate its source revision before it is trusted.
func (s *ClusteredIndexScanner) FindByKey(ctx context.Context, key interface{}) (*InsertRowData, uint32, bool, error) {
	requestedKey, requestedOK := storageKeyToBytes(key)
	if !requestedOK {
		return nil, 0, false, fmt.Errorf("clustered index point lookup key cannot be encoded")
	}
	return s.FindByLookupKeyAndStorageKey(ctx, key, requestedKey)
}

// FindByLookupKeyAndStorageKey separates the B+Tree lookup representation
// from the canonical storage-key bytes used to verify the returned row. This
// is needed by source-bound derived records that persist only the canonical
// primary-key bytes while a numeric clustered index expects a native number.
func (s *ClusteredIndexScanner) FindByLookupKeyAndStorageKey(ctx context.Context, lookupKey interface{}, storageKey []byte) (*InsertRowData, uint32, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.btree == nil || s.tableMeta == nil || lookupKey == nil || len(storageKey) == 0 {
		return nil, 0, false, fmt.Errorf("clustered index point lookup requires a B+Tree manager, table metadata, and key")
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, false, err
	}
	rows, err := s.btree.RangeSearch(ctx, lookupKey, lookupKey)
	if err != nil {
		return nil, 0, false, err
	}
	for _, storedRow := range rows {
		if storedRow == nil {
			continue
		}
		storedPrimaryKey := storedRow.GetPrimaryKey()
		if storedPrimaryKey == nil {
			continue
		}
		storedKey, storedOK := storageKeyToBytes(storedPrimaryKey.Raw())
		if storedOK && !bytes.Equal(storageKey, storedKey) {
			continue
		}
		payload := storedRow.ToByte()
		if !strings.HasPrefix(string(payload), clusteredRecordMagic) {
			continue
		}
		row, err := DecodeClusteredRecord(payload, s.tableMeta)
		if err != nil {
			return nil, 0, false, fmt.Errorf("decode clustered record at key lookup: %w", err)
		}
		return row, storedRow.GetPageNumber(), true, nil
	}
	return nil, 0, false, nil
}

func (s *ClusteredIndexScanner) scanWithProjection(ctx context.Context, whereConditions, requiredColumns []string) ([]clusteredScannedRow, error) {
	rows := make([]clusteredScannedRow, 0)
	err := s.VisitProjectedWithKeys(ctx, whereConditions, requiredColumns, func(data *InsertRowData, lookupKey, storageKey interface{}, pageNumber uint32) error {
		rows = append(rows, clusteredScannedRow{data: data, lookupKey: lookupKey, storageKey: storageKey, pageNumber: pageNumber})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// VisitProjected streams projected clustered records to visit. Engines with a
// streaming B+Tree scanner keep memory bounded by the current leaf page; the
// fallback preserves compatibility with older managers that expose only a
// slice-returning scan method.
func (s *ClusteredIndexScanner) VisitProjected(ctx context.Context, whereConditions, requiredColumns []string, visit func(*InsertRowData, interface{}, uint32) error) error {
	if visit == nil {
		return fmt.Errorf("clustered index scanner requires a visitor")
	}
	return s.VisitProjectedWithKeys(ctx, whereConditions, requiredColumns, func(data *InsertRowData, _ interface{}, storageKey interface{}, pageNumber uint32) error {
		return visit(data, storageKey, pageNumber)
	})
}

// VisitProjectedWithKeys exposes both the key exactly as used by the B+Tree
// and the byte-oriented storage key used by DML journals. The native key lets
// database-managed secondary directories perform correct point lookups for
// numeric, string, binary, and composite clustered keys.
func (s *ClusteredIndexScanner) VisitProjectedWithKeys(ctx context.Context, whereConditions, requiredColumns []string, visit func(*InsertRowData, interface{}, interface{}, uint32) error) error {
	if s != nil {
		s.rowsExamined = 0
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.btree == nil || visit == nil {
		return fmt.Errorf("clustered index scanner requires a B+Tree manager and visitor")
	}
	if s.tableMeta == nil {
		return fmt.Errorf("clustered index scanner requires table metadata")
	}
	_, err := clusteredRecordProjection(s.tableMeta, requiredColumns)
	if err != nil {
		return err
	}
	visitStoredRow := func(storedRow basic.Row) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if storedRow == nil {
			return nil
		}
		payload := storedRow.ToByte()
		if !strings.HasPrefix(string(payload), clusteredRecordMagic) {
			return nil
		}
		s.rowsExamined++
		var rowData *InsertRowData
		if len(requiredColumns) > 0 {
			rowData, err = DecodeClusteredRecordProjected(payload, s.tableMeta, requiredColumns)
		} else {
			rowData, err = DecodeClusteredRecord(payload, s.tableMeta)
		}
		if err != nil {
			return fmt.Errorf("decode clustered record: %v", err)
		}
		addColumnCollationContext(rowData.ColumnValues, s.tableMeta)
		matches, err := rowMatchesWhereConditions(rowData.ColumnValues, whereConditions)
		if err != nil {
			return err
		}
		if matches {
			var lookupKey interface{}
			if primaryKey := storedRow.GetPrimaryKey(); primaryKey != nil {
				lookupKey = primaryKey.Raw()
				if bytesKey, ok := lookupKey.([]byte); ok {
					lookupKey = append([]byte(nil), bytesKey...)
				}
			}
			return visit(rowData, lookupKey, scannedRowStorageKey(storedRow), storedRow.GetPageNumber())
		}
		return nil
	}
	if streaming, ok := s.btree.(clusteredIndexStreamingScanner); ok {
		return streaming.FullScanEach(ctx, visitStoredRow)
	}
	storedRows, err := s.scanRawRows(ctx)
	if err != nil {
		return err
	}
	for _, storedRow := range storedRows {
		if err := visitStoredRow(storedRow); err != nil {
			return err
		}
	}
	return nil
}

func scannedRowStorageKey(row basic.Row) interface{} {
	if row == nil || row.GetPrimaryKey() == nil {
		return nil
	}
	if keyBytes, ok := storageKeyToBytes(row.GetPrimaryKey().Raw()); ok {
		return string(keyBytes)
	}
	return nil
}

func (s *ClusteredIndexScanner) scanRawRows(ctx context.Context) ([]basic.Row, error) {
	if fullScanner, ok := s.btree.(clusteredIndexFullScanner); ok {
		return fullScanner.FullScan(ctx)
	}

	startKey, endKey := clusteredPrimaryKeyScanBounds(s.tableMeta)
	return s.btree.RangeSearch(ctx, startKey, endKey)
}

func clusteredPrimaryKeyScanBounds(tableMeta *metadata.TableMeta) (interface{}, interface{}) {
	pkColumn := clusteredPrimaryKeyColumn(tableMeta)
	if pkColumn == nil {
		return int64(math.MinInt64), int64(math.MaxInt64)
	}

	switch pkColumn.Type {
	case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt,
		metadata.TypeBigInt, metadata.TypeYear:
		return int64(math.MinInt64), int64(math.MaxInt64)
	case metadata.TypeFloat, metadata.TypeDouble, metadata.TypeDecimal:
		return math.Inf(-1), math.Inf(1)
	case metadata.TypeBinary, metadata.TypeVarBinary, metadata.TypeTinyBlob, metadata.TypeBlob,
		metadata.TypeMediumBlob, metadata.TypeLongBlob, metadata.TypeContent, metadata.TypeVector:
		return []byte{}, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	default:
		return "", string([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	}
}

func clusteredPrimaryKeyColumn(tableMeta *metadata.TableMeta) *metadata.ColumnMeta {
	if tableMeta == nil {
		return nil
	}
	if len(tableMeta.PrimaryKey) > 0 {
		for _, col := range tableMeta.Columns {
			if col != nil && strings.EqualFold(strings.TrimSpace(col.Name), strings.TrimSpace(tableMeta.PrimaryKey[0])) {
				return col
			}
		}
	}
	for _, col := range tableMeta.Columns {
		if col != nil && col.IsPrimary {
			return col
		}
	}
	return nil
}
