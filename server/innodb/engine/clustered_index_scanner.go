package engine

import (
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

type ClusteredIndexScanner struct {
	btree     basic.BPlusTreeManager
	tableMeta *metadata.TableMeta
}

type clusteredScannedRow struct {
	data       *InsertRowData
	storageKey interface{}
	pageNumber uint32
}

func NewClusteredIndexScanner(btree basic.BPlusTreeManager, tableMeta *metadata.TableMeta) *ClusteredIndexScanner {
	return &ClusteredIndexScanner{btree: btree, tableMeta: tableMeta}
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

func (s *ClusteredIndexScanner) ScanWithStorageKeys(ctx context.Context, whereConditions []string) ([]clusteredScannedRow, error) {
	if s == nil || s.btree == nil {
		return nil, fmt.Errorf("clustered index scanner requires a B+Tree manager")
	}
	if s.tableMeta == nil {
		return nil, fmt.Errorf("clustered index scanner requires table metadata")
	}

	storedRows, err := s.scanRawRows(ctx)
	if err != nil {
		return nil, err
	}

	rows := make([]clusteredScannedRow, 0, len(storedRows))
	for _, storedRow := range storedRows {
		if storedRow == nil {
			continue
		}
		payload := storedRow.ToByte()
		if !strings.HasPrefix(string(payload), clusteredRecordMagic) {
			continue
		}
		rowData, err := DecodeClusteredRecord(payload, s.tableMeta)
		if err != nil {
			return nil, fmt.Errorf("decode clustered record: %v", err)
		}
		matches, err := rowMatchesWhereConditions(rowData.ColumnValues, whereConditions)
		if err != nil {
			return nil, err
		}
		if matches {
			rows = append(rows, clusteredScannedRow{
				data:       rowData,
				storageKey: scannedRowStorageKey(storedRow),
				pageNumber: storedRow.GetPageNumber(),
			})
		}
	}

	return rows, nil
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
		metadata.TypeMediumBlob, metadata.TypeLongBlob:
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
			if col != nil && col.Name == tableMeta.PrimaryKey[0] {
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
