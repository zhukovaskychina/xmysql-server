package engine

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// contentSourcePrimaryKey matches source identities built by ordinary SELECT
// and source-bound INSERT. It does not change physical index key encoding.
func contentSourcePrimaryKey(row map[string]interface{}, table *metadata.TableMeta) ([]byte, bool, error) {
	if table == nil {
		return nil, false, nil
	}
	columns := effectivePrimaryKeyColumns(table)
	if len(columns) != 1 {
		return buildPrimaryKeyIfAvailable(row, table)
	}
	value, exists := resolveExpressionRowValue(row, columns[0])
	if !exists || value == nil {
		return nil, false, fmt.Errorf("missing CONTENT source primary key column %s", columns[0])
	}
	key, ok := storageKeyToBytes(value)
	if !ok {
		return nil, false, fmt.Errorf("cannot encode CONTENT source primary key column %s", columns[0])
	}
	return key, true, nil
}
