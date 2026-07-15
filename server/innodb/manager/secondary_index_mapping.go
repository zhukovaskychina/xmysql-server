package manager

import (
	"encoding/binary"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

var secondaryIndexKeyMagic = []byte("XSI1")

// EncodeSecondaryIndexKey builds a deterministic durable key for a secondary index.
// UNIQUE indexes are keyed only by indexed columns; non-unique indexes append the
// clustered primary key so duplicate indexed values can coexist.
func EncodeSecondaryIndexKey(tableID uint64, index metadata.IndexMeta, row map[string]interface{}, primaryKey []byte) ([]byte, error) {
	if len(index.Columns) == 0 {
		return nil, fmt.Errorf("secondary index %s has no columns", index.Name)
	}

	key := make([]byte, 0)
	key = append(key, secondaryIndexKeyMagic...)
	key = binary.BigEndian.AppendUint64(key, tableID)
	key = appendLengthPrefixedBytes(key, []byte(index.Name))
	if index.Unique {
		key = append(key, 1)
	} else {
		key = append(key, 0)
	}
	key = binary.BigEndian.AppendUint16(key, uint16(len(index.Columns)))

	for _, columnName := range index.Columns {
		value, exists := row[columnName]
		if !exists {
			return nil, fmt.Errorf("secondary index column %s missing", columnName)
		}
		key = appendLengthPrefixedBytes(key, []byte(columnName))
		key = appendLengthPrefixedBytes(key, []byte(fmt.Sprintf("%v", value)))
	}

	if !index.Unique {
		key = appendLengthPrefixedBytes(key, primaryKey)
	}

	return key, nil
}

func appendLengthPrefixedBytes(dst []byte, value []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}
