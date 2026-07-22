package manager

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

var secondaryIndexKeyMagic = []byte("XSI1")
var secondaryIndexValueMagic = []byte("XSIV1")

// SecondaryIndexTableID provides a stable identifier that survives tablespace
// ID reassignment during storage startup.
func SecondaryIndexTableID(schemaName, tableName string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(strings.ToLower(schemaName)))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(strings.ToLower(tableName)))
	return hash.Sum64()
}

func SecondaryIndexID(tableID uint64, indexName string) uint64 {
	hash := fnv.New64a()
	var encodedTableID [8]byte
	binary.BigEndian.PutUint64(encodedTableID[:], tableID)
	_, _ = hash.Write(encodedTableID[:])
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(strings.ToLower(indexName)))
	return hash.Sum64()
}

// EncodeSecondaryIndexKey builds a deterministic durable key for a secondary index.
// UNIQUE indexes are keyed only by indexed columns; non-unique indexes append the
// clustered primary key so duplicate indexed values can coexist.
func EncodeSecondaryIndexKey(tableID uint64, index metadata.IndexMeta, row map[string]interface{}, primaryKey []byte) ([]byte, error) {
	key, err := EncodeSecondaryIndexKeyPrefix(tableID, index, row)
	if err != nil {
		return nil, err
	}
	if !index.Unique {
		key = appendLengthPrefixedBytes(key, primaryKey)
	}
	return key, nil
}

func EncodeSecondaryIndexKeyPrefix(tableID uint64, index metadata.IndexMeta, row map[string]interface{}) ([]byte, error) {
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
			return nil, fmt.Errorf("secondary index column %s not found", columnName)
		}
		key = appendLengthPrefixedBytes(key, []byte(columnName))
		key = appendLengthPrefixedBytes(key, []byte(fmt.Sprintf("%v", value)))
	}

	return key, nil
}

func SecondaryIndexEqualityRange(tableID uint64, index metadata.IndexMeta, row map[string]interface{}) ([]byte, []byte, error) {
	start, err := EncodeSecondaryIndexKeyPrefix(tableID, index, row)
	if err != nil {
		return nil, nil, err
	}
	if index.Unique {
		return start, append([]byte(nil), start...), nil
	}
	end := append(append([]byte(nil), start...), 0xFF)
	return start, end, nil
}

func EncodeSecondaryIndexValue(primaryKey []byte) []byte {
	value := append([]byte(nil), secondaryIndexValueMagic...)
	return appendLengthPrefixedBytes(value, primaryKey)
}

func DecodeSecondaryIndexValue(value []byte) ([]byte, error) {
	if len(value) < len(secondaryIndexValueMagic)+4 || string(value[:len(secondaryIndexValueMagic)]) != string(secondaryIndexValueMagic) {
		return nil, fmt.Errorf("invalid secondary index value")
	}
	lengthOffset := len(secondaryIndexValueMagic)
	length := binary.BigEndian.Uint32(value[lengthOffset : lengthOffset+4])
	start := lengthOffset + 4
	end := start + int(length)
	if end != len(value) {
		return nil, fmt.Errorf("invalid secondary index value length")
	}
	return append([]byte(nil), value[start:end]...), nil
}

func IsSecondaryIndexValue(value []byte) bool {
	return len(value) >= len(secondaryIndexValueMagic) && string(value[:len(secondaryIndexValueMagic)]) == string(secondaryIndexValueMagic)
}

func appendLengthPrefixedBytes(dst []byte, value []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}
