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
	return encodeSecondaryIndexKeyPrefixColumns(tableID, index, row, len(index.Columns))
}

func encodeSecondaryIndexKeyPrefixColumns(tableID uint64, index metadata.IndexMeta, row map[string]interface{}, columnCount int) ([]byte, error) {
	if len(index.Columns) == 0 {
		return nil, fmt.Errorf("secondary index %s has no columns", index.Name)
	}
	if columnCount < 1 || columnCount > len(index.Columns) {
		return nil, fmt.Errorf("secondary index %s prefix column count %d is invalid", index.Name, columnCount)
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
	for _, columnName := range index.Columns[:columnCount] {
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
	return SecondaryIndexPrefixEqualityRange(tableID, index, row, len(index.Columns))
}

// SecondaryIndexPrefixEqualityRange returns the range for equality predicates
// covering the first prefixColumns of a secondary index. A partial prefix is
// deliberately widened to include all remaining indexed columns; the executor
// applies residual predicates after clustered lookup.
func SecondaryIndexPrefixEqualityRange(tableID uint64, index metadata.IndexMeta, row map[string]interface{}, prefixColumns int) ([]byte, []byte, error) {
	start, err := encodeSecondaryIndexKeyPrefixColumns(tableID, index, row, prefixColumns)
	if err != nil {
		return nil, nil, err
	}
	if index.Unique && prefixColumns == len(index.Columns) {
		return start, append([]byte(nil), start...), nil
	}
	end := append(append([]byte(nil), start...), 0xFF)
	return start, end, nil
}

// SecondaryIndexFullRange returns the durable key range containing every entry
// for one secondary index. Predicate filtering happens after clustered lookup
// because secondary values are not encoded in sortable type order.
func SecondaryIndexFullRange(tableID uint64, index metadata.IndexMeta) ([]byte, []byte, error) {
	prefix, err := secondaryIndexKeyHeader(tableID, index)
	if err != nil {
		return nil, nil, err
	}
	return prefix, append(prefix, 0xFF), nil
}

func secondaryIndexKeyHeader(tableID uint64, index metadata.IndexMeta) ([]byte, error) {
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
	return key, nil
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

// DecodeSecondaryIndexKey returns the durable header and encoded indexed
// column values. Values are returned in their persisted string form because
// the secondary-key format intentionally stores a stable textual encoding.
// The helper is used by skip-scan planning to enumerate distinct leading
// prefixes without reading clustered rows first.
func DecodeSecondaryIndexKey(key []byte) (tableID uint64, indexName string, unique bool, columns []string, values []string, err error) {
	if len(key) < len(secondaryIndexKeyMagic)+8 || string(key[:len(secondaryIndexKeyMagic)]) != string(secondaryIndexKeyMagic) {
		return 0, "", false, nil, nil, fmt.Errorf("invalid secondary index key")
	}
	offset := len(secondaryIndexKeyMagic)
	tableID = binary.BigEndian.Uint64(key[offset : offset+8])
	offset += 8
	read := func() ([]byte, error) {
		if offset+4 > len(key) {
			return nil, fmt.Errorf("invalid secondary index key length prefix")
		}
		length := int(binary.BigEndian.Uint32(key[offset : offset+4]))
		offset += 4
		if length < 0 || offset+length > len(key) {
			return nil, fmt.Errorf("invalid secondary index key payload length")
		}
		result := append([]byte(nil), key[offset:offset+length]...)
		offset += length
		return result, nil
	}
	name, readErr := read()
	if readErr != nil {
		return 0, "", false, nil, nil, readErr
	}
	indexName = string(name)
	if offset+1+2 > len(key) {
		return 0, "", false, nil, nil, fmt.Errorf("invalid secondary index key header")
	}
	unique = key[offset] == 1
	offset++
	columnCount := int(binary.BigEndian.Uint16(key[offset : offset+2]))
	offset += 2
	if columnCount <= 0 {
		return 0, "", false, nil, nil, fmt.Errorf("secondary index key has no columns")
	}
	columns = make([]string, 0, columnCount)
	values = make([]string, 0, columnCount)
	for i := 0; i < columnCount; i++ {
		column, columnErr := read()
		if columnErr != nil {
			return 0, "", false, nil, nil, columnErr
		}
		value, valueErr := read()
		if valueErr != nil {
			return 0, "", false, nil, nil, valueErr
		}
		columns = append(columns, string(column))
		values = append(values, string(value))
	}
	return tableID, indexName, unique, columns, values, nil
}

func IsSecondaryIndexValue(value []byte) bool {
	return len(value) >= len(secondaryIndexValueMagic) && string(value[:len(secondaryIndexValueMagic)]) == string(secondaryIndexValueMagic)
}

func secondaryIndexKeyContainsNull(key interface{}) bool {
	rawKey, ok := key.([]byte)
	if !ok {
		return false
	}
	_, _, _, _, values, err := DecodeSecondaryIndexKey(rawKey)
	if err != nil {
		return false
	}
	for _, value := range values {
		if value == "<nil>" {
			return true
		}
	}
	return false
}

func appendLengthPrefixedBytes(dst []byte, value []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}
