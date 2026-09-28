package engine

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// executeInformationSchemaInnoDBTablespacesSelect exposes the durable tablespace
// catalog through the subset of INFORMATION_SCHEMA.INNODB_TABLESPACES that is
// useful to clients and Connector/J. Values that require upstream SDI parsing
// remain NULL instead of being fabricated from unrelated metadata.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBTablespacesSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"SPACE", "NAME", "FLAG", "ROW_FORMAT", "PAGE_SIZE", "ZIP_PAGE_SIZE", "SPACE_TYPE", "FS_BLOCK_SIZE", "FILE_SIZE", "ALLOCATED_SIZE",
		"AUTOEXTEND_SIZE", "SERVER_VERSION", "SPACE_VERSION", "ENCRYPTION", "STATE",
	})
	if e == nil || e.storageManager == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_tablespaces", columns, nil)
	}
	spaces, err := e.storageManager.ListSpaces()
	if err != nil {
		return newInformationSchemaSelectResult("information_schema.innodb_tablespaces", columns, nil)
	}
	sort.Slice(spaces, func(i, j int) bool { return spaces[i].SpaceID < spaces[j].SpaceID })
	rows := make([][]interface{}, 0, len(spaces))
	namePattern := informationSchemaTablespaceNamePattern(query)
	for _, space := range spaces {
		if !metadataPatternMatches(space.Name, namePattern) {
			continue
		}
		pageSize := space.PageSize
		if pageSize == 0 {
			pageSize = 16 * 1024
		}
		fileSize := int64(space.TotalPages) * int64(pageSize)
		spaceFlags := e.informationSchemaInnoDBSpaceFlags(space.SpaceID)
		rowFormat := "Compact or Redundant"
		if space.IsCompressed {
			rowFormat = "Compressed"
		}
		values := map[string]interface{}{
			"SPACE":           int64(space.SpaceID),
			"NAME":            space.Name,
			"SPACE_TYPE":      innoDBSpaceType(space),
			"FS_BLOCK_SIZE":   4096,
			"FILE_SIZE":       fileSize,
			"ALLOCATED_SIZE":  fileSize,
			"SERVER_VERSION":  "8.0.0",
			"SPACE_VERSION":   int64(1),
			"ROW_FORMAT":      rowFormat,
			"PAGE_SIZE":       int64(pageSize),
			"ZIP_PAGE_SIZE":   int64(0),
			"AUTOEXTEND_SIZE": nil,
			"STATE":           strings.ToUpper(space.State),
			"FLAG":            spaceFlags,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema.innodb_tablespaces", columns, rows)
}

// informationSchemaInnoDBSpaceFlags reads the persisted FSP header flags from
// page 0. The storage wrapper places the FSP header at offset 87 and stores
// the four-byte flags in little-endian order. A missing or malformed page
// conservatively returns zero instead of deriving a value from unrelated
// table metadata.
func (e *XMySQLExecutor) informationSchemaInnoDBSpaceFlags(spaceID uint32) int64 {
	if e == nil || e.storageManager == nil {
		return 0
	}
	space, err := e.storageManager.GetSpaceManager().GetSpace(spaceID)
	if err != nil || space == nil {
		return 0
	}
	data, err := space.LoadPageByPageNumber(0)
	if err != nil || data == nil {
		return 0
	}
	if len(data) < 95 {
		return 0
	}
	return int64(binary.LittleEndian.Uint32(data[91:95]))
}

// executeInformationSchemaInnoDBDatafilesSelect exposes the durable file path
// for each discovered InnoDB tablespace. This is intentionally sourced from
// the storage manager rather than reconstructed from a schema/table name.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBDatafilesSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"SPACE", "PATH"})
	if e == nil || e.storageManager == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_datafiles", columns, nil)
	}
	spaces, err := e.storageManager.ListSpaces()
	if err != nil {
		return newInformationSchemaSelectResult("information_schema.innodb_datafiles", columns, nil)
	}
	sort.Slice(spaces, func(i, j int) bool { return spaces[i].SpaceID < spaces[j].SpaceID })
	rows := make([][]interface{}, 0, len(spaces))
	for _, space := range spaces {
		path := space.Path
		if path == "" {
			path = filepath.Join(e.storageManager.DataDir(), filepath.FromSlash(space.Name+".ibd"))
		}
		values := map[string]interface{}{
			"SPACE": int64(space.SpaceID),
			"PATH":  path,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema.innodb_datafiles", columns, rows)
}

// executeInformationSchemaInnoDBTableStatsSelect exposes table statistics
// backed by the current .frm/table-storage mapping and physical tablespace.
// Counters that need an upstream InnoDB background sampler remain zero or NULL
// rather than being inferred from unrelated application metadata.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBTableStatsSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"TABLE_ID", "NAME", "STATS_INITIALIZED", "NUM_ROWS", "CLUST_INDEX_SIZE",
		"OTHER_INDEX_SIZE", "MODIFIED_COUNTER", "AUTOINC", "REF_COUNT",
	})
	if e == nil || e.tableStorageManager == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_tablestats", columns, nil)
	}
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		if !metadataPatternMatches(table.schemaName, filters["table_schema"]) ||
			!metadataPatternMatches(table.tableName, filters["table_name"]) {
			continue
		}
		storage, err := e.tableStorageManager.GetTableStorageInfo(table.schemaName, table.tableName)
		if err != nil || storage == nil {
			continue
		}
		numRows := e.physicalTableRowCount(table.schemaName, table.tableName)
		clusterSize := e.informationSchemaIndexPages(context.Background(), table.schemaName, table.tableName, "PRIMARY")
		otherIndexSize := int64(0)
		for _, index := range table.indexes {
			if index.primary || strings.EqualFold(index.name, "PRIMARY") {
				continue
			}
			otherIndexSize += e.informationSchemaIndexPages(context.Background(), table.schemaName, table.tableName, index.name)
		}
		if e.storageManager != nil {
			if clusterSize == 0 {
				if info, infoErr := e.storageManager.GetSpaceInfo(storage.SpaceID); infoErr == nil && info != nil {
					clusterSize = int64(info.TotalPages)
				}
			}
		}
		modifiedCounter := int64(0)
		if e.infosSchemaManager != nil {
			if counter, ok := e.infosSchemaManager.(interface {
				GetTableModifyCount(string, string) uint64
			}); ok {
				modifiedCounter = int64(counter.GetTableModifyCount(table.schemaName, table.tableName))
			} else if stats, statsErr := e.infosSchemaManager.GetTableStats(context.Background(), table.schemaName, table.tableName); statsErr == nil && stats != nil {
				modifiedCounter = int64(stats.ModifyCount)
			}
		}
		values := map[string]interface{}{
			"TABLE_ID":          int64(storage.SpaceID),
			"NAME":              table.schemaName + "/" + table.tableName,
			"STATS_INITIALIZED": "Initialized",
			"NUM_ROWS":          numRows,
			"CLUST_INDEX_SIZE":  clusterSize,
			"OTHER_INDEX_SIZE":  otherIndexSize,
			"MODIFIED_COUNTER":  modifiedCounter,
			"AUTOINC":           persistedAutoIncrementValue(e.getDataDir(), table.schemaName, table.tableName, table.columns),
			"REF_COUNT":         int64(1),
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return fmt.Sprint(rows[i][0]) < fmt.Sprint(rows[j][0])
	})
	return newInformationSchemaSelectResult("information_schema.innodb_tablestats", columns, rows)
}

// executeInformationSchemaInnoDBIndexesSelect exposes persisted InnoDB index
// dictionary rows. Secondary root pages are not persisted by this metadata
// layer, so PAGE_NO remains NULL for secondary indexes.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBIndexesSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"INDEX_ID", "NAME", "TABLE_ID", "TYPE", "N_FIELDS", "PAGE_NO", "SPACE", "MERGE_THRESHOLD",
	})
	if e == nil || e.tableStorageManager == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_indexes", columns, nil)
	}
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		if !metadataPatternMatches(table.schemaName, filters["table_schema"]) ||
			!metadataPatternMatches(table.tableName, filters["table_name"]) {
			continue
		}
		storage, err := e.tableStorageManager.GetTableStorageInfo(table.schemaName, table.tableName)
		if err != nil || storage == nil {
			continue
		}
		indexes := innodbMetadataIndexes(table)
		for indexPosition, index := range indexes {
			indexType := int64(0)
			if index.primary || strings.EqualFold(index.name, "PRIMARY") {
				indexType = 3
			} else if index.unique {
				indexType = 2
			} else if strings.EqualFold(index.name, "GEN_CLUST_INDEX") {
				indexType = 1
			}
			var pageNo interface{}
			if index.primary || strings.EqualFold(index.name, "PRIMARY") {
				pageNo = int64(storage.IndexPageNo)
			}
			values := map[string]interface{}{
				"INDEX_ID":        int64(storage.SpaceID)<<32 | int64(indexPosition+1),
				"NAME":            index.name,
				"TABLE_ID":        int64(storage.SpaceID),
				"TYPE":            indexType,
				"N_FIELDS":        int64(len(index.columns)),
				"PAGE_NO":         pageNo,
				"SPACE":           int64(storage.SpaceID),
				"MERGE_THRESHOLD": int64(50),
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return fmt.Sprint(rows[i][0]) < fmt.Sprint(rows[j][0])
	})
	return newInformationSchemaSelectResult("information_schema.innodb_indexes", columns, rows)
}

// innodbMetadataIndexes normalizes the durable index list for the two InnoDB
// dictionary projections that share INDEX_ID. Older metadata may contain the
// column-level primary-key flags without an explicit index object, so rebuild
// that user-defined PRIMARY before falling back to InnoDB's hidden clustered
// index for a table with no primary key.
func innodbMetadataIndexes(table frmMetadataTable) []frmMetadataIndex {
	primaryColumns := make([]string, 0)
	for _, column := range table.columns {
		if column.isPrimary {
			primaryColumns = append(primaryColumns, column.name)
		}
	}

	indexes := make([]frmMetadataIndex, 0, len(table.indexes)+1)
	hasPrimary := false
	for _, persisted := range table.indexes {
		index := persisted
		if index.primary || strings.EqualFold(index.name, "PRIMARY") {
			index.primary = true
			hasPrimary = true
			if len(index.columns) == 0 && len(primaryColumns) > 0 {
				index.columns = append([]string(nil), primaryColumns...)
			}
		}
		indexes = append(indexes, index)
	}
	if !hasPrimary && len(primaryColumns) > 0 {
		indexes = append([]frmMetadataIndex{{name: "PRIMARY", unique: true, primary: true, columns: primaryColumns}}, indexes...)
		hasPrimary = true
	}
	if !hasPrimary {
		// InnoDB creates an internal clustered index for tables without a
		// user-defined PRIMARY KEY. It has no user columns in this metadata
		// projection and is named GEN_CLUST_INDEX.
		indexes = append([]frmMetadataIndex{{name: "GEN_CLUST_INDEX"}}, indexes...)
	}
	return indexes
}

// executeInformationSchemaInnoDBColumnsSelect exposes the MySQL 8.4
// INNODB_COLUMNS shape from the durable table definition. Internal InnoDB
// type/precision encodings are left NULL/0 until a native dictionary reader
// can prove them.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBColumnsSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"TABLE_ID", "NAME", "POS", "MTYPE", "PRTYPE", "LEN", "HAS_DEFAULT", "DEFAULT_VALUE",
	})
	if e == nil || e.tableStorageManager == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_columns", columns, nil)
	}
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		if !metadataPatternMatches(table.schemaName, filters["table_schema"]) ||
			!metadataPatternMatches(table.tableName, filters["table_name"]) {
			continue
		}
		storage, err := e.tableStorageManager.GetTableStorageInfo(table.schemaName, table.tableName)
		if err != nil || storage == nil {
			continue
		}
		for pos, column := range table.columns {
			values := map[string]interface{}{
				"TABLE_ID":      int64(storage.SpaceID),
				"NAME":          column.name,
				"POS":           int64(pos),
				"MTYPE":         innodbColumnMainType(column),
				"PRTYPE":        innodbColumnPreciseType(column),
				"LEN":           int64(innodbColumnLength(column)),
				"HAS_DEFAULT":   boolToInt64(column.instantAdded),
				"DEFAULT_VALUE": innodbInstantDefaultValue(column),
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if fmt.Sprint(rows[i][0]) != fmt.Sprint(rows[j][0]) {
			return fmt.Sprint(rows[i][0]) < fmt.Sprint(rows[j][0])
		}
		return fmt.Sprint(rows[i][1]) < fmt.Sprint(rows[j][1])
	})
	return newInformationSchemaSelectResult("information_schema.innodb_columns", columns, rows)
}

func innodbColumnLength(column frmMetadataColumn) int {
	if column.length > 0 {
		if informationSchemaCharacterSet(column.typeName) != nil {
			// INNODB_COLUMNS.LEN is the maximum byte length, not the SQL
			// character count. The durable metadata path currently uses the
			// server's utf8mb4 default character set for character columns.
			return column.length * 4
		}
		return column.length
	}
	switch strings.ToLower(strings.TrimSpace(column.typeName)) {
	case "tinyint", "bool", "boolean":
		return 1
	case "smallint":
		return 2
	case "int", "integer", "float", "date":
		return 4
	case "mediumint":
		return 3
	case "bigint", "double", "datetime":
		return 8
	case "timestamp", "time":
		return 4
	default:
		return 0
	}
}

// innodbColumnMainType maps the durable SQL type name to the documented
// InnoDB main-type code.
func innodbColumnMainType(column frmMetadataColumn) interface{} {
	base := strings.ToUpper(strings.TrimSpace(strings.SplitN(column.typeName, "(", 2)[0]))
	switch base {
	case "VARCHAR":
		return int64(1)
	case "CHAR":
		return int64(2)
	case "BINARY":
		return int64(3)
	case "VARBINARY":
		return int64(4)
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
		return int64(6)
	case "FLOAT":
		return int64(9)
	case "DOUBLE":
		return int64(10)
	case "DECIMAL", "NUMERIC":
		return int64(11)
	case "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB":
		return int64(5)
	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION":
		return int64(14)
	default:
		return nil
	}
}

// innodbColumnPreciseType mirrors the portable part of InnoDB's precise-type
// encoding. The low byte is the MySQL field type, 256 marks NOT NULL, 512
// marks UNSIGNED, and 1024 marks binary storage. Character types additionally
// carry the MySQL collation id in bits 16..23. The persisted metadata only
// exposes collation names, so unknown collations remain NULL rather than being
// assigned a guessed id.
func innodbColumnPreciseType(column frmMetadataColumn) interface{} {
	base := strings.ToUpper(strings.TrimSpace(strings.SplitN(column.typeName, "(", 2)[0]))
	mysqlType, ok := innodbMySQLFieldTypeCode(base)
	if !ok {
		return nil
	}
	precise := mysqlType
	if !column.nullable {
		precise |= 256
	}
	if column.unsigned {
		precise |= 512
	}
	if innodbColumnUsesBinaryStorage(base) {
		precise |= 1024
	}
	if innodbColumnNeedsCollation(base) {
		collation := fmt.Sprint(informationSchemaColumnCollation(column))
		collationID, ok := innodbMySQLCollationID(collation)
		if !ok {
			return nil
		}
		precise |= collationID << 16
	}
	return int64(precise)
}

// innodbInstantDefaultValue converts the subset of persisted SQL defaults that
// can be represented without a live MySQL Field implementation into the
// InnoDB row-format bytes exposed by INNODB_COLUMNS.DEFAULT_VALUE. Integer
// values use the same little-endian-to-big-endian conversion and signed-bit
// flip as row_mysql_store_col_in_innobase_format; character/binary values are
// already stored as their payload bytes. Expressions and unsupported temporal
// encodings remain NULL instead of being returned as misleading SQL text.
func innodbInstantDefaultValue(column frmMetadataColumn) interface{} {
	if !column.instantAdded || column.defaultValue == nil {
		return nil
	}
	text := strings.TrimSpace(fmt.Sprint(column.defaultValue))
	if strings.EqualFold(text, "null") {
		return nil
	}
	base := strings.ToUpper(strings.TrimSpace(strings.SplitN(column.typeName, "(", 2)[0]))
	if bytes, ok := innodbInstantDefaultBytes(text); ok {
		if base == "BINARY" || base == "VARBINARY" || base == "TINYBLOB" || base == "BLOB" || base == "MEDIUMBLOB" || base == "LONGBLOB" {
			return bytes
		}
	}
	if width, ok := innodbIntegerStorageWidth(base); ok {
		if column.unsigned {
			value, err := strconv.ParseUint(text, 10, 64)
			if err != nil {
				return nil
			}
			return innodbEncodeInteger(value, width, false)
		}
		value, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil
		}
		return innodbEncodeInteger(uint64(value), width, true)
	}
	switch base {
	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT":
		return []byte(text)
	default:
		return nil
	}
}

func innodbInstantDefaultBytes(text string) ([]byte, bool) {
	if !strings.HasPrefix(strings.ToLower(text), "0x") {
		return nil, false
	}
	decoded, err := hex.DecodeString(text[2:])
	if err != nil {
		return nil, false
	}
	return decoded, true
}

func innodbIntegerStorageWidth(base string) (int, bool) {
	switch base {
	case "TINYINT":
		return 1, true
	case "SMALLINT":
		return 2, true
	case "MEDIUMINT":
		return 3, true
	case "INT", "INTEGER":
		return 4, true
	case "BIGINT":
		return 8, true
	default:
		return 0, false
	}
}

func innodbEncodeInteger(value uint64, width int, signed bool) []byte {
	encoded := make([]byte, 8)
	binary.LittleEndian.PutUint64(encoded, value)
	encoded = encoded[:width]
	for left, right := 0, len(encoded)-1; left < right; left, right = left+1, right-1 {
		encoded[left], encoded[right] = encoded[right], encoded[left]
	}
	if signed {
		encoded[0] ^= 128
	}
	return encoded
}

func innodbMySQLFieldTypeCode(base string) (int64, bool) {
	switch base {
	case "TINYINT":
		return 1, true
	case "SMALLINT":
		return 2, true
	case "INT", "INTEGER":
		return 3, true
	case "FLOAT":
		return 4, true
	case "DOUBLE", "REAL":
		return 5, true
	case "TIMESTAMP":
		return 7, true
	case "BIGINT":
		return 8, true
	case "MEDIUMINT":
		return 9, true
	case "DATE":
		return 10, true
	case "TIME":
		return 11, true
	case "DATETIME":
		return 12, true
	case "YEAR":
		return 13, true
	case "VARCHAR", "VARBINARY":
		return 15, true
	case "BIT":
		return 16, true
	case "CHAR", "BINARY":
		return 254, true
	case "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
		return 252, true
	case "DECIMAL", "NUMERIC":
		return 246, true
	case "ENUM":
		return 247, true
	case "SET":
		return 248, true
	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION":
		return 255, true
	default:
		return 0, false
	}
}

func innodbColumnUsesBinaryStorage(base string) bool {
	switch base {
	case "TINYINT", "SMALLINT", "INT", "INTEGER", "MEDIUMINT", "BIGINT", "FLOAT", "DOUBLE", "REAL", "TIMESTAMP", "DATE", "TIME", "DATETIME", "YEAR", "BIT", "VARBINARY", "BINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
		return true
	default:
		return false
	}
}

func innodbColumnNeedsCollation(base string) bool {
	switch base {
	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM", "SET":
		return true
	default:
		return false
	}
}

func innodbMySQLCollationID(collation string) (int64, bool) {
	switch strings.ToLower(strings.TrimSpace(collation)) {
	case "latin1_swedish_ci":
		return 8, true
	case "utf8mb3_general_ci":
		return 33, true
	case "utf8mb4_general_ci":
		return 45, true
	case "utf8mb4_bin":
		return 46, true
	case "utf8mb4_0900_ai_ci":
		return 255, true
	case "binary":
		return 63, true
	default:
		return 0, false
	}
}

func informationSchemaTablespaceNamePattern(query string) string {
	match := regexp.MustCompile(`(?is)\bname\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`).FindStringSubmatch(query)
	if len(match) == 0 {
		return ""
	}
	if match[1] != "" {
		return match[1]
	}
	return match[2]
}

func innoDBSpaceType(space basic.SpaceInfo) string {
	if space.SpaceID == 0 {
		return "System"
	}
	return "Single"
}
