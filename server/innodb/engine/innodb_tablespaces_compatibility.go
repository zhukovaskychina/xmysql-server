package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// executeInformationSchemaInnoDBTablespacesSelect exposes the durable tablespace
// catalog through the subset of INFORMATION_SCHEMA.INNODB_TABLESPACES that is
// useful to clients and Connector/J. Values that require upstream SDI parsing
// remain NULL instead of being fabricated from unrelated metadata.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBTablespacesSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"SPACE", "NAME", "SPACE_TYPE", "FS_BLOCK_SIZE", "FILE_SIZE", "ALLOCATED_SIZE",
		"SERVER_VERSION", "SPACE_VERSION", "ROW_FORMAT", "PAGE_SIZE", "ZIP_PAGE_SIZE", "AUTOEXTEND_SIZE", "STATE", "FLAGS", "FLAG", "SDI_VERSION",
		"SDI_OFFSET", "SDI_LENGTH", "SDI_SPACE", "SPACE_FLAGS", "SPACE_FLAGS2",
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
			"FLAGS":           spaceFlags,
			"FLAG":            spaceFlags,
			"SDI_VERSION":     nil,
			"SDI_OFFSET":      nil,
			"SDI_LENGTH":      nil,
			"SDI_SPACE":       nil,
			"SPACE_FLAGS":     spaceFlags,
			"SPACE_FLAGS2":    int64(0),
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

// executeInformationSchemaInnoDBColumnsSelect exposes column dictionary data
// that is directly available from the durable table definition. Internal
// InnoDB type/precision encodings are left NULL/0 until a native dictionary
// reader can prove them.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBColumnsSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"TABLE_ID", "POS", "NAME", "MTYPE", "PRTYPE", "LEN", "HAS_DEFAULT",
		"DEFAULT_VALUE", "DEFAULT_VALUE_UTF8", "VERSION", "HAS_NO_DEFAULT",
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
				"TABLE_ID":           int64(storage.SpaceID),
				"POS":                int64(pos),
				"NAME":               column.name,
				"MTYPE":              innodbColumnMainType(column),
				"PRTYPE":             nil,
				"LEN":                int64(innodbColumnLength(column)),
				"HAS_DEFAULT":        boolToInt64(column.instantAdded),
				"DEFAULT_VALUE":      nil,
				"DEFAULT_VALUE_UTF8": nil,
				"VERSION":            int64(1),
				"HAS_NO_DEFAULT":     nil,
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
// InnoDB main-type code.  The mapping deliberately covers only types whose
// code is unambiguous from the persisted definition; PRTYPE still requires
// InnoDB's internal charset/flag encoding and remains NULL.
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
