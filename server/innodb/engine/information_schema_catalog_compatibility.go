package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

type informationSchemaVisibleView struct {
	Schema     string
	Name       string
	Definition string
}

// informationSchemaVisibleViewDefinitions applies the usage-table rule that
// a user needs some privilege on the view, not specifically SHOW VIEW. The
// VIEWS table itself has the stricter SHOW VIEW contract and therefore cannot
// be used as the source for VIEW_TABLE_USAGE or VIEW_ROUTINE_USAGE.
func (e *XMySQLExecutor) informationSchemaVisibleViewDefinitions(session server.MySQLServerSession) []informationSchemaVisibleView {
	if e == nil {
		return nil
	}
	views := make([]informationSchemaVisibleView, 0)
	entries, _ := os.ReadDir(e.getDataDir())
	for _, schemaEntry := range entries {
		if !schemaEntry.IsDir() {
			continue
		}
		viewEntries, _ := os.ReadDir(filepath.Join(e.getDataDir(), schemaEntry.Name()))
		for _, viewEntry := range viewEntries {
			if viewEntry.IsDir() || !strings.HasSuffix(strings.ToLower(viewEntry.Name()), ".view.json") {
				continue
			}
			name := strings.TrimSuffix(viewEntry.Name(), ".view.json")
			if !e.tableMetadataVisibleToSession(&ExecutionContext{Session: session}, schemaEntry.Name(), name, true) {
				continue
			}
			raw, err := os.ReadFile(filepath.Join(e.getDataDir(), schemaEntry.Name(), viewEntry.Name()))
			if err != nil {
				continue
			}
			var info struct {
				Definition string `json:"definition"`
			}
			if json.Unmarshal(raw, &info) != nil || strings.TrimSpace(info.Definition) == "" {
				continue
			}
			views = append(views, informationSchemaVisibleView{Schema: schemaEntry.Name(), Name: name, Definition: info.Definition})
		}
	}
	sort.Slice(views, func(i, j int) bool {
		left := strings.ToLower(views[i].Schema + "\x00" + views[i].Name)
		right := strings.ToLower(views[j].Schema + "\x00" + views[j].Name)
		return left < right
	})
	return views
}

var informationSchemaSpaceNameFilterPattern = regexp.MustCompile(`(?is)\b(?:tablespace_name|name)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)

func informationSchemaSpaceNamePattern(query string) string {
	match := informationSchemaSpaceNameFilterPattern.FindStringSubmatch(query)
	if len(match) == 0 {
		return ""
	}
	if match[1] != "" {
		return match[1]
	}
	return match[2]
}

func informationSchemaSpaceFileSize(space basic.SpaceInfo) int64 {
	pageSize := space.PageSize
	if pageSize == 0 {
		pageSize = 16 * 1024
	}
	return int64(space.TotalPages) * int64(pageSize)
}

func informationSchemaSpacePath(e *XMySQLExecutor, space basic.SpaceInfo) string {
	if space.Path != "" {
		return space.Path
	}
	if e == nil || e.storageManager == nil {
		return ""
	}
	return filepath.Join(e.storageManager.DataDir(), filepath.FromSlash(space.Name+".ibd"))
}

func informationSchemaSpaceTableParts(name string) (string, string) {
	name = strings.TrimSpace(name)
	if parts := strings.SplitN(name, "/", 2); len(parts) == 2 {
		return parts[0], parts[1]
	}
	if parts := strings.SplitN(name, ".", 2); len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", ""
}

func informationSchemaFilesStatus(state string) string {
	status := strings.ToUpper(strings.TrimSpace(state))
	if status == "" {
		return "NORMAL"
	}
	return status
}

// executeInformationSchemaTablespacesSelect projects the durable InnoDB
// space catalog into INFORMATION_SCHEMA.TABLESPACES. Fields that require a
// general-tablespace or redo-log catalog remain NULL instead of being guessed.
func (e *XMySQLExecutor) executeInformationSchemaTablespacesSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"TABLESPACE_NAME", "ENGINE", "TABLESPACE_TYPE", "LOGFILE_GROUP_NAME",
		"EXTENT_SIZE", "AUTOEXTEND_SIZE", "MAXIMUM_SIZE", "NODEGROUP_ID",
		"TABLESPACE_COMMENT", "FILE_BLOCK_SIZE", "STATUS", "ENCRYPTION",
		"ENGINE_ATTRIBUTE", "SE_PRIVATE_DATA",
	})
	if e == nil || e.storageManager == nil {
		return newInformationSchemaSelectResult("information_schema.tablespaces", columns, nil)
	}
	spaces, err := e.storageManager.ListSpaces()
	if err != nil {
		return newInformationSchemaSelectResult("information_schema.tablespaces", columns, nil)
	}
	sort.Slice(spaces, func(i, j int) bool { return spaces[i].SpaceID < spaces[j].SpaceID })
	pattern := informationSchemaSpaceNamePattern(query)
	rows := make([][]interface{}, 0, len(spaces))
	for _, space := range spaces {
		if !metadataPatternMatches(space.Name, pattern) {
			continue
		}
		pageSize := space.PageSize
		if pageSize == 0 {
			pageSize = 16 * 1024
		}
		extentSize := int64(space.ExtentSize) * int64(pageSize)
		if extentSize == 0 {
			extentSize = int64(pageSize)
		}
		status := strings.ToUpper(strings.TrimSpace(space.State))
		if status == "" {
			status = "ACTIVE"
		}
		values := map[string]interface{}{
			"TABLESPACE_NAME":    space.Name,
			"ENGINE":             "InnoDB",
			"TABLESPACE_TYPE":    innoDBSpaceType(space),
			"LOGFILE_GROUP_NAME": nil,
			"EXTENT_SIZE":        extentSize,
			"AUTOEXTEND_SIZE":    nil,
			"MAXIMUM_SIZE":       nil,
			"NODEGROUP_ID":       nil,
			"TABLESPACE_COMMENT": nil,
			"FILE_BLOCK_SIZE":    int64(pageSize),
			"STATUS":             status,
			"ENCRYPTION":         nil,
			"ENGINE_ATTRIBUTE":   nil,
			"SE_PRIVATE_DATA":    nil,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema.tablespaces", columns, rows)
}

// executeInformationSchemaFilesSelect exposes one durable .ibd or system
// space file per discovered InnoDB tablespace. Logical table fields are filled
// only when the space name carries a schema/table pair.
func (e *XMySQLExecutor) executeInformationSchemaFilesSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"FILE_ID", "FILE_NAME", "FILE_TYPE", "TABLESPACE_NAME", "TABLE_CATALOG",
		"TABLE_SCHEMA", "TABLE_NAME", "LOGFILE_GROUP_NAME", "LOGFILE_GROUP_NUMBER",
		"ENGINE", "FULLTEXT_KEYS", "DELETED_ROWS", "UPDATE_COUNT", "FREE_EXTENTS",
		"TOTAL_EXTENTS", "EXTENT_SIZE", "INITIAL_SIZE", "MAXIMUM_SIZE",
		"AUTOEXTEND_SIZE", "CREATION_TIME", "LAST_UPDATE_TIME", "LAST_ACCESS_TIME",
		"RECOVER_TIME", "TRANSACTION_COUNTER", "VERSION", "ROW_FORMAT", "TABLE_ROWS",
		"AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE",
		"CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "CHECKSUM", "STATUS", "EXTRA",
	})
	if e == nil || e.storageManager == nil {
		return newInformationSchemaSelectResult("information_schema.files", columns, nil)
	}
	spaces, err := e.storageManager.ListSpaces()
	if err != nil {
		return newInformationSchemaSelectResult("information_schema.files", columns, nil)
	}
	sort.Slice(spaces, func(i, j int) bool { return spaces[i].SpaceID < spaces[j].SpaceID })
	pattern := informationSchemaSpaceNamePattern(query)
	rows := make([][]interface{}, 0, len(spaces))
	for _, space := range spaces {
		if !metadataPatternMatches(space.Name, pattern) {
			continue
		}
		path := informationSchemaSpacePath(e, space)
		pageSize := space.PageSize
		if pageSize == 0 {
			pageSize = 16 * 1024
		}
		extentPages := uint64(space.ExtentSize)
		if extentPages == 0 {
			extentPages = 1
		}
		values := map[string]interface{}{
			"FILE_ID":         int64(space.SpaceID),
			"FILE_NAME":       path,
			"FILE_TYPE":       "TABLESPACE",
			"TABLESPACE_NAME": space.Name,
			// MySQL 8.4 reports these table identity columns as empty/NULL for
			// InnoDB FILES rows. The tablespace name is the supported identity.
			"TABLE_CATALOG":        "",
			"TABLE_SCHEMA":         nil,
			"TABLE_NAME":           nil,
			"LOGFILE_GROUP_NAME":   nil,
			"LOGFILE_GROUP_NUMBER": nil,
			"ENGINE":               "InnoDB",
			"FULLTEXT_KEYS":        nil,
			"DELETED_ROWS":         nil,
			"UPDATE_COUNT":         nil,
			"FREE_EXTENTS":         int64(space.FreePages / extentPages),
			"TOTAL_EXTENTS":        int64(space.TotalPages / extentPages),
			"EXTENT_SIZE":          int64(extentPages) * int64(pageSize),
			"INITIAL_SIZE":         informationSchemaSpaceFileSize(space),
			"MAXIMUM_SIZE":         nil,
			"AUTOEXTEND_SIZE":      nil,
			"CREATION_TIME":        nil,
			"LAST_UPDATE_TIME":     nil,
			"LAST_ACCESS_TIME":     nil,
			"RECOVER_TIME":         nil,
			"TRANSACTION_COUNTER":  nil,
			"VERSION":              nil,
			"ROW_FORMAT":           nil,
			"TABLE_ROWS":           nil,
			"AVG_ROW_LENGTH":       nil,
			"DATA_LENGTH":          nil,
			"MAX_DATA_LENGTH":      nil,
			"INDEX_LENGTH":         nil,
			"DATA_FREE":            int64(space.FreePages) * int64(pageSize),
			"CREATE_TIME":          nil,
			"UPDATE_TIME":          nil,
			"CHECK_TIME":           nil,
			"CHECKSUM":             nil,
			"STATUS":               informationSchemaFilesStatus(space.State),
			"EXTRA":                nil,
			"NODEGROUP_ID":         nil,
			"TABLESPACE_TYPE":      innoDBSpaceType(space),
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema.files", columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaInnoDBTablespacesBriefSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"SPACE", "NAME", "PATH", "FLAG", "SPACE_TYPE"})
	if e == nil || e.storageManager == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_tablespaces_brief", columns, nil)
	}
	spaces, err := e.storageManager.ListSpaces()
	if err != nil {
		return newInformationSchemaSelectResult("information_schema.innodb_tablespaces_brief", columns, nil)
	}
	sort.Slice(spaces, func(i, j int) bool { return spaces[i].SpaceID < spaces[j].SpaceID })
	rows := make([][]interface{}, 0, len(spaces))
	pattern := informationSchemaSpaceNamePattern(query)
	for _, space := range spaces {
		if !metadataPatternMatches(space.Name, pattern) {
			continue
		}
		values := map[string]interface{}{
			"SPACE":      int64(space.SpaceID),
			"NAME":       space.Name,
			"PATH":       informationSchemaSpacePath(e, space),
			"FLAG":       e.informationSchemaInnoDBSpaceFlags(space.SpaceID),
			"SPACE_TYPE": innoDBSpaceType(space),
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema.innodb_tablespaces_brief", columns, rows)
}

func (e *XMySQLExecutor) informationSchemaInnoDBTableRows(query string) []frmMetadataTable {
	pattern := informationSchemaSpaceNamePattern(query)
	tables := e.scanFrmTables()
	filtered := make([]frmMetadataTable, 0, len(tables))
	for _, table := range tables {
		if !metadataPatternMatches(table.schemaName+"/"+table.tableName, pattern) &&
			!metadataPatternMatches(table.schemaName+"."+table.tableName, pattern) {
			continue
		}
		filtered = append(filtered, table)
	}
	sort.Slice(filtered, func(i, j int) bool {
		left := filtered[i].schemaName + "." + filtered[i].tableName
		right := filtered[j].schemaName + "." + filtered[j].tableName
		return left < right
	})
	return filtered
}

func (e *XMySQLExecutor) executeInformationSchemaInnoDBTablesSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry["innodb_tables"])
	if e == nil || e.tableStorageManager == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_tables", columns, nil)
	}
	rows := make([][]interface{}, 0)
	for _, table := range e.informationSchemaInnoDBTableRows(query) {
		storage, err := e.tableStorageManager.GetTableStorageInfo(table.schemaName, table.tableName)
		if err != nil || storage == nil {
			continue
		}
		rowFormat := table.rowFormat
		if strings.TrimSpace(rowFormat) == "" {
			rowFormat = "Dynamic"
		}
		spaceType := "Single"
		if !storage.OwnsTablespace && strings.TrimSpace(storage.TablespaceName) != "" {
			spaceType = "General"
		}
		values := map[string]interface{}{
			"TABLE_ID":           int64(storage.SpaceID),
			"NAME":               table.schemaName + "/" + table.tableName,
			"FLAG":               e.informationSchemaInnoDBSpaceFlags(storage.SpaceID),
			"N_COLS":             int64(len(table.columns) + 3),
			"SPACE":              int64(storage.SpaceID),
			"ROW_FORMAT":         rowFormat,
			"ZIP_PAGE_SIZE":      int64(0),
			"SPACE_TYPE":         spaceType,
			"INSTANT_COLS":       innodbInstantColumnCount(table.columns),
			"TOTAL_ROW_VERSIONS": int64(table.totalRowVersions),
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema.innodb_tables", columns, rows)
}

func innodbInstantColumnCount(columns []frmMetadataColumn) int64 {
	for position, column := range columns {
		if column.instantAdded {
			return int64(position)
		}
	}
	return 0
}

func (e *XMySQLExecutor) executeInformationSchemaInnoDBFieldsSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry["innodb_fields"])
	if e == nil || e.tableStorageManager == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_fields", columns, nil)
	}
	rows := make([][]interface{}, 0)
	for _, table := range e.informationSchemaInnoDBTableRows(query) {
		storage, err := e.tableStorageManager.GetTableStorageInfo(table.schemaName, table.tableName)
		if err != nil || storage == nil {
			continue
		}
		for indexPosition, index := range innodbMetadataIndexes(table) {
			indexID := int64(storage.SpaceID)<<32 | int64(indexPosition+1)
			for fieldPosition, columnName := range index.columns {
				values := map[string]interface{}{
					"INDEX_ID": indexID,
					"NAME":     columnName,
					"POS":      int64(fieldPosition),
				}
				if !performanceSchemaLockValuesMatch(query, values) {
					continue
				}
				rows = append(rows, projectInformationSchemaRow(columns, values))
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema.innodb_fields", columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaInnoDBVirtualSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry["innodb_virtual"])
	if e == nil || e.tableStorageManager == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_virtual", columns, nil)
	}
	rows := make([][]interface{}, 0)
	for _, table := range e.informationSchemaInnoDBTableRows(query) {
		storage, err := e.tableStorageManager.GetTableStorageInfo(table.schemaName, table.tableName)
		if err != nil || storage == nil {
			continue
		}
		for position, column := range table.columns {
			if strings.TrimSpace(asString(column.generatedExpression)) == "" {
				continue
			}
			basePositions := innodbVirtualBasePositions(asString(column.generatedExpression), table.columns, position)
			var basePos interface{}
			if len(basePositions) > 0 {
				basePos = int64(basePositions[0])
			}
			values := map[string]interface{}{
				"TABLE_ID": int64(storage.SpaceID),
				"POS":      int64(position),
				"BASE_POS": basePos,
			}
			if performanceSchemaLockValuesMatch(query, values) {
				rows = append(rows, projectInformationSchemaRow(columns, values))
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema.innodb_virtual", columns, rows)
}

// innodbVirtualBasePositions derives the persisted virtual-column dependency
// metadata from the same generated expression and durable column definition
// used by the DML path.  The dictionary exposes the first referenced base
// position and the number of referenced base columns; no synthetic dependency
// is emitted when an expression has no resolvable column reference.
func innodbVirtualBasePositions(expression string, columns []frmMetadataColumn, generatedPosition int) []int {
	// Ignore quoted literals before matching identifiers.  This avoids treating
	// an expression such as `concat('qty', price)` as a dependency on qty.
	expression = regexp.MustCompile(`(?s)'(?:''|[^'])*'|"(?:""|[^"])*"`).ReplaceAllString(expression, " ")
	positions := make([]int, 0, len(columns))
	for position, column := range columns {
		if position == generatedPosition || strings.TrimSpace(column.name) == "" {
			continue
		}
		name := strings.Trim(strings.TrimSpace(column.name), "`")
		if name == "" {
			continue
		}
		pattern := `(?i)(^|[^a-z0-9_$])` + regexp.QuoteMeta(name) + `([^a-z0-9_$]|$)`
		if regexp.MustCompile(pattern).MatchString(expression) {
			positions = append(positions, position)
		}
	}
	return positions
}

func informationSchemaRoutineCallMatches(definition string, object persistedStoredObject, viewSchema string) bool {
	name := regexp.QuoteMeta(strings.Trim(object.Name, "`"))
	if name == "" {
		return false
	}
	qualified := regexp.QuoteMeta(strings.Trim(object.Schema, "`")) + `\s*\.\s*` + name
	if regexp.MustCompile(`(?i)(?:^|[^a-z0-9_$])` + qualified + `\s*\(`).MatchString(definition) {
		return true
	}
	if !strings.EqualFold(object.Schema, viewSchema) {
		return false
	}
	return regexp.MustCompile(`(?i)(?:^|[^a-z0-9_$])` + name + `\s*\(`).MatchString(definition)
}

// executeInformationSchemaViewRoutineUsageSelect reports persisted stored
// functions referenced by persisted view definitions. Procedures are not
// callable from a SELECT expression and are intentionally excluded.
func (e *XMySQLExecutor) executeInformationSchemaViewRoutineUsageSelect(query string, session server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry["view_routine_usage"])
	filters := informationSchemaViewUsageFilters(query)
	views := e.informationSchemaVisibleViewDefinitions(session)
	if len(views) == 0 {
		return newInformationSchemaSelectResult("information_schema.view_routine_usage", columns, nil)
	}
	functions := e.scanStoredObjects("function")
	type usageEntry struct {
		viewSchema     string
		viewName       string
		functionSchema string
		functionName   string
	}
	entries := make([]usageEntry, 0)
	seen := make(map[string]struct{})
	for _, view := range views {
		viewSchema := view.Schema
		viewName := view.Name
		definition := view.Definition
		if !informationSchemaViewUsageFilterMatches(filters, "TABLE_CATALOG", "def") ||
			!informationSchemaViewUsageFilterMatches(filters, "TABLE_SCHEMA", viewSchema) ||
			!informationSchemaViewUsageFilterMatches(filters, "TABLE_NAME", viewName) {
			continue
		}
		for _, function := range functions {
			functionVisible, _ := e.storedRoutineMetadataVisibility(&ExecutionContext{Session: session}, function)
			if !functionVisible {
				continue
			}
			if !informationSchemaRoutineCallMatches(definition, function, viewSchema) {
				continue
			}
			if !informationSchemaViewUsageFilterMatches(filters, "SPECIFIC_CATALOG", "def") ||
				!informationSchemaViewUsageFilterMatches(filters, "SPECIFIC_SCHEMA", function.Schema) ||
				!informationSchemaViewUsageFilterMatches(filters, "SPECIFIC_NAME", function.Name) {
				continue
			}
			key := strings.ToLower(viewSchema + "." + viewName + "\x00" + function.Schema + "." + function.Name)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			entries = append(entries, usageEntry{viewSchema: viewSchema, viewName: viewName, functionSchema: function.Schema, functionName: function.Name})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		for _, values := range [][2]string{{entries[i].viewSchema, entries[j].viewSchema}, {entries[i].viewName, entries[j].viewName}, {entries[i].functionSchema, entries[j].functionSchema}, {entries[i].functionName, entries[j].functionName}} {
			left, right := strings.ToLower(values[0]), strings.ToLower(values[1])
			if left == right {
				continue
			}
			return left < right
		}
		return false
	})
	rows := make([][]interface{}, 0, len(entries))
	for _, entry := range entries {
		rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
			"TABLE_CATALOG": "def", "TABLE_SCHEMA": entry.viewSchema, "TABLE_NAME": entry.viewName,
			"SPECIFIC_CATALOG": "def", "SPECIFIC_SCHEMA": entry.functionSchema, "SPECIFIC_NAME": entry.functionName,
		}))
	}
	return newInformationSchemaSelectResult("information_schema.view_routine_usage", columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaSTGeometryColumnsSelect(query string, session server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry["st_geometry_columns"])
	if e == nil {
		return newInformationSchemaSelectResult("information_schema.st_geometry_columns", columns, nil)
	}
	filters := informationSchemaMetadataFilters(query)
	geometryFilters := informationSchemaGeometryColumnFilters(query)
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		if !e.informationSchemaTableVisible(session, table.schemaName, table.tableName, false) {
			continue
		}
		if !metadataPatternMatches(table.schemaName, filters["table_schema"]) || !metadataPatternMatches(table.tableName, filters["table_name"]) ||
			!metadataPatternMatches("def", geometryFilters["TABLE_CATALOG"]) {
			continue
		}
		for _, column := range table.columns {
			if !strings.EqualFold(strings.TrimSpace(column.typeName), "geometry") && !strings.HasSuffix(strings.ToLower(strings.TrimSpace(column.typeName)), "geometry") {
				continue
			}
			if !metadataPatternMatches(column.name, filters["column_name"]) ||
				!metadataPatternMatches(strings.ToUpper(column.typeName), geometryFilters["GEOMETRY_TYPE_NAME"]) ||
				!metadataPatternMatches("", geometryFilters["SRS_ID"]) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
				"TABLE_CATALOG": "def", "TABLE_SCHEMA": table.schemaName, "TABLE_NAME": table.tableName,
				"COLUMN_NAME": column.name, "SRS_NAME": nil, "SRS_ID": nil,
				"GEOMETRY_TYPE_NAME": strings.ToUpper(column.typeName),
			}))
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		left := strings.ToLower(asString(rows[i][1]) + "\x00" + asString(rows[i][2]) + "\x00" + asString(rows[i][3]))
		right := strings.ToLower(asString(rows[j][1]) + "\x00" + asString(rows[j][2]) + "\x00" + asString(rows[j][3]))
		return left < right
	})
	return newInformationSchemaSelectResult("information_schema.st_geometry_columns", columns, rows)
}

var informationSchemaGeometryColumnFilterPattern = regexp.MustCompile(`(?i)\b(table_catalog|geometry_type_name|geometry_type|srs_id)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)

func informationSchemaGeometryColumnFilters(query string) map[string]string {
	filters := make(map[string]string)
	for _, match := range informationSchemaGeometryColumnFilterPattern.FindAllStringSubmatch(query, -1) {
		value := match[2]
		if value == "" {
			value = match[3]
		}
		column := strings.ToUpper(match[1])
		if column == "GEOMETRY_TYPE" {
			column = "GEOMETRY_TYPE_NAME"
		}
		filters[column] = value
	}
	return filters
}

func (e *XMySQLExecutor) executeInformationSchemaUserAttributesSelect(query string, session server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry["user_attributes"])
	if e == nil {
		return newInformationSchemaSelectResult("information_schema.user_attributes", columns, nil)
	}
	filters := mysqlSystemTableFilters(query)
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return newInformationSchemaSelectResult("information_schema.user_attributes", columns, nil)
	}
	rows := make([][]interface{}, 0)
	for _, account := range file.Accounts {
		if strings.TrimSpace(account.UserAttributes) == "" ||
			!metadataFilterMatches(account.Host, filters["host"]) ||
			!metadataFilterMatches(account.User, filters["user"]) {
			continue
		}
		if !e.informationSchemaUserAttributesVisible(file, session, account) {
			continue
		}
		values := map[string]interface{}{
			"USER": account.User, "HOST": account.Host, "ATTRIBUTE": account.UserAttributes,
		}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		left := strings.ToLower(asString(rows[i][0]) + "\x00" + asString(rows[i][1]))
		right := strings.ToLower(asString(rows[j][0]) + "\x00" + asString(rows[j][1]))
		return left < right
	})
	return newInformationSchemaSelectResult("information_schema.user_attributes", columns, rows)
}

// informationSchemaUserAttributesVisible follows the MySQL 8.4 visibility
// rules for INFORMATION_SCHEMA.USER_ATTRIBUTES.  The table is not a generic
// account dump: ordinary accounts see themselves, CREATE USER accounts may
// see other non-SYSTEM_USER accounts, and accounts with the required mysql.user
// or CREATE USER + SYSTEM_USER privileges can see all rows.  Replication
// threads are allowed to inspect all rows as part of applying metadata.
func (e *XMySQLExecutor) informationSchemaUserAttributesVisible(file persistedAccountFile, session server.MySQLServerSession, target persistedAccount) bool {
	if session == nil || sessionBoolParam(session, "replication_replay") {
		return true
	}
	currentUser, _ := session.GetParamByName("user").(string)
	currentUser = strings.TrimSpace(currentUser)
	if currentUser == "" || strings.EqualFold(currentUser, "root") {
		return strings.EqualFold(currentUser, "root")
	}
	current := sessionAccount(file, session)
	if current == nil {
		return false
	}
	effective := effectiveAccountGrants(file, *current, session)
	if grantsContain(effective, "mysql.user", "SELECT") || grantsContain(effective, "mysql.user", "UPDATE") {
		return true
	}
	canCreateUser := grantsContain(effective, "*.*", "CREATE USER")
	if canCreateUser && grantsContain(effective, "*.*", "SYSTEM_USER") {
		return true
	}
	targetEffective := effectiveAccountGrants(file, target, nil)
	targetIsSystemUser := grantsContain(targetEffective, "*.*", "SYSTEM_USER")
	if canCreateUser && !targetIsSystemUser {
		return true
	}
	return strings.EqualFold(current.User, target.User) && strings.EqualFold(current.Host, target.Host)
}

func informationSchemaBufferPoolStatInt64(stats map[string]interface{}, key string) int64 {
	value := stats[key]
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case int32:
		return int64(typed)
	case uint:
		return int64(typed)
	case uint64:
		return int64(typed)
	case uint32:
		return int64(typed)
	case float64:
		return int64(typed)
	case float32:
		return int64(typed)
	default:
		return 0
	}
}

func informationSchemaBufferPoolStatFloat64(stats map[string]interface{}, key string) float64 {
	switch typed := stats[key].(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case uint64:
		return float64(typed)
	default:
		return 0
	}
}

func (e *XMySQLExecutor) executeInformationSchemaInnoDBBufferPoolStatsSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry["innodb_buffer_pool_stats"])
	if e == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_buffer_pool_stats", columns, nil)
	}
	provider, ok := e.bufferPoolManager.(interface{ GetStats() map[string]interface{} })
	if !ok || provider == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_buffer_pool_stats", columns, nil)
	}
	stats := provider.GetStats()
	lruIOTotal := informationSchemaBufferPoolStatInt64(stats, "lru_io_total")
	lruIOCurrent := informationSchemaBufferPoolStatInt64(stats, "lru_io_current")
	if lruProvider, ok := e.bufferPoolManager.(interface {
		GetLRUIOStatsAndResetCurrent() (uint64, uint64)
	}); ok && lruProvider != nil {
		total, current := lruProvider.GetLRUIOStatsAndResetCurrent()
		lruIOTotal = int64(total)
		lruIOCurrent = int64(current)
	}
	total := informationSchemaBufferPoolStatInt64(stats, "total_pages")
	cache := informationSchemaBufferPoolStatInt64(stats, "cache_size")
	free := total - cache
	if free < 0 {
		free = 0
	}
	// InnoDB reports buffer-pool hit rate on a per-thousand scale: a fully
	// warm pool is 1000, not 100. The manager keeps the source ratio as 0..1,
	// so convert it at the INFORMATION_SCHEMA boundary.
	hitRate := int64(informationSchemaBufferPoolStatFloat64(stats, "hit_rate") * 1000)
	pagesGet := informationSchemaBufferPoolStatInt64(stats, "hits") + informationSchemaBufferPoolStatInt64(stats, "misses")
	pagesMadeYoung := informationSchemaBufferPoolStatInt64(stats, "pages_made_young")
	pagesNotMadeYoung := informationSchemaBufferPoolStatInt64(stats, "pages_not_made_young")
	uncompressTotal := int64(0)
	uncompressCurrent := int64(0)
	if e.storageManager != nil {
		if compression := e.storageManager.GetCompressionManager(); compression != nil {
			compressionStats := compression.GetStats()
			uncompressTotal = int64(compressionStats.UncompressedPages)
			uncompressCurrent = int64(compression.GetUncompressedCurrentAndReset())
		}
	}
	pendingDecompress := int64(0)
	if e.storageManager != nil {
		if compression := e.storageManager.GetCompressionManager(); compression != nil {
			pendingDecompress = int64(compression.GetPendingDecompress())
		}
	}
	values := map[string]interface{}{
		"POOL_ID": 0, "POOL_SIZE": total, "FREE_BUFFERS": free, "DATABASE_PAGES": cache,
		"OLD_DATABASE_PAGES":      informationSchemaBufferPoolStatInt64(stats, "old_hits"),
		"MODIFIED_DATABASE_PAGES": informationSchemaBufferPoolStatInt64(stats, "dirty_pages"),
		"PENDING_DECOMPRESS":      pendingDecompress, "PENDING_READS": informationSchemaBufferPoolStatInt64(stats, "pending_reads"), "PENDING_FLUSH_LRU": informationSchemaBufferPoolStatInt64(stats, "pending_flush_lru"), "PENDING_FLUSH_LIST": informationSchemaBufferPoolStatInt64(stats, "pending_flush_list"),
		"PAGES_MADE_YOUNG":      pagesMadeYoung,
		"PAGES_NOT_MADE_YOUNG":  pagesNotMadeYoung,
		"PAGES_MADE_YOUNG_RATE": informationSchemaBufferPoolStatFloat64(stats, "pages_made_young_rate"), "PAGES_MADE_NOT_YOUNG_RATE": informationSchemaBufferPoolStatFloat64(stats, "pages_not_made_young_rate"),
		"NUMBER_PAGES_READ":    informationSchemaBufferPoolStatInt64(stats, "page_reads"),
		"NUMBER_PAGES_CREATED": informationSchemaBufferPoolStatInt64(stats, "page_creates"), "NUMBER_PAGES_WRITTEN": informationSchemaBufferPoolStatInt64(stats, "page_writes"),
		"PAGES_READ_RATE":   informationSchemaBufferPoolStatFloat64(stats, "page_reads_rate"),
		"PAGES_CREATE_RATE": informationSchemaBufferPoolStatFloat64(stats, "page_creates_rate"), "PAGES_WRITTEN_RATE": informationSchemaBufferPoolStatFloat64(stats, "page_writes_rate"),
		"NUMBER_PAGES_GET": pagesGet,
		"HIT_RATE":         hitRate, "YOUNG_MAKE_PER_THOUSAND_GETS": informationSchemaPagesPerThousand(pagesMadeYoung, pagesGet), "NOT_YOUNG_MAKE_PER_THOUSAND_GETS": informationSchemaPagesPerThousand(pagesNotMadeYoung, pagesGet),
		"NUMBER_PAGES_READ_AHEAD":   informationSchemaBufferPoolStatInt64(stats, "read_ahead"),
		"NUMBER_READ_AHEAD_EVICTED": informationSchemaBufferPoolStatInt64(stats, "read_ahead_evicted"),
		"READ_AHEAD_RATE":           informationSchemaBufferPoolStatFloat64(stats, "read_ahead_rate"),
		"READ_AHEAD_EVICTED_RATE":   informationSchemaBufferPoolStatFloat64(stats, "read_ahead_evicted_rate"),
		"LRU_IO_TOTAL":              lruIOTotal, "LRU_IO_CURRENT": lruIOCurrent, "UNCOMPRESS_TOTAL": uncompressTotal, "UNCOMPRESS_CURRENT": uncompressCurrent,
	}
	if !performanceSchemaLockValuesMatch(query, values) {
		return newInformationSchemaSelectResult("information_schema.innodb_buffer_pool_stats", columns, nil)
	}
	return newInformationSchemaSelectResult("information_schema.innodb_buffer_pool_stats", columns, [][]interface{}{projectInformationSchemaRow(columns, values)})
}

func informationSchemaPagesPerThousand(pages, gets int64) int64 {
	if pages <= 0 || gets <= 0 {
		return 0
	}
	return pages * 1000 / gets
}

func asString(value interface{}) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}
