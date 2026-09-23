package engine

import (
	"regexp"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

var informationSchemaBufferSpaceFilterPattern = regexp.MustCompile(`(?is)\bspace\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)"|([0-9]+))`)

func informationSchemaBufferSpacePattern(query string) string {
	match := informationSchemaBufferSpaceFilterPattern.FindStringSubmatch(query)
	if len(match) == 0 {
		return ""
	}
	for _, value := range match[1:] {
		if value != "" {
			return value
		}
	}
	return ""
}

func informationSchemaBufferSpaceMatches(spaceID uint32, pattern string) bool {
	if strings.TrimSpace(pattern) == "" {
		return true
	}
	return metadataPatternMatches(formatUint32(spaceID), pattern)
}

func formatUint32(value uint32) string {
	if value == 0 {
		return "0"
	}
	var digits [10]byte
	index := len(digits)
	for value > 0 {
		index--
		digits[index] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[index:])
}

func (e *XMySQLExecutor) informationSchemaBufferPageTable(spaceID uint32) (string, string) {
	if e == nil || e.tableStorageManager == nil {
		return "", ""
	}
	info, err := e.tableStorageManager.GetTableBySpaceID(spaceID)
	if err != nil || info == nil {
		return "", ""
	}
	return info.SchemaName, info.TableName
}

func informationSchemaBufferPageSize(e *XMySQLExecutor, spaceID uint32) int64 {
	if e != nil && e.storageManager != nil {
		if space, err := e.storageManager.GetSpaceInfo(spaceID); err == nil && space != nil && space.PageSize != 0 {
			return int64(space.PageSize)
		}
	}
	return 16 * 1024
}

func informationSchemaBufferPageValues(e *XMySQLExecutor, snapshot buffer_pool.PageSnapshot) map[string]interface{} {
	pageState := "FILE_PAGE"
	if snapshot.Page == nil || snapshot.Page.IsFree() {
		pageState = "NOT_USED"
	}
	schemaName, tableName := e.informationSchemaBufferPageTable(snapshot.SpaceID)
	qualifiedTable := ""
	if schemaName != "" && tableName != "" {
		qualifiedTable = schemaName + "/" + tableName
	}
	return map[string]interface{}{
		"POOL_ID": 0, "BLOCK_ID": int64(snapshot.LRUPosition), "SPACE": int64(snapshot.SpaceID),
		"PAGE_NUMBER": int64(snapshot.PageNo), "PAGE_TYPE": nil, "FLUSH_TYPE": nil,
		"FIX_COUNT": func() int64 {
			if snapshot.Page == nil {
				return 0
			}
			return int64(snapshot.Page.GetPinCount())
		}(),
		"IS_HASHED": 0, "NEWEST_MODIFICATION": func() interface{} {
			if snapshot.Page == nil {
				return nil
			}
			return int64(snapshot.Page.GetLSN())
		}(),
		"OLDEST_MODIFICATION": nil, "ACCESS_TIME": int64(snapshot.Page.GetAccessTime()),
		"TABLE_NAME": qualifiedTable, "INDEX_NAME": func() interface{} {
			if qualifiedTable == "" {
				return nil
			}
			return "PRIMARY"
		}(),
		"NUMBER_RECORDS": nil, "DATA_SIZE": informationSchemaBufferPageSize(e, snapshot.SpaceID),
		"COMPRESSED_SIZE": nil, "PAGE_STATE": pageState, "IO_FIX": nil,
		"IS_OLD": func() int64 {
			if snapshot.IsOld {
				return 1
			}
			return 0
		}(), "FREE_PAGE_CLOCK": nil,
	}
}

func (e *XMySQLExecutor) executeInformationSchemaInnoDBBufferPagesSelect(query string, lru bool) *SelectResult {
	name := "information_schema.innodb_buffer_page"
	if lru {
		name += "_lru"
	}
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry[strings.TrimPrefix(name, "information_schema.")])
	provider, ok := e.bufferPoolManager.(interface {
		SnapshotPages() []buffer_pool.PageSnapshot
	})
	if !ok || provider == nil {
		return newInformationSchemaSelectResult(name, columns, nil)
	}
	spacePattern := informationSchemaBufferSpacePattern(query)
	snapshots := provider.SnapshotPages()
	if !lru {
		sort.SliceStable(snapshots, func(i, j int) bool {
			if snapshots[i].SpaceID != snapshots[j].SpaceID {
				return snapshots[i].SpaceID < snapshots[j].SpaceID
			}
			return snapshots[i].PageNo < snapshots[j].PageNo
		})
	}
	rows := make([][]interface{}, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if !informationSchemaBufferSpaceMatches(snapshot.SpaceID, spacePattern) {
			continue
		}
		values := informationSchemaBufferPageValues(e, snapshot)
		if lru {
			values["LRU_POSITION"] = int64(snapshot.LRUPosition)
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult(name, columns, rows)
}

func informationSchemaCompressionRow(columns []string, pageSize int64, stats manager.CompressionStats) []interface{} {
	return projectInformationSchemaRow(columns, map[string]interface{}{
		"PAGE_SIZE": pageSize, "COMPRESS_OPS": int64(stats.TotalPages), "COMPRESS_OPS_OK": int64(stats.CompressedPages),
		"COMPRESS_TIME": int64(0), "UNCOMPRESS_OPS": int64(0), "UNCOMPRESS_TIME": int64(0),
	})
}

func (e *XMySQLExecutor) informationSchemaCompressionStats(query string) (int64, manager.CompressionStats, bool) {
	if e == nil || e.storageManager == nil {
		return 0, manager.CompressionStats{}, false
	}
	compression := e.storageManager.GetCompressionManager()
	if compression == nil {
		return 0, manager.CompressionStats{}, false
	}
	pageSize := int64(16 * 1024)
	if settings := compression.GetCompressionSettings(0); settings != nil && settings.BlockSize != 0 {
		pageSize = int64(settings.BlockSize)
	}
	return pageSize, compression.GetStats(), true
}

func (e *XMySQLExecutor) executeInformationSchemaInnoDBCompressionSelect(query, name string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry[name])
	pageSize, stats, ok := e.informationSchemaCompressionStats(query)
	if !ok {
		return newInformationSchemaSelectResult("information_schema."+name, columns, nil)
	}
	if name == "innodb_cmpmem" || name == "innodb_cmpmem_reset" {
		values := map[string]interface{}{
			"PAGE_SIZE": pageSize, "BUFFER_POOL_INSTANCE": 0, "COMPRESS_OPS": int64(stats.CompressedPages), "COMPRESS_TIME": int64(0), "UNCOMPRESS_OPS": int64(0), "UNCOMPRESS_TIME": int64(0),
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			return newInformationSchemaSelectResult("information_schema."+name, columns, nil)
		}
		return newInformationSchemaSelectResult("information_schema."+name, columns, [][]interface{}{projectInformationSchemaRow(columns, values)})
	}
	if name == "innodb_cmp_per_index" || name == "innodb_cmp_per_index_reset" {
		// The compressor currently records global counters only; do not project
		// those counters onto arbitrary indexes.
		return newInformationSchemaSelectResult("information_schema."+name, columns, nil)
	}
	values := map[string]interface{}{
		"PAGE_SIZE": pageSize, "COMPRESS_OPS": int64(stats.TotalPages), "COMPRESS_OPS_OK": int64(stats.CompressedPages),
		"COMPRESS_TIME": int64(0), "UNCOMPRESS_OPS": int64(0), "UNCOMPRESS_TIME": int64(0),
	}
	if !performanceSchemaLockValuesMatch(query, values) {
		return newInformationSchemaSelectResult("information_schema."+name, columns, nil)
	}
	return newInformationSchemaSelectResult("information_schema."+name, columns, [][]interface{}{projectInformationSchemaRow(columns, values)})
}

func (e *XMySQLExecutor) executeInformationSchemaInnoDBCachedIndexesSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry["innodb_cached_indexes"])
	if e == nil || e.tableStorageManager == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_cached_indexes", columns, nil)
	}
	provider, ok := e.bufferPoolManager.(interface {
		SnapshotPages() []buffer_pool.PageSnapshot
	})
	if !ok || provider == nil {
		return newInformationSchemaSelectResult("information_schema.innodb_cached_indexes", columns, nil)
	}
	pageCounts := make(map[uint32]int64)
	for _, page := range provider.SnapshotPages() {
		pageCounts[page.SpaceID]++
	}
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		if !metadataPatternMatches(table.schemaName, filters["table_schema"]) || !metadataPatternMatches(table.tableName, filters["table_name"]) {
			continue
		}
		storage, err := e.tableStorageManager.GetTableStorageInfo(table.schemaName, table.tableName)
		if err != nil || storage == nil {
			continue
		}
		indexes := []*manager.Index(nil)
		if e.indexManager != nil {
			indexes = e.indexManager.ListIndexes(uint64(storage.SpaceID))
		}
		if len(indexes) == 0 {
			indexes = []*manager.Index{{IndexID: uint64(storage.SpaceID)<<32 | 1, SpaceID: storage.SpaceID, Name: "PRIMARY"}}
		}
		for _, index := range indexes {
			if index == nil {
				continue
			}
			values := map[string]interface{}{
				"SPACE_ID": int64(storage.SpaceID), "INDEX_ID": int64(index.IndexID), "INDEX_NAME": index.Name, "N_CACHED_PAGES": pageCounts[storage.SpaceID],
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("information_schema.innodb_cached_indexes", columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaTemporaryTablesSelect(query string, session server.MySQLServerSession, name string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry[name])
	state := getTemporaryTableSessionState(session, false)
	if state == nil {
		return newInformationSchemaSelectResult("information_schema."+name, columns, nil)
	}
	state.mu.RLock()
	bindings := make([]temporaryTableBinding, 0, len(state.tables))
	for _, binding := range state.tables {
		bindings = append(bindings, binding)
	}
	state.mu.RUnlock()
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Logical < bindings[j].Logical })
	rows := make([][]interface{}, 0, len(bindings))
	for index, binding := range bindings {
		var spaceID interface{}
		if e.tableStorageManager != nil {
			if storage, err := e.tableStorageManager.GetTableStorageInfo(binding.Database, binding.Physical); err == nil && storage != nil {
				spaceID = int64(storage.SpaceID)
			}
		}
		if name == "innodb_session_temp_tablespaces" {
			values := map[string]interface{}{
				"SPACE": spaceID, "PATH": nil, "PURPOSE": "TEMPORARY", "STATE": "ACTIVE", "SIZE": nil, "ALLOCATED_SIZE": nil,
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		} else {
			values := map[string]interface{}{
				"TABLE_ID": int64(index + 1), "NAME": binding.Database + "/" + binding.Logical, "N_COLS": nil, "SPACE": spaceID, "PER_TABLE_TABLESPACE": "YES",
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("information_schema."+name, columns, rows)
}
