package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

var fullTextAgainstPattern = regexp.MustCompile(`(?is)MATCH\s*\(([^)]*)\)\s+AGAINST\s*\(\s*'([^']*)'(?:\s+IN\s+(?:BOOLEAN|NATURAL\s+LANGUAGE)\s+MODE)?\s*\)`)
var spatialPredicateColumnFirstPattern = regexp.MustCompile("(?is)(ST_WITHIN|ST_CONTAINS|ST_INTERSECTS|ST_DISJOINT|ST_TOUCHES|ST_OVERLAPS|ST_CROSSES|ST_EQUALS|MBRCONTAINS|MBRWITHIN|MBRINTERSECTS|MBREQUALS|MBRDISJOINT)\\s*\\(\\s*([a-zA-Z0-9_`.]+)\\s*,\\s*ST_GEOMFROMTEXT\\s*\\(\\s*'([^']+)'(?:\\s*,\\s*([0-9]+))?\\s*\\)\\s*\\)")
var spatialPredicateGeometryFirstPattern = regexp.MustCompile("(?is)(ST_WITHIN|ST_CONTAINS|ST_INTERSECTS|ST_DISJOINT|ST_TOUCHES|ST_OVERLAPS|ST_CROSSES|ST_EQUALS|MBRCONTAINS|MBRWITHIN|MBRINTERSECTS|MBREQUALS|MBRDISJOINT)\\s*\\(\\s*ST_GEOMFROMTEXT\\s*\\(\\s*'([^']+)'(?:\\s*,\\s*([0-9]+))?\\s*\\)\\s*,\\s*([a-zA-Z0-9_`.]+)\\s*\\)")

const spatialProbeColumn = "__xmysql_spatial_probe"

type spatialPredicateMatch struct {
	predicate      string
	column         string
	wkt            string
	srid           string
	geometryFirst  bool
	fullExpression string
}

type advancedIndexDefinition struct {
	Name    string
	Kind    string
	Columns []string
}

type fullTextIndexState struct {
	Version   int                 `json:"version"`
	Tokenizer string              `json:"tokenizer"`
	Stopwords []string            `json:"stopwords"`
	Segments  map[string][]string `json:"segments"`
}

// spatialIndexState is the durable compatibility representation of a spatial
// index. It stores one minimum bounding rectangle per clustered row and a
// deterministic packed R-tree hierarchy over those entries. The executor
// still performs the exact geometry predicate after the candidate read, so
// this state is not a claim of upstream InnoDB R-tree page-format
// compatibility.
type spatialIndexState struct {
	Version int                 `json:"version"`
	Columns []string            `json:"columns"`
	Entries []spatialIndexEntry `json:"entries"`
	Root    int                 `json:"root"`
	Nodes   []spatialIndexNode  `json:"nodes,omitempty"`
}

type spatialIndexEntry struct {
	RowKey string  `json:"row_key"`
	MinX   float64 `json:"min_x"`
	MinY   float64 `json:"min_y"`
	MaxX   float64 `json:"max_x"`
	MaxY   float64 `json:"max_y"`
}

type spatialIndexNode struct {
	MinX     float64 `json:"min_x"`
	MinY     float64 `json:"min_y"`
	MaxX     float64 `json:"max_x"`
	MaxY     float64 `json:"max_y"`
	Children []int   `json:"children,omitempty"`
	Entries  []int   `json:"entries,omitempty"`
	Leaf     bool    `json:"leaf"`
}

const spatialIndexNodeFanout = 8

func buildSpatialIndexState(columns []string, entries []spatialIndexEntry) spatialIndexState {
	state := spatialIndexState{
		Version: 2,
		Columns: append([]string(nil), columns...),
		Entries: append([]spatialIndexEntry(nil), entries...),
		Root:    -1,
	}
	if len(state.Entries) == 0 {
		return state
	}

	order := make([]int, len(state.Entries))
	for index := range order {
		order[index] = index
	}
	sort.SliceStable(order, func(left, right int) bool {
		leftEntry, rightEntry := state.Entries[order[left]], state.Entries[order[right]]
		leftCenterX := (leftEntry.MinX + leftEntry.MaxX) / 2
		rightCenterX := (rightEntry.MinX + rightEntry.MaxX) / 2
		if leftCenterX != rightCenterX {
			return leftCenterX < rightCenterX
		}
		leftCenterY := (leftEntry.MinY + leftEntry.MaxY) / 2
		rightCenterY := (rightEntry.MinY + rightEntry.MaxY) / 2
		if leftCenterY != rightCenterY {
			return leftCenterY < rightCenterY
		}
		return leftEntry.RowKey < rightEntry.RowKey
	})

	level := make([]int, 0, (len(order)+spatialIndexNodeFanout-1)/spatialIndexNodeFanout)
	for start := 0; start < len(order); start += spatialIndexNodeFanout {
		end := start + spatialIndexNodeFanout
		if end > len(order) {
			end = len(order)
		}
		node := spatialIndexNode{Leaf: true, Entries: append([]int(nil), order[start:end]...)}
		spatialIndexNodeBounds(&node, state.Entries, nil)
		state.Nodes = append(state.Nodes, node)
		level = append(level, len(state.Nodes)-1)
	}
	for len(level) > 1 {
		sort.SliceStable(level, func(left, right int) bool {
			leftNode, rightNode := state.Nodes[level[left]], state.Nodes[level[right]]
			leftCenter := (leftNode.MinX + leftNode.MaxX) / 2
			rightCenter := (rightNode.MinX + rightNode.MaxX) / 2
			if leftCenter != rightCenter {
				return leftCenter < rightCenter
			}
			return (leftNode.MinY+leftNode.MaxY)/2 < (rightNode.MinY+rightNode.MaxY)/2
		})
		next := make([]int, 0, (len(level)+spatialIndexNodeFanout-1)/spatialIndexNodeFanout)
		for start := 0; start < len(level); start += spatialIndexNodeFanout {
			end := start + spatialIndexNodeFanout
			if end > len(level) {
				end = len(level)
			}
			node := spatialIndexNode{Children: append([]int(nil), level[start:end]...)}
			spatialIndexNodeBounds(&node, nil, state.Nodes)
			state.Nodes = append(state.Nodes, node)
			next = append(next, len(state.Nodes)-1)
		}
		level = next
	}
	state.Root = level[0]
	return state
}

func spatialIndexNodeBounds(node *spatialIndexNode, entries []spatialIndexEntry, nodes []spatialIndexNode) {
	if node == nil {
		return
	}
	first := true
	update := func(minX, minY, maxX, maxY float64) {
		if first {
			node.MinX, node.MinY, node.MaxX, node.MaxY = minX, minY, maxX, maxY
			first = false
			return
		}
		node.MinX = math.Min(node.MinX, minX)
		node.MinY = math.Min(node.MinY, minY)
		node.MaxX = math.Max(node.MaxX, maxX)
		node.MaxY = math.Max(node.MaxY, maxY)
	}
	if node.Leaf {
		for _, index := range node.Entries {
			if index >= 0 && index < len(entries) {
				entry := entries[index]
				update(entry.MinX, entry.MinY, entry.MaxX, entry.MaxY)
			}
		}
		return
	}
	for _, index := range node.Children {
		if index >= 0 && index < len(nodes) {
			child := nodes[index]
			update(child.MinX, child.MinY, child.MaxX, child.MaxY)
		}
	}
}

var defaultFullTextStopwords = map[string]struct{}{"a": {}, "an": {}, "and": {}, "of": {}, "the": {}, "to": {}}

// executeRawFullTextSpatialCreate strips the parser-unsupported index clauses,
// creates the ordinary table, and then persists the advanced index dictionary.
func (e *XMySQLExecutor) executeRawFullTextSpatialCreate(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	baseQuery, indexes, ok, err := parseAdvancedIndexCreate(query)
	if !ok {
		return false, err
	}
	stmt, err := sqlparser.Parse(baseQuery)
	if err != nil {
		return true, fmt.Errorf("parse advanced-index CREATE TABLE base statement: %w", err)
	}
	ddl, ok := stmt.(*sqlparser.DDL)
	if !ok || !strings.EqualFold(ddl.Action, sqlparser.CreateStr) {
		return true, fmt.Errorf("advanced index clause is only supported on CREATE TABLE")
	}
	original := ctx.RawQuery
	ctx.RawQuery = baseQuery
	e.executeCreateTableStatement(ctx, databaseName, ddl)
	ctx.RawQuery = original
	tableName := ddl.NewName.Name.String()
	info, err := readTableMetadataMap(filepathForTable(e, databaseName, tableName))
	if err != nil {
		return true, err
	}
	stored, _ := info["indexes"].([]interface{})
	for _, definition := range indexes {
		columns := make([]interface{}, len(definition.Columns))
		for i, column := range definition.Columns {
			columns[i] = column
		}
		stored = append(stored, map[string]interface{}{
			"name": definition.Name, "type": definition.Kind, "unique": false, "primary": false,
			"columns": columns, "advanced": true,
		})
		if definition.Kind == "FULLTEXT" {
			info["fulltext_segments_"+definition.Name] = fullTextIndexState{
				Version: 1, Tokenizer: "unicode-word-lowercase", Stopwords: sortedStopwords(defaultFullTextStopwords),
				Segments: map[string][]string{},
			}
		} else if definition.Kind == "SPATIAL" {
			info["spatial_entries_"+definition.Name] = spatialIndexState{
				Version: 2, Columns: append([]string(nil), definition.Columns...), Entries: []spatialIndexEntry{}, Root: -1,
			}
		}
	}
	info["indexes"] = stored
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return true, err
	}
	// The table has just been created and no concurrent metadata reader can
	// observe it yet. A direct write avoids Windows rename semantics colliding
	// with the storage manager's freshly opened .frm handle.
	return true, os.WriteFile(filepathForTable(e, databaseName, tableName), data, 0644)
}

func filepathForTable(e *XMySQLExecutor, databaseName, tableName string) string {
	return filepath.Join(e.getDataDir(), databaseName, tableName+".frm")
}

func parseAdvancedIndexCreate(query string) (string, []advancedIndexDefinition, bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "create table") {
		return "", nil, false, nil
	}
	open := strings.Index(trimmed, "(")
	if open < 0 {
		return "", nil, true, fmt.Errorf("CREATE TABLE column list is missing")
	}
	close := matchingParenIndex(trimmed, open)
	if close < 0 {
		return "", nil, true, fmt.Errorf("CREATE TABLE column list is unbalanced")
	}
	definitions := splitTopLevelComma(trimmed[open+1 : close])
	kept := make([]string, 0, len(definitions))
	indexes := make([]advancedIndexDefinition, 0)
	seenIndexNames := make(map[string]struct{})
	for _, definition := range definitions {
		parsed, isAdvanced := parseAdvancedIndexDefinition(definition)
		if !isAdvanced {
			kept = append(kept, definition)
			continue
		}
		key := strings.ToLower(parsed.Name)
		if _, exists := seenIndexNames[key]; exists {
			return "", nil, true, fmt.Errorf("duplicate advanced index '%s'", parsed.Name)
		}
		seenIndexNames[key] = struct{}{}
		indexes = append(indexes, parsed)
	}
	if len(indexes) == 0 {
		return "", nil, false, nil
	}
	base := trimmed[:open+1] + strings.Join(kept, ", ") + trimmed[close:]
	return base, indexes, true, nil
}

func parseAdvancedIndexDefinition(definition string) (advancedIndexDefinition, bool) {
	trimmed := strings.TrimSpace(definition)
	upper := strings.ToUpper(trimmed)
	kind := ""
	if strings.HasPrefix(upper, "FULLTEXT") {
		kind = "FULLTEXT"
	} else if strings.HasPrefix(upper, "SPATIAL") {
		kind = "SPATIAL"
	} else {
		return advancedIndexDefinition{}, false
	}
	open := strings.Index(trimmed, "(")
	close := strings.LastIndex(trimmed, ")")
	if open < 0 || close <= open {
		return advancedIndexDefinition{}, true
	}
	prefix := strings.Fields(strings.TrimSpace(trimmed[:open]))
	name := fmt.Sprintf("%s_%d", strings.ToLower(kind), open)
	if len(prefix) >= 3 {
		candidate := strings.Trim(prefix[len(prefix)-1], "`")
		if !strings.EqualFold(candidate, "INDEX") && !strings.EqualFold(candidate, "KEY") {
			name = candidate
		}
	}
	columns := make([]string, 0)
	for _, column := range strings.Split(trimmed[open+1:close], ",") {
		column = strings.Trim(strings.TrimSpace(column), "`")
		if column != "" {
			columns = append(columns, column)
		}
	}
	return advancedIndexDefinition{Name: name, Kind: kind, Columns: columns}, true
}

func (e *XMySQLExecutor) executeFullTextQuery(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	match := fullTextAgainstPattern.FindStringSubmatch(query)
	if len(match) != 3 {
		return false, nil
	}
	columns := make([]string, 0)
	for _, column := range strings.Split(match[1], ",") {
		column = strings.Trim(strings.TrimSpace(column), "`")
		if column != "" {
			columns = append(columns, column)
		}
	}
	if len(columns) == 0 || strings.TrimSpace(match[2]) == "" {
		return true, fmt.Errorf("MATCH AGAINST requires at least one column and a search term")
	}
	rewritten := fullTextAgainstPattern.ReplaceAllString(query, "1 = 1")
	stmt, err := sqlparser.Parse(rewritten)
	if err != nil {
		return true, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return true, fmt.Errorf("MATCH AGAINST is only supported in SELECT")
	}
	oldRaw := ctx.RawQuery
	ctx.RawQuery = rewritten
	result, err := e.executeSelectStatement(ctx, selectStmt, databaseName)
	ctx.RawQuery = oldRaw
	if err != nil {
		return true, err
	}
	term := strings.ToLower(strings.TrimSpace(match[2]))
	filtered := result.Records[:0]
	for _, record := range result.Records {
		matched := false
		for _, column := range columns {
			value, valueErr := record.GetValueByName(column)
			if valueErr == nil && fullTextScore(strings.ToLower(value.ToString()), term) > 0 {
				matched = true
				break
			}
		}
		if matched {
			filtered = append(filtered, record)
		}
	}
	result.Records = filtered
	result.RowCount = len(filtered)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("FULLTEXT query returned %d rows", len(filtered))}
	return true, nil
}

func fullTextScore(text, term string) int {
	if term == "" || text == "" {
		return 0
	}
	count := 0
	for _, token := range tokenizeFullText(text, defaultFullTextStopwords) {
		if strings.EqualFold(token, term) || strings.Contains(strings.ToLower(token), term) {
			count++
		}
	}
	return count
}

func tokenizeFullText(text string, stopwords map[string]struct{}) []string {
	raw := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
	})
	result := make([]string, 0, len(raw))
	for _, token := range raw {
		if token != "" {
			if _, stop := stopwords[token]; !stop {
				result = append(result, token)
			}
		}
	}
	return result
}

func sortedStopwords(stopwords map[string]struct{}) []string {
	result := make([]string, 0, len(stopwords))
	for word := range stopwords {
		result = append(result, word)
	}
	sort.Strings(result)
	return result
}

// rebuildFullTextSegments creates a deterministic persisted inverted segment
// for compatibility metadata. The SQL path may still scan rows, but restart
// and DDL operations now have a durable index-maintenance contract.
func rebuildFullTextSegments(info map[string]interface{}, indexName string, rows []map[string]interface{}, columns []string) error {
	segments := make(map[string][]string)
	for _, row := range rows {
		rowID := fmt.Sprint(row["id"])
		for _, column := range columns {
			for _, token := range tokenizeFullText(fmt.Sprint(row[column]), defaultFullTextStopwords) {
				segments[token] = appendUniqueString(segments[token], rowID)
			}
		}
	}
	info["fulltext_segments_"+indexName] = fullTextIndexState{
		Version: 1, Tokenizer: "unicode-word-lowercase", Stopwords: sortedStopwords(defaultFullTextStopwords), Segments: segments,
	}
	return nil
}

func appendUniqueString(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

// refreshSpatialIndexState rebuilds the durable MBR candidate entries from
// the clustered rows after a committed DML statement. Rebuilding the small
// compatibility sidecar keeps INSERT/UPDATE/DELETE and restart behavior
// deterministic while exact predicate evaluation remains in the query path.
func (dml *StorageIntegratedDMLExecutor) refreshSpatialIndexState(
	ctx context.Context,
	tableMeta *metadata.TableMeta,
	tableStorageInfo *manager.TableStorageInfo,
	btreeManager basic.BPlusTreeManager,
) error {
	if dml == nil || tableMeta == nil || tableStorageInfo == nil || btreeManager == nil {
		return nil
	}
	frmPath := filepath.Join(dml.dataDir, dml.schemaName, dml.tableName+".frm")
	info, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	indexes, _ := info["indexes"].([]interface{})
	spatialIndexes := make([]struct {
		name   string
		column string
	}, 0)
	for _, raw := range indexes {
		index, ok := raw.(map[string]interface{})
		if !ok || !strings.EqualFold(fmt.Sprint(index["type"]), "SPATIAL") || index["advanced"] != true {
			continue
		}
		columns, _ := index["columns"].([]interface{})
		if len(columns) == 0 || strings.TrimSpace(fmt.Sprint(columns[0])) == "" {
			continue
		}
		spatialIndexes = append(spatialIndexes, struct {
			name   string
			column string
		}{name: fmt.Sprint(index["name"]), column: fmt.Sprint(columns[0])})
	}
	if len(spatialIndexes) == 0 {
		return nil
	}
	rows, err := dml.scanRowsForTableConditions(ctx, dml.schemaName, dml.tableName, nil, tableMeta, tableStorageInfo, btreeManager)
	if err != nil {
		return fmt.Errorf("scan rows for spatial index state: %w", err)
	}
	for _, index := range spatialIndexes {
		entries := make([]spatialIndexEntry, 0, len(rows))
		for _, row := range rows {
			if row == nil {
				continue
			}
			value, exists := row.OldValues[index.column]
			if !exists {
				for name, candidate := range row.OldValues {
					if strings.EqualFold(name, index.column) {
						value, exists = candidate, true
						break
					}
				}
			}
			if !exists || value == nil {
				continue
			}
			text, textErr := plan.EvaluateSpatialFunction("ST_AsText", []interface{}{value})
			if textErr != nil || strings.TrimSpace(fmt.Sprint(text)) == "" {
				continue
			}
			minX, minY, maxX, maxY, boundsOK := spatialBounds(fmt.Sprint(text))
			if !boundsOK {
				continue
			}
			rowKey := fmt.Sprint(row.StorageKey)
			if strings.TrimSpace(rowKey) == "" {
				rowKey = fmt.Sprintf("row:%d", row.RowId)
			}
			entries = append(entries, spatialIndexEntry{RowKey: rowKey, MinX: minX, MinY: minY, MaxX: maxX, MaxY: maxY})
		}
		sort.SliceStable(entries, func(left, right int) bool { return entries[left].RowKey < entries[right].RowKey })
		info["spatial_entries_"+index.name] = buildSpatialIndexState([]string{index.column}, entries)
	}
	return writeTableMetadataMapAtomic(frmPath, info)
}

func (e *XMySQLExecutor) refreshSpatialIndexStateForTable(databaseName, tableName string) error {
	if e == nil || e.tableStorageManager == nil {
		return nil
	}
	dml, err := e.newStorageIntegratedDMLExecutor()
	if err != nil {
		return err
	}
	dml.schemaName = databaseName
	dml.tableName = tableName
	tableMeta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), databaseName, tableName)
	if err != nil {
		return err
	}
	storageInfo, err := e.tableStorageManager.GetTableStorageInfo(databaseName, tableName)
	if err != nil {
		return err
	}
	btreeManager, err := dml.createBTreeManagerForDML(context.Background(), databaseName, tableName, tableMeta)
	if err != nil {
		return err
	}
	return dml.refreshSpatialIndexState(context.Background(), tableMeta, storageInfo, btreeManager)
}

func (e *XMySQLExecutor) executeSpatialQuery(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	match, ok := parseSpatialPredicate(query)
	if !ok {
		return false, nil
	}
	minX, minY, maxX, maxY, ok := spatialBounds(match.wkt)
	if !ok {
		return true, fmt.Errorf("invalid geometry WKT")
	}
	rewritten := strings.Replace(query, match.fullExpression, "1 = 1", 1)
	probeInjected := false
	if !querySelectProjectsColumn(rewritten, match.column) {
		var injected bool
		rewritten, injected = injectSpatialProbeColumn(rewritten, match.column, spatialProbeColumn)
		if !injected {
			return true, fmt.Errorf("spatial predicate requires a SELECT query with a FROM clause")
		}
		probeInjected = true
	}
	stmt, err := sqlparser.Parse(rewritten)
	if err != nil {
		return true, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return true, fmt.Errorf("spatial predicate is only supported in SELECT")
	}
	tableMeta, candidateKeys, candidateOK := e.spatialIndexCandidatesForSelect(databaseName, selectStmt, match, minX, minY, maxX, maxY)
	previousCandidates := ctx.SpatialCandidateKeys
	if candidateOK && tableMeta != nil && len(effectivePrimaryKeyColumns(tableMeta)) > 0 {
		ctx.SpatialCandidateKeys = candidateKeys
	} else {
		ctx.SpatialCandidateKeys = nil
	}
	oldRaw := ctx.RawQuery
	defer func() { ctx.SpatialCandidateKeys = previousCandidates }()
	ctx.RawQuery = rewritten
	result, err := e.executeSelectStatement(ctx, selectStmt, databaseName)
	ctx.RawQuery = oldRaw
	if err != nil {
		return true, err
	}
	probeIndex := -1
	if probeInjected {
		probeIndex = findResultColumn(result.Columns, spatialProbeColumn)
		if probeIndex < 0 {
			return true, fmt.Errorf("spatial probe column is missing from SELECT result")
		}
	}
	filtered := result.Records[:0]
	geometryArgs := []interface{}{match.wkt}
	if match.srid != "" {
		var srid int64
		if _, scanErr := fmt.Sscan(match.srid, &srid); scanErr != nil {
			return true, fmt.Errorf("invalid geometry SRID %q", match.srid)
		}
		geometryArgs = append(geometryArgs, srid)
	}
	queryGeometry, queryGeometryErr := plan.EvaluateSpatialFunction("ST_GeomFromText", geometryArgs)
	predicate := strings.ToUpper(strings.TrimSpace(match.predicate))
	for _, record := range result.Records {
		if candidateOK {
			if rowKey, rowKeyOK := spatialRecordStorageKey(record, tableMeta); rowKeyOK {
				if _, exists := candidateKeys[rowKey]; !exists {
					continue
				}
			}
		}
		columnName := strings.Trim(match.column, "`")
		if probeInjected {
			columnName = spatialProbeColumn
		}
		value, valueErr := record.GetValueByName(columnName)
		if valueErr != nil {
			continue
		}
		if queryGeometryErr == nil {
			var raw interface{} = value.Raw()
			if _, isBinary := raw.([]byte); isBinary {
				arguments := []interface{}{raw, queryGeometry}
				if match.geometryFirst {
					arguments = []interface{}{queryGeometry, raw}
				}
				if matched, spatialErr := plan.EvaluateSpatialFunction(predicate, arguments); spatialErr == nil {
					if numeric, numericOK := rawSpatialBoolean(matched); numericOK && numeric {
						filtered = append(filtered, record)
					}
					continue
				}
			} else if strings.TrimSpace(value.ToString()) != "" {
				arguments := []interface{}{value.ToString(), queryGeometry}
				if match.geometryFirst {
					arguments = []interface{}{queryGeometry, value.ToString()}
				}
				if matched, spatialErr := plan.EvaluateSpatialFunction(predicate, arguments); spatialErr == nil {
					if numeric, numericOK := rawSpatialBoolean(matched); numericOK && numeric {
						filtered = append(filtered, record)
					}
					continue
				}
			}
		}
		x, y, pointOK := spatialPoint(value.ToString())
		if pointOK && spatialFallbackMatches(predicate, match.geometryFirst, x, y, minX, minY, maxX, maxY) {
			filtered = append(filtered, record)
		}
	}
	if probeInjected {
		result = removeSpatialProbeColumn(result, probeIndex, filtered)
		filtered = result.Records
	} else {
		result.Records = filtered
	}
	result.RowCount = len(filtered)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("spatial query returned %d rows", len(filtered))}
	return true, nil
}

func spatialIndexCandidateKeys(state spatialIndexState, predicate string, geometryFirst bool, minX, minY, maxX, maxY float64) (map[string]struct{}, bool) {
	if state.Version <= 0 || len(state.Entries) == 0 {
		return map[string]struct{}{}, true
	}
	predicate = strings.ToUpper(strings.TrimSpace(predicate))
	if predicate == "ST_DISJOINT" {
		return nil, false
	}
	if predicate == "MBRDISJOINT" {
		candidates := make(map[string]struct{})
		for _, entryIndex := range spatialIndexCandidateEntryIndexes(state, predicate, minX, minY, maxX, maxY) {
			if entryIndex < 0 || entryIndex >= len(state.Entries) {
				continue
			}
			entry := state.Entries[entryIndex]
			if entry.MaxX < minX || maxX < entry.MinX || entry.MaxY < minY || maxY < entry.MinY {
				candidates[entry.RowKey] = struct{}{}
			}
		}
		return candidates, true
	}
	candidates := make(map[string]struct{})
	for _, entryIndex := range spatialIndexCandidateEntryIndexes(state, predicate, minX, minY, maxX, maxY) {
		if entryIndex < 0 || entryIndex >= len(state.Entries) {
			continue
		}
		entry := state.Entries[entryIndex]
		matches := entry.MinX <= maxX && entry.MaxX >= minX && entry.MinY <= maxY && entry.MaxY >= minY
		switch predicate {
		case "MBRCONTAINS":
			if geometryFirst {
				matches = minX <= entry.MinX && minY <= entry.MinY && maxX >= entry.MaxX && maxY >= entry.MaxY
			} else {
				matches = entry.MinX <= minX && entry.MinY <= minY && entry.MaxX >= maxX && entry.MaxY >= maxY
			}
		case "MBRWITHIN":
			if geometryFirst {
				matches = minX >= entry.MinX && minY >= entry.MinY && maxX <= entry.MaxX && maxY <= entry.MaxY
			} else {
				matches = entry.MinX >= minX && entry.MinY >= minY && entry.MaxX <= maxX && entry.MaxY <= maxY
			}
		case "MBREQUALS":
			matches = entry.MinX == minX && entry.MinY == minY && entry.MaxX == maxX && entry.MaxY == maxY
		}
		if matches {
			candidates[entry.RowKey] = struct{}{}
		}
	}
	return candidates, true
}

func spatialIndexCandidateEntryIndexes(state spatialIndexState, predicate string, minX, minY, maxX, maxY float64) []int {
	if len(state.Nodes) == 0 || state.Root < 0 || state.Root >= len(state.Nodes) {
		indexes := make([]int, len(state.Entries))
		for index := range indexes {
			indexes[index] = index
		}
		return indexes
	}
	predicate = strings.ToUpper(strings.TrimSpace(predicate))
	indexes := make([]int, 0, len(state.Entries))
	var visit func(int)
	visit = func(nodeIndex int) {
		if nodeIndex < 0 || nodeIndex >= len(state.Nodes) {
			return
		}
		node := state.Nodes[nodeIndex]
		overlaps := node.MinX <= maxX && node.MaxX >= minX && node.MinY <= maxY && node.MaxY >= minY
		if predicate == "MBRDISJOINT" && !overlaps {
			indexes = append(indexes, node.Entries...)
			if !node.Leaf {
				for _, child := range node.Children {
					appendSpatialIndexSubtreeEntryIndexes(&indexes, state.Nodes, child)
				}
			}
			return
		}
		if predicate != "MBRCONTAINS" && predicate != "MBRWITHIN" && predicate != "MBREQUALS" && !overlaps {
			return
		}
		if node.Leaf {
			indexes = append(indexes, node.Entries...)
			return
		}
		for _, child := range node.Children {
			visit(child)
		}
	}
	visit(state.Root)
	return indexes
}

func appendSpatialIndexSubtreeEntryIndexes(indexes *[]int, nodes []spatialIndexNode, nodeIndex int) {
	if indexes == nil || nodeIndex < 0 || nodeIndex >= len(nodes) {
		return
	}
	node := nodes[nodeIndex]
	if node.Leaf {
		*indexes = append(*indexes, node.Entries...)
		return
	}
	for _, child := range node.Children {
		appendSpatialIndexSubtreeEntryIndexes(indexes, nodes, child)
	}
}

func (e *XMySQLExecutor) spatialIndexCandidatesForSelect(databaseName string, stmt *sqlparser.Select, match spatialPredicateMatch, minX, minY, maxX, maxY float64) (*metadata.TableMeta, map[string]struct{}, bool) {
	if e == nil || stmt == nil || len(stmt.From) != 1 {
		return nil, nil, false
	}
	aliased, ok := stmt.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, nil, false
	}
	tableExpr, ok := aliased.Expr.(sqlparser.TableName)
	if !ok {
		return nil, nil, false
	}
	tableName := tableExpr.Name.String()
	info, err := readTableMetadataMap(filepathForTable(e, databaseName, tableName))
	if err != nil {
		return nil, nil, false
	}
	columnName := strings.Trim(match.column, "`")
	if dot := strings.LastIndex(columnName, "."); dot >= 0 {
		columnName = strings.Trim(columnName[dot+1:], "`")
	}
	indexes, _ := info["indexes"].([]interface{})
	for _, raw := range indexes {
		index, ok := raw.(map[string]interface{})
		if !ok || index["advanced"] != true || !strings.EqualFold(fmt.Sprint(index["type"]), "SPATIAL") {
			continue
		}
		columns, _ := index["columns"].([]interface{})
		if len(columns) == 0 || !strings.EqualFold(strings.Trim(fmt.Sprint(columns[0]), "`"), columnName) {
			continue
		}
		state, ok := decodeSpatialIndexState(info["spatial_entries_"+fmt.Sprint(index["name"])])
		if !ok {
			return nil, nil, false
		}
		candidates, candidateOK := spatialIndexCandidateKeys(state, match.predicate, match.geometryFirst, minX, minY, maxX, maxY)
		if !candidateOK {
			return nil, nil, false
		}
		tableMeta, metaErr := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), databaseName, tableName)
		if metaErr != nil {
			return nil, nil, false
		}
		return tableMeta, candidates, true
	}
	return nil, nil, false
}

func decodeSpatialIndexState(value interface{}) (spatialIndexState, bool) {
	switch state := value.(type) {
	case spatialIndexState:
		return state, true
	case map[string]interface{}:
		raw, err := json.Marshal(state)
		if err != nil {
			return spatialIndexState{}, false
		}
		var decoded spatialIndexState
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return spatialIndexState{}, false
		}
		return decoded, true
	default:
		return spatialIndexState{}, false
	}
}

func spatialRecordStorageKey(record Record, tableMeta *metadata.TableMeta) (string, bool) {
	primaryKeyColumns := effectivePrimaryKeyColumns(tableMeta)
	if len(primaryKeyColumns) == 0 || record == nil {
		return "", false
	}
	row := make(map[string]interface{}, len(primaryKeyColumns))
	for _, column := range primaryKeyColumns {
		value, err := record.GetValueByName(column)
		if err != nil || value.Raw() == nil {
			return "", false
		}
		row[column] = value.Raw()
	}
	if len(primaryKeyColumns) == 1 {
		key, ok := storageKeyToBytes(row[primaryKeyColumns[0]])
		return string(key), ok
	}
	key, err := buildCompositeKey(row, primaryKeyColumns)
	if err != nil {
		return "", false
	}
	return string(key), true
}

func parseSpatialPredicate(query string) (spatialPredicateMatch, bool) {
	if groups := spatialPredicateColumnFirstPattern.FindStringSubmatch(query); len(groups) == 5 {
		return spatialPredicateMatch{
			predicate: groups[1], column: groups[2], wkt: groups[3], srid: groups[4], fullExpression: groups[0],
		}, true
	}
	if groups := spatialPredicateGeometryFirstPattern.FindStringSubmatch(query); len(groups) == 5 {
		return spatialPredicateMatch{
			predicate: groups[1], wkt: groups[2], srid: groups[3], column: groups[4], geometryFirst: true, fullExpression: groups[0],
		}, true
	}
	return spatialPredicateMatch{}, false
}

func querySelectProjectsColumn(query, column string) bool {
	needle := strings.Trim(strings.TrimSpace(column), "`")
	if needle == "" {
		return false
	}
	for _, expression := range splitTopLevelComma(selectProjectionText(query)) {
		expression = strings.TrimSpace(expression)
		if expression == "*" || strings.HasSuffix(strings.TrimSpace(expression), ".*") {
			return true
		}
		if strings.EqualFold(strings.Trim(strings.TrimSpace(expression), "`"), needle) {
			return true
		}
		if dot := strings.LastIndex(expression, "."); dot >= 0 && strings.EqualFold(strings.Trim(expression[dot+1:], "` "), needle[strings.LastIndex(needle, ".")+1:]) {
			return true
		}
	}
	return false
}

func selectProjectionText(query string) string {
	from := topLevelKeywordIndex(query, "from")
	if from < 0 {
		return ""
	}
	selectEnd := topLevelKeywordIndex(query, "select")
	if selectEnd < 0 || selectEnd >= from {
		return ""
	}
	return strings.TrimSpace(query[selectEnd+len("select") : from])
}

func injectSpatialProbeColumn(query, column, alias string) (string, bool) {
	from := topLevelKeywordIndex(query, "from")
	selectStart := topLevelKeywordIndex(query, "select")
	if selectStart < 0 || from <= selectStart+len("select") {
		return query, false
	}
	projectionStart := selectStart + len("select")
	projection := strings.TrimSpace(query[projectionStart:from])
	modifier := ""
	for _, candidate := range []string{"distinct", "all"} {
		if len(projection) >= len(candidate) && strings.EqualFold(projection[:len(candidate)], candidate) && (len(projection) == len(candidate) || projection[len(candidate)] <= ' ') {
			modifier = projection[:len(candidate)]
			projection = strings.TrimSpace(projection[len(candidate):])
			break
		}
	}
	if projection == "" {
		return query, false
	}
	if modifier != "" {
		projection = modifier + " " + projection
	}
	replacement := projection + ", " + column + " AS " + alias + " "
	return query[:projectionStart] + " " + replacement + query[from:], true
}

func topLevelKeywordIndex(query, keyword string) int {
	depth := 0
	quote := byte(0)
	for index := 0; index+len(keyword) <= len(query); index++ {
		ch := query[index]
		if quote != 0 {
			if ch == quote {
				if index+1 < len(query) && query[index+1] == quote {
					index++
					continue
				}
				quote = 0
			}
			if ch == '\\' && quote == '\'' {
				index++
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && strings.EqualFold(query[index:index+len(keyword)], keyword) &&
			(index == 0 || !spatialSQLIdentifierByte(query[index-1])) &&
			(index+len(keyword) == len(query) || !spatialSQLIdentifierByte(query[index+len(keyword)])) {
			return index
		}
	}
	return -1
}

func spatialSQLIdentifierByte(ch byte) bool {
	return ch == '_' || ch == '$' || ch == '`' || ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
}

func spatialFallbackMatches(predicate string, geometryFirst bool, x, y, minX, minY, maxX, maxY float64) bool {
	if predicate == "ST_EQUALS" || predicate == "ST_INTERSECTS" {
		return x >= minX && x <= maxX && y >= minY && y <= maxY
	}
	if geometryFirst {
		return x >= minX && x <= maxX && y >= minY && y <= maxY
	}
	return x >= minX && x <= maxX && y >= minY && y <= maxY
}

func removeSpatialProbeColumn(result *SelectResult, probeIndex int, records []Record) *SelectResult {
	if result == nil || probeIndex < 0 {
		return result
	}
	columns := append([]string(nil), result.Columns...)
	if probeIndex < len(columns) {
		columns = append(columns[:probeIndex], columns[probeIndex+1:]...)
	}
	types := append([]string(nil), result.ColumnTypes...)
	if probeIndex < len(types) {
		types = append(types[:probeIndex], types[probeIndex+1:]...)
	}
	result.Columns = columns
	result.ColumnTypes = types
	result.Records = make([]Record, 0, len(records))
	for _, record := range records {
		values := record.GetValues()
		if probeIndex >= len(values) {
			continue
		}
		projected := make([]basic.Value, 0, len(values)-1)
		projected = append(projected, values[:probeIndex]...)
		projected = append(projected, values[probeIndex+1:]...)
		schema := metadata.NewQuerySchema()
		for _, column := range result.Columns {
			schema.AddColumn(metadata.NewQueryColumn(strings.Trim(column, "`"), metadata.TypeVarchar))
		}
		result.Records = append(result.Records, NewExecutorRecordFromValues(projected, schema))
	}
	return result
}

func rawSpatialBoolean(value interface{}) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case int64:
		return typed != 0, true
	case int:
		return typed != 0, true
	case basic.Value:
		return rawSpatialBoolean(typed.Raw())
	default:
		return false, false
	}
}

func spatialBounds(wkt string) (float64, float64, float64, float64, bool) {
	points := regexp.MustCompile(`[-+]?[0-9]*\.?[0-9]+\s+[-+]?[0-9]*\.?[0-9]+`).FindAllString(wkt, -1)
	if len(points) == 0 {
		return 0, 0, 0, 0, false
	}
	minX, minY := math.Inf(1), math.Inf(1)
	maxX, maxY := math.Inf(-1), math.Inf(-1)
	for _, raw := range points {
		var x, y float64
		if _, err := fmt.Sscanf(raw, "%f %f", &x, &y); err != nil {
			return 0, 0, 0, 0, false
		}
		minX, minY = math.Min(minX, x), math.Min(minY, y)
		maxX, maxY = math.Max(maxX, x), math.Max(maxY, y)
	}
	return minX, minY, maxX, maxY, true
}

func spatialPoint(wkt string) (float64, float64, bool) {
	points := regexp.MustCompile(`[-+]?[0-9]*\.?[0-9]+\s+[-+]?[0-9]*\.?[0-9]+`).FindString(wkt)
	if points == "" {
		return 0, 0, false
	}
	var x, y float64
	if _, err := fmt.Sscanf(points, "%f %f", &x, &y); err != nil {
		return 0, 0, false
	}
	return x, y, true
}

func advancedIndexNames(info map[string]interface{}) []string {
	indexes, _ := info["indexes"].([]interface{})
	result := make([]string, 0)
	for _, raw := range indexes {
		if index, ok := raw.(map[string]interface{}); ok && index["advanced"] == true {
			if name, ok := index["name"].(string); ok {
				result = append(result, name)
			}
		}
	}
	sort.Strings(result)
	return result
}
