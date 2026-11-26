package engine

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
)

// TABLE(VECTOR_SEARCH(schema, table, index_name, query_vector, k, mode[, ef|filters[, filters]]))
// exposes a bounded search over one READY native vector index. mode is either
// ANN or EXACT; returned rows carry the complete source identity so callers
// can fetch the authoritative row or CONTENT value through ordinary SQL.
func (e *XMySQLExecutor) evaluateVectorSearchTable(ctx *ExecutionContext, function *sqlparser.FuncExpr) (*derivedJoinSource, error) {
	if e != nil && e.vectorIndexCatalog != nil {
		releaseSearchFence := e.vectorIndexCatalog.AcquireSearchFence()
		defer releaseSearchFence()
	}
	result := emptyVectorSearchSource()
	if e == nil || e.vectorIndexCatalog == nil || !e.VectorIndexCatalogUsable() {
		return nil, fmt.Errorf("native vector index catalog is unavailable")
	}
	if function == nil || len(function.Exprs) < 6 || len(function.Exprs) > 8 {
		return nil, fmt.Errorf("VECTOR_SEARCH requires schema, table, index name, query vector, k, mode, and optional search parameters")
	}
	arguments := make([]interface{}, len(function.Exprs))
	for i, raw := range function.Exprs {
		argument, ok := raw.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("VECTOR_SEARCH arguments must be scalar expressions")
		}
		var value interface{}
		var err error
		if i == 3 {
			if embed, ok := argument.Expr.(*sqlparser.FuncExpr); ok && strings.EqualFold(strings.Trim(embed.Name.String(), "` "), "AI_EMBED") {
				value, err = evaluateAIEmbedSQL(e.embeddingRuntimeSnapshot(), ctx, embed, nil)
			} else {
				value, err = evaluateExpressionWithRow(argument.Expr, nil)
			}
		} else {
			value, err = evaluateExpressionWithRow(argument.Expr, nil)
		}
		if err != nil {
			return nil, err
		}
		arguments[i] = value
	}
	for _, value := range arguments {
		if value == nil {
			return result, nil
		}
	}
	schema, err := vectorStringArgument(arguments[0], "schema")
	if err != nil {
		return nil, err
	}
	table, err := vectorStringArgument(arguments[1], "table")
	if err != nil {
		return nil, err
	}
	name, err := vectorStringArgument(arguments[2], "index name")
	if err != nil {
		return nil, err
	}
	query, ok := vectorArgument(arguments[3])
	if !ok {
		return nil, fmt.Errorf("VECTOR_SEARCH query must be a VECTOR value")
	}
	k, err := vectorSearchInteger(arguments[4], "k")
	if err != nil || k < 1 || k > 10000 {
		return nil, fmt.Errorf("VECTOR_SEARCH k must be between 1 and 10000")
	}
	mode, err := vectorStringArgument(arguments[5], "mode")
	if err != nil {
		return nil, err
	}
	mode = strings.ToUpper(strings.TrimSpace(mode))
	ef := k
	filterJSON := ""
	hasFilterArgument := false
	switch mode {
	case "EXACT":
		if len(arguments) == 7 {
			hasFilterArgument = true
			filterJSON, err = vectorStringArgument(arguments[6], "filter JSON")
		} else if len(arguments) == 8 {
			return nil, fmt.Errorf("VECTOR_SEARCH EXACT accepts a filter JSON argument but no ef")
		}
	case "ANN":
		if len(arguments) >= 7 {
			ef, err = vectorSearchInteger(arguments[6], "ef")
		}
		if err == nil && len(arguments) == 8 {
			hasFilterArgument = true
			filterJSON, err = vectorStringArgument(arguments[7], "filter JSON")
		}
	default:
		return nil, fmt.Errorf("VECTOR_SEARCH mode must be ANN or EXACT")
	}
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(strings.TrimSpace(schema), schema) || strings.TrimSpace(schema) == "" || strings.TrimSpace(table) == "" || strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("VECTOR_SEARCH schema, table, and index name must be non-empty identifiers")
	}
	if err := e.checkTablePrivilege(ctx, schema, table, "SELECT"); err != nil {
		return nil, err
	}
	indexID := vectorIndexID(schema, table, name)
	manifest, found, err := e.vectorIndexCatalog.ReadLatest(indexID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("VECTOR index %s does not exist on %s.%s", name, schema, table)
	}
	if !strings.EqualFold(manifest.Schema, schema) || !strings.EqualFold(manifest.Table, table) {
		return nil, fmt.Errorf("VECTOR index identity does not match requested table")
	}
	if manifest.Freshness != vector.IndexFreshnessReady {
		return nil, vector.ErrIndexNotReady
	}
	var verifier *vectorSearchSourceVerifier
	var filters []vectorSearchFilter
	if hasFilterArgument {
		verifier, err = e.newVectorSearchSourceVerifier(vectorSearchExecutionContext(ctx), manifest)
		if err != nil {
			return nil, err
		}
		filters, err = parseVectorSearchFilters(filterJSON, verifier.tableMeta)
		if err != nil {
			return nil, err
		}
		for _, filter := range filters {
			if err := e.checkColumnPrivilege(ctx, schema, table, filter.column.Name, "SELECT"); err != nil {
				return nil, err
			}
		}
	}
	var filterErr error
	var accept func(vector.HNSWEntry) bool
	if len(filters) > 0 {
		accept = func(entry vector.HNSWEntry) bool {
			if filterErr != nil {
				return false
			}
			row, err := verifier.verifyAndRead(vector.HNSWHit{Entry: entry})
			if err != nil {
				filterErr = err
				return false
			}
			matched, err := vectorSearchFiltersMatch(row, filters)
			if err != nil {
				filterErr = err
				return false
			}
			return matched
		}
	}
	var hits []vector.HNSWHit
	approximate := false
	visited := 0
	mayHaveMore := false
	searchBudget := 0
	switch mode {
	case "EXACT":
		hits, manifest, err = e.vectorIndexCatalog.SearchExact(indexID, query, k, accept)
	case "ANN":
		if ef < k || ef > 4096 {
			return nil, fmt.Errorf("VECTOR_SEARCH ANN ef must be k..4096")
		}
		var search vector.HNSWSearchResult
		search, manifest, err = e.vectorIndexCatalog.SearchApproximate(indexID, query, k, ef, accept)
		hits, approximate, visited, mayHaveMore, searchBudget = search.Hits, search.Approximate, search.CandidatesVisited, search.MayHaveMore, search.SearchBudget
	default:
		return nil, fmt.Errorf("VECTOR_SEARCH mode must be ANN or EXACT")
	}
	if err != nil {
		return nil, err
	}
	if filterErr != nil {
		return nil, filterErr
	}
	if len(hits) > 0 {
		if verifier == nil {
			verifier, err = e.newVectorSearchSourceVerifier(vectorSearchExecutionContext(ctx), manifest)
			if err != nil {
				return nil, err
			}
		}
		for _, hit := range hits {
			if err := verifier.verify(hit); err != nil {
				return nil, err
			}
		}
	}
	for _, hit := range hits {
		value := hit.Entry.Value
		if value.Source == nil {
			return nil, fmt.Errorf("VECTOR index returned an entry without source provenance")
		}
		source := value.Source
		result.rows = append(result.rows, derivedRow{values: map[string]interface{}{
			"entry_id": hit.Entry.ID, "distance": hit.Distance, "source_schema": source.Schema,
			"source_table": source.Table, "source_column": source.Column,
			"primary_key": append([]byte(nil), source.PrimaryKey...), "source_revision": source.Revision,
			"source_sha256": fmt.Sprintf("%x", source.Digest), "source_block_id": value.SourceBlockID,
			"source_modality": value.SourceModality, "model_id": value.Space.ModelID,
			"model_version": value.Space.ModelVersion, "space_id": value.Space.ID,
			"index_generation": manifest.Generation, "approximate": approximate,
			"candidates_visited": int64(visited), "search_budget": int64(searchBudget), "may_have_more": mayHaveMore,
		}})
	}
	return result, nil
}

type vectorSearchSourceVerifier struct {
	executor     *XMySQLExecutor
	manifest     vector.IndexManifest
	ctx          context.Context
	tableMeta    *metadata.TableMeta
	scanner      *ClusteredIndexScanner
	columns      map[string]*metadata.ColumnMeta
	vectorColumn *metadata.ColumnMeta
}

func (e *XMySQLExecutor) newVectorSearchSourceVerifier(ctx *ExecutionContext, manifest vector.IndexManifest) (*vectorSearchSourceVerifier, error) {
	dml, err := e.newStorageIntegratedDMLExecutor()
	if err != nil {
		return nil, err
	}
	tableMeta, _, btree, err := dml.tableAccess(ctxContext(ctx), manifest.Schema, manifest.Table)
	if err != nil {
		return nil, fmt.Errorf("read VECTOR index source table %s.%s: %w", manifest.Schema, manifest.Table, err)
	}
	columns := make(map[string]*metadata.ColumnMeta, len(tableMeta.Columns))
	for _, column := range tableMeta.Columns {
		if column != nil {
			columns[strings.ToLower(strings.TrimSpace(column.Name))] = column
		}
	}
	vectorColumn := columns[strings.ToLower(strings.TrimSpace(manifest.Column))]
	if !columnHasDataType(vectorColumn, metadata.TypeVector) {
		return nil, e.rejectStaleVectorSearchSource(manifest, "indexed column no longer matches its manifest")
	}
	return &vectorSearchSourceVerifier{
		executor: e, manifest: manifest, ctx: ctxContext(ctx), tableMeta: tableMeta,
		scanner:      NewClusteredIndexScanner(btree, tableMeta),
		columns:      columns,
		vectorColumn: vectorColumn,
	}, nil
}

func (verifier *vectorSearchSourceVerifier) verify(hit vector.HNSWHit) error {
	_, err := verifier.verifyAndRead(hit)
	return err
}

func (verifier *vectorSearchSourceVerifier) verifyAndRead(hit vector.HNSWHit) (*InsertRowData, error) {
	if verifier == nil || verifier.executor == nil || verifier.scanner == nil {
		return nil, fmt.Errorf("VECTOR index source verifier is unavailable")
	}
	e := verifier.executor
	manifest := verifier.manifest
	indexed := hit.Entry.Value
	if indexed.Source == nil {
		return nil, fmt.Errorf("VECTOR index returned an entry without source provenance")
	}
	source := *indexed.Source
	sourceColumn := verifier.columns[strings.ToLower(strings.TrimSpace(source.Column))]
	if !columnHasDataType(sourceColumn, metadata.TypeContent) {
		return nil, e.rejectStaleVectorSearchSource(manifest, "source column no longer matches its manifest")
	}
	lookupKey, err := vectorSearchPrimaryKeyLookupKey(source.PrimaryKey, verifier.tableMeta)
	if err != nil {
		return nil, fmt.Errorf("decode VECTOR index primary key: %w", err)
	}
	row, _, found, err := verifier.scanner.FindByLookupKeyAndStorageKey(verifier.ctx, lookupKey, source.PrimaryKey)
	if err != nil {
		return nil, fmt.Errorf("re-read VECTOR index source row: %w", err)
	}
	if !found || row == nil {
		return nil, e.rejectStaleVectorSearchSource(manifest, "source row is missing")
	}
	var contentValue content.Value
	sourceValue, exists := clusteredRowColumnValue(row, sourceColumn.Name)
	if !exists {
		return nil, fmt.Errorf("VECTOR index source CONTENT column %s is missing", sourceColumn.Name)
	}
	switch typed := sourceValue.(type) {
	case content.Value:
		contentValue = typed
	case *content.Value:
		if typed == nil {
			return nil, fmt.Errorf("VECTOR index source CONTENT column %s is NULL", sourceColumn.Name)
		}
		contentValue = *typed
	default:
		return nil, fmt.Errorf("VECTOR index source column %s has unsupported value %T", sourceColumn.Name, sourceValue)
	}
	currentSource, err := content.NewSourceRef(manifest.Schema, manifest.Table, sourceColumn.Name, source.PrimaryKey, contentValue)
	if err != nil {
		return nil, fmt.Errorf("validate current VECTOR index source: %w", err)
	}
	if !source.Matches(currentSource) {
		return nil, e.rejectStaleVectorSearchSource(manifest, "source revision is stale")
	}
	currentVectorValue, exists := clusteredRowColumnValue(row, verifier.vectorColumn.Name)
	if !exists {
		return nil, fmt.Errorf("VECTOR index column %s is missing from its source row", verifier.vectorColumn.Name)
	}
	currentVector, ok := vectorArgument(currentVectorValue)
	if !ok || currentVector.State != vector.StateReady || currentVector.Source == nil || !currentVector.Source.Matches(currentSource) || currentVector.SourceBlockID != indexed.SourceBlockID {
		return nil, e.rejectStaleVectorSearchSource(manifest, "index entry no longer matches its current source row")
	}
	indexedBytes, indexedErr := indexed.MarshalBinary()
	currentBytes, currentErr := currentVector.MarshalBinary()
	if indexedErr != nil || currentErr != nil || !bytes.Equal(indexedBytes, currentBytes) {
		return nil, e.rejectStaleVectorSearchSource(manifest, "index entry differs from its current source row")
	}
	expectedEntryID := vectorIndexEntryID(manifest.IndexID, currentSource, currentVector.SourceBlockID)
	if hit.Entry.ID != expectedEntryID {
		return nil, e.rejectStaleVectorSearchSource(manifest, "entry identity does not match its current source row")
	}
	return row, nil
}

func (e *XMySQLExecutor) rejectStaleVectorSearchSource(manifest vector.IndexManifest, reason string) error {
	if e == nil || e.vectorIndexCatalog == nil {
		return fmt.Errorf("VECTOR index source is stale: %s", reason)
	}
	if _, err := e.vectorIndexCatalog.MarkTableDirty(manifest.Schema, manifest.Table); err != nil {
		e.vectorIndexCatalogUsable.Store(false)
		return fmt.Errorf("VECTOR index source is stale (%s), and index invalidation failed: %w", reason, err)
	}
	return fmt.Errorf("%w: VECTOR index source is stale (%s); rebuild index %s", vector.ErrIndexNotReady, reason, manifest.IndexID)
}

func clusteredRowColumnValue(row *InsertRowData, name string) (interface{}, bool) {
	if row == nil {
		return nil, false
	}
	if value, ok := row.ColumnValues[name]; ok {
		return value, true
	}
	for column, value := range row.ColumnValues {
		if strings.EqualFold(strings.TrimSpace(column), strings.TrimSpace(name)) {
			return value, true
		}
	}
	return nil, false
}

func vectorSearchPrimaryKeyLookupKey(primaryKey []byte, tableMeta *metadata.TableMeta) (interface{}, error) {
	primaryKeyColumns := effectivePrimaryKeyColumns(tableMeta)
	if len(primaryKeyColumns) != 1 {
		return append([]byte(nil), primaryKey...), nil
	}
	var primaryColumn *metadata.ColumnMeta
	for _, column := range tableMeta.Columns {
		if column != nil && strings.EqualFold(strings.TrimSpace(column.Name), strings.TrimSpace(primaryKeyColumns[0])) {
			primaryColumn = column
			break
		}
	}
	if primaryColumn == nil {
		return nil, fmt.Errorf("primary key column %s is missing from table metadata", primaryKeyColumns[0])
	}
	text := string(primaryKey)
	switch metadata.DataType(strings.ToUpper(strings.TrimSpace(string(primaryColumn.Type)))) {
	case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt, metadata.TypeBigInt, metadata.TypeYear, metadata.TypeBit:
		if primaryColumn.IsUnsigned {
			return strconv.ParseUint(text, 10, 64)
		}
		return strconv.ParseInt(text, 10, 64)
	case metadata.TypeFloat, metadata.TypeDouble:
		return strconv.ParseFloat(text, 64)
	case metadata.TypeDecimal:
		return strconv.ParseFloat(text, 64)
	case metadata.TypeBinary, metadata.TypeVarBinary, metadata.TypeTinyBlob, metadata.TypeBlob,
		metadata.TypeMediumBlob, metadata.TypeLongBlob, metadata.TypeContent, metadata.TypeVector:
		return append([]byte(nil), primaryKey...), nil
	default:
		return text, nil
	}
}

func vectorSearchInteger(value interface{}, label string) (int, error) {
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int8:
		return int(typed), nil
	case int16:
		return int(typed), nil
	case int32:
		return int(typed), nil
	case int64:
		return int(typed), nil
	case uint:
		if uint64(typed) <= uint64(^uint(0)>>1) {
			return int(typed), nil
		}
	case uint64:
		if typed <= uint64(^uint(0)>>1) {
			return int(typed), nil
		}
	case []byte:
		n, err := strconv.Atoi(string(typed))
		if err == nil {
			return n, nil
		}
	case string:
		n, err := strconv.Atoi(typed)
		if err == nil {
			return n, nil
		}
	}
	return 0, fmt.Errorf("VECTOR_SEARCH %s must be an integer", label)
}

func emptyVectorSearchSource() *derivedJoinSource {
	return emptyDeclaredTableFunctionSource("vector_search", vectorSearchResultColumns())
}

func vectorSearchFunction(table *sqlparser.AliasedTableExpr) (*sqlparser.FuncExpr, string, bool) {
	if table == nil {
		return nil, "", false
	}
	tableFunction, ok := table.Expr.(*sqlparser.TableFunction)
	if !ok || tableFunction.Function == nil || !strings.EqualFold(tableFunction.Function.Name.String(), "VECTOR_SEARCH") {
		return nil, "", false
	}
	alias := strings.TrimSpace(table.As.String())
	if alias == "" {
		alias = "vector_search"
	}
	return tableFunction.Function, alias, true
}

func vectorSearchFunctionInExpr(expression sqlparser.TableExpr) (*sqlparser.AliasedTableExpr, bool) {
	table, ok := expression.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, false
	}
	_, _, ok = vectorSearchFunction(table)
	return table, ok
}

func vectorSearchExecutionContext(ctx *ExecutionContext) *ExecutionContext {
	if ctx != nil {
		return ctx
	}
	return &ExecutionContext{Context: context.Background()}
}
