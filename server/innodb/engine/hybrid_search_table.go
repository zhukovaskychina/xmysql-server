package engine

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
)

const hybridSearchRRFK = 60
const maxHybridSearchLimit = 1000

// TABLE(HYBRID_SEARCH(schema, table, vector_index, query_vector, fulltext_index, query, k, EXACT|ANN[, ef]))
// fuses row-level lexical and semantic candidate ranks with reciprocal rank
// fusion. When multiple vector blocks share a row key, the closest block is
// retained as that row's vector representative.
func (e *XMySQLExecutor) evaluateHybridSearchTable(ctx *ExecutionContext, function *sqlparser.FuncExpr) (*derivedJoinSource, error) {
	result := emptyHybridSearchSource()
	if e == nil || function == nil || len(function.Exprs) < 8 || len(function.Exprs) > 9 {
		return nil, fmt.Errorf("HYBRID_SEARCH requires schema, table, vector index, query vector, fulltext index, query text, k, mode, and optional ANN ef")
	}
	modeArgument, ok := function.Exprs[7].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("HYBRID_SEARCH mode must be a scalar expression")
	}
	modeValue, err := evaluateExpressionWithRow(modeArgument.Expr, nil)
	if err != nil {
		return nil, err
	}
	mode, err := vectorStringArgument(modeValue, "mode")
	if err != nil {
		return nil, err
	}
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if mode != "EXACT" && mode != "ANN" {
		return nil, fmt.Errorf("HYBRID_SEARCH mode must be EXACT or ANN")
	}
	if len(function.Exprs) == 9 && mode != "ANN" {
		return nil, fmt.Errorf("HYBRID_SEARCH accepts ef only in ANN mode")
	}
	kArgument, ok := function.Exprs[6].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("HYBRID_SEARCH k must be a scalar expression")
	}
	kValue, err := evaluateExpressionWithRow(kArgument.Expr, nil)
	if err != nil {
		return nil, err
	}
	k, err := vectorSearchInteger(kValue, "k")
	if err != nil || k < 1 || k > maxHybridSearchLimit {
		return nil, fmt.Errorf("HYBRID_SEARCH k must be between 1 and %d", maxHybridSearchLimit)
	}
	vectorCandidateLimit := k * 4
	if vectorCandidateLimit > 4000 {
		vectorCandidateLimit = 4000
	}
	vectorIndexArgument, ok := function.Exprs[2].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("HYBRID_SEARCH vector index must be a scalar expression")
	}
	vectorIndexNameValue, err := evaluateExpressionWithRow(vectorIndexArgument.Expr, nil)
	if err != nil {
		return nil, err
	}
	vectorIndexName, err := vectorStringArgument(vectorIndexNameValue, "vector index name")
	if err != nil {
		return nil, err
	}
	schemaArgument, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("HYBRID_SEARCH schema must be a scalar expression")
	}
	schemaValue, err := evaluateExpressionWithRow(schemaArgument.Expr, nil)
	if err != nil {
		return nil, err
	}
	schema, err := vectorStringArgument(schemaValue, "schema")
	if err != nil {
		return nil, err
	}
	tableArgument, ok := function.Exprs[1].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("HYBRID_SEARCH table must be a scalar expression")
	}
	tableValue, err := evaluateExpressionWithRow(tableArgument.Expr, nil)
	if err != nil {
		return nil, err
	}
	table, err := vectorStringArgument(tableValue, "table")
	if err != nil {
		return nil, err
	}
	if e.vectorIndexCatalog == nil || !e.VectorIndexCatalogUsable() {
		return nil, fmt.Errorf("HYBRID_SEARCH native vector index catalog is unavailable")
	}
	vectorIndexIDValue := vectorIndexID(schema, table, vectorIndexName)
	initialManifest, found, err := e.vectorIndexCatalog.ReadLatest(vectorIndexIDValue)
	if err != nil || !found || initialManifest.Freshness != vector.IndexFreshnessReady {
		if err != nil {
			return nil, fmt.Errorf("HYBRID_SEARCH vector index lookup failed: %w", err)
		}
		return nil, fmt.Errorf("HYBRID_SEARCH vector index must be READY")
	}

	fullTextArguments := sqlparser.SelectExprs{
		function.Exprs[0], function.Exprs[1], function.Exprs[4], function.Exprs[5], function.Exprs[6],
		&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("NATURAL"))},
	}
	fullTextFunction := &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("FULLTEXT_SEARCH"), Exprs: fullTextArguments}
	fullTextSource, err := e.evaluateFullTextSearchTable(ctx, fullTextFunction)
	if err != nil {
		return nil, fmt.Errorf("HYBRID_SEARCH fulltext channel failed: %w", err)
	}
	vectorArguments := sqlparser.SelectExprs{
		function.Exprs[0], function.Exprs[1], function.Exprs[2], function.Exprs[3],
		&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte(fmt.Sprint(vectorCandidateLimit)))}, function.Exprs[7],
	}
	if len(function.Exprs) == 9 {
		vectorArguments = append(vectorArguments, function.Exprs[8])
	}
	vectorFunction := &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("VECTOR_SEARCH"), Exprs: vectorArguments}
	vectorSource, err := e.evaluateVectorSearchTable(vectorSearchExecutionContext(ctx), vectorFunction)
	if err != nil {
		return nil, fmt.Errorf("HYBRID_SEARCH vector channel failed: %w", err)
	}
	latestManifest, latestFound, err := e.vectorIndexCatalog.ReadLatest(vectorIndexIDValue)
	if err != nil || !latestFound || latestManifest.IndexID != initialManifest.IndexID ||
		latestManifest.Generation != initialManifest.Generation || latestManifest.Freshness != vector.IndexFreshnessReady {
		if err != nil {
			return nil, fmt.Errorf("HYBRID_SEARCH vector index revalidation failed: %w", err)
		}
		return nil, fmt.Errorf("HYBRID_SEARCH vector index changed during retrieval; retry the statement")
	}

	vectorRowsByKey := make(map[string]derivedRow, len(vectorSource.rows))
	mayHaveMore := false
	for _, row := range vectorSource.rows {
		primaryKey, ok := hybridPrimaryKeyBytes(row.values["primary_key"])
		if !ok || len(primaryKey) == 0 {
			return nil, fmt.Errorf("HYBRID_SEARCH vector channel returned an invalid primary key")
		}
		if _, validDistance := hybridDistance(row.values["distance"]); !validDistance {
			return nil, fmt.Errorf("HYBRID_SEARCH vector channel returned an invalid distance")
		}
		if row.values["may_have_more"] == true {
			mayHaveMore = true
		}
		key := string(primaryKey)
		if existing, exists := vectorRowsByKey[key]; exists {
			newDistance, newOK := hybridDistance(row.values["distance"])
			oldDistance, oldOK := hybridDistance(existing.values["distance"])
			if newOK && (!oldOK || newDistance < oldDistance) {
				vectorRowsByKey[key] = row
			}
			continue
		}
		vectorRowsByKey[key] = row
	}
	vectorRows := make([]derivedRow, 0, len(vectorRowsByKey))
	for _, row := range vectorRowsByKey {
		vectorRows = append(vectorRows, row)
	}
	sort.Slice(vectorRows, func(left, right int) bool {
		leftDistance, leftOK := hybridDistance(vectorRows[left].values["distance"])
		rightDistance, rightOK := hybridDistance(vectorRows[right].values["distance"])
		if leftOK && rightOK && leftDistance != rightDistance {
			return leftDistance < rightDistance
		}
		leftKey, _ := hybridPrimaryKeyBytes(vectorRows[left].values["primary_key"])
		rightKey, _ := hybridPrimaryKeyBytes(vectorRows[right].values["primary_key"])
		return bytes.Compare(leftKey, rightKey) < 0
	})
	vectorRanks := make(map[string]int, len(vectorRows))
	for index, row := range vectorRows {
		primaryKey, _ := hybridPrimaryKeyBytes(row.values["primary_key"])
		vectorRanks[string(primaryKey)] = index + 1
	}

	type fusedCandidate struct {
		primaryKey  []byte
		vectorRow   derivedRow
		hasVector   bool
		lexicalRank int
		matched     int64
		vectorRank  int
		score       float64
	}
	candidates := make(map[string]*fusedCandidate, len(vectorRows)+len(fullTextSource.rows))
	for key, row := range vectorRowsByKey {
		primaryKey, _ := hybridPrimaryKeyBytes(row.values["primary_key"])
		candidates[key] = &fusedCandidate{
			primaryKey: primaryKey, vectorRow: row, hasVector: true, vectorRank: vectorRanks[key],
		}
	}
	for _, row := range fullTextSource.rows {
		primaryKey, ok := hybridPrimaryKeyBytes(row.values["primary_key"])
		if !ok || len(primaryKey) == 0 {
			return nil, fmt.Errorf("HYBRID_SEARCH fulltext channel returned an invalid primary key")
		}
		if row.values["may_have_more"] == true {
			mayHaveMore = true
		}
		key := string(primaryKey)
		candidate := candidates[key]
		if candidate == nil {
			candidate = &fusedCandidate{primaryKey: primaryKey}
			candidates[key] = candidate
		}
		candidate.lexicalRank, _ = hybridRankInteger(row.values["lexical_rank"])
		matchedTerms, _ := hybridRankInteger(row.values["matched_terms"])
		candidate.matched = int64(matchedTerms)
	}

	ordered := make([]*fusedCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.vectorRank > 0 {
			candidate.score += 1 / (float64(hybridSearchRRFK) + float64(candidate.vectorRank))
		}
		if candidate.lexicalRank > 0 {
			candidate.score += 1 / (float64(hybridSearchRRFK) + float64(candidate.lexicalRank))
		}
		ordered = append(ordered, candidate)
	}
	sort.Slice(ordered, func(left, right int) bool {
		if ordered[left].score != ordered[right].score {
			return ordered[left].score > ordered[right].score
		}
		return bytes.Compare(ordered[left].primaryKey, ordered[right].primaryKey) < 0
	})
	if len(ordered) > k {
		ordered = ordered[:k]
	}
	for _, candidate := range ordered {
		row := map[string]interface{}{
			"primary_key": append([]byte(nil), candidate.primaryKey...),
			"vector_rank": nil, "lexical_rank": nil, "matched_terms": nil,
			"fusion_score": candidate.score, "retrieval_channels": "FULLTEXT",
			"may_have_more": mayHaveMore,
		}
		if candidate.lexicalRank > 0 {
			row["lexical_rank"] = int64(candidate.lexicalRank)
			row["matched_terms"] = candidate.matched
		}
		if candidate.hasVector {
			for _, name := range []string{
				"source_schema", "source_table", "source_column", "source_content", "source_revision", "source_sha256",
				"source_block_id", "source_modality", "distance", "entry_id", "source_block", "model_id", "model_version",
				"space_id", "index_generation", "approximate",
			} {
				row[name] = candidate.vectorRow.values[name]
			}
			if rowContext, ok := candidate.vectorRow.values[contentSourceRowContextKey].(contentSourceRowContext); ok {
				row[contentSourceRowContextKey] = rowContext
			}
			row["vector_rank"] = int64(candidate.vectorRank)
			if candidate.lexicalRank > 0 {
				row["retrieval_channels"] = "VECTOR+FULLTEXT"
			} else {
				row["retrieval_channels"] = "VECTOR"
			}
		} else {
			row["source_schema"] = fullTextSource.rows[0].values["source_schema"]
			row["source_table"] = fullTextSource.rows[0].values["source_table"]
		}
		result.rows = append(result.rows, derivedRow{values: row})
	}
	return result, nil
}

func hybridPrimaryKeyBytes(value interface{}) ([]byte, bool) {
	switch typed := value.(type) {
	case []byte:
		return append([]byte(nil), typed...), len(typed) > 0
	case string:
		return []byte(typed), typed != ""
	default:
		return nil, false
	}
}

func hybridDistance(value interface{}) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, !math.IsNaN(typed) && !math.IsInf(typed, 0)
	case float32:
		return float64(typed), !math.IsNaN(float64(typed)) && !math.IsInf(float64(typed), 0)
	default:
		return 0, false
	}
}

func hybridRankInteger(value interface{}) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, typed > 0
	case int64:
		if typed > 0 && typed <= int64(^uint(0)>>1) {
			return int(typed), true
		}
	case uint64:
		if typed > 0 && typed <= uint64(^uint(0)>>1) {
			return int(typed), true
		}
	}
	return 0, false
}

func emptyHybridSearchSource() *derivedJoinSource {
	return emptyDeclaredTableFunctionSource("hybrid_search", hybridSearchResultColumns())
}

func hybridSearchResultColumns() []joinOutputColumn {
	return []joinOutputColumn{
		{name: "source_schema", typ: metadata.TypeVarchar},
		{name: "source_table", typ: metadata.TypeVarchar},
		{name: "primary_key", typ: metadata.TypeLongBlob},
		{name: "vector_rank", typ: metadata.TypeBigInt},
		{name: "lexical_rank", typ: metadata.TypeBigInt},
		{name: "matched_terms", typ: metadata.TypeBigInt},
		{name: "fusion_score", typ: metadata.TypeDouble},
		{name: "retrieval_channels", typ: metadata.TypeVarchar},
		{name: "entry_id", typ: metadata.TypeVarchar},
		{name: "distance", typ: metadata.TypeDouble},
		{name: "source_column", typ: metadata.TypeVarchar},
		{name: "source_revision", typ: metadata.TypeBigInt},
		{name: "source_sha256", typ: metadata.TypeVarchar},
		{name: "source_block_id", typ: metadata.TypeVarchar},
		{name: "source_modality", typ: metadata.TypeVarchar},
		{name: "source_content", typ: metadata.TypeContent},
		{name: "source_block", typ: metadata.TypeContent},
		{name: "model_id", typ: metadata.TypeVarchar},
		{name: "model_version", typ: metadata.TypeVarchar},
		{name: "space_id", typ: metadata.TypeVarchar},
		{name: "index_generation", typ: metadata.TypeBigInt},
		{name: "approximate", typ: metadata.TypeTinyInt},
		{name: "may_have_more", typ: metadata.TypeTinyInt},
	}
}
