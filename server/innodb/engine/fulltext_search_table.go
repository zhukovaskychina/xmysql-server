package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

const (
	maxFullTextSearchLimit     = 10000
	maxFullTextSearchQueryByte = 64 << 10
	maxFullTextSearchTerms     = 1024
	maxFullTextPrefixTerms     = 16
	maxFullTextPrefixTokenScan = 1_000_000
	maxFullTextRankVisits      = 1_000_000
)

// TABLE(FULLTEXT_SEARCH(schema, table, index_name, query, limit[, NATURAL|BOOLEAN]))
// returns a bounded lexical candidate list that can be joined to other SQL
// retrieval channels by its canonical single-column primary-key bytes.
func (e *XMySQLExecutor) evaluateFullTextSearchTable(ctx *ExecutionContext, function *sqlparser.FuncExpr) (*derivedJoinSource, error) {
	result := emptyFullTextSearchSource()
	if e == nil || function == nil || len(function.Exprs) < 5 || len(function.Exprs) > 6 {
		return nil, fmt.Errorf("FULLTEXT_SEARCH requires schema, table, index name, query, limit, and an optional NATURAL or BOOLEAN mode")
	}
	arguments := make([]interface{}, len(function.Exprs))
	for index, raw := range function.Exprs {
		argument, ok := raw.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("FULLTEXT_SEARCH arguments must be scalar expressions")
		}
		value, err := evaluateExpressionWithRow(argument.Expr, nil)
		if err != nil {
			return nil, err
		}
		arguments[index] = value
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
	indexName, err := vectorStringArgument(arguments[2], "index name")
	if err != nil {
		return nil, err
	}
	query, err := vectorStringArgument(arguments[3], "query")
	if err != nil || !utf8.ValidString(query) || len(query) > maxFullTextSearchQueryByte {
		return nil, fmt.Errorf("FULLTEXT_SEARCH query must be valid UTF-8 no larger than %d bytes", maxFullTextSearchQueryByte)
	}
	limit, err := vectorSearchInteger(arguments[4], "limit")
	if err != nil || limit < 1 || limit > maxFullTextSearchLimit {
		return nil, fmt.Errorf("FULLTEXT_SEARCH limit must be between 1 and %d", maxFullTextSearchLimit)
	}
	mode := "NATURAL"
	if len(arguments) == 6 {
		mode, err = vectorStringArgument(arguments[5], "mode")
		if err != nil {
			return nil, err
		}
		mode = strings.ToUpper(strings.TrimSpace(mode))
	}
	if mode != "NATURAL" && mode != "BOOLEAN" {
		return nil, fmt.Errorf("FULLTEXT_SEARCH mode must be NATURAL or BOOLEAN")
	}
	if strings.TrimSpace(schema) == "" || !strings.EqualFold(strings.TrimSpace(schema), schema) || strings.TrimSpace(table) == "" || strings.TrimSpace(indexName) == "" {
		return nil, fmt.Errorf("FULLTEXT_SEARCH schema must be canonical and schema, table, and index name must be non-empty")
	}
	if err := e.checkTablePrivilege(ctx, schema, table, "SELECT"); err != nil {
		return nil, fmt.Errorf("FULLTEXT_SEARCH source authorization failed: %w", err)
	}
	if err := ctxContext(ctx).Err(); err != nil {
		return nil, fmt.Errorf("FULLTEXT_SEARCH cancelled before retrieval: %w", err)
	}

	e.fullTextIndexMu.RLock()
	defer e.fullTextIndexMu.RUnlock()
	if ctx != nil && sessionHasQueuedFullTextRefresh(ctx.Session, schema, table) {
		return nil, fmt.Errorf("FULLTEXT_SEARCH index state is stale within the current transaction")
	}
	info, err := readTableMetadataMap(filepathForTable(e, schema, table))
	if err != nil {
		return nil, err
	}
	indexes, _ := info["indexes"].([]interface{})
	primaryKeyColumn, singlePrimaryKey := singlePrimaryKeyColumnFromIndexMetadata(indexes)
	if !singlePrimaryKey {
		return nil, fmt.Errorf("FULLTEXT_SEARCH currently requires a single-column primary key")
	}
	var state fullTextIndexState
	var indexColumns []string
	found := false
	for _, raw := range indexes {
		index, ok := raw.(map[string]interface{})
		if !ok || !strings.EqualFold(strings.TrimSpace(fmt.Sprint(index["type"])), "FULLTEXT") || !strings.EqualFold(strings.TrimSpace(fmt.Sprint(index["name"])), indexName) {
			continue
		}
		columnsRaw, _ := index["columns"].([]interface{})
		for _, columnRaw := range columnsRaw {
			column := strings.Trim(strings.TrimSpace(fmt.Sprint(columnRaw)), "`")
			if column != "" {
				indexColumns = append(indexColumns, column)
			}
		}
		if len(indexColumns) == 0 {
			return nil, fmt.Errorf("FULLTEXT_SEARCH index %s has no indexed columns", indexName)
		}
		encoded, marshalErr := json.Marshal(info["fulltext_segments_"+strings.TrimSpace(fmt.Sprint(index["name"]))])
		if marshalErr != nil || json.Unmarshal(encoded, &state) != nil ||
			!fullTextStatePrimaryKeyCompatible(state, primaryKeyColumn) || !state.Fresh ||
			state.Tokenizer != "unicode-word-lowercase" ||
			!equalFullTextStringSlice(state.Stopwords, sortedStopwords(defaultFullTextStopwords)) || state.Segments == nil {
			return nil, fmt.Errorf("FULLTEXT_SEARCH index %s is stale, unsupported, or not keyed by the current primary key", indexName)
		}
		found = true
		break
	}
	if !found {
		return nil, fmt.Errorf("FULLTEXT_SEARCH index %s does not exist on %s.%s", indexName, schema, table)
	}
	for _, column := range indexColumns {
		if err := e.checkColumnPrivilege(ctx, schema, table, column, "SELECT"); err != nil {
			return nil, fmt.Errorf("FULLTEXT_SEARCH index column authorization failed: %w", err)
		}
	}
	terms := fullTextSearchTerms(query, mode == "BOOLEAN")
	if len(terms) > maxFullTextSearchTerms {
		return nil, fmt.Errorf("FULLTEXT_SEARCH query exceeds %d normalized terms", maxFullTextSearchTerms)
	}
	prefixTerms := 0
	for _, term := range terms {
		if term.prefix {
			prefixTerms++
		}
	}
	if prefixTerms > maxFullTextPrefixTerms {
		return nil, fmt.Errorf("FULLTEXT_SEARCH query exceeds %d prefix terms", maxFullTextPrefixTerms)
	}
	if prefixTerms > 0 && len(state.Segments) > maxFullTextPrefixTokenScan/prefixTerms {
		return nil, fmt.Errorf("FULLTEXT_SEARCH prefix expansion exceeds its token-scan budget")
	}
	candidateIDs, supported := fullTextCandidateIDsFromState(state, query, mode == "BOOLEAN")
	if !supported {
		return nil, fmt.Errorf("FULLTEXT_SEARCH query cannot be represented by the current posting index")
	}
	candidateSet := make(map[string]struct{}, len(candidateIDs))
	for _, candidate := range candidateIDs {
		candidateSet[candidate] = struct{}{}
	}
	matchedCounts, ranked := fullTextMatchedTermCounts(state, terms, candidateSet)
	if !ranked {
		return nil, fmt.Errorf("FULLTEXT_SEARCH ranking exceeded its posting-read budget")
	}
	ranks := make([]fullTextRankedCandidate, 0, len(candidateIDs))
	for index, candidate := range candidateIDs {
		if index&1023 == 0 {
			if err := ctxContext(ctx).Err(); err != nil {
				return nil, fmt.Errorf("FULLTEXT_SEARCH cancelled while ranking candidates: %w", err)
			}
		}
		score := matchedCounts[candidate]
		ranks = append(ranks, fullTextRankedCandidate{primaryKey: candidate, matchedTerms: score})
	}
	sort.Slice(ranks, func(left, right int) bool {
		if ranks[left].matchedTerms != ranks[right].matchedTerms {
			return ranks[left].matchedTerms > ranks[right].matchedTerms
		}
		return ranks[left].primaryKey < ranks[right].primaryKey
	})
	mayHaveMore := len(ranks) > limit
	if mayHaveMore {
		ranks = ranks[:limit]
	}
	for index, ranked := range ranks {
		result.rows = append(result.rows, derivedRow{values: map[string]interface{}{
			"source_schema": schema, "source_table": table, "index_name": indexName,
			"primary_key": []byte(ranked.primaryKey), "matched_terms": int64(ranked.matchedTerms),
			"lexical_rank": int64(index + 1), "may_have_more": mayHaveMore,
		}})
	}
	return result, nil
}

type fullTextRankedCandidate struct {
	primaryKey   string
	matchedTerms int
}

func fullTextSearchTerms(query string, booleanMode bool) []fullTextQueryTerm {
	if booleanMode {
		return parseFullTextBooleanTerms(query)
	}
	terms := make([]fullTextQueryTerm, 0)
	for _, token := range tokenizeFullText(query, defaultFullTextStopwords) {
		terms = append(terms, fullTextQueryTerm{token: token})
	}
	return terms
}

func fullTextMatchedTermCounts(state fullTextIndexState, terms []fullTextQueryTerm, candidates map[string]struct{}) (map[string]int, bool) {
	counts := make(map[string]int)
	seenTerms := make(map[string]struct{}, len(terms))
	visits := 0
	for _, term := range terms {
		if term.prohibited {
			continue
		}
		identity := term.token
		if term.prefix {
			identity += "*"
		}
		if _, exists := seenTerms[identity]; exists {
			continue
		}
		seenTerms[identity] = struct{}{}
		matchedIDs := make(map[string]struct{})
		visitPosting := func(primaryKey string) bool {
			visits++
			if visits > maxFullTextRankVisits {
				return false
			}
			if _, candidate := candidates[primaryKey]; candidate {
				matchedIDs[primaryKey] = struct{}{}
			}
			return true
		}
		if term.prefix {
			for token, ids := range state.Segments {
				if strings.HasPrefix(token, term.token) {
					for _, primaryKey := range ids {
						if primaryKey != "" && !visitPosting(primaryKey) {
							return nil, false
						}
					}
				}
			}
		} else {
			for _, primaryKey := range state.Segments[term.token] {
				if primaryKey != "" && !visitPosting(primaryKey) {
					return nil, false
				}
			}
		}
		for primaryKey := range matchedIDs {
			counts[primaryKey]++
		}
	}
	return counts, true
}

func emptyFullTextSearchSource() *derivedJoinSource {
	return emptyDeclaredTableFunctionSource("fulltext_search", fullTextSearchResultColumns())
}

func fullTextSearchResultColumns() []joinOutputColumn {
	return []joinOutputColumn{
		{name: "source_schema", typ: metadata.TypeVarchar},
		{name: "source_table", typ: metadata.TypeVarchar},
		{name: "index_name", typ: metadata.TypeVarchar},
		{name: "primary_key", typ: metadata.TypeLongBlob},
		{name: "matched_terms", typ: metadata.TypeBigInt},
		{name: "lexical_rank", typ: metadata.TypeBigInt},
		{name: "may_have_more", typ: metadata.TypeTinyInt},
	}
}
