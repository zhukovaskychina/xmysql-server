package engine

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

const (
	maxBM25QueryTerms     = 4096
	maxBM25TermStatsBytes = 1 << 20
)

// evaluateBM25TermScore computes one-token Okapi BM25 using corpus statistics
// supplied by an ordinary SQL aggregate over the candidate corpus.
func evaluateBM25TermScore(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) < 5 || len(function.Exprs) > 7 {
		return nil, fmt.Errorf("BM25_TERM_SCORE requires text, one term, document count, document frequency, average document length, and optional k1/b")
	}
	arguments := make([]interface{}, len(function.Exprs))
	for index, expression := range function.Exprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("BM25_TERM_SCORE arguments must be scalar expressions")
		}
		value, err := evaluateExpressionWithRow(aliased.Expr, values)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		arguments[index] = value
	}
	text, err := fullTextStringArgument(arguments[0], "document", values)
	if err != nil {
		return nil, err
	}
	termText, err := fullTextStringArgument(arguments[1], "term", values)
	if err != nil {
		return nil, err
	}
	if !utf8.ValidString(text) || !utf8.ValidString(termText) {
		return nil, fmt.Errorf("BM25_TERM_SCORE text and term must be valid UTF-8")
	}
	term, ok := singleFullTextToken(termText)
	if !ok {
		return nil, fmt.Errorf("BM25_TERM_SCORE term must tokenize to exactly one non-stopword token")
	}
	documentCount, err := bm25CountArgument(arguments[2], "document_count", "BM25_TERM_SCORE")
	if err != nil || documentCount == 0 {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("BM25_TERM_SCORE document_count must be greater than zero")
	}
	documentFrequency, err := bm25CountArgument(arguments[3], "document_frequency", "BM25_TERM_SCORE")
	if err != nil {
		return nil, err
	}
	if documentFrequency > documentCount {
		return nil, fmt.Errorf("BM25_TERM_SCORE document_frequency cannot exceed document_count")
	}
	averageLength, err := finiteFloatArgument(arguments[4], "average_document_length", "BM25_TERM_SCORE")
	if err != nil || averageLength <= 0 {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("BM25_TERM_SCORE average_document_length must be greater than zero")
	}
	k1, b := 1.2, 0.75
	if len(arguments) >= 6 {
		k1, err = finiteFloatArgument(arguments[5], "k1", "BM25_TERM_SCORE")
		if err != nil || k1 <= 0 {
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("BM25_TERM_SCORE k1 must be greater than zero")
		}
	}
	if len(arguments) == 7 {
		b, err = finiteFloatArgument(arguments[6], "b", "BM25_TERM_SCORE")
		if err != nil || b < 0 || b > 1 {
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("BM25_TERM_SCORE b must be between zero and one")
		}
	}

	tokens := tokenizeFullText(text, defaultFullTextStopwords)
	termFrequency := uint64(0)
	for _, token := range tokens {
		if token == term {
			termFrequency++
		}
	}
	if termFrequency == 0 {
		return float64(0), nil
	}
	if documentFrequency == 0 {
		return nil, fmt.Errorf("BM25_TERM_SCORE document_frequency is zero for a document containing the term")
	}
	score, err := bm25TermContribution(termFrequency, documentCount, documentFrequency, uint64(len(tokens)), averageLength, k1, b)
	if err != nil {
		return nil, fmt.Errorf("BM25_TERM_SCORE: %w", err)
	}
	return score, nil
}

// evaluateBM25Score computes a multi-term Okapi BM25 score. document_frequencies
// is a JSON object mapping normalized tokens to their corpus document frequency.
func evaluateBM25Score(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) < 5 || len(function.Exprs) > 7 {
		return nil, fmt.Errorf("BM25_SCORE requires document, query, document_count, term_document_frequencies_json, average_document_length, and optional k1/b")
	}
	arguments := make([]interface{}, len(function.Exprs))
	for index, expression := range function.Exprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("BM25_SCORE arguments must be scalar expressions")
		}
		value, err := evaluateExpressionWithRow(aliased.Expr, values)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		arguments[index] = value
	}
	document, err := fullTextStringArgument(arguments[0], "document", values)
	if err != nil {
		return nil, err
	}
	query, err := fullTextStringArgument(arguments[1], "query", values)
	if err != nil {
		return nil, err
	}
	if !utf8.ValidString(document) || !utf8.ValidString(query) {
		return nil, fmt.Errorf("BM25_SCORE document and query must be valid UTF-8")
	}
	if uint64(len(document)) > content.MaxInlineTextBytes || uint64(len(query)) > content.MaxInlineTextBytes {
		return nil, fmt.Errorf("BM25_SCORE document and query are limited to %d bytes each", content.MaxInlineTextBytes)
	}
	documentCount, err := bm25CountArgument(arguments[2], "document_count", "BM25_SCORE")
	if err != nil || documentCount == 0 {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("BM25_SCORE document_count must be greater than zero")
	}
	frequenciesJSON, err := fullTextStringArgument(arguments[3], "term_document_frequencies_json", values)
	if err != nil {
		return nil, err
	}
	if len(frequenciesJSON) > maxBM25TermStatsBytes {
		return nil, fmt.Errorf("BM25_SCORE term statistics exceed %d bytes", maxBM25TermStatsBytes)
	}
	frequencies, err := parseBM25TermDocumentFrequencies(frequenciesJSON, documentCount)
	if err != nil {
		return nil, err
	}
	averageLength, err := finiteFloatArgument(arguments[4], "average_document_length", "BM25_SCORE")
	if err != nil || averageLength <= 0 {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("BM25_SCORE average_document_length must be greater than zero")
	}
	k1, b := 1.2, 0.75
	if len(arguments) >= 6 {
		k1, err = finiteFloatArgument(arguments[5], "k1", "BM25_SCORE")
		if err != nil || k1 <= 0 {
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("BM25_SCORE k1 must be greater than zero")
		}
	}
	if len(arguments) == 7 {
		b, err = finiteFloatArgument(arguments[6], "b", "BM25_SCORE")
		if err != nil || b < 0 || b > 1 {
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("BM25_SCORE b must be between zero and one")
		}
	}
	queryTokens := tokenizeFullText(query, defaultFullTextStopwords)
	if len(queryTokens) > maxBM25QueryTerms {
		return nil, fmt.Errorf("BM25_SCORE query exceeds %d normalized terms", maxBM25QueryTerms)
	}
	if len(queryTokens) == 0 {
		return float64(0), nil
	}
	queryTerms := make([]string, 0, len(queryTokens))
	queryTermSet := make(map[string]struct{}, len(queryTokens))
	for _, term := range queryTokens {
		if _, seen := queryTermSet[term]; seen {
			continue
		}
		queryTermSet[term] = struct{}{}
		queryTerms = append(queryTerms, term)
	}
	for _, term := range queryTerms {
		if _, exists := frequencies[term]; !exists {
			return nil, fmt.Errorf("BM25_SCORE is missing document frequency for query term %q", term)
		}
	}
	frequencyTerms := make([]string, 0, len(frequencies))
	for term := range frequencies {
		frequencyTerms = append(frequencyTerms, term)
	}
	sort.Strings(frequencyTerms)
	for _, term := range frequencyTerms {
		if _, exists := queryTermSet[term]; !exists {
			return nil, fmt.Errorf("BM25_SCORE contains statistics for non-query term %q", term)
		}
	}
	documentTokens := tokenizeFullText(document, defaultFullTextStopwords)
	if len(documentTokens) > maxFullTextTokenRows {
		return nil, fmt.Errorf("BM25_SCORE document exceeds %d normalized terms", maxFullTextTokenRows)
	}
	termFrequencies := make(map[string]uint64, len(queryTerms))
	for _, term := range documentTokens {
		if _, needed := queryTermSet[term]; needed {
			termFrequencies[term]++
		}
	}
	score := float64(0)
	for _, term := range queryTerms {
		frequency := termFrequencies[term]
		if frequency == 0 {
			continue
		}
		documentFrequency := frequencies[term]
		termScore, scoreErr := bm25TermContribution(frequency, documentCount, documentFrequency, uint64(len(documentTokens)), averageLength, k1, b)
		if scoreErr != nil {
			return nil, fmt.Errorf("BM25_SCORE term %q: %w", term, scoreErr)
		}
		score += termScore
	}
	if math.IsNaN(score) || math.IsInf(score, 0) || score < 0 {
		return nil, fmt.Errorf("BM25_SCORE produced an invalid score")
	}
	return score, nil
}

func parseBM25TermDocumentFrequencies(encoded string, documentCount uint64) (map[string]uint64, error) {
	decoder := json.NewDecoder(strings.NewReader(encoded))
	first, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("BM25_SCORE term statistics must be a JSON object: %w", err)
	}
	delimiter, ok := first.(json.Delim)
	if !ok || delimiter != '{' {
		return nil, fmt.Errorf("BM25_SCORE term statistics must be a JSON object")
	}
	frequencies := make(map[string]uint64)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("BM25_SCORE term statistics contain an invalid key: %w", err)
		}
		term, ok := keyToken.(string)
		if !ok || term == "" {
			return nil, fmt.Errorf("BM25_SCORE term statistics keys must be non-empty normalized tokens")
		}
		if _, exists := frequencies[term]; exists {
			return nil, fmt.Errorf("BM25_SCORE term statistics contain duplicate term %q", term)
		}
		if normalized, single := singleFullTextToken(term); !single || normalized != term {
			return nil, fmt.Errorf("BM25_SCORE term statistic key %q is not a normalized token", term)
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, fmt.Errorf("BM25_SCORE document frequency for %q is invalid: %w", term, err)
		}
		frequency, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
		if err != nil || frequency > documentCount {
			return nil, fmt.Errorf("BM25_SCORE document frequency for %q must be an integer in 0..document_count", term)
		}
		frequencies[term] = frequency
		if len(frequencies) > maxBM25QueryTerms {
			return nil, fmt.Errorf("BM25_SCORE term statistics exceed %d terms", maxBM25QueryTerms)
		}
	}
	last, err := decoder.Token()
	if err != nil || last != json.Delim('}') {
		return nil, fmt.Errorf("BM25_SCORE term statistics object is not closed")
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("BM25_SCORE term statistics contain trailing data")
	}
	return frequencies, nil
}

func bm25TermContribution(termFrequency, documentCount, documentFrequency, documentLength uint64, averageLength, k1, b float64) (float64, error) {
	if documentCount == 0 || documentFrequency > documentCount || averageLength <= 0 || k1 <= 0 || b < 0 || b > 1 {
		return 0, fmt.Errorf("invalid corpus statistics or BM25 parameters")
	}
	if documentFrequency == 0 {
		return 0, fmt.Errorf("document frequency is zero for a document containing the term")
	}
	idf := math.Log1p((float64(documentCount-documentFrequency) + 0.5) / (float64(documentFrequency) + 0.5))
	lengthNorm := 1 - b + b*float64(documentLength)/averageLength
	tf := float64(termFrequency)
	score := idf * tf * (k1 + 1) / (tf + k1*lengthNorm)
	if math.IsNaN(score) || math.IsInf(score, 0) || score < 0 {
		return 0, fmt.Errorf("produced an invalid score")
	}
	return score, nil
}

func evaluateFullTextTokenCount(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) != 1 {
		return nil, fmt.Errorf("FULLTEXT_TOKEN_COUNT requires exactly one text or textual CONTENT argument")
	}
	argument, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("FULLTEXT_TOKEN_COUNT argument must be a scalar expression")
	}
	value, err := evaluateExpressionWithRow(argument.Expr, values)
	if err != nil || value == nil {
		return nil, err
	}
	text, err := fullTextStringArgument(value, "document", values)
	if err != nil {
		return nil, err
	}
	if !utf8.ValidString(text) {
		return nil, fmt.Errorf("FULLTEXT_TOKEN_COUNT text must be valid UTF-8")
	}
	return int64(len(tokenizeFullText(text, defaultFullTextStopwords))), nil
}

func singleFullTextToken(text string) (string, bool) {
	tokens := tokenizeFullText(text, defaultFullTextStopwords)
	if len(tokens) != 1 {
		return "", false
	}
	return tokens[0], true
}

func finiteFloatArgument(value interface{}, name, function string) (float64, error) {
	parsed, err := codecToFloat64(value)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		if err != nil {
			return 0, fmt.Errorf("%s %s must be a finite number: %w", function, name, err)
		}
		return 0, fmt.Errorf("%s %s must be a finite number", function, name)
	}
	return parsed, nil
}

func bm25CountArgument(value interface{}, name, function string) (uint64, error) {
	if parsed, ok := autoIncrementValueAsUint64(value); ok {
		return parsed, nil
	}
	var text string
	switch typed := value.(type) {
	case string:
		text = typed
	case []byte:
		text = string(typed)
	default:
		return 0, fmt.Errorf("%s %s must be a non-negative integer; got %T", function, name, value)
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(text), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s %s must be a non-negative integer: %w", function, name, err)
	}
	return parsed, nil
}
