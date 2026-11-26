package engine

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
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
	documentCount, err := bm25CountArgument(arguments[2], "document_count")
	if err != nil || documentCount == 0 {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("BM25_TERM_SCORE document_count must be greater than zero")
	}
	documentFrequency, err := bm25CountArgument(arguments[3], "document_frequency")
	if err != nil {
		return nil, err
	}
	if documentFrequency > documentCount {
		return nil, fmt.Errorf("BM25_TERM_SCORE document_frequency cannot exceed document_count")
	}
	averageLength, err := finiteFloatArgument(arguments[4], "average_document_length")
	if err != nil || averageLength <= 0 {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("BM25_TERM_SCORE average_document_length must be greater than zero")
	}
	k1, b := 1.2, 0.75
	if len(arguments) >= 6 {
		k1, err = finiteFloatArgument(arguments[5], "k1")
		if err != nil || k1 <= 0 {
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("BM25_TERM_SCORE k1 must be greater than zero")
		}
	}
	if len(arguments) == 7 {
		b, err = finiteFloatArgument(arguments[6], "b")
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
	idf := math.Log1p((float64(documentCount-documentFrequency) + 0.5) / (float64(documentFrequency) + 0.5))
	lengthNorm := 1 - b + b*float64(len(tokens))/averageLength
	tf := float64(termFrequency)
	score := idf * tf * (k1 + 1) / (tf + k1*lengthNorm)
	if math.IsNaN(score) || math.IsInf(score, 0) || score < 0 {
		return nil, fmt.Errorf("BM25_TERM_SCORE produced an invalid score")
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

func finiteFloatArgument(value interface{}, name string) (float64, error) {
	parsed, err := codecToFloat64(value)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		if err != nil {
			return 0, fmt.Errorf("BM25_TERM_SCORE %s must be a finite number: %w", name, err)
		}
		return 0, fmt.Errorf("BM25_TERM_SCORE %s must be a finite number", name)
	}
	return parsed, nil
}

func bm25CountArgument(value interface{}, name string) (uint64, error) {
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
		return 0, fmt.Errorf("BM25_TERM_SCORE %s must be a non-negative integer; got %T", name, value)
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(text), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("BM25_TERM_SCORE %s must be a non-negative integer: %w", name, err)
	}
	return parsed, nil
}
