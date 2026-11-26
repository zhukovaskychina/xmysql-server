package engine

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestFullTextTokensShareTokenizerAndStableFilteredPositions(t *testing.T) {
	parsed, err := sqlparser.Parse("select token, position from table(FULLTEXT_TOKENS('The alpha, beta alpha!'))")
	require.NoError(t, err)
	statement, ok := parsed.(*sqlparser.Select)
	require.True(t, ok)
	table, ok := statement.From[0].(*sqlparser.AliasedTableExpr)
	require.True(t, ok)
	tableFunction := table.Expr.(*sqlparser.TableFunction)
	source, err := evaluateFullTextTokensTable(tableFunction.Function, nil)
	require.NoError(t, err)

	require.Len(t, source.rows, 3)
	require.Equal(t, "alpha", source.rows[0].values["token"])
	require.EqualValues(t, 0, source.rows[0].values["position"])
	require.Equal(t, "beta", source.rows[1].values["token"])
	require.EqualValues(t, 1, source.rows[1].values["position"])
	require.Equal(t, "alpha", source.rows[2].values["token"])
	require.EqualValues(t, 2, source.rows[2].values["position"])
	require.EqualValues(t, int64(len(tokenizeFullText("The alpha, beta alpha!", defaultFullTextStopwords))), source.rows[2].values["position"].(int64)+1)
}

func TestFullTextRetrievalFunctionsExecuteThroughSQL(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	tokens := mustQuerySQL(t, executor, "app", "select token, position from table(fulltext_tokens('The alpha, beta alpha!'))")
	require.Equal(t, [][]interface{}{{"alpha", "0"}, {"beta", "1"}, {"alpha", "2"}}, tokens)

	scores := mustQuerySQL(t, executor, "app", "select BM25_TERM_SCORE('alpha beta', 'alpha', 4, 2, 3)")
	require.Len(t, scores, 1)
	require.Len(t, scores[0], 1)
	score, err := strconv.ParseFloat(scores[0][0].(string), 64)
	require.NoError(t, err)
	expected := math.Log(2) * 2.2 / (1 + 1.2*(1-0.75+0.75*2.0/3.0))
	require.InDelta(t, expected, score, 1e-6)

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table bm25_docs (id int primary key, body text)")
	mustExecSQL(t, executor, "app", "insert into bm25_docs values (1, 'alpha alpha beta'), (2, 'alpha gamma'), (3, 'the beta')")
	frequencies := mustQuerySQL(t, executor, "app", "select terms.token, count(distinct docs.id) as document_frequency from bm25_docs docs cross join table(fulltext_tokens(docs.body)) as terms group by terms.token order by terms.token")
	require.Equal(t, [][]interface{}{{"alpha", "2"}, {"beta", "2"}, {"gamma", "1"}}, frequencies)
}

func TestBM25TermScoreMatchesOkapiFormulaAndLengthNormalization(t *testing.T) {
	short := evaluateBM25ForTest(t, "alpha beta", "alpha", 4, 2, 3)
	long := evaluateBM25ForTest(t, "alpha beta gamma delta", "alpha", 4, 2, 3)
	idf := math.Log1p((4.0 - 2.0 + 0.5) / (2.0 + 0.5))
	expectedShort := idf * (1.2 + 1) / (1 + 1.2*(1-0.75+0.75*2.0/3.0))
	expectedLong := idf * (1.2 + 1) / (1 + 1.2*(1-0.75+0.75*4.0/3.0))
	require.InDelta(t, expectedShort, short, 1e-12)
	require.InDelta(t, expectedLong, long, 1e-12)
	require.Greater(t, short, long, "length normalization should lower the otherwise identical term score")

	zero := evaluateBM25ForTest(t, "beta gamma", "alpha", 4, 0, 2)
	require.Zero(t, zero, "a term absent from the document has zero score even at zero corpus frequency")
}

func TestBM25TermScoreRejectsInconsistentCorpusStatistics(t *testing.T) {
	parsed, err := sqlparser.Parse("select BM25_TERM_SCORE('alpha', 'alpha', 4, 5, 2)")
	require.NoError(t, err)
	statement := parsed.(*sqlparser.Select)
	function := statement.SelectExprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.FuncExpr)
	_, err = evaluateBM25TermScore(function, nil)
	require.ErrorContains(t, err, "document_frequency cannot exceed document_count")
}

func evaluateBM25ForTest(t *testing.T, text, term string, documentCount, documentFrequency, averageLength int64) float64 {
	t.Helper()
	query := "select BM25_TERM_SCORE('" + text + "', '" + term + "', " + integerLiteral(documentCount) + ", " + integerLiteral(documentFrequency) + ", " + integerLiteral(averageLength) + ")"
	parsed, err := sqlparser.Parse(query)
	require.NoError(t, err)
	statement := parsed.(*sqlparser.Select)
	function := statement.SelectExprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.FuncExpr)
	value, err := evaluateBM25TermScore(function, nil)
	require.NoError(t, err)
	return value.(float64)
}

func integerLiteral(value int64) string {
	return strconv.FormatInt(value, 10)
}
