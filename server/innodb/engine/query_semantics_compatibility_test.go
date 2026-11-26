package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// These expectations follow MySQL's documented SQL semantics rather than
// deriving the answer from the implementation's operators or rewrite paths.
func TestMySQLQueryCompatibilitySemanticMatrix(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database query_compat")
	mustExecSQL(t, executor, "query_compat", "create table lhs (id int primary key, k int, label varchar(20))")
	mustExecSQL(t, executor, "query_compat", "create table rhs (id int primary key, k int, label varchar(20))")
	mustExecSQL(t, executor, "query_compat", "create table empty_rows (id int primary key, k int)")
	mustExecSQL(t, executor, "query_compat", "insert into lhs values (1, 10, 'a'), (2, 20, 'b'), (3, NULL, 'c')")
	mustExecSQL(t, executor, "query_compat", "insert into rhs values (1, 10, 'x'), (4, NULL, 'y')")
	tests := []struct {
		name string
		sql  string
		rows [][]interface{}
	}{
		{"null_comparison", "select k = NULL, k <=> NULL from lhs where id = 3", [][]interface{}{{nil, "1"}}},
		{"null_not_in_empty_subquery", "select id from lhs where k not in (select k from empty_rows) order by id", [][]interface{}{{"1"}, {"2"}, {"3"}}},
		{"null_in_empty_subquery", "select id from lhs where k in (select k from empty_rows) order by id", [][]interface{}{}},
		{"not_in_null_bearing_subquery", "select id from lhs where k not in (select k from rhs) order by id", [][]interface{}{}},
		{"in_null_bearing_subquery", "select id from lhs where k in (select k from rhs) order by id", [][]interface{}{{"1"}}},
		{"all_empty_subquery", "select id from lhs where k > all (select k from empty_rows) order by id", [][]interface{}{{"1"}, {"2"}, {"3"}}},
		{"any_empty_subquery", "select id from lhs where k > any (select k from empty_rows) order by id", [][]interface{}{}},
		{"empty_aggregates", "select count(*), count(k), sum(k), avg(k), min(k), max(k) from empty_rows", [][]interface{}{{"0", "0", nil, nil, nil, nil}}},
		{"scalar_empty_subquery", "select id, (select k from empty_rows) as missing from lhs order by id", [][]interface{}{{"1", nil}, {"2", nil}, {"3", nil}}},
		{"correlated_not_in_empty", "select l.id from lhs l where l.k not in (select r.k from rhs r where r.id = l.id + 10) order by l.id", [][]interface{}{{"1"}, {"2"}, {"3"}}},
		{"correlated_in_projection", "select l.id, l.k in (select r.k from rhs r where r.id = l.id) as in_match from lhs l order by l.id", [][]interface{}{{"1", "1"}, {"2", "0"}, {"3", "0"}}},
		{"correlated_exists_projection", "select l.id, exists (select 1 from rhs r where r.id = l.id) as exists_match from lhs l order by l.id", [][]interface{}{{"1", "1"}, {"2", "0"}, {"3", "0"}}},
		{"correlated_exists_having", "select l.k, count(*) as total from lhs l group by l.k having exists (select 1 from rhs r where r.k = l.k) order by l.k", [][]interface{}{{"10", "1"}}},
		{"left_join_on_preserves_unmatched", "select l.id, r.id from lhs l left join rhs r on r.k = l.k and r.id = 4 order by l.id", [][]interface{}{{"1", nil}, {"2", nil}, {"3", nil}}},
		{"left_join_where_removes_unmatched", "select l.id from lhs l left join rhs r on r.k = l.k where r.id = 1 order by l.id", [][]interface{}{{"1"}}},
		{"left_join_using_coalesced_key", "select k, lhs.id, rhs.id from lhs left join rhs using (k) order by lhs.id", [][]interface{}{{"10", "1", "1"}, {"20", "2", nil}, {nil, "3", nil}}},
		{"right_join_using_coalesced_key", "select k, lhs.id, rhs.id from lhs right join rhs using (k) order by rhs.id", [][]interface{}{{"10", "1", "1"}, {nil, nil, "4"}}},
		{"order_alias_ordinal", "select id, -id as reverse_id from lhs order by 2, id limit 2", [][]interface{}{{"3", "-3"}, {"2", "-2"}}},
		{"nulls_first_ascending", "select id from lhs order by k asc, id", [][]interface{}{{"3"}, {"1"}, {"2"}}},
		{"nulls_last_descending", "select id from lhs order by k desc, id", [][]interface{}{{"2"}, {"1"}, {"3"}}},
		{"group_null_and_having_alias", "select k, count(*) as total from lhs group by k having total = 1 order by k", [][]interface{}{{nil, "1"}, {"10", "1"}, {"20", "1"}}},
		{"union_all_preserves_duplicates", "select k from lhs where id = 1 union all select k from rhs where id = 1", [][]interface{}{{"10"}, {"10"}}},
		{"union_distinct_coalesces_null", "select k from lhs where id = 3 union select k from rhs where id = 4", [][]interface{}{{nil}}},
		{"derived_aggregate_filter", "select d.k from (select k, count(*) as n from lhs group by k) d where d.n = 1 order by d.k", [][]interface{}{{nil}, {"10"}, {"20"}}},
		{"projected_in_unknown", "select k in (select k from rhs), k not in (select k from rhs) from lhs order by id", [][]interface{}{{"1", "0"}, {nil, nil}, {nil, nil}}},
		{"multiple_in_subqueries", "select id from lhs where id in (select id from lhs) and k in (select k from rhs) order by id", [][]interface{}{{"1"}}},
		{"in_subquery_aggregate", "select id from lhs where k in (select max(k) from rhs) order by id", [][]interface{}{{"1"}}},
		{"not_in_unknown_under_not", "select id from lhs where not (k not in (select k from rhs)) order by id", [][]interface{}{{"1"}}},
		{"literal_subquery_text", "select 'k in (select k from missing)' as text from lhs where id = 1", [][]interface{}{{"k in (select k from missing)"}}},
		{"arithmetic_in_empty", "select id from lhs where k + 1 in (select k from empty_rows) order by id", [][]interface{}{}},
		{"arithmetic_not_in_empty", "select k + 1 not in (select k from empty_rows) from lhs order by id", [][]interface{}{{"1"}, {"1"}, {"1"}}},
		{"quoted_in_operand", "select l.id from lhs l where `l`.`k` in (select k from rhs) order by l.id", [][]interface{}{{"1"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := mustSelectResultSQL(t, executor, "query_compat", test.sql)
			rows := make([][]interface{}, 0, len(result.Records))
			for _, record := range result.Records {
				row := make([]interface{}, len(record.GetValues()))
				for index, value := range record.GetValues() {
					if value == nil || value.IsNull() {
						continue
					}
					// Raw is the internal storage encoding. Decode typed integers
					// as the dispatcher does before comparing SQL result values.
					switch value.Type() {
					case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeInt, basic.ValueTypeBigInt:
						row[index] = fmt.Sprint(value.Int())
					default:
						row[index] = value.String()
					}
				}
				rows = append(rows, row)
			}
			require.Equal(t, test.rows, rows, test.sql)
		})
	}
}

func TestMySQLExplicitCollationConflictIsRejectedInProjection(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("select lhs collate utf8mb4_bin = rhs collate utf8mb4_general_ci")
	require.NoError(t, err)
	selectStmt, ok := parsed.AST.(*sqlparser.Select)
	require.True(t, ok)
	require.Len(t, selectStmt.SelectExprs, 1)
	aliasedExpr, ok := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	require.True(t, ok)
	comparison, ok := aliasedExpr.Expr.(*sqlparser.ComparisonExpr)
	require.True(t, ok)
	_, err = evaluateExpressionWithRow(comparison, map[string]interface{}{"lhs": "a", "rhs": "a"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "1267")

	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database explicit_collation_conflict")
	mustExecSQL(t, executor, "explicit_collation_conflict", "create table mixed (id int primary key, ci_text varchar(20) collate utf8mb4_general_ci, sv_text varchar(20) collate utf8mb4_swedish_ci, bin_text varchar(20) collate utf8mb4_bin, latin_text varchar(20) character set latin1 collate latin1_swedish_ci)")
	mustExecSQL(t, executor, "explicit_collation_conflict", "insert into mixed values (1, 'A', 'A', 'a', 'A'), (2, 'x', 'x', 'X', 'x')")
	result := <-executor.ExecuteQuery(nil, "select ci_text collate utf8mb4_bin = sv_text collate utf8mb4_general_ci from mixed", "explicit_collation_conflict")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "1267")

	result = <-executor.ExecuteQuery(nil, "select ci_text = sv_text from mixed", "explicit_collation_conflict")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "1267")
	result = <-executor.ExecuteQuery(nil, "select l.id from mixed l join mixed r on l.ci_text = r.sv_text", "explicit_collation_conflict")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "1267")
	require.Empty(t, mustSelectResultSQL(t, executor, "explicit_collation_conflict", "select id from mixed where ci_text = bin_text").Records)
	require.Empty(t, mustSelectResultSQL(t, executor, "explicit_collation_conflict", "select id from mixed where latin_text = bin_text").Records)
}

func TestINMaterializationRejectsUnsupportedCorrelatedScope(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database scope_compat")
	mustExecSQL(t, executor, "scope_compat", "create table lhs(id int primary key,k int)")
	mustExecSQL(t, executor, "scope_compat", "create table rhs(id int primary key,k int)")
	mustExecSQL(t, executor, "scope_compat", "insert into lhs values(1,10)")
	mustExecSQL(t, executor, "scope_compat", "insert into rhs values(1,20)")
	query := "select l.id from lhs l group by l.id,l.k having l.id in (select r.id from rhs r where r.k=l.k)"
	result := <-executor.ExecuteQuery(nil, query, "scope_compat")
	// This clause has no per-row correlation operator yet. It must report
	// that boundary rather than execute l.k against the inner row's k.
	require.Error(t, result.Err, "unsupported correlation must not produce a fabricated result")
	if result.Err != nil {
		require.Contains(t, strings.ToLower(result.Err.Error()), "correlated")
	}
}

func TestINSubqueryScopeBindsDatabaseQualifier(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	for _, schema := range []string{"scope_a", "scope_b"} {
		mustExecSQL(t, executor, "", "create database "+schema)
		mustExecSQL(t, executor, schema, "create table t(id int primary key,k int)")
	}
	for _, test := range []struct {
		query       string
		independent bool
	}{
		{"select id from scope_b.t where scope_b.t.k=20", true},
		{"select id from scope_b.t where scope_b.t.k=scope_a.t.k", false},
		{"select id from scope_b.t as inner_t where scope_b.t.k=20", false},
	} {
		statement, err := sqlparser.Parse(test.query)
		require.NoError(t, err)
		_, independent := executor.QueryExecutor.independentINColumns(statement.(sqlparser.SelectStatement), "scope_a")
		require.Equal(t, test.independent, independent, test.query)
	}
}

func TestMySQLCollationEqualityAndDistinctUsePadSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database collation_compat")
	mustExecSQL(t, executor, "collation_compat", "create table collation_rows (id int primary key, modern varchar(20) collate utf8mb4_0900_ai_ci, legacy varchar(20) collate utf8mb4_general_ci, binary_text varchar(20) collate utf8mb4_bin, inherited varchar(20)) default charset=utf8mb4 collate=utf8mb4_general_ci")
	mustExecSQL(t, executor, "collation_compat", "insert into collation_rows values (1, 'x', 'x', 'x', 'x'), (2, 'X', 'X', 'X', 'X'), (3, 'x ', 'x ', 'x ', 'x '), (4, 'e', 'e', 'e', 'e'), (5, 'é', 'é', 'é', 'é'), (6, 'b', 'b', 'b', 'b'), (7, 'A', 'A', 'A', 'A')")
	tableMeta, err := (&SelectExecutor{}).loadTableMetaFromFrm(executor.GetDataDir(), "collation_compat", "collation_rows")
	require.NoError(t, err)
	require.Equal(t, "utf8mb4_general_ci", tableMeta.Collation)
	require.Equal(t, []string{"utf8mb4_0900_ai_ci", "utf8mb4_general_ci", "utf8mb4_bin", "utf8mb4_general_ci"}, []string{tableMeta.Columns[1].Collation, tableMeta.Columns[2].Collation, tableMeta.Columns[3].Collation, tableMeta.Columns[4].Collation})
	collationContext := make(map[string]interface{})
	addColumnCollationContext(collationContext, tableMeta)
	require.Equal(t, "utf8mb4_bin", collationContext[columnCollationContextKey("binary_text")])
	binaryMatches := mustSelectResultSQL(t, executor, "collation_compat", "select id from collation_rows where binary_text = 'x' order by id")
	var binaryMatchIDs []int64
	for _, record := range binaryMatches.Records {
		binaryMatchIDs = append(binaryMatchIDs, record.GetValues()[0].Int())
	}
	require.Equal(t, []int64{1, 3}, binaryMatchIDs, "utf8mb4_bin equality must remain case-sensitive while using PAD SPACE")

	tests := []struct {
		name string
		sql  string
		want int64
	}{
		{"no_pad_case_insensitive_equality", "select count(*) from collation_rows where modern = 'x'", 2},
		{"no_pad_accent_insensitive_equality", "select count(*) from collation_rows where modern = 'e'", 2},
		{"pad_space_case_insensitive_equality", "select count(*) from collation_rows where legacy = 'x'", 3},
		{"pad_space_accent_insensitive_equality", "select count(*) from collation_rows where legacy = 'e'", 2},
		{"binary_case_sensitive_equality", "select count(*) from collation_rows where binary_text = 'x'", 2},
		{"binary_accent_sensitive_equality", "select count(*) from collation_rows where binary_text = 'e'", 1},
		{"table_default_collation_equality", "select count(*) from collation_rows where inherited = 'x'", 3},
		{"no_pad_like_keeps_trailing_space_significant", "select count(*) from collation_rows where modern like 'X'", 2},
		{"legacy_ci_like_is_case_insensitive", "select count(*) from collation_rows where legacy like 'x'", 2},
		{"binary_like_is_case_sensitive", "select count(*) from collation_rows where binary_text like 'x'", 1},
		{"no_pad_distinct", "select count(distinct modern) from collation_rows", 5},
		{"pad_space_distinct", "select count(distinct legacy) from collation_rows", 4},
		{"binary_distinct", "select count(distinct binary_text) from collation_rows", 6},
		{"table_default_collation_distinct", "select count(distinct inherited) from collation_rows", 4},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := mustSelectResultSQL(t, executor, "collation_compat", test.sql)
			require.Len(t, result.Records, 1, test.sql)
			require.Equal(t, test.want, result.Records[0].GetValues()[0].Int(), test.sql)
		})
	}
	for _, test := range []struct {
		name string
		sql  string
		want interface{}
	}{
		{"simple_case_uses_operand_collation", "select case 'X' collate utf8mb4_0900_ai_ci when 'x' then 'matched' else 'missed' end", "matched"},
		{"regexp_uses_case_insensitive_collation", "select count(*) from collation_rows where modern regexp '^x$'", int64(2)},
		{"group_concat_order_uses_collation", "select group_concat(legacy order by legacy separator ',') from collation_rows where id in (6,7)", "A,b"},
		{"min_max_use_collation_aware_aggregate", "select min(legacy), max(legacy) from collation_rows where id in (6,7)", []string{"A", "b"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := mustSelectResultSQL(t, executor, "collation_compat", test.sql)
			require.Len(t, result.Records, 1, test.sql)
			if count, ok := test.want.(int64); ok {
				require.Equal(t, count, result.Records[0].GetValues()[0].Int(), test.sql)
				return
			}
			if value, ok := test.want.(string); ok {
				require.Equal(t, value, result.Records[0].GetValues()[0].String(), test.sql)
				return
			}
			wantValues, ok := test.want.([]string)
			require.True(t, ok, "unsupported expected value type %T", test.want)
			require.Len(t, result.Records[0].GetValues(), len(wantValues), test.sql)
			for index, value := range wantValues {
				require.Equal(t, value, result.Records[0].GetValues()[index].String(), test.sql)
			}
		})
	}
}

func TestMySQLSwedishCollationUsesLocaleWeights(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database swedish_collation_compat")
	mustExecSQL(t, executor, "swedish_collation_compat", "create table values_table (id int primary key, value varchar(20) collate utf8mb4_swedish_ci)")
	mustExecSQL(t, executor, "swedish_collation_compat", "insert into values_table values (1, 'ä'), (2, 'z'), (3, 'å'), (4, 'ö'), (5, 'a')")

	ordered := mustSelectResultSQL(t, executor, "swedish_collation_compat", "select value from values_table order by value")
	var actual []string
	for _, record := range ordered.Records {
		actual = append(actual, record.GetValues()[0].String())
	}
	require.Equal(t, []string{"a", "z", "å", "ä", "ö"}, actual)

	distinct := mustSelectResultSQL(t, executor, "swedish_collation_compat", "select count(distinct value) from values_table")
	require.Equal(t, int64(5), distinct.Records[0].GetValues()[0].Int())
}

func TestMySQLGerman2CollationUsesPhonebookExpansions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database german2_collation_compat")
	mustExecSQL(t, executor, "german2_collation_compat", "create table values_table (id int primary key, value varchar(20) collate utf8mb4_german2_ci)")
	mustExecSQL(t, executor, "german2_collation_compat", "insert into values_table values (1,'a'),(2,'ä'),(3,'ae'),(4,'æ'),(5,'o'),(6,'ö'),(7,'oe'),(8,'œ'),(9,'u'),(10,'ü'),(11,'ue'),(12,'ß'),(13,'ss')")

	ordered := mustSelectResultSQL(t, executor, "german2_collation_compat", "select value from values_table order by value,id")
	var actual []string
	for _, record := range ordered.Records {
		actual = append(actual, record.GetValues()[0].String())
	}
	require.Equal(t, []string{"a", "ä", "ae", "æ", "o", "ö", "oe", "œ", "ß", "ss", "u", "ü", "ue"}, actual)

	distinct := mustSelectResultSQL(t, executor, "german2_collation_compat", "select count(distinct value) from values_table")
	require.Equal(t, int64(7), distinct.Records[0].GetValues()[0].Int())

	mustExecSQL(t, executor, "german2_collation_compat", "create table latin_values (id int primary key, value varchar(20) character set latin1 collate latin1_german2_ci)")
	mustExecSQL(t, executor, "german2_collation_compat", "insert into latin_values values (1,'Ä'),(2,'AE'),(3,'Æ'),(4,'Ö'),(5,'OE'),(6,'Œ'),(7,'ß'),(8,'ss')")
	latinDistinct := mustSelectResultSQL(t, executor, "german2_collation_compat", "select count(distinct value) from latin_values")
	require.Equal(t, int64(5), latinDistinct.Records[0].GetValues()[0].Int(), "latin1 German2 does not equate Æ/AE or Œ/OE")
}
