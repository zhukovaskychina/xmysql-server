package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComplexSelectCoreSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(50), score int)")
	mustExecSQL(t, executor, "app", "insert into users (id, name, score) values (1, 'alice', 10), (2, 'alice', 20), (3, 'bob', 30)")

	require.Equal(t, [][]interface{}{{"alice"}, {"bob"}}, mustQuerySQL(t, executor, "app", "select distinct name from users order by name"))
	havingRows := mustQuerySQL(t, executor, "app", "select name, count(*) from users group by name having count(*) > 1")
	require.Len(t, havingRows, 1)
	require.Equal(t, "alice", havingRows[0][0])
	require.NotEmpty(t, havingRows[0][1])
	havingAliasRows := mustQuerySQL(t, executor, "app", "select name, count(*) as total from users group by name having total > 1")
	require.Len(t, havingAliasRows, 1)
	require.Equal(t, "alice", havingAliasRows[0][0])
	require.NotEmpty(t, havingAliasRows[0][1])
	require.Equal(t, [][]interface{}{{"1"}, {"2"}, {"3"}}, mustQuerySQL(t, executor, "app", "select id from users where id in (select id from users) order by id"))
}

func TestCorrelatedScalarProjectionPreservesOuterJoinSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, group_id int, name varchar(50))")
	mustExecSQL(t, executor, "app", "create table groups (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, group_id, name) values (1, 10, 'p1'), (2, 20, 'p2')")
	mustExecSQL(t, executor, "app", "insert into groups (id, label) values (10, 'g10'), (20, 'g20')")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (101, 1, 7), (102, 1, 11), (201, 2, 5)")

	rows := mustQuerySQL(t, executor, "app", "select p.id, g.label, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p join groups g on g.id = p.group_id order by p.id")
	require.Equal(t, [][]interface{}{{"1", "g10", "11"}, {"2", "g20", "5"}}, rows)
}

func TestIntersectAndExceptSetOperations(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select 1 as n intersect select 1"))
	require.Empty(t, mustQuerySQL(t, executor, "app", "select 1 as n intersect select 2"))
	require.Empty(t, mustQuerySQL(t, executor, "app", "select 1 as n except select 1"))
}

func TestMixedSetOperationsHonorIntersectPrecedenceAndOuterOrdering(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	require.Equal(t, [][]interface{}{{"1"}, {"2"}}, mustQuerySQL(t, executor, "app", "select 2 as n union all select 1 intersect select 1 order by n"))
	require.Empty(t, mustQuerySQL(t, executor, "app", "select 1 as n except select 1 intersect select 1"))
	require.Equal(t, [][]interface{}{{"1"}, {"2"}}, mustQuerySQL(t, executor, "app", "(select 2 as n) union (select 1 as n) order by n"))
}

func TestUnionSupportsExplicitDistinctModifier(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select 1 as n union distinct select 1"))
	require.Equal(t, [][]interface{}{{"1"}, {"2"}}, mustQuerySQL(t, executor, "app", "select 1 as n union distinct select 2"))
}

func TestNestedParenthesizedSetBranchesAreMaterialized(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySQL(t, executor, "app", "(select 1 as n union all select 2) intersect select 2"))
}

func TestDeeplyNestedParenthesizedSetBranchesAreMaterialized(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	rows := mustQuerySQL(t, executor, "app", "((select 1 as n union all select 2) intersect (select 2 as n union all select 3)) union all ((select 3 as n except select 4 as n)) order by n")
	require.Equal(t, [][]interface{}{{"2"}, {"3"}}, rows)
}

func TestDerivedTableSupportsMixedSetExpressionAndOuterQueryTail(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (1, 'A'), (2, 'B'), (3, 'C')")

	rows := mustQuerySQL(t, executor, "app", "select d.n from ((select 1 as n union all select 2) intersect select 2) as d where d.n = 2 order by d.n limit 1")
	require.Equal(t, [][]interface{}{{"2"}}, rows)
	starRows := mustQuerySQL(t, executor, "app", "select * from ((select 1 as n union all select 2) except select 2) d order by n")
	require.Equal(t, [][]interface{}{{"1"}}, starRows)
	projectedRows := mustQuerySQL(t, executor, "app", "select d.n, d.n + 1 as next_n from (select 1 as n except select 2) d order by d.n")
	require.Equal(t, [][]interface{}{{"1", "2"}}, projectedRows)
	aggregateRows := mustQuerySQL(t, executor, "app", "select count(*) as total from ((select 1 as n union all select 2) intersect select 2) d")
	require.Equal(t, [][]interface{}{{"1"}}, aggregateRows)
	groupedRows := mustQuerySQL(t, executor, "app", "select d.n, count(*) as total from ((select 1 as n union all select 2) intersect select 2) d group by d.n")
	require.Equal(t, [][]interface{}{{"2", "1"}}, groupedRows)
	distinctRows := mustQuerySQL(t, executor, "app", "select distinct d.n from ((select 1 as n union all select 1) intersect all (select 1 as n union all select 1)) d order by d.n")
	require.Equal(t, [][]interface{}{{"1"}}, distinctRows)
	orderedRows := mustQuerySQL(t, executor, "app", "select d.n, d.label from ((select 1 as n, 'b' as label union all select 1, 'a') intersect all (select 1 as n, 'b' as label union all select 1, 'a')) d order by d.n asc, d.label asc")
	require.Equal(t, [][]interface{}{{"1", "a"}, {"1", "b"}}, orderedRows)
	joinedRows := mustQuerySQL(t, executor, "app", "select d.n, l.label from ((select 1 as n union all select 2) intersect all (select 2 as n union all select 3)) d join labels l on d.n = l.id order by d.n")
	require.Equal(t, [][]interface{}{{"2", "B"}}, joinedRows)
	rightDerivedRows := mustQuerySQL(t, executor, "app", "select l.label, d.n from labels l join ((select 1 as n union all select 2) intersect all (select 2 as n union all select 3)) d on d.n = l.id order by l.id")
	require.Equal(t, [][]interface{}{{"B", "2"}}, rightDerivedRows)
	leftOuterDerivedRows := mustQuerySQL(t, executor, "app", "select l.label, d.n from labels l left join ((select 1 as n union all select 2) intersect all (select 2 as n union all select 3)) d on d.n = l.id order by l.id")
	require.Equal(t, [][]interface{}{{"A", nil}, {"B", "2"}, {"C", nil}}, leftOuterDerivedRows)
	rightOuterDerivedRows := mustQuerySQL(t, executor, "app", "select l.label, d.n from labels l right join ((select 2 as n union all select 3) except select 1) d on d.n = l.id order by d.n")
	require.Equal(t, [][]interface{}{{"B", "2"}, {"C", "3"}}, rightOuterDerivedRows)
	joinedAggregateRows := mustQuerySQL(t, executor, "app", "select l.label, count(*) as total from ((select 1 as n union all select 2) intersect all (select 1 as n union all select 2)) d join labels l on d.n = l.id group by l.label order by l.label")
	require.Equal(t, [][]interface{}{{"A", "1"}, {"B", "1"}}, joinedAggregateRows)
}

func TestNestedDerivedTableSupportsMixedSetExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	rows := mustQuerySQL(t, executor, "app", "select outer_rows.n from (select inner_rows.n from ((select 1 as n union all select 2) intersect select 2) as inner_rows) as outer_rows order by outer_rows.n")
	require.Equal(t, [][]interface{}{{"2"}}, rows)
}

func TestDeeplyNestedDerivedTableSupportsMixedSetExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (1, 'A'), (2, 'B')")

	rows := mustQuerySQL(t, executor, "app", "select top_rows.n from (select middle_rows.n from (select inner_rows.n from ((select 1 as n union all select 2) intersect select 2) as inner_rows) as middle_rows) as top_rows order by top_rows.n")
	require.Equal(t, [][]interface{}{{"2"}}, rows)
	joinedRows := mustQuerySQL(t, executor, "app", "select top_rows.n, l.label from (select inner_rows.n from ((select 1 as n union all select 2) intersect select 2) as inner_rows) as top_rows join labels l on top_rows.n = l.id order by top_rows.n")
	require.Equal(t, [][]interface{}{{"2", "B"}}, joinedRows)
}

func TestDerivedTableSupportsUnionSetExpressionWithOuterQuery(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	rows := mustQuerySQL(t, executor, "app", "select d.n from (select 1 as n union all select 2) as d where d.n = 2 order by d.n limit 1")
	require.Equal(t, [][]interface{}{{"2"}}, rows)
	aggregateRows := mustQuerySQL(t, executor, "app", "select count(*) as total from (select 1 as n union all select 2) as d")
	require.Equal(t, [][]interface{}{{"2"}}, aggregateRows)
}

func TestCTESupportsUnionSetExpressionWithOuterAggregate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	rows := mustQuerySQL(t, executor, "app", "with vals as (select 1 as n union all select 2 as n) select count(*) as total, sum(n) as sum_n from vals")
	require.Equal(t, [][]interface{}{{"2", "3"}}, rows)
	orderedRows := mustQuerySQL(t, executor, "app", "with vals as (select 2 as n union all select 1 as n) select n from vals order by n limit 1")
	require.Equal(t, [][]interface{}{{"1"}}, orderedRows)
	setRows := mustQuerySQL(t, executor, "app", "with vals as ((select 1 as n union all select 2 as n) intersect select 2 as n) select count(*) as total, sum(n) as sum_n from vals")
	require.Equal(t, [][]interface{}{{"1", "2"}}, setRows)
}

func TestCTESupportsNestedDerivedCTESource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, label) values (1, 'one'), (2, 'two'), (3, 'three')")

	query := "with outer_rows as (select id from (with inner_rows as (select id from users where id > 1) select id from inner_rows) derived_rows) select id from outer_rows order by id"
	rows := mustQuerySQL(t, executor, "app", query)
	require.Equal(t, [][]interface{}{{"2"}, {"3"}}, rows)
}

func TestSetOperationsSupportLimitOffsetAndMultiColumnOrdering(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	rows := mustQuerySQL(t, executor, "app", "select 1 as n, 'z' as label union all select 1, 'a' union all select 2, 'b' order by n asc, label asc limit 2")
	require.Equal(t, [][]interface{}{{"1", "a"}, {"1", "z"}}, rows)

	offsetRows := mustQuerySQL(t, executor, "app", "select 1 as n union all select 2 union all select 3 order by n limit 1 offset 1")
	require.Equal(t, [][]interface{}{{"2"}}, offsetRows)

	mixedRows := mustQuerySQL(t, executor, "app", "select 1 as n union all select 2 intersect select 2 order by n limit 1 offset 0")
	require.Equal(t, [][]interface{}{{"1"}}, mixedRows)
}

func TestSetOperationOuterOrderByEvaluatesExpressions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	rows := mustQuerySQL(t, executor, "app", "select 1 as n union all select 2 union all select 3 order by abs(n - 2) asc, n desc")
	require.Equal(t, [][]interface{}{{"2"}, {"3"}, {"1"}}, rows)
}

func TestDerivedTableProjectsFiltersAndOrdersMaterializedRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(50))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice'), (2, 'bob'), (3, 'carol')")

	rows := mustQuerySQL(t, executor, "app", "select d.id, d.name from (select id, name from users where id >= 2) as d where d.id < 3 order by d.id desc")
	require.Equal(t, [][]interface{}{{"2", "bob"}}, rows)
	starRows := mustQuerySQL(t, executor, "app", "select * from (select id, name from users) as d order by id limit 1, 1")
	require.Equal(t, [][]interface{}{{"2", "bob"}}, starRows)
	aggregateRows := mustQuerySQL(t, executor, "app", "select d.id, d.total from (select id, count(*) as total from users group by id) as d order by d.id")
	require.Equal(t, [][]interface{}{{"1", "1"}, {"2", "1"}, {"3", "1"}}, aggregateRows)
	outerAggregateRows := mustQuerySQL(t, executor, "app", "select count(*) as total from (select id from users) as d")
	require.Equal(t, [][]interface{}{{"3"}}, outerAggregateRows)
	groupedOuterRows := mustQuerySQL(t, executor, "app", "select d.name, count(*) as total from (select name from users) as d group by d.name having total = 1 order by d.name")
	require.Equal(t, [][]interface{}{{"alice", "1"}, {"bob", "1"}, {"carol", "1"}}, groupedOuterRows)
	nestedRows := mustQuerySQL(t, executor, "app", "select n.id from (select d.id from (select id from users) as d) as n where n.id = 2")
	require.Equal(t, [][]interface{}{{"2"}}, nestedRows)
}

func TestDerivedTablePreservesMultiColumnOrderBeforeLimit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ordered_users (id int primary key, name varchar(50))")
	mustExecSQL(t, executor, "app", "insert into ordered_users (id, name) values (1, 'alice'), (2, 'alice'), (3, 'bob'), (4, 'carol')")

	rows := mustQuerySQL(t, executor, "app", "select id, name from (select id, name from ordered_users order by name asc, id desc limit 3) as d")
	require.Equal(t, [][]interface{}{{"2", "alice"}, {"1", "alice"}, {"3", "bob"}}, rows)
}

func TestDerivedAggregatePreservesMultiColumnOrderBeforeLimit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table grouped_users (id int primary key, name varchar(50))")
	mustExecSQL(t, executor, "app", "insert into grouped_users (id, name) values (1, 'alice'), (2, 'alice'), (3, 'bob'), (4, 'bob'), (5, 'carol')")

	rows := mustQuerySQL(t, executor, "app", "select name, total from (select name, count(*) as total from grouped_users group by name order by total desc, name desc limit 2) as d")
	require.Equal(t, [][]interface{}{{"bob", "2"}, {"alice", "2"}}, rows)
}

func TestDerivedTableJoinSupportsInnerOuterAndUsingSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(50))")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice'), (2, 'bob'), (3, 'carol')")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (2, 'B'), (3, 'C'), (4, 'D')")
	innerRows := mustQuerySQL(t, executor, "app", "select d.id, l.label from (select id from users where id >= 2) as d join labels l on d.id = l.id order by d.id")
	require.Equal(t, [][]interface{}{{"2", "B"}, {"3", "C"}}, innerRows)
	leftRows := mustQuerySQL(t, executor, "app", "select d.id, l.label from (select id from users) as d left join labels l on d.id = l.id order by d.id")
	require.Equal(t, [][]interface{}{{"1", nil}, {"2", "B"}, {"3", "C"}}, leftRows)
	usingRows := mustQuerySQL(t, executor, "app", "select d.id, l.label from (select id from users) as d join labels l using (id) order by d.id")
	require.Equal(t, [][]interface{}{{"2", "B"}, {"3", "C"}}, usingRows)
}

func TestJoinSupportsRangeAndBooleanOnPredicates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (2, 'B'), (3, 'C'), (4, 'D')")

	rows := mustQuerySQL(t, executor, "app", "select u.id, l.id from users u join labels l on u.id < l.id and l.label <> 'D' order by u.id, l.id")
	require.Equal(t, [][]interface{}{{"1", "2"}, {"1", "3"}, {"2", "3"}}, rows)
}

func TestJoinSupportsScalarFunctionOnPredicate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, score int)")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, score) values (1, -2), (2, 3), (3, -4)")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (2, 'B'), (3, 'C'), (4, 'D')")

	rows := mustQuerySQL(t, executor, "app", "select u.id, l.label from users u join labels l on abs(u.score) = l.id order by u.id")
	require.Equal(t, [][]interface{}{{"1", "B"}, {"2", "C"}, {"3", "D"}}, rows)
}

func TestJoinSupportsRegexpPredicate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(20))")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice'), (2, 'bob'), (3, 'carol')")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (1, 'active'), (2, 'blocked'), (3, 'closed')")

	rows := mustQuerySQL(t, executor, "app", "select u.id, l.label from users u join labels l on u.id = l.id and l.label regexp '^act|^clo' order by u.id")
	require.Equal(t, [][]interface{}{{"1", "active"}, {"3", "closed"}}, rows)
}

func TestJoinSupportsLikeAndInPredicates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (1, 'active'), (2, 'blocked'), (3, 'closed')")

	rows := mustQuerySQL(t, executor, "app", "select u.id, l.label from users u join labels l on u.id in (l.id, 99) and l.label like 'act%' order by u.id")
	require.Equal(t, [][]interface{}{{"1", "active"}}, rows)
}

func TestJoinNotPreservesUnknownPredicate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1), (2)")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (1, 'active'), (2, 'blocked')")

	rows := mustQuerySQL(t, executor, "app", "select u.id, l.id from users u join labels l on not (l.label = null) order by u.id, l.id")
	require.Empty(t, rows)
}

func TestCorrelatedExistsBindsOuterRowPerEvaluation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, active int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3), (4)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, active) values (10, 1, 1), (20, 2, 0)")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where exists (select 1 from children c where c.parent_id = p.id and c.active = 1)")
	require.Equal(t, [][]interface{}{{"1"}}, rows)
}

func TestCorrelatedExistsUsesUnknownForNullEqualityPerOuterRow(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, active int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (null), (1), (2)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, active) values (10, null, 1), (20, 1, 1)")

	existsRows := mustQuerySQL(t, executor, "app", "select p.id from parents p where exists (select 1 from children c where c.parent_id = p.id and c.active = 1)")
	require.Equal(t, [][]interface{}{{"1"}}, existsRows)
	notExistsRows := mustQuerySQL(t, executor, "app", "select p.id from parents p where not exists (select 1 from children c where c.parent_id = p.id and c.active = 1)")
	require.Equal(t, [][]interface{}{{nil}, {"2"}}, notExistsRows)
}

func TestCorrelatedExistsSupportsOuterPredicateAndOrder(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents values (1, 1), (2, 1), (3, 0)")
	mustExecSQL(t, executor, "app", "insert into children values (10, 1), (20, 2)")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where p.active = 1 and exists (select 1 from children c where c.parent_id = p.id) order by p.id desc")
	require.Equal(t, [][]interface{}{{"2"}, {"1"}}, rows)
}

func TestCorrelatedExistsPreservesOuterORSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents values (1, 1), (2, 0), (3, 0)")
	mustExecSQL(t, executor, "app", "insert into children values (10, 1), (20, 2)")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where p.active = 1 or exists (select 1 from children c where c.parent_id = p.id) order by p.id")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}}, rows)
}

func TestCorrelatedExistsPreservesTrailingORSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents values (1, 1), (2, 0), (3, 0)")
	mustExecSQL(t, executor, "app", "insert into children values (10, 1), (20, 2)")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where exists (select 1 from children c where c.parent_id = p.id) or p.active = 1 order by p.id")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}}, rows)
}

func TestCorrelatedExistsSupportsInnerJoinSubquery(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "create table child_labels (child_id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1), (20, 2), (30, 3)")
	mustExecSQL(t, executor, "app", "insert into child_labels (child_id, label) values (10, 'active'), (20, 'inactive')")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where exists (select 1 from children c join child_labels l on c.id = l.child_id where c.parent_id = p.id and l.label = 'active') order by p.id")
	require.Equal(t, [][]interface{}{{"1"}}, rows)
}

func TestCorrelatedExistsSupportsOuterJoinSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table parent_meta (parent_id int primary key, enabled int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "create table child_labels (child_id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into parent_meta (parent_id, enabled) values (1, 1), (2, 0), (3, 1)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1), (20, 2), (30, 3)")
	mustExecSQL(t, executor, "app", "insert into child_labels (child_id, label) values (10, 'active'), (20, 'active'), (30, 'inactive')")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p join parent_meta m on p.id = m.parent_id where m.enabled = 1 and exists (select 1 from children c join child_labels l on c.id = l.child_id where c.parent_id = p.id and l.label = 'active') order by p.id")
	require.Equal(t, [][]interface{}{{"1"}}, rows)
}

func TestCorrelatedPredicateSubqueriesRebindInAnyAndAllPerOuterRow(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (20, 2, 1), (21, 2, 8)")
	inRows := mustQuerySQL(t, executor, "app", "select p.id from parents p where p.id in (select c.parent_id from children c where c.parent_id = p.id) order by p.id")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}}, inRows)
	anyRows := mustQuerySQL(t, executor, "app", "select p.id from parents p where p.id > any (select c.score from children c where c.parent_id = p.id) order by p.id")
	require.Equal(t, [][]interface{}{{"2"}}, anyRows)
	allRows := mustQuerySQL(t, executor, "app", "select p.id from parents p where p.id < all (select c.score from children c where c.parent_id = p.id) order by p.id")
	require.Equal(t, [][]interface{}{{"1"}, {"3"}}, allRows)
}

func TestCorrelatedPredicateSubquerySupportsOuterPredicateAndOrder(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents values (1, 1), (2, 1), (3, 0)")
	mustExecSQL(t, executor, "app", "insert into children values (10, 1), (20, 2)")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where p.active = 1 and p.id in (select c.parent_id from children c where c.parent_id = p.id) order by p.id desc")
	require.Equal(t, [][]interface{}{{"2"}, {"1"}}, rows)
}

func TestCorrelatedPredicateSubqueryPreservesOuterORSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents values (1, 1), (2, 0), (3, 0)")
	mustExecSQL(t, executor, "app", "insert into children values (10, 1), (20, 2)")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where p.active = 1 or p.id in (select c.parent_id from children c where c.parent_id = p.id) order by p.id")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}}, rows)
}

func TestCorrelatedPredicateSubqueryPreservesTrailingORSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents values (1, 1), (2, 0), (3, 0)")
	mustExecSQL(t, executor, "app", "insert into children values (10, 1), (20, 2)")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where p.id in (select c.parent_id from children c where c.parent_id = p.id) or p.active = 1 order by p.id")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}}, rows)
}

func TestMultipleCorrelatedPredicateSubqueriesCombineWithAND(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, active int)")
	mustExecSQL(t, executor, "app", "insert into parents values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children values (10, 1, 1), (20, 2, 0)")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where p.id in (select c.parent_id from children c where c.parent_id = p.id) and p.id not in (select c.parent_id from children c where c.parent_id = p.id and c.active = 0) order by p.id")
	require.Equal(t, [][]interface{}{{"1"}}, rows)
}

func TestMultipleCorrelatedExistsSubqueriesCombineWithAND(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, active int)")
	mustExecSQL(t, executor, "app", "insert into parents values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children values (10, 1, 1), (20, 2, 0), (21, 2, 1)")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where exists (select 1 from children c where c.parent_id = p.id and c.active = 1) and not exists (select 1 from children c where c.parent_id = p.id and c.active = 0) order by p.id")
	require.Equal(t, [][]interface{}{{"1"}}, rows)
}

func TestMultipleCorrelatedExistsSubqueriesCombineWithOR(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, active int)")
	mustExecSQL(t, executor, "app", "insert into parents values (1), (2), (3), (4)")
	mustExecSQL(t, executor, "app", "insert into children values (10, 1, 1), (20, 2, 0), (21, 2, 1), (30, 3, 0)")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where exists (select 1 from children c where c.parent_id = p.id and c.active = 1) or exists (select 1 from children c where c.parent_id = p.id and c.active = 0) order by p.id")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}, {"3"}}, rows)
}

func TestMultipleCorrelatedSubqueriesPreserveMixedOuterORSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, active int)")
	mustExecSQL(t, executor, "app", "insert into parents values (1, 1), (2, 0), (3, 0), (4, 0)")
	mustExecSQL(t, executor, "app", "insert into children values (10, 2, 1), (20, 3, 0)")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where p.active = 1 or exists (select 1 from children c where c.parent_id = p.id and c.active = 1) or exists (select 1 from children c where c.parent_id = p.id and c.active = 0) order by p.id")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}, {"3"}}, rows)
}

func TestCorrelatedPredicateSubquerySupportsOuterJoinSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table parent_meta (parent_id int primary key, enabled int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "create table child_labels (child_id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into parent_meta (parent_id, enabled) values (1, 1), (2, 0), (3, 1)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1), (20, 2), (30, 3)")
	mustExecSQL(t, executor, "app", "insert into child_labels (child_id, label) values (10, 'active'), (20, 'active'), (30, 'inactive')")
	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p join parent_meta m on p.id = m.parent_id where m.enabled = 1 and p.id in (select c.parent_id from children c join child_labels l on c.id = l.child_id where c.parent_id = p.id and l.label = 'active') order by p.id")
	require.Equal(t, [][]interface{}{{"1"}}, rows)
}

func TestCorrelatedExistsSubqueryUpdatesOuterTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, active) values (1, 0), (2, 0), (3, 0)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1), (11, 3)")
	mustExecSQL(t, executor, "app", "update parents set active = 1 where exists (select 1 from children c where c.parent_id = parents.id)")
	require.Equal(t, [][]interface{}{{"1", "1"}, {"2", "0"}, {"3", "1"}}, mustQuerySQL(t, executor, "app", "select id, active from parents order by id"))
}

func TestCorrelatedNotExistsSubqueryDeletesOuterTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1), (11, 3)")
	mustExecSQL(t, executor, "app", "delete from parents where not exists (select 1 from children c where c.parent_id = parents.id)")
	require.Equal(t, [][]interface{}{{"1"}, {"3"}}, mustQuerySQL(t, executor, "app", "select id from parents order by id"))
}

func TestCorrelatedInSubqueryUpdatesOuterTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, active) values (1, 0), (2, 0), (3, 0)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1), (11, 3)")
	mustExecSQL(t, executor, "app", "update parents set active = 1 where id in (select c.parent_id from children c where c.parent_id = parents.id)")
	require.Equal(t, [][]interface{}{{"1", "1"}, {"2", "0"}, {"3", "1"}}, mustQuerySQL(t, executor, "app", "select id, active from parents order by id"))
}

func TestCorrelatedNotInSubqueryDeletesOuterTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1), (11, 3)")
	mustExecSQL(t, executor, "app", "delete from parents where id not in (select c.parent_id from children c where c.parent_id = parents.id)")
	require.Equal(t, [][]interface{}{{"1"}, {"3"}}, mustQuerySQL(t, executor, "app", "select id from parents order by id"))
}

func TestCorrelatedExistsSubqueryUpdatesCompositeKeyTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int, tenant_id int, active int, primary key (id, tenant_id))")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, tenant_id int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, tenant_id, active) values (1, 10, 0), (1, 20, 0), (2, 10, 0)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, tenant_id) values (10, 1, 20), (11, 2, 10)")
	mustExecSQL(t, executor, "app", "update parents set active = 1 where exists (select 1 from children c where c.parent_id = parents.id and c.tenant_id = parents.tenant_id)")
	require.Equal(t, [][]interface{}{{"1", "10", "0"}, {"1", "20", "1"}, {"2", "10", "1"}}, mustQuerySQL(t, executor, "app", "select id, tenant_id, active from parents order by id, tenant_id"))
}

func TestCorrelatedScalarSubqueryUpdatesOuterTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, active) values (1, 0), (2, 0), (3, 0)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1), (11, 1), (12, 3)")
	mustExecSQL(t, executor, "app", "update parents set active = (select count(*) from children c where c.parent_id = parents.id)")
	require.Equal(t, [][]interface{}{{"1", "2"}, {"2", "0"}, {"3", "1"}}, mustQuerySQL(t, executor, "app", "select id, active from parents order by id"))
}

func TestCorrelatedScalarSubqueriesUpdateMultipleColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, child_count int, child_max int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, child_count, child_max) values (1, 0, 0), (2, 0, 0), (3, 0, 0)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (12, 3, 4)")
	mustExecSQL(t, executor, "app", "update parents set child_count = (select count(*) from children c where c.parent_id = parents.id), child_max = (select max(c.score) from children c where c.parent_id = parents.id)")
	require.Equal(t, [][]interface{}{{"1", "2", "9"}, {"2", "0", nil}, {"3", "1", "4"}}, mustQuerySQL(t, executor, "app", "select id, child_count, child_max from parents order by id"))
}

func TestCorrelatedScalarSubqueryUpdateSupportsOuterExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, child_count int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, child_count) values (1, 0), (2, 0), (3, 0)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1), (11, 1), (12, 3)")
	query := "update parents set child_count = coalesce((select count(*) from children c where c.parent_id = parents.id), 0) + 1"
	mustExecSQL(t, executor, "app", query)
	require.Equal(t, [][]interface{}{{"1", "3"}, {"2", "1"}, {"3", "2"}}, mustQuerySQL(t, executor, "app", "select id, child_count from parents order by id"))
}

func TestCorrelatedScalarSubqueriesUpdateMultipleColumnsWithOuterExpressions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, child_count int, child_max int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, child_count, child_max) values (1, 0, 0), (2, 0, 0), (3, 0, 0)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (12, 3, 4)")
	query := "update parents set child_count = coalesce((select count(*) from children c where c.parent_id = parents.id), 0), child_max = coalesce((select max(c.score) from children c where c.parent_id = parents.id), 0)"
	mustExecSQL(t, executor, "app", query)
	require.Equal(t, [][]interface{}{{"1", "2", "9"}, {"2", "0", "0"}, {"3", "1", "4"}}, mustQuerySQL(t, executor, "app", "select id, child_count, child_max from parents order by id"))
}

func TestCorrelatedScalarSubqueryUpdateSupportsMultipleSubqueriesInExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, total int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, total) values (1, 0), (2, 0), (3, 0)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (12, 3, 4)")
	query := "update parents set total = coalesce((select count(*) from children c where c.parent_id = parents.id), 0) + coalesce((select max(c.score) from children c where c.parent_id = parents.id), 0)"
	mustExecSQL(t, executor, "app", query)
	require.Equal(t, [][]interface{}{{"1", "11"}, {"2", "0"}, {"3", "5"}}, mustQuerySQL(t, executor, "app", "select id, total from parents order by id"))
}

func TestCorrelatedScalarSubqueriesUpdateMultipleColumnsWithMultipleSubqueriesPerExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, total int, adjusted int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, total, adjusted) values (1, 0, 0), (2, 0, 0), (3, 0, 0)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (12, 3, 4)")
	query := "update parents set total = coalesce((select count(*) from children c where c.parent_id = parents.id), 0) + coalesce((select max(c.score) from children c where c.parent_id = parents.id), 0), adjusted = coalesce((select max(c.score) from children c where c.parent_id = parents.id), 0) + 1"
	mustExecSQL(t, executor, "app", query)
	require.Equal(t, [][]interface{}{{"1", "11", "10"}, {"2", "0", "1"}, {"3", "5", "5"}}, mustQuerySQL(t, executor, "app", "select id, total, adjusted from parents order by id"))
}

func TestCorrelatedScalarProjectionSupportsOuterExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (12, 3, 4)")
	require.Equal(t, [][]interface{}{{"1", "10"}, {"2", "1"}, {"3", "5"}}, mustQuerySQL(t, executor, "app", "select p.id, coalesce((select max(c.score) from children c where c.parent_id = p.id), 0) + 1 as adjusted from parents p order by p.id"))
}

func TestCorrelatedScalarProjectionSupportsCaseExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (12, 3, 4)")
	require.Equal(t, [][]interface{}{{"1", "has"}, {"2", "none"}, {"3", "none"}}, mustQuerySQL(t, executor, "app", "select p.id, case when (select count(*) from children c where c.parent_id = p.id) > 1 then 'has' else 'none' end as state from parents p order by p.id"))
}

func TestCorrelatedScalarProjectionHonorsOuterLimitOffset(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (20, 2, 4), (30, 3, 9)")
	rows := mustQuerySQL(t, executor, "app", "select p.id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p order by p.id limit 1 offset 1")
	require.Equal(t, [][]interface{}{{"2", "4"}}, rows)
}

func TestCorrelatedScalarProjectionSupportsOuterGroupByHaving(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (20, 2, 4), (30, 3, 9)")
	rows := mustQuerySQL(t, executor, "app", "select p.id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p group by p.id having p.id > 1 order by p.id")
	require.Equal(t, [][]interface{}{{"2", "4"}, {"3", "9"}}, rows)
}

func TestCorrelatedScalarProjectionSupportsGroupingByScalarAlias(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (20, 2, 7), (30, 3, 9)")
	rows := mustQuerySQL(t, executor, "app", "select (select max(c.score) from children c where c.parent_id = p.id) as max_score, count(*) as total from parents p group by max_score order by max_score")
	require.Equal(t, [][]interface{}{{"7", "2"}, {"9", "1"}}, rows)
}

func TestCorrelatedScalarProjectionSupportsGroupingByScalarExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3), (4)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (20, 2, 7), (30, 3, 9)")
	rows := mustQuerySQL(t, executor, "app", "select coalesce((select max(c.score) from children c where c.parent_id = p.id), 0) as score_bucket, count(*) as total from parents p group by coalesce((select max(c.score) from children c where c.parent_id = p.id), 0) order by score_bucket")
	require.Equal(t, [][]interface{}{{"0", "1"}, {"7", "2"}, {"9", "1"}}, rows)
	aggregateRows := mustQuerySQL(t, executor, "app", "select coalesce((select max(c.score) from children c where c.parent_id = p.id), 0) as score_bucket, sum(p.id) as id_sum, avg(p.id) as id_avg from parents p group by coalesce((select max(c.score) from children c where c.parent_id = p.id), 0) order by score_bucket")
	require.Equal(t, [][]interface{}{{"0", "4", "4"}, {"7", "3", "1.5"}, {"9", "3", "3"}}, aggregateRows)
	havingRows := mustQuerySQL(t, executor, "app", "select coalesce((select max(c.score) from children c where c.parent_id = p.id), 0) as score_bucket, count(*) as total from parents p group by coalesce((select max(c.score) from children c where c.parent_id = p.id), 0) having coalesce((select max(c.score) from children c where c.parent_id = p.id), 0) >= 7 order by score_bucket")
	require.Equal(t, [][]interface{}{{"7", "2"}, {"9", "1"}}, havingRows)
}

func TestCorrelatedScalarHavingCanUseUnprojectedSubquery(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1), (11, 1), (20, 2)")

	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p group by p.id having (select count(*) from children c where c.parent_id = p.id) > 0 order by p.id")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}}, rows)
}

func TestCorrelatedScalarProjectionAppliesOuterDistinctAfterScalarEvaluation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, group_id int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, group_id) values (1, 10), (2, 10), (3, 20)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (20, 2, 7), (30, 3, 9)")
	rows := mustQuerySQL(t, executor, "app", "select distinct p.group_id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p order by p.group_id")
	require.Equal(t, [][]interface{}{{"10", "7"}, {"20", "9"}}, rows)
	pageRows := mustQuerySQL(t, executor, "app", "select distinct p.group_id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p order by p.group_id limit 1 offset 1")
	require.Equal(t, [][]interface{}{{"20", "9"}}, pageRows)
	orderedRows := mustQuerySQL(t, executor, "app", "select distinct p.group_id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p order by max_score desc limit 1")
	require.Equal(t, [][]interface{}{{"20", "9"}}, orderedRows)
}

func TestCorrelatedScalarProjectionOrdersByScalarAlias(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 12), (20, 2, 4), (30, 3, 9)")
	rows := mustQuerySQL(t, executor, "app", "select p.id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p order by max_score desc limit 2")
	require.Equal(t, [][]interface{}{{"1", "12"}, {"3", "9"}}, rows)
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (21, 2, 12)")
	mixedOrderRows := mustQuerySQL(t, executor, "app", "select p.id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p order by max_score desc, p.id asc limit 2")
	require.Equal(t, [][]interface{}{{"1", "12"}, {"2", "12"}}, mixedOrderRows)
	mustExecSQL(t, executor, "app", "insert into parents (id) values (4)")
	expressionOrderRows := mustQuerySQL(t, executor, "app", "select p.id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p order by coalesce(max_score, 0) desc limit 4")
	require.Equal(t, [][]interface{}{{"1", "12"}, {"2", "12"}, {"3", "9"}, {"4", nil}}, expressionOrderRows)
	ascendingExpressionOrderRows := mustQuerySQL(t, executor, "app", "select p.id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p order by coalesce(max_score, 0) asc limit 2")
	require.Equal(t, [][]interface{}{{"4", nil}, {"3", "9"}}, ascendingExpressionOrderRows)
}

func TestCorrelatedScalarProjectionFiltersByScalarAliasInHaving(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 12), (20, 2, 4), (30, 3, 9)")
	rows := mustQuerySQL(t, executor, "app", "select p.id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p group by p.id having max_score >= 9 order by p.id")
	require.Equal(t, [][]interface{}{{"1", "12"}, {"3", "9"}}, rows)
	mixedRows := mustQuerySQL(t, executor, "app", "select p.id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p group by p.id having max_score >= 9 and p.id <> 3 order by p.id")
	require.Equal(t, [][]interface{}{{"1", "12"}}, mixedRows)
}

func TestCorrelatedScalarPredicateFiltersOuterRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (20, 2, 4), (30, 3, 9)")

	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where (select count(*) from children c where c.parent_id = p.id) >= 2 order by p.id")
	require.Equal(t, [][]interface{}{{"1"}}, rows)
}

func TestCorrelatedScalarPredicatePreservesOuterFiltersAndLimit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (20, 2, 4), (30, 3, 9), (31, 3, 8)")

	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where p.id > 1 and (select max(c.score) from children c where c.parent_id = p.id) >= 8 order by p.id limit 1")
	require.Equal(t, [][]interface{}{{"3"}}, rows)
}

func TestCorrelatedScalarPredicatesSupportMultipleAndConditions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (20, 2, 4), (21, 2, 8), (30, 3, 9)")

	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where (select count(*) from children c where c.parent_id = p.id) >= 2 and (select max(c.score) from children c where c.parent_id = p.id) >= 8 order by p.id")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}}, rows)
}

func TestCorrelatedScalarPredicatePreservesOrSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (20, 2, 4)")

	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where (select count(*) from children c where c.parent_id = p.id) >= 2 or p.id = 3 order by p.id")
	require.Equal(t, [][]interface{}{{"1"}, {"3"}}, rows)
}

func TestCorrelatedScalarPredicateSupportsMultipleSubqueriesInOr(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (20, 2, 4), (21, 2, 8)")

	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p where (select count(*) from children c where c.parent_id = p.id) >= 2 or (select max(c.score) from children c where c.parent_id = p.id) >= 9 order by p.id")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}}, rows)
}

func TestRegexpPredicatesUseMySQLBooleanAndNullSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(50))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, null)")

	rows := mustQuerySQL(t, executor, "app", "select id from users where name regexp '^a|^b' order by id")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}}, rows)
	notRows := mustQuerySQL(t, executor, "app", "select id from users where name not regexp '^a|^b' order by id")
	require.Equal(t, [][]interface{}{{"3"}}, notRows)
}

func TestCorrelatedScalarProjectionSupportsMultipleSubqueriesInExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (12, 3, 4)")
	require.Equal(t, [][]interface{}{{"1", "11"}, {"2", "0"}, {"3", "5"}}, mustQuerySQL(t, executor, "app", "select p.id, coalesce((select count(*) from children c where c.parent_id = p.id), 0) + coalesce((select max(c.score) from children c where c.parent_id = p.id), 0) as total from parents p order by p.id"))
}

func TestCorrelatedScalarProjectionSupportsImplicitAlias(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (20, 2, 4)")

	rows := mustQuerySQL(t, executor, "app", "select p.id, (select max(c.score) from children c where c.parent_id = p.id) max_score from parents p order by p.id")
	require.Equal(t, [][]interface{}{{"1", "7"}, {"2", "4"}}, rows)
}

func TestCorrelatedScalarSubquerySupportsJoinOnPredicate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (20, 2, 4)")

	rows := mustQuerySQL(t, executor, "app", "select p.id, c.id from parents p join children c on c.parent_id = p.id and c.score = (select max(c2.score) from children c2 where c2.parent_id = p.id) order by p.id")
	require.Equal(t, [][]interface{}{{"1", "11"}, {"2", "20"}}, rows)
	leftRows := mustQuerySQL(t, executor, "app", "select p.id, c.id from parents p left join children c on c.parent_id = p.id and c.score = (select max(c2.score) from children c2 where c2.parent_id = p.id) order by p.id")
	require.Equal(t, [][]interface{}{{"1", "11"}, {"2", "20"}, {"3", nil}}, leftRows)
	wrappedRows := mustQuerySQL(t, executor, "app", "select p.id, c.id from parents p left join children c on c.parent_id = p.id and c.score = coalesce((select max(c2.score) from children c2 where c2.parent_id = p.id), 0) order by p.id")
	require.Equal(t, [][]interface{}{{"1", "11"}, {"2", "20"}, {"3", nil}}, wrappedRows)
	inRows := mustQuerySQL(t, executor, "app", "select p.id, c.id from parents p left join children c on c.parent_id = p.id and c.id in (select c2.id from children c2 where c2.parent_id = p.id) order by p.id, c.id")
	require.Equal(t, [][]interface{}{{"1", "10"}, {"1", "11"}, {"2", "20"}, {"3", nil}}, inRows)
	derivedInRows := mustQuerySQL(t, executor, "app", "select p.id, c.id from (select id from parents where id > 0) p left join children c on c.parent_id = p.id and c.id in (select c2.id from children c2 where c2.parent_id = p.id) order by p.id, c.id")
	require.Equal(t, [][]interface{}{{"1", "10"}, {"1", "11"}, {"2", "20"}, {"3", nil}}, derivedInRows)
	mustExecSQL(t, executor, "app", "create table allowed_child_ids (parent_id int, child_id int)")
	mustExecSQL(t, executor, "app", "insert into allowed_child_ids (parent_id, child_id) values (1, 10), (1, null), (2, 999)")
	notInRows := mustQuerySQL(t, executor, "app", "select p.id, c.id from parents p left join children c on c.parent_id = p.id and c.id not in (select a.child_id from allowed_child_ids a where a.parent_id = p.id) order by p.id, c.id")
	require.Equal(t, [][]interface{}{{"1", nil}, {"2", "20"}, {"3", nil}}, notInRows)
	existsRows := mustQuerySQL(t, executor, "app", "select p.id, c.id from parents p left join children c on c.parent_id = p.id and exists (select c2.id from children c2 where c2.parent_id = p.id and c2.id = c.id) order by p.id, c.id")
	require.Equal(t, [][]interface{}{{"1", "10"}, {"1", "11"}, {"2", "20"}, {"3", nil}}, existsRows)
	notExistsRows := mustQuerySQL(t, executor, "app", "select p.id, c.id from parents p left join children c on c.parent_id = p.id and not exists (select c2.id from children c2 where c2.parent_id = p.id) order by p.id, c.id")
	require.Equal(t, [][]interface{}{{"1", nil}, {"2", nil}, {"3", nil}}, notExistsRows)
	derivedExistsRows := mustQuerySQL(t, executor, "app", "select p.id, c.id from (select id from parents where id > 0) p left join children c on c.parent_id = p.id and exists (select c2.id from children c2 where c2.parent_id = p.id and c2.id = c.id) order by p.id, c.id")
	require.Equal(t, [][]interface{}{{"1", "10"}, {"1", "11"}, {"2", "20"}, {"3", nil}}, derivedExistsRows)
	derivedRows := mustQuerySQL(t, executor, "app", "select p.id, c.id from (select id from parents where id > 0) p left join children c on c.parent_id = p.id and c.score = (select max(c2.score) from children c2 where c2.parent_id = p.id) order by p.id")
	require.Equal(t, [][]interface{}{{"1", "11"}, {"2", "20"}, {"3", nil}}, derivedRows)
	multipleRows := <-executor.ExecuteQuery(nil, "select p.id, c.id from parents p join children c on c.parent_id = p.id and c.score = (select c2.score from children c2 where c2.parent_id = p.id) order by p.id", "app")
	require.Error(t, multipleRows.Err)
	require.Contains(t, strings.ToLower(multipleRows.Err.Error()), "more than 1 row")
}

func TestCorrelatedScalarProjectionSupportsCaseWrapper(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1)")

	rows := mustQuerySQL(t, executor, "app", "select p.id, case when (select count(*) from children c where c.parent_id = p.id) > 0 then 'has' else 'none' end as status from parents p order by p.id")
	require.Equal(t, [][]interface{}{{"1", "has"}, {"2", "none"}}, rows)
}

func TestCorrelatedScalarOrderByCanUseUnprojectedSubquery(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (20, 2, 4), (21, 2, 9)")

	rows := mustQuerySQL(t, executor, "app", "select p.id from parents p order by (select count(*) from children c where c.parent_id = p.id) desc, (select max(c.score) from children c where c.parent_id = p.id) desc, p.id limit 2")
	require.Equal(t, [][]interface{}{{"2"}, {"1"}}, rows)
}

func TestCorrelatedExistsSupportsDerivedOuterSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, active) values (1, 1), (2, 0), (3, 1)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1), (11, 3)")
	require.Equal(t, [][]interface{}{{"1"}, {"3"}}, mustQuerySQL(t, executor, "app", "select id from parents where active = 1"))
	require.Equal(t, [][]interface{}{{"1"}, {"3"}}, mustQuerySQL(t, executor, "app", "select d.id from (select id from parents where active = 1) d where exists (select 1 from children c where c.parent_id = d.id)"))
	require.Equal(t, [][]interface{}{{"3"}, {"1"}}, mustQuerySQL(t, executor, "app", "select d.id from (select id, active from parents) d where d.active = 1 and exists (select 1 from children c where c.parent_id = d.id) order by d.id desc"))
	require.Equal(t, [][]interface{}{{"3"}}, mustQuerySQL(t, executor, "app", "select d.id from (select id from parents where active = 1) d where exists (select 1 from children c where c.parent_id = d.id) order by d.id desc limit 1"))
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select d.id from (select id from parents where active = 1) d where exists (select 1 from children c where c.parent_id = d.id) order by d.id desc limit 1 offset 1"))
}

func TestCorrelatedExistsSupportsMultiSourceDerivedOuterSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table parent_meta (parent_id int primary key, enabled int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into parent_meta (parent_id, enabled) values (1, 1), (2, 0), (3, 1)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (10, 1), (11, 3)")
	require.Equal(t, [][]interface{}{{"3"}, {"1"}}, mustQuerySQL(t, executor, "app", "select d.id from (select p.id, m.enabled from parents p join parent_meta m on p.id = m.parent_id) d where d.enabled = 1 and exists (select 1 from children c where c.parent_id = d.id) order by d.id desc"))
	require.Equal(t, [][]interface{}{{"3"}, {"1"}}, mustQuerySQL(t, executor, "app", "select d.id from (select p.id, m.enabled from parents p join parent_meta m on p.id = m.parent_id) d where d.enabled = 1 and exists (select 1 from children c where c.parent_id = d.id) order by d.enabled desc, d.id desc"))
}

func TestCorrelatedScalarSubqueryRebindsOuterRowAndAggregates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (20, 2, 4)")
	rows := mustQuerySQL(t, executor, "app", "select p.id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p order by p.id")
	require.Equal(t, [][]interface{}{{"1", "9"}, {"2", "4"}, {"3", nil}}, rows)
}

func TestCorrelatedScalarSubquerySupportsDerivedOuterSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, active) values (1, 1), (2, 0), (3, 1)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (30, 3, 4)")
	rows := mustQuerySQL(t, executor, "app", "select d.id, (select max(c.score) from children c where c.parent_id = d.id) as max_score from (select id from parents where active = 1) d order by d.id")
	require.Equal(t, [][]interface{}{{"1", "9"}, {"3", "4"}}, rows)
}

func TestCorrelatedScalarSubquerySupportsMultiSourceDerivedOuterSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table parent_meta (parent_id int primary key, enabled int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into parent_meta (parent_id, enabled) values (1, 1), (2, 0), (3, 1)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (30, 3, 4)")
	rows := mustQuerySQL(t, executor, "app", "select d.id, (select max(c.score) from children c where c.parent_id = d.id) as max_score from (select p.id, m.enabled from parents p join parent_meta m on p.id = m.parent_id) d where d.enabled = 1 order by d.id")
	require.Equal(t, [][]interface{}{{"1", "9"}, {"3", "4"}}, rows)
}

func TestCorrelatedScalarSubquerySupportsNestedDerivedOuterSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id, active) values (1, 1), (2, 0), (3, 1)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (30, 3, 4)")
	rows := mustQuerySQL(t, executor, "app", "select d.id, (select max(c.score) from children c where c.parent_id = d.id) as max_score from (select n.id from (select id from parents where active = 1) n) d order by d.id")
	require.Equal(t, [][]interface{}{{"1", "9"}, {"3", "4"}}, rows)
}

func TestCorrelatedScalarSubquerySupportsOuterJoinSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table parent_meta (parent_id int primary key, enabled int)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, score int)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "insert into parent_meta (parent_id, enabled) values (1, 1), (2, 0), (3, 1)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, score) values (10, 1, 7), (11, 1, 9), (30, 3, 4)")
	rows := mustQuerySQL(t, executor, "app", "select p.id, (select max(c.score) from children c where c.parent_id = p.id) as max_score from parents p join parent_meta m on p.id = m.parent_id where m.enabled = 1 order by p.id")
	require.Equal(t, [][]interface{}{{"1", "9"}, {"3", "4"}}, rows)
}

func TestInSubqueryPreservesNullThreeValuedLogic(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create table candidates (id int primary key, value int)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1), (2)")
	mustExecSQL(t, executor, "app", "insert into candidates (id, value) values (1, 1), (2, null)")
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select id from users where id in (select value from candidates) order by id"))
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from users where id not in (select value from candidates) order by id"))
}

func TestWhereIsNullAndIsNotNullPredicates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table values_table (id int primary key, value int)")
	mustExecSQL(t, executor, "app", "insert into values_table (id, value) values (1, null), (2, 0), (3, 7)")
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select id from values_table where value is null"))
	require.Equal(t, [][]interface{}{{"2"}, {"3"}}, mustQuerySQL(t, executor, "app", "select id from values_table where value is not null order by id"))
}

func TestScalarSubqueryMaterializesSingleValueAndEmptyResultAsNull(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table values_table (id int primary key, value int)")
	mustExecSQL(t, executor, "app", "insert into values_table (id, value) values (1, 7)")
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", "select value from values_table where id = 1"))
	rows := mustQuerySQL(t, executor, "app", "select id, (select value from values_table where id = 1) as selected_value from values_table where id = 1")
	require.Equal(t, [][]interface{}{{"1", "7"}}, rows)
	empty := mustQuerySQL(t, executor, "app", "select id, (select value from values_table where id = 99) as selected_value from values_table where id = 1")
	require.Equal(t, [][]interface{}{{"1", nil}}, empty)
}

func TestScalarSubqueryRejectsMultipleRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table values_table (id int primary key, value int)")
	mustExecSQL(t, executor, "app", "insert into values_table (id, value) values (1, 7), (2, 8)")
	result := <-executor.ExecuteQuery(nil, "select (select value from values_table)", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "more than 1 row")
}

func TestQuantifiedSubqueriesHonorAnyAllEmptyAndNullSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create table candidates (id int primary key, value int)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1), (2), (3), (4)")
	mustExecSQL(t, executor, "app", "insert into candidates (id, value) values (1, 1), (2, 3), (3, null)")

	require.Equal(t, [][]interface{}{{"1"}, {"3"}}, mustQuerySQL(t, executor, "app", "select id from users where id = any (select value from candidates) order by id"))
	require.Equal(t, [][]interface{}{{"2"}, {"3"}, {"4"}}, mustQuerySQL(t, executor, "app", "select id from users where id > any (select value from candidates) order by id"))
	require.Equal(t, [][]interface{}{{"4"}}, mustQuerySQL(t, executor, "app", "select id from users where id > all (select value from candidates where value > 0) order by id"))
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from users where id = any (select value from candidates where value > 99)"))
	require.Equal(t, [][]interface{}{{"1"}, {"2"}, {"3"}, {"4"}}, mustQuerySQL(t, executor, "app", "select id from users where id > all (select value from candidates where value > 99) order by id"))
}

func TestUnionAllReturnsRowsFromBothBranches(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1), (2)")

	rows := mustQuerySQL(t, executor, "app", "select id from users where id = 1 union all select id from users where id = 2")
	require.Len(t, rows, 2)
}

func TestUnionAppliesOuterOrderByAndLimitAfterDuplicateRemoval(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1), (2), (3)")
	rows := mustQuerySQL(t, executor, "app", "select id from users where id >= 2 union select id from users where id <= 2 order by id desc limit 2")
	require.Equal(t, [][]interface{}{{"3"}, {"2"}}, rows)
}

func TestUnionSupportsParenthesizedBranchOrderAndLimit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1), (2), (3)")
	rows := mustQuerySQL(t, executor, "app", "select id from users where id <= 2 union all (select id from users order by id desc limit 1)")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}, {"3"}}, rows)
	ordered := mustQuerySQL(t, executor, "app", "(select id from users where id <= 2) union all (select id from users order by id desc limit 2) order by id desc limit 3")
	require.Equal(t, [][]interface{}{{"3"}, {"2"}, {"2"}}, ordered)
}

func TestMixedUnionAllAndUnionDistinctPreserveOperatorSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1)")
	rows := mustQuerySQL(t, executor, "app", "select id from users union all select id from users union select id from users")
	require.Equal(t, [][]interface{}{{"1"}}, rows)
}

func TestSimpleCTESelectExpandsToMaterializedQuery(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1), (2)")
	rows := mustQuerySQL(t, executor, "app", "with recent as (select * from users where id = 2) select * from recent")
	require.Len(t, rows, 1)
}

func TestMultipleCTEChainExpandsInDeclarationOrder(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1)")
	rows := mustQuerySQL(t, executor, "app", "with first as (select * from users), second as (select * from first) select * from second")
	require.Equal(t, [][]interface{}{{"1"}}, rows)
}

func TestCTEProjectionAndOuterPredicateAreRewrittenAgainstBaseTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice'), (2, 'bob')")
	rows := mustQuerySQL(t, executor, "app", "with recent as (select id from users where id > 1) select id from recent where id = 2")
	require.Equal(t, [][]interface{}{{"2"}}, rows)
}

func TestCTEJoinPreservesDefinitionPredicate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(20))")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice'), (2, 'bob')")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (1, 'A'), (2, 'B')")
	rows := mustQuerySQL(t, executor, "app", "with recent as (select id from users where id > 1) select recent.id, labels.label from recent join labels on recent.id = labels.id")
	require.Equal(t, [][]interface{}{{"2", "B"}}, rows)
}

func TestCTEDMLInSubqueryRewritesAgainstBaseTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "insert into users (id, active) values (1, 0), (2, 0)")
	mustExecSQL(t, executor, "app", "with recent as (select id from users where id > 1) update users set active = 1 where id in (select id from recent)")
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySQL(t, executor, "app", "select id from users where active = 1"))
}

func TestSimpleRowNumberWindowQuery(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (2), (1)")
	rows := mustQuerySQL(t, executor, "app", "select id, row_number() over (order by id) as rn from users")
	require.Len(t, rows, 2)
	require.NotEmpty(t, rows[0][1])
	require.NotEmpty(t, rows[1][1])
}

func TestWindowQuerySupportsWindowOnlyProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (2), (1)")
	rows := mustQuerySQL(t, executor, "app", "select row_number() over (order by id) as rn from users")
	require.Equal(t, [][]interface{}{{"2"}, {"1"}}, rows)
}

func TestPartitionedRankWindowQuery(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table scores (id int primary key, team varchar(20), score int)")
	mustExecSQL(t, executor, "app", "insert into scores (id, team, score) values (1, 'a', 20), (2, 'a', 10), (3, 'b', 30), (4, 'b', 30)")
	rows := mustQuerySQL(t, executor, "app", "select id, rank() over (partition by team order by score desc) as ranking from scores")
	require.Len(t, rows, 4)
	require.Equal(t, "1", rows[0][0])
	require.NotEmpty(t, rows[0][1])
}

func TestGeneralWindowCompatibilitySupportsLagAndNtile(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table scores (id int primary key, score int)")
	mustExecSQL(t, executor, "app", "insert into scores (id, score) values (1, 10), (2, 20), (3, 30)")
	rows := mustQuerySQL(t, executor, "app", "select id, lag(score, 1) over (order by id rows between 1 preceding and current row) as previous from scores")
	require.Len(t, rows, 3)
	require.Nil(t, rows[0][1])
	require.NotNil(t, rows[1][1])
	ntile := mustQuerySQL(t, executor, "app", "select id, ntile(2) over (order by id) as bucket from scores")
	require.Len(t, ntile, 3)
}

func TestWindowQuerySupportsMultipleWindowExpressions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table scores (id int primary key, team varchar(20), score int)")
	mustExecSQL(t, executor, "app", "insert into scores (id, team, score) values (1, 'a', 20), (2, 'a', 10), (3, 'b', 7)")
	rows := mustQuerySQL(t, executor, "app", "select id, row_number() over (partition by team order by id) as row_no, sum(score) over (partition by team order by id rows between unbounded preceding and current row) as running_total from scores")
	require.Equal(t, [][]interface{}{{"1", "1", "20"}, {"2", "2", "30"}, {"3", "1", "7"}}, rows)
}

func TestWindowValueFunctionsHonorRowsFrameAndDefaultRange(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table values_table (id int primary key, score int)")
	mustExecSQL(t, executor, "app", "insert into values_table (id, score) values (1, 10), (2, 20), (3, 30)")
	rows := mustQuerySQL(t, executor, "app", "select id, first_value(score) over (order by id rows between 1 preceding and current row) as first_score from values_table")
	require.Equal(t, [][]interface{}{{"1", "10"}, {"2", "10"}, {"3", "20"}}, rows)
	last := mustQuerySQL(t, executor, "app", "select id, last_value(score) over (order by id) as last_score from values_table")
	require.Equal(t, [][]interface{}{{"1", "10"}, {"2", "20"}, {"3", "30"}}, last)
	sortedResult := <-executor.ExecuteQuery(nil, "select id, row_number() over (order by score) as rn from values_table", "app")
	require.NoError(t, sortedResult.Err)
	sorted, ok := sortedResult.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"int", "bigint"}, sorted.ColumnTypes)
	require.Equal(t, int64(1), sorted.Records[0].GetValues()[1].Int())
	require.Equal(t, int64(2), sorted.Records[1].GetValues()[1].Int())
	require.Equal(t, int64(3), sorted.Records[2].GetValues()[1].Int())
}

func TestWindowAggregateFunctionsHonorPartitionAndFrame(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table scores (id int primary key, team varchar(20), score int)")
	mustExecSQL(t, executor, "app", "insert into scores (id, team, score) values (1, 'a', 20), (2, 'a', 10), (3, 'b', 7), (4, 'b', 5)")
	running := mustQuerySQL(t, executor, "app", "select id, sum(score) over (partition by team order by id rows between unbounded preceding and current row) as running_total from scores")
	require.Equal(t, [][]interface{}{{"1", "20"}, {"2", "30"}, {"3", "7"}, {"4", "12"}}, running)
	partitioned := mustQuerySQL(t, executor, "app", "select id, count(*) over (partition by team) as team_count from scores")
	require.Equal(t, [][]interface{}{{"1", "2"}, {"2", "2"}, {"3", "2"}, {"4", "2"}}, partitioned)
	average := mustQuerySQL(t, executor, "app", "select id, avg(score) over (partition by team) as team_average from scores")
	require.Equal(t, [][]interface{}{{"1", "15"}, {"2", "15"}, {"3", "6"}, {"4", "6"}}, average)
	ranged := mustQuerySQL(t, executor, "app", "select id, sum(score) over (partition by team order by score range between 10 preceding and current row) as ranged_total from scores")
	require.Equal(t, [][]interface{}{{"1", "30"}, {"2", "10"}, {"3", "12"}, {"4", "5"}}, ranged)
	jsonRunning := mustQuerySQL(t, executor, "app", "select id, json_arrayagg(score) over (partition by team order by id rows between unbounded preceding and current row) as scores_so_far from scores")
	require.Equal(t, [][]interface{}{{"1", "[20]"}, {"2", "[20,10]"}, {"3", "[7]"}, {"4", "[7,5]"}}, jsonRunning)
	jsonObjectRunning := mustQuerySQL(t, executor, "app", "select id, json_objectagg(id, score) over (partition by team order by id rows between unbounded preceding and current row) as scores_by_id from scores")
	require.Equal(t, [][]interface{}{{"1", `{"1":20}`}, {"2", `{"1":20,"2":10}`}, {"3", `{"3":7}`}, {"4", `{"3":7,"4":5}`}}, jsonObjectRunning)
}

func TestWindowStatAndBitAggregatesHonorFrame(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table values_table (id int primary key, score int)")
	mustExecSQL(t, executor, "app", "insert into values_table (id, score) values (1, 10), (2, 20), (3, 30)")

	variance := mustQuerySQL(t, executor, "app", "select id, var_pop(score) over (order by id rows between unbounded preceding and current row) as v from values_table")
	require.Equal(t, "0", variance[0][1])
	require.Equal(t, "25", variance[1][1])
	require.Equal(t, "66.66666666666667", variance[2][1])

	stddev := mustQuerySQL(t, executor, "app", "select id, stddev_samp(score) over (order by id rows between unbounded preceding and current row) as s from values_table")
	require.Nil(t, stddev[0][1])
	require.Equal(t, "7.0710678118654755", stddev[1][1])
	require.Equal(t, "10", stddev[2][1])

	bits := mustQuerySQL(t, executor, "app", "select id, bit_or(score) over (order by id rows between unbounded preceding and current row) as b from values_table")
	require.Equal(t, [][]interface{}{{"1", "10"}, {"2", "30"}, {"3", "30"}}, bits)
}

func TestWindowFramesHonorDescendingRowsAndRangeBounds(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table values_table (id int primary key, score int)")
	mustExecSQL(t, executor, "app", "insert into values_table (id, score) values (1, 10), (2, 20), (3, 30)")
	rows := mustQuerySQL(t, executor, "app", "select id, sum(score) over (order by id desc rows between current row and 1 following) as trailing_total from values_table")
	require.Equal(t, [][]interface{}{{"1", "10"}, {"2", "30"}, {"3", "50"}}, rows)
	ranged := mustQuerySQL(t, executor, "app", "select id, sum(score) over (order by score desc range between 10 preceding and current row) as ranged_total from values_table")
	require.Equal(t, [][]interface{}{{"1", "30"}, {"2", "50"}, {"3", "30"}}, ranged)
}

func TestNamedWindowSpecificationCanBeReused(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table scores (id int primary key, team varchar(20), score int)")
	mustExecSQL(t, executor, "app", "insert into scores (id, team, score) values (1, 'a', 20), (2, 'a', 10), (3, 'b', 7)")
	rows := mustQuerySQL(t, executor, "app", "select id, sum(score) over w as running_total from scores window w as (partition by team order by id rows between unbounded preceding and current row)")
	require.Equal(t, [][]interface{}{{"1", "20"}, {"2", "30"}, {"3", "7"}}, rows)
}

func TestWindowOrderingSupportsMultipleColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table scores (id int primary key, bucket int, score int)")
	mustExecSQL(t, executor, "app", "insert into scores (id, bucket, score) values (1, 1, 10), (2, 1, 10), (3, 1, 5), (4, 2, 20)")
	result := <-executor.ExecuteQuery(nil, "select id, rank() over (order by bucket, score desc) as ranking from scores", "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []int64{1, 1, 3, 4}, []int64{
		selectResult.Records[0].GetValues()[1].Int(),
		selectResult.Records[1].GetValues()[1].Int(),
		selectResult.Records[2].GetValues()[1].Int(),
		selectResult.Records[3].GetValues()[1].Int(),
	})
}

func TestWindowDistributionAndNthValueFunctions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table scores (id int primary key, team varchar(20), score int)")
	mustExecSQL(t, executor, "app", "insert into scores (id, team, score) values (1, 'a', 20), (2, 'a', 10), (3, 'b', 7), (4, 'b', 5)")

	cume := mustQuerySQL(t, executor, "app", "select id, cume_dist() over (partition by team order by score) as cume from scores")
	require.Equal(t, "1", cume[0][0])
	require.Equal(t, "1", cume[0][1])
	require.Equal(t, "0.5", cume[1][1])
	require.Equal(t, "1", cume[2][1])
	require.Equal(t, "0.5", cume[3][1])

	percent := mustQuerySQL(t, executor, "app", "select id, percent_rank() over (partition by team order by score) as pct from scores")
	require.Equal(t, "1", percent[0][1])
	require.Equal(t, "0", percent[1][1])
	require.Equal(t, "1", percent[2][1])
	require.Equal(t, "0", percent[3][1])

	nth := mustQuerySQL(t, executor, "app", "select id, nth_value(score, 2) over (partition by team order by id rows between unbounded preceding and current row) as second_score from scores")
	require.Nil(t, nth[0][1])
	require.Equal(t, "10", nth[1][1])
	require.Nil(t, nth[2][1])
	require.Equal(t, "5", nth[3][1])
}
