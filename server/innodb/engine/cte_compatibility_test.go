package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestRecursiveCTECompatibilityMaterializesAndTerminates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "with recursive nums(n) as (select 1 union all select n + 1 from nums where n < 4) select n from nums", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, 4, selectResult.RowCount)
	require.Equal(t, "n", selectResult.Columns[0])
	values := make([]string, 0, len(selectResult.Records))
	for _, record := range selectResult.Records {
		values = append(values, fmt.Sprintf("%d", record.GetValues()[0].Int()))
	}
	require.Equal(t, []string{"1", "2", "3", "4"}, values)
}

func TestNonRecursiveCTEUnionSupportsOuterAggregateOrderAndLimit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	rows := mustQuerySQL(t, executor, "app", "with numbers (n) as (select 1 as n union all select 2 as n union all select 2 as n) select n, count(*) as cnt from numbers group by n having count(*) >= 1 order by n desc limit 1")
	require.Equal(t, [][]interface{}{{"2", "2"}}, rows)
}

func TestRecursiveCTECompatibilitySupportsUnionDistinctTermination(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "with recursive nums(n) as (select 1 union select n + 0 from nums where n < 4) select n from nums", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, 1, selectResult.RowCount)
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilitySupportsMultipleColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "with recursive nums(n, label) as (select 1, 'a' union all select n + 1, concat(label, 'x') from nums where n < 3) select n, label from nums order by n", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"1", "a"}, {"2", "ax"}, {"3", "axx"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilitySupportsMultipleIndependentDefinitions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "with recursive first(n) as (select 1 union all select n + 1 from first where n < 2), second(n) as (select 10 union all select n + 1 from second where n < 12) select n from second order by n", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"10"}, {"11"}, {"12"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilitySupportsMultiColumnDefinitionsInJoin(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "with recursive first(n, label) as (select 1, 'a' union all select n + 1, concat(label, 'x') from first where n < 2), second(n) as (select 10 union all select n + 10 from second where n < 20) select first.n, second.n, first.label from first join second on first.n = 1 order by second.n", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"1", "10", "a"}, {"1", "20", "a"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilitySupportsThreeIndependentDefinitionsInJoinChain(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "with recursive first(n) as (select 1 union all select n + 1 from first where n < 2), second(n) as (select 10 union all select n + 1 from second where n < 11), third(n) as (select 20 union all select n + 1 from third where n < 21) select first.n, second.n, third.n from first join second on second.n = 10 join third on third.n = 20 order by first.n", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"1", "10", "20"}, {"2", "10", "20"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilitySupportsThreeIndependentDefinitionsInLeftJoinChain(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "with recursive first(n) as (select 1 union all select n + 1 from first where n < 2), second(n) as (select 99 union all select n + 1 from second where n < 99), third(n) as (select 20 union all select n + 1 from third where n < 20) select first.n, second.n, third.n from first left join second on second.n = 10 left join third on third.n = 20 order by first.n", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"1", "", "20"}, {"2", "", "20"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilitySupportsIndependentDefinitionsJoinedWithBaseTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, name varchar(20))")
	mustExecSQL(t, executor, "app", "insert into labels (id, name) values (1, 'one'), (2, 'two')")
	result := <-executor.ExecuteQuery(nil, "with recursive first(n) as (select 1 union all select n + 1 from first where n < 2), second(n) as (select 10 union all select n + 1 from second where n < 10) select first.n, second.n, labels.name from first join second on second.n = 10 join labels on labels.id = first.n order by first.n", "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"1", "10", "one"}, {"2", "10", "two"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilitySupportsAggregateOverJoinedDefinitions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "with recursive first(n, label) as (select 1, 'a' union all select n + 1, concat(label, 'x') from first where n < 2), second(n) as (select 10 union all select n + 10 from second where n < 20) select first.n, count(*) as total from first join second on first.n = 1 group by first.n order by first.n", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"1", "2"}}, selectResultRows(selectResult))
	aggregate := <-executor.ExecuteQuery(nil, "with recursive first(n) as (select 1 union all select n + 1 from first where n < 2), second(n) as (select 10 union all select n + 10 from second where n < 20) select count(*) as total, sum(second.n) as total_sum from first join second on first.n = 1", "")
	require.NoError(t, aggregate.Err)
	aggregateResult, ok := aggregate.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"2", "30"}}, selectResultRows(aggregateResult))
}

func TestRecursiveCTECompatibilitySupportsGenericMemberJoinFromDualAnchor(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table edges (id int primary key, parent_id int, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into edges (id, parent_id, label) values (2, 1, 'eng'), (3, 2, 'dev'), (4, 1, 'sales')")
	result := <-executor.ExecuteQuery(nil, "with recursive org(id, path) as (select 1, 'root' union all select e.id, concat(o.path, '/', e.label) from org o join edges e on e.parent_id = o.id) select id, path from org order by id", "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	rows := selectResultRows(selectResult)
	require.Equal(t, [][]interface{}{{"1", "root"}, {"2", "root/eng"}, {"3", "root/eng/dev"}, {"4", "root/sales"}}, rows)
}

func TestRecursiveCTECompatibilitySupportsNestedBaseTableJoinMember(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table edges (id int primary key, parent_id int, label_id int)")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, name varchar(20))")
	mustExecSQL(t, executor, "app", "insert into edges (id, parent_id, label_id) values (2, 1, 10), (3, 2, 11), (4, 1, 12)")
	mustExecSQL(t, executor, "app", "insert into labels (id, name) values (10, 'eng'), (11, 'dev'), (12, 'sales')")
	result := <-executor.ExecuteQuery(nil, "with recursive org(id, path) as (select 1, 'root' union all select e.id, concat(o.path, '/', l.name) from org o join edges e on e.parent_id = o.id join labels l on l.id = e.label_id) select id, path from org order by id", "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"1", "root"}, {"2", "root/eng"}, {"3", "root/eng/dev"}, {"4", "root/sales"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilitySupportsLeftJoinInMemberGraph(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table edges (id int primary key, parent_id int, label_id int)")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, name varchar(32))")
	mustExecSQL(t, executor, "app", "insert into edges (id, parent_id, label_id) values (2, 1, 10), (3, 1, 99)")
	mustExecSQL(t, executor, "app", "insert into labels (id, name) values (10, 'eng')")

	query := "with recursive org(id, path) as (select 1, 'root' union all select e.id, concat(o.path, '/', coalesce(l.name, 'unknown')) from org o join edges e on e.parent_id = o.id left join labels l on l.id = e.label_id) select id, path from org order by id"
	result := <-executor.ExecuteQuery(nil, query, "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"1", "root"}, {"2", "root/eng"}, {"3", "root/unknown"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilitySupportsRightJoinInMemberGraph(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table edges (id int primary key, parent_id int, label_id int)")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, name varchar(32))")
	mustExecSQL(t, executor, "app", "insert into edges (id, parent_id, label_id) values (2, 1, 10), (3, 2, 11)")
	mustExecSQL(t, executor, "app", "insert into labels (id, name) values (10, 'eng'), (11, 'dev')")

	query := "with recursive org(id, path) as (select 1, 'root' union all select e.id, concat(o.path, '/', l.name) from org o join edges e on e.parent_id = o.id right join labels l on l.id = e.label_id where e.id is not null) select id, path from org order by id"
	result := <-executor.ExecuteQuery(nil, query, "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"1", "root"}, {"2", "root/eng"}, {"3", "root/eng/dev"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilityEvaluatesGenericSingleColumnExpressions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "with recursive nums(n) as (select 1 + 0 union all select n * 2 from nums where n < 5) select n from nums order by n", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"1"}, {"2"}, {"4"}, {"8"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilityRejectsDuplicateCycle(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "with recursive nums(n) as (select 1 union all select n + 0 from nums where n < 4) select n from nums", "")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "step")
}

func TestRecursiveCTECompatibilitySupportsMainProjectionFilterOrderAndLimit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "with recursive nums(n) as (select 1 union all select n + 1 from nums where n < 5) select n * 2 as doubled from nums where n >= 2 order by n desc limit 2", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"10"}, {"8"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilitySupportsMainAggregate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "with recursive nums(n) as (select 1 union all select n + 1 from nums where n < 4) select count(*) as total, sum(n) as total_sum from nums", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"4", "10"}}, selectResultRows(selectResult))
}

func TestRecursiveCTECompatibilitySupportsInsertSelect(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table generated_numbers (n int primary key)")
	result := <-executor.ExecuteQuery(nil, "with recursive nums(n) as (select 1 union all select n + 1 from nums where n < 3) insert into generated_numbers (n) select n from nums", "app")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"1"}, {"2"}, {"3"}}, mustQuerySQL(t, executor, "app", "select n from generated_numbers order by n"))
}

func TestRecursiveCTECompatibilitySupportsBaseTableJoin(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table employees (id int primary key, manager_id int, name varchar(20))")
	mustExecSQL(t, executor, "app", "insert into employees (id, manager_id, name) values (1, NULL, 'ceo'), (2, 1, 'eng'), (3, 2, 'dev'), (4, 1, 'sales')")
	rows := mustQuerySQL(t, executor, "app", "with recursive org(id, manager_id, name) as (select id, manager_id, name from employees where manager_id is null union all select e.id, e.manager_id, e.name from org o join employees e on e.manager_id = o.id) select id, name from org order by id")
	require.Equal(t, [][]interface{}{{"1", "ceo"}, {"2", "eng"}, {"3", "dev"}, {"4", "sales"}}, rows)
}

func TestRecursiveCTECompatibilitySupportsSingleColumnBaseTableJoin(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table employee_edges (id int primary key, manager_id int)")
	mustExecSQL(t, executor, "app", "insert into employee_edges (id, manager_id) values (1, NULL), (2, 1), (3, 2)")
	rows := mustQuerySQL(t, executor, "app", "with recursive org(id) as (select id from employee_edges where manager_id is null union all select e.id from org o join employee_edges e on e.manager_id = o.id) select id from org order by id")
	require.Equal(t, [][]interface{}{{"1"}, {"2"}, {"3"}}, rows)
}

func TestRecursiveCTECompatibilitySupportsMainQueryJoinWithBaseTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (1, 'one'), (2, 'two'), (3, 'three')")

	rows := mustQuerySQL(t, executor, "app", "with recursive nums(n) as (select 1 union all select n + 1 from nums where n < 3) select nums.n, labels.label from nums join labels on nums.n = labels.id order by nums.n")
	require.Equal(t, [][]interface{}{{"1", "one"}, {"2", "two"}, {"3", "three"}}, rows)
	leftRows := mustQuerySQL(t, executor, "app", "with recursive nums(n) as (select 1 union all select n + 1 from nums where n < 4) select nums.n, labels.label from nums left join labels on nums.n = labels.id order by nums.n")
	require.Equal(t, [][]interface{}{{"1", "one"}, {"2", "two"}, {"3", "three"}, {"4", nil}}, leftRows)
}

func TestRecursiveCTECompatibilitySupportsMultiColumnMainQueryJoinWithBaseTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (1, 'one'), (2, 'two')")

	rows := mustQuerySQL(t, executor, "app", "with recursive nums(n, path) as (select 1, 'root' union all select n + 1, concat(path, 'x') from nums where n < 2) select nums.n, labels.label, nums.path from nums join labels on nums.n = labels.id order by nums.n")
	require.Equal(t, [][]interface{}{{"1", "one", "root"}, {"2", "two", "rootx"}}, rows)
}

func TestRecursiveCTECompatibilitySupportsMainQueryJoinChainWithBaseTables(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "create table label_groups (label varchar(20) primary key, group_name varchar(20))")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (1, 'one'), (2, 'two')")
	mustExecSQL(t, executor, "app", "insert into label_groups (label, group_name) values ('one', 'odd'), ('two', 'even')")

	rows := mustQuerySQL(t, executor, "app", "with recursive nums(n) as (select 1 union all select n + 1 from nums where n < 2) select nums.n, labels.label, label_groups.group_name from nums join labels on nums.n = labels.id join label_groups on labels.label = label_groups.label order by nums.n")
	require.Equal(t, [][]interface{}{{"1", "one", "odd"}, {"2", "two", "even"}}, rows)
}

func TestRecursiveCTECompatibilitySupportsDependentNonRecursiveDefinition(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	rows := mustQuerySQL(t, executor, "app", "with recursive nums(n) as (select 1 union all select n + 1 from nums where n < 3), doubled(n2) as (select n * 2 from nums) select n2 from doubled order by n2")
	require.Equal(t, [][]interface{}{{"2"}, {"4"}, {"6"}}, rows)
}

func TestRecursiveCTECompatibilitySupportsMainQueryLeftJoinChainWithFilterAndLimit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "create table label_groups (label varchar(20) primary key, group_name varchar(20))")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (1, 'one'), (2, 'two'), (3, 'three')")
	mustExecSQL(t, executor, "app", "insert into label_groups (label, group_name) values ('one', 'odd'), ('two', 'even')")

	rows := mustQuerySQL(t, executor, "app", "with recursive nums(n) as (select 1 union all select n + 1 from nums where n < 4) select nums.n, labels.label, label_groups.group_name from nums left join labels on nums.n = labels.id left join label_groups on labels.label = label_groups.label where label_groups.group_name is null or labels.id >= 2 order by nums.n desc limit 3")
	require.Equal(t, [][]interface{}{{"4", nil, nil}, {"3", "three", nil}, {"2", "two", "even"}}, rows)
}

func TestRecursiveCTECompatibilitySupportsMainQuerySelfJoin(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")

	rows := mustQuerySQL(t, executor, "app", "with recursive nums(n) as (select 1 union all select n + 1 from nums where n < 3) select left_nums.n, right_nums.n from nums left_nums join nums right_nums on left_nums.n = right_nums.n where left_nums.n >= 2 order by left_nums.n desc")
	require.Equal(t, [][]interface{}{{"3", "3"}, {"2", "2"}}, rows)
}

func TestRecursiveCTECompatibilitySupportsUpdateAndDeleteSelection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table generated_numbers (n int primary key, label varchar(20))")
	mustExecSQL(t, executor, "app", "insert into generated_numbers values (1, 'old'), (2, 'old'), (3, 'old')")

	updated := <-executor.ExecuteQuery(nil, "with recursive nums(n) as (select 1 union all select n + 1 from nums where n < 2) update generated_numbers set label = 'hit' where n in (select n from nums)", "app")
	require.NoError(t, updated.Err)
	require.Equal(t, [][]interface{}{{"1", "hit"}, {"2", "hit"}, {"3", "old"}}, mustQuerySQL(t, executor, "app", "select n, label from generated_numbers order by n"))

	deleted := <-executor.ExecuteQuery(nil, "with recursive nums(n) as (select 3 union all select n + 1 from nums where n < 3) delete from generated_numbers where n in (select n from nums)", "app")
	require.NoError(t, deleted.Err)
	require.Equal(t, [][]interface{}{{"1", "hit"}, {"2", "hit"}}, mustQuerySQL(t, executor, "app", "select n, label from generated_numbers order by n"))
}

func selectResultRows(result *SelectResult) [][]interface{} {
	rows := make([][]interface{}, 0, result.RowCount)
	for _, record := range result.Records {
		values := make([]interface{}, 0, len(record.GetValues()))
		for _, value := range record.GetValues() {
			switch value.Type() {
			case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeInt, basic.ValueTypeBigInt:
				values = append(values, fmt.Sprintf("%d", value.Int()))
			default:
				values = append(values, value.String())
			}
		}
		rows = append(rows, values)
	}
	return rows
}

func TestCTEContextEnforcesNamesColumnsAndDeclarationScope(t *testing.T) {
	base, err := sqlparser.Parse("select * from users")
	require.NoError(t, err)
	first, err := sqlparser.Parse("select * from users")
	require.NoError(t, err)
	second, err := sqlparser.Parse("select * from first")
	require.NoError(t, err)
	ctx, err := BuildCTEContext([]*CTEDefinition{
		{Name: "first", Columns: []string{"id"}, Query: first},
		{Name: "second", Columns: []string{"id"}, Query: second},
	})
	require.NoError(t, err)
	_, ok := ctx.GetDefinition("FIRST")
	require.True(t, ok)
	require.NoError(t, ResolveCTEReferences(base, ctx))

	_, err = BuildCTEContext([]*CTEDefinition{{Name: "dup", Columns: []string{"id", "ID"}, Query: first}})
	require.Error(t, err)
	_, err = BuildCTEContext([]*CTEDefinition{{Name: "later", Query: second}, {Name: "first", Query: first}})
	require.Error(t, err)
}

func TestNonRecursiveCTEMaterializesMultipleSourcesAndChains(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(50))")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice'), (2, 'bob'), (3, 'carol')")
	mustExecSQL(t, executor, "app", "insert into labels (id, label) values (2, 'B'), (3, 'C'), (4, 'D')")

	rows := mustQuerySQL(t, executor, "app", "with first as (select id from users where id >= 2), second as (select id, label from labels where id >= 2) select first.id, second.label from first join second on first.id = second.id order by first.id")
	require.Equal(t, [][]interface{}{{"2", "B"}, {"3", "C"}}, rows)
	chained := mustQuerySQL(t, executor, "app", "with base as (select id from users), filtered as (select id from base where id > 1) select id from filtered order by id")
	require.Equal(t, [][]interface{}{{"2"}, {"3"}}, chained)
	aliased := mustQuerySQL(t, executor, "app", "with eligible (user_id) as (select id from users where id >= 2) select user_id from eligible order by user_id")
	require.Equal(t, [][]interface{}{{"2"}, {"3"}}, aliased)
	mustExecSQL(t, executor, "app", "create table copied_users (id int primary key)")
	mustExecSQL(t, executor, "app", "with eligible as (select id from users where id >= 2) insert into copied_users (id) select id from eligible")
	require.Equal(t, [][]interface{}{{"2"}, {"3"}}, mustQuerySQL(t, executor, "app", "select id from copied_users order by id"))
}
