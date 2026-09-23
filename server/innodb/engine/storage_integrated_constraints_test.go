package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestInsertRejectsNullForNotNullColumn(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, username varchar(50) not null)")

	err := execSQLExpectError(t, executor, "app", "insert into users (id, username) values (1, null)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "not null")
}

func TestRowMatchesWhereConditionsWithValueColumnRange(t *testing.T) {
	matches, err := rowMatchesWhereConditions(map[string]interface{}{"value": int64(5500)}, []string{"value > 5000 and value < 6000"})
	require.NoError(t, err)
	require.True(t, matches)

	matches, err = rowMatchesWhereConditions(map[string]interface{}{"value": int64(6000)}, []string{"value > 5000 and value < 6000"})
	require.NoError(t, err)
	require.False(t, matches)
}

func TestCompositePrimaryKeyUsesAllColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table memberships (user_id int, group_id int, primary key (user_id, group_id))")
	mustExecSQL(t, executor, "app", "insert into memberships (user_id, group_id) values (1, 10)")
	mustExecSQL(t, executor, "app", "insert into memberships (user_id, group_id) values (1, 20)")

	err := execSQLExpectError(t, executor, "app", "insert into memberships (user_id, group_id) values (1, 10)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "duplicate")
}

func TestCreateTablePreservesSecondaryIndexMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, username varchar(50), index idx_username (username))")

	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	dml.SetDataDir(executor.conf.InnodbDataDir)
	dml.schemaName = "app"
	dml.tableName = "users"
	tableMeta, err := dml.getTableMetadata()
	require.NoError(t, err)

	require.Len(t, tableMeta.Indices, 2)
	require.Equal(t, "PRIMARY", tableMeta.Indices[0].Name)
	require.Equal(t, []string{"id"}, tableMeta.Indices[0].Columns)
	require.Equal(t, "idx_username", tableMeta.Indices[1].Name)
	require.Equal(t, []string{"username"}, tableMeta.Indices[1].Columns)
	require.False(t, tableMeta.Indices[1].Unique)
}

func TestCreateTableAllowsForeignKeyMetadataSyntax(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(100) not null)")
	mustExecSQL(t, executor, "app", "create table orders (id int primary key, user_id int, foreign key (user_id) references users(id))")
}

func TestCreateTableStoresParsedForeignKeyMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create table orders (id int primary key, user_id int, constraint fk_user foreign key (user_id) references users(id) on delete restrict on update set null)")

	data, err := os.ReadFile(filepath.Join(executor.conf.InnodbDataDir, "app", "orders.frm"))
	require.NoError(t, err)
	var tableInfo struct {
		ForeignKeys []map[string]interface{} `json:"foreign_keys"`
	}
	require.NoError(t, json.Unmarshal(data, &tableInfo))
	require.Len(t, tableInfo.ForeignKeys, 1)
	require.Equal(t, "fk_user", tableInfo.ForeignKeys[0]["name"])
	require.Equal(t, "restrict", tableInfo.ForeignKeys[0]["on_delete"])
	require.Equal(t, "set null", tableInfo.ForeignKeys[0]["on_update"])
}

func TestFallbackForeignKeyParsesReferentialActions(t *testing.T) {
	foreignKeys := parseCreateTableForeignKeysFallback("create table child (id int, parent_id int, constraint fk_fallback foreign key (parent_id) references parent(id) on delete set null on update no action)")
	require.Len(t, foreignKeys, 1)
	require.Equal(t, "fk_fallback", foreignKeys[0]["name"])
	require.Equal(t, "set null", foreignKeys[0]["on_delete"])
	require.Equal(t, "no action", foreignKeys[0]["on_update"])
}

func TestCreateForeignKeySetNullRequiresNullableChildColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table set_null_parent (id int primary key)")
	parsed, err := sqlparser.Parse("create table set_null_child (id int primary key, parent_id int not null, foreign key (parent_id) references set_null_parent(id) on delete set null)")
	require.NoError(t, err)
	ddl, ok := parsed.(*sqlparser.DDL)
	require.True(t, ok)
	require.Equal(t, "set null", strings.ToLower(ddl.TableSpec.ForeignKeys[0].OnDelete))
	columns := (&XMySQLExecutor{}).parseTableColumns(ddl.TableSpec)
	require.False(t, columns[1]["nullable"].(bool))
	err = execSQLExpectError(t, executor, "app", "create table set_null_child (id int primary key, parent_id int not null, foreign key (parent_id) references set_null_parent(id) on delete set null)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "must be nullable")
}

func TestAlterForeignKeySetNullRequiresNullableChildColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table alter_set_null_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table alter_set_null_child (id int primary key, parent_id int not null)")
	err := execSQLExpectError(t, executor, "app", "alter table alter_set_null_child add constraint fk_set_null foreign key (parent_id) references alter_set_null_parent(id) on update set null")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "must be nullable")
}

func TestForeignKeyConstraintSymbolMustBeUniqueWithinSchema(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table symbol_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table symbol_child_one (id int primary key, parent_id int, constraint fk_same_symbol foreign key (parent_id) references symbol_parent(id))")
	err := execSQLExpectError(t, executor, "app", "create table symbol_child_two (id int primary key, parent_id int, constraint fk_same_symbol foreign key (parent_id) references symbol_parent(id))")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "duplicate foreign key constraint")
}

func TestForeignKeyConstraintSymbolsCannotRepeatWithinOneTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table symbol_parent_a (id int primary key)")
	mustExecSQL(t, executor, "app", "create table symbol_parent_b (id int primary key)")
	err := execSQLExpectError(t, executor, "app", "create table symbol_child (id int primary key, a_id int, b_id int, constraint fk_same_symbol foreign key (a_id) references symbol_parent_a(id), constraint fk_same_symbol foreign key (b_id) references symbol_parent_b(id))")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "duplicate foreign key constraint")
}

func TestCreateForeignKeyRejectsMissingLocalColumn(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table missing_local_parent (id int primary key)")
	err := execSQLExpectError(t, executor, "app", "create table missing_local_child (id int primary key, foreign key (parent_id) references missing_local_parent(id))")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "parent_id")
}

func TestDropReferencedTableRejectsForeignKeyDependency(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table drop_child (id int primary key, parent_id int, constraint fk_drop_parent foreign key (parent_id) references drop_parent(id))")

	err := execSQLExpectError(t, executor, "app", "drop table drop_parent")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
	require.FileExists(t, filepath.Join(executor.conf.InnodbDataDir, "app", "drop_parent.frm"))
}

func TestForeignKeyChecksSessionVariableDisablesChildValidation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table fk_checks_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table fk_checks_child (id int primary key, parent_id int, constraint fk_checks_parent foreign key (parent_id) references fk_checks_parent(id))")

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "set foreign_key_checks = 0")
	result := <-executor.ExecuteQuery(session, "insert into fk_checks_child values (1, 999)", "app")
	require.NoError(t, result.Err)
	mustExecSessionSQL(t, executor, session, "app", "set foreign_key_checks = 1")
	result = <-executor.ExecuteQuery(session, "insert into fk_checks_child values (2, 999)", "app")
	require.Error(t, result.Err)
}

func TestUpdateRejectsInvalidForeignKeyValue(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table update_fk_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table update_fk_child (id int primary key, parent_id int, constraint fk_update_parent foreign key (parent_id) references update_fk_parent(id))")
	mustExecSQL(t, executor, "app", "insert into update_fk_parent values (1)")
	mustExecSQL(t, executor, "app", "insert into update_fk_child values (1, 1)")

	err := execSQLExpectError(t, executor, "app", "update update_fk_child set parent_id = 999 where id = 1")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select parent_id from update_fk_child where id = 1"))
}

func TestUpdateCanMoveRowToANewPrimaryKey(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table movable_primary (id int primary key, value varchar(20), index idx_value (value))")
	mustExecSQL(t, executor, "app", "insert into movable_primary values (1, 'before')")

	mustExecSQL(t, executor, "app", "update movable_primary set id = 2, value = 'after' where id = 1")
	require.Equal(t, [][]interface{}{{"2", "after"}}, mustQuerySQL(t, executor, "app", "select id, value from movable_primary"))
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from movable_primary where id = 1"))
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySQL(t, executor, "app", "select id from movable_primary where value = 'after'"))
}

func TestForeignKeyChecksSessionVariableAllowsDroppingReferencedTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_checks_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table drop_checks_child (id int primary key, parent_id int, constraint fk_drop_checks foreign key (parent_id) references drop_checks_parent(id))")

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "set foreign_key_checks = 0")
	result := <-executor.ExecuteQuery(session, "drop table drop_checks_parent", "app")
	require.NoError(t, result.Err)
	require.NoFileExists(t, filepath.Join(executor.conf.InnodbDataDir, "app", "drop_checks_parent.frm"))
}

func TestCheckConstraintChecksSessionVariableDisablesValidation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table check_switch (id int primary key, value int, check (value > 0))")

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "set check_constraint_checks = 0")
	result := <-executor.ExecuteQuery(session, "insert into check_switch values (1, -1)", "app")
	require.NoError(t, result.Err)
	mustExecSessionSQL(t, executor, session, "app", "set check_constraint_checks = 1")
	result = <-executor.ExecuteQuery(session, "insert into check_switch values (2, -1)", "app")
	require.Error(t, result.Err)
}

func TestForeignKeyChecksSessionVariableAllowsDroppingReferencedIndex(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_index_parent (id int, unique key uq_drop_index_parent (id))")
	mustExecSQL(t, executor, "app", "create table drop_index_child (id int primary key, parent_id int, constraint fk_drop_index_parent foreign key (parent_id) references drop_index_parent(id))")

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "set foreign_key_checks = 0")
	result := <-executor.ExecuteQuery(session, "alter table drop_index_parent drop index uq_drop_index_parent", "app")
	require.NoError(t, result.Err)
}

func TestForeignKeyChecksSessionVariableAllowsDroppingReferencedColumn(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_column_parent (id int, note int, unique key uq_drop_column_parent (id))")
	mustExecSQL(t, executor, "app", "create table drop_column_child (id int primary key, parent_id int, constraint fk_drop_column_parent foreign key (parent_id) references drop_column_parent(id))")

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "set foreign_key_checks = 0")
	result := <-executor.ExecuteQuery(session, "alter table drop_column_parent drop column id", "app")
	require.NoError(t, result.Err)
}

func TestUniqueChecksSessionVariableDisablesUniqueValidation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table unique_checks (id int primary key, code int unique)")
	mustExecSQL(t, executor, "app", "insert into unique_checks values (1, 10)")

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "set unique_checks = 0")
	result := <-executor.ExecuteQuery(session, "insert into unique_checks values (2, 10)", "app")
	require.NoError(t, result.Err)
	mustExecSessionSQL(t, executor, session, "app", "set unique_checks = 1")
	result = <-executor.ExecuteQuery(session, "insert into unique_checks values (3, 10)", "app")
	require.Error(t, result.Err)

	mustExecSQL(t, executor, "app", "create table unique_checks_updates (id int primary key, code int unique)")
	mustExecSQL(t, executor, "app", "insert into unique_checks_updates values (1, 10), (2, 20), (3, 30)")
	mustExecSessionSQL(t, executor, session, "app", "set unique_checks = 0")
	result = <-executor.ExecuteQuery(session, "update unique_checks_updates set code = 10 where id = 2", "app")
	require.NoError(t, result.Err)
	mustExecSessionSQL(t, executor, session, "app", "set unique_checks = 1")
	result = <-executor.ExecuteQuery(session, "update unique_checks_updates set code = 10 where id = 3", "app")
	require.Error(t, result.Err)
}

func TestDropReferencedPrimaryKeyRejectsForeignKeyDependency(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_primary_parent (id int primary key, note int)")
	mustExecSQL(t, executor, "app", "create table drop_primary_child (id int primary key, parent_id int, constraint fk_drop_primary foreign key (parent_id) references drop_primary_parent(id))")

	err := execSQLExpectError(t, executor, "app", "alter table drop_primary_parent drop primary key")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
}

func TestForeignKeyChecksSessionVariableAllowsDroppingReferencedPrimaryKey(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_primary_checks_parent (id int primary key, note int)")
	mustExecSQL(t, executor, "app", "create table drop_primary_checks_child (id int primary key, parent_id int, constraint fk_drop_primary_checks foreign key (parent_id) references drop_primary_checks_parent(id))")

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "set foreign_key_checks = 0")
	result := <-executor.ExecuteQuery(session, "alter table drop_primary_checks_parent drop primary key", "app")
	require.NoError(t, result.Err)
}

func TestCompositeForeignKeyEnforcesAndCascades(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id_a int, id_b int, primary key (id_a, id_b))")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_a int, parent_b int, foreign key (parent_a, parent_b) references parents(id_a, id_b) on delete cascade)")
	mustExecSQL(t, executor, "app", "insert into parents (id_a, id_b) values (1, 2)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_a, parent_b) values (1, 1, 2)")

	err := execSQLExpectError(t, executor, "app", "insert into children (id, parent_a, parent_b) values (2, 1, 9)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")

	mustExecSQL(t, executor, "app", "delete from parents where id_a = 1 and id_b = 2")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from children where id = 1"))
}

func TestForeignKeyRestrictAndSetNullDeleteActions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table restricted_children (id int primary key, parent_id int, foreign key (parent_id) references parents(id) on delete restrict)")
	mustExecSQL(t, executor, "app", "create table nullable_children (id int primary key, parent_id int, foreign key (parent_id) references parents(id) on delete set null)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1)")
	mustExecSQL(t, executor, "app", "insert into restricted_children (id, parent_id) values (1, 1)")
	mustExecSQL(t, executor, "app", "insert into nullable_children (id, parent_id) values (1, 1)")

	err := execSQLExpectError(t, executor, "app", "delete from parents where id = 1")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")

	mustExecSQL(t, executor, "app", "delete from restricted_children where id = 1")
	mustExecSQL(t, executor, "app", "delete from parents where id = 1")
	rows := mustQuerySQL(t, executor, "app", "select id, parent_id from nullable_children where id = 1")
	require.Len(t, rows, 1)
	require.Nil(t, rows[0][1])
}

func TestForeignKeyUpdateActions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table cascaded (id int primary key, parent_id int, foreign key (parent_id) references parents(id) on update cascade)")
	mustExecSQL(t, executor, "app", "create table nullable_child (id int primary key, parent_id int, foreign key (parent_id) references parents(id) on update set null)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1)")
	mustExecSQL(t, executor, "app", "insert into cascaded (id, parent_id) values (1, 1)")
	mustExecSQL(t, executor, "app", "insert into nullable_child (id, parent_id) values (1, 1)")

	mustExecSQL(t, executor, "app", "update parents set id = 2 where id = 1")
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySQL(t, executor, "app", "select parent_id from cascaded"))
	require.Equal(t, [][]interface{}{{nil}}, mustQuerySQL(t, executor, "app", "select parent_id from nullable_child"))
}

func TestForeignKeyCascadesPropagateAcrossMultipleLevels(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table root_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table middle_child (id int primary key, parent_id int, foreign key (parent_id) references root_parent(id) on delete cascade on update cascade)")
	mustExecSQL(t, executor, "app", "create table leaf_child (id int primary key, middle_id int, foreign key (middle_id) references middle_child(id) on delete cascade on update cascade)")
	mustExecSQL(t, executor, "app", "insert into root_parent values (1)")
	mustExecSQL(t, executor, "app", "insert into middle_child values (10, 1)")
	mustExecSQL(t, executor, "app", "insert into leaf_child values (100, 10)")

	mustExecSQL(t, executor, "app", "update root_parent set id = 2 where id = 1")
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySQL(t, executor, "app", "select parent_id from middle_child"))
	mustExecSQL(t, executor, "app", "update middle_child set id = 20 where id = 10")
	require.Equal(t, [][]interface{}{{"20"}}, mustQuerySQL(t, executor, "app", "select middle_id from leaf_child"))
	mustExecSQL(t, executor, "app", "delete from root_parent where id = 2")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from middle_child"))
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from leaf_child"))
}

func TestCheckConstraintRejectsFalseAndAllowsUnknown(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table checked_values (id int primary key, age int, check (age >= 0))")

	err := execSQLExpectError(t, executor, "app", "insert into checked_values (id, age) values (1, -1)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "check")
	mustExecSQL(t, executor, "app", "insert into checked_values (id, age) values (2, null)")
}

func TestInlineCheckConstraintIsPersistedAndEnforced(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table inline_checks (id int primary key, age int check (age >= 0))")
	mustExecSQL(t, executor, "app", "insert into inline_checks (id, age) values (1, 18)")

	err := execSQLExpectError(t, executor, "app", "insert into inline_checks (id, age) values (2, -1)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "check")
}

func TestNotEnforcedCheckConstraintIsPersistedButNotValidated(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table unenforced_checks (id int primary key, age int constraint chk_age check (age >= 0) not enforced)")
	mustExecSQL(t, executor, "app", "insert into unenforced_checks (id, age) values (1, -1)")

	rows := mustQuerySQL(t, executor, "app", "select constraint_name, check_clause, enforced from information_schema.check_constraints where constraint_schema = 'app' and table_name = 'unenforced_checks'")
	require.Equal(t, [][]interface{}{{"chk_age", "age >= 0", "NO"}}, rows)
}

func TestTableConstraintsIncludesCheckConstraints(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table table_constraints_checks (id int primary key, age int constraint chk_age check (age >= 0) not enforced, check (id > 0))")

	rows := mustQuerySQL(t, executor, "app", "select constraint_name, constraint_type, enforced from information_schema.table_constraints where constraint_schema = 'app' and table_name = 'table_constraints_checks' and constraint_type = 'CHECK'")
	require.ElementsMatch(t, [][]interface{}{{"chk_age", "CHECK", "NO"}, {"check_2", "CHECK", "YES"}}, rows)
}

func TestShowCreateTablePreservesCheckConstraintNamesAndEnforcement(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table show_check (id int primary key, age int constraint chk_age check (age >= 0) not enforced)")

	result := <-executor.ExecuteQuery(nil, "show create table show_check", "app")
	require.NoError(t, result.Err)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	definition := strings.ToLower(fmt.Sprint(rows[0][1]))
	require.Contains(t, definition, "constraint `chk_age` check (age >= 0) not enforced")
}

func TestTableConstraintsExcludesNonUniqueIndexes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table constraint_index_filter (id int primary key, value int, key idx_value (value))")

	rows := mustQuerySQL(t, executor, "app", "select constraint_name, constraint_type from information_schema.table_constraints where constraint_schema = 'app' and table_name = 'constraint_index_filter'")
	require.Equal(t, [][]interface{}{{"PRIMARY", "PRIMARY KEY"}}, rows)
}

func TestAlterNotEnforcedCheckConstraintIsPersistedButNotValidated(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table alter_checks (id int primary key, age int)")
	mustExecSQL(t, executor, "app", "alter table alter_checks add constraint chk_age check (age >= 0) not enforced")
	mustExecSQL(t, executor, "app", "insert into alter_checks (id, age) values (1, -1)")

	rows := mustQuerySQL(t, executor, "app", "select constraint_name, enforced from information_schema.check_constraints where constraint_schema = 'app' and table_name = 'alter_checks'")
	require.Equal(t, [][]interface{}{{"chk_age", "NO"}}, rows)
}

func TestAlterEnforcedCheckConstraintValidatesExistingRowsBeforePersisting(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table existing_check_rows (id int primary key, age int)")
	mustExecSQL(t, executor, "app", "insert into existing_check_rows values (1, -1)")

	err := execSQLExpectError(t, executor, "app", "alter table existing_check_rows add constraint chk_age check (age >= 0)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "check")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select constraint_name from information_schema.check_constraints where constraint_schema = 'app' and table_name = 'existing_check_rows'"))

	// The failed ALTER must not change the table's future DML behavior.
	mustExecSQL(t, executor, "app", "insert into existing_check_rows values (2, -2)")
}

func TestAlterCheckEnforcementCanBeToggled(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table toggle_checks (id int primary key, age int constraint chk_age check (age >= 0))")
	mustExecSQL(t, executor, "app", "alter table toggle_checks alter check chk_age not enforced")
	mustExecSQL(t, executor, "app", "insert into toggle_checks (id, age) values (1, -1)")
	require.Equal(t, [][]interface{}{{"NO"}}, mustQuerySQL(t, executor, "app", "select enforced from information_schema.check_constraints where constraint_schema = 'app' and table_name = 'toggle_checks' and constraint_name = 'chk_age'"))

	require.Error(t, execSQLExpectError(t, executor, "app", "alter table toggle_checks alter check chk_age enforced"))
	require.Equal(t, [][]interface{}{{"NO"}}, mustQuerySQL(t, executor, "app", "select enforced from information_schema.check_constraints where constraint_schema = 'app' and table_name = 'toggle_checks' and constraint_name = 'chk_age'"))
	mustExecSQL(t, executor, "app", "delete from toggle_checks where id = 1")
	mustExecSQL(t, executor, "app", "alter table toggle_checks alter check chk_age enforced")
	require.Equal(t, [][]interface{}{{"YES"}}, mustQuerySQL(t, executor, "app", "select enforced from information_schema.check_constraints where constraint_schema = 'app' and table_name = 'toggle_checks' and constraint_name = 'chk_age'"))
	require.Error(t, execSQLExpectError(t, executor, "app", "insert into toggle_checks (id, age) values (2, -2)"))
}

func TestAlterCheckEnforcementValidatesExistingRowsBeforeEnabling(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table toggle_existing_checks (id int primary key, age int)")
	mustExecSQL(t, executor, "app", "alter table toggle_existing_checks add constraint chk_age check (age >= 0) not enforced")
	mustExecSQL(t, executor, "app", "insert into toggle_existing_checks values (1, -1)")

	err := execSQLExpectError(t, executor, "app", "alter table toggle_existing_checks alter check chk_age enforced")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "check")
	require.Equal(t, [][]interface{}{{"NO"}}, mustQuerySQL(t, executor, "app", "select enforced from information_schema.check_constraints where constraint_schema = 'app' and table_name = 'toggle_existing_checks' and constraint_name = 'chk_age'"))
}

func TestAlterForeignKeyValidatesExistingRowsBeforePersisting(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table fk_existing_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table fk_existing_child (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into fk_existing_child values (1, 99)")

	err := execSQLExpectError(t, executor, "app", "alter table fk_existing_child add constraint fk_parent foreign key (parent_id) references fk_existing_parent(id)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select constraint_name from information_schema.key_column_usage where constraint_schema = 'app' and table_name = 'fk_existing_child' and constraint_name = 'fk_parent'"))

	// Once the existing data is repaired, the same ALTER succeeds and future
	// writes are checked by the newly persisted foreign key.
	mustExecSQL(t, executor, "app", "insert into fk_existing_parent values (99)")
	mustExecSQL(t, executor, "app", "alter table fk_existing_child add constraint fk_parent foreign key (parent_id) references fk_existing_parent(id)")
	require.Equal(t, [][]interface{}{{"fk_parent"}}, mustQuerySQL(t, executor, "app", "select constraint_name from information_schema.key_column_usage where constraint_schema = 'app' and table_name = 'fk_existing_child' and constraint_name = 'fk_parent'"))
	require.Error(t, execSQLExpectError(t, executor, "app", "insert into fk_existing_child values (2, 100)"))
}

func TestReferentialConstraintsExposeReferencedUniqueConstraint(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ref_parent (id int primary key, code int, unique key uq_parent_code (code))")
	mustExecSQL(t, executor, "app", "create table ref_child (id int primary key, parent_code int, constraint fk_parent_code foreign key (parent_code) references ref_parent(code))")

	rows := mustQuerySQL(t, executor, "app", "select constraint_name, unique_constraint_schema, unique_constraint_name, update_rule, delete_rule from information_schema.referential_constraints where constraint_schema = 'app' and table_name = 'ref_child'")
	require.Equal(t, [][]interface{}{{"fk_parent_code", "app", "uq_parent_code", "RESTRICT", "RESTRICT"}}, rows)
}

func TestAlterForeignKeyRequiresReferencedIndex(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table fk_index_parent (id int primary key, code int)")
	mustExecSQL(t, executor, "app", "create table fk_index_child (id int primary key, parent_code int)")

	err := execSQLExpectError(t, executor, "app", "alter table fk_index_child add constraint fk_code foreign key (parent_code) references fk_index_parent(code)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "index")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select constraint_name from information_schema.key_column_usage where constraint_schema = 'app' and table_name = 'fk_index_child' and constraint_name = 'fk_code'"))
}

func TestAlterForeignKeyRejectsMismatchedReferencedColumnType(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table fk_type_parent (id bigint primary key)")
	mustExecSQL(t, executor, "app", "create table fk_type_child (id int primary key, parent_id int)")

	err := execSQLExpectError(t, executor, "app", "alter table fk_type_child add constraint fk_type foreign key (parent_id) references fk_type_parent(id)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "type")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select constraint_name from information_schema.key_column_usage where constraint_schema = 'app' and table_name = 'fk_type_child' and constraint_name = 'fk_type'"))
}

func TestQualifiedAlterForeignKeyUsesQualifiedChildSchema(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "", "create database other")
	mustExecSQL(t, executor, "app", "create table qualified_fk_parent (id int primary key)")
	mustExecSQL(t, executor, "other", "create table qualified_fk_child (id int primary key, parent_id int)")

	mustExecSQL(t, executor, "other", "alter table app.qualified_fk_parent add index idx_parent_id (id)")
	result := <-executor.ExecuteQuery(nil, "alter table other.qualified_fk_child add constraint fk_qualified foreign key (parent_id) references app.qualified_fk_parent(id)", "app")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"fk_qualified"}}, mustQuerySQL(t, executor, "other", "select constraint_name from information_schema.key_column_usage where constraint_schema = 'other' and table_name = 'qualified_fk_child' and constraint_name = 'fk_qualified'"))

	result = <-executor.ExecuteQuery(nil, "alter table other.qualified_fk_child drop foreign key fk_qualified", "app")
	require.NoError(t, result.Err)
	require.Empty(t, mustQuerySQL(t, executor, "other", "select constraint_name from information_schema.key_column_usage where constraint_schema = 'other' and table_name = 'qualified_fk_child' and constraint_name = 'fk_qualified'"))
}

func TestCreateForeignKeyRequiresReferencedIndex(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table create_fk_parent (id int primary key, code int)")

	err := execSQLExpectError(t, executor, "app", "create table create_fk_child (id int primary key, parent_code int, constraint fk_code foreign key (parent_code) references create_fk_parent(code))")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "index")
	require.NoFileExists(t, filepath.Join(executor.conf.InnodbDataDir, "app", "create_fk_child.frm"))
}

func TestCreateForeignKeyRejectsSetDefaultAction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table set_default_fk_parent (id int primary key)")

	err := execSQLExpectError(t, executor, "app", "create table set_default_fk_child (id int primary key, parent_id int, constraint fk_set_default foreign key (parent_id) references set_default_fk_parent(id) on delete set default)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "set default")
	require.NoFileExists(t, filepath.Join(executor.conf.InnodbDataDir, "app", "set_default_fk_child.frm"))
}

func TestAlterForeignKeyRejectsSetDefaultAction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table alter_set_default_fk_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table alter_set_default_fk_child (id int primary key, parent_id int)")

	err := execSQLExpectError(t, executor, "app", "alter table alter_set_default_fk_child add constraint fk_alter_set_default foreign key (parent_id) references alter_set_default_fk_parent(id) on update set default")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "set default")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select constraint_name from information_schema.key_column_usage where table_schema = 'app' and table_name = 'alter_set_default_fk_child' and constraint_name = 'fk_alter_set_default'"))
}

func TestCreateForeignKeyRejectsMismatchedReferencedColumnType(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table create_fk_type_parent (id bigint primary key)")

	err := execSQLExpectError(t, executor, "app", "create table create_fk_type_child (id int primary key, parent_id int, constraint fk_type foreign key (parent_id) references create_fk_type_parent(id))")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "type")
	require.NoFileExists(t, filepath.Join(executor.conf.InnodbDataDir, "app", "create_fk_type_child.frm"))
}

func TestDropForeignKeyKeepsSharedAutomaticIndex(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table shared_fk_parent_a (id int primary key)")
	mustExecSQL(t, executor, "app", "create table shared_fk_parent_b (id int primary key)")
	mustExecSQL(t, executor, "app", "create table shared_fk_child (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "alter table shared_fk_child add constraint fk_shared_a foreign key (parent_id) references shared_fk_parent_a(id)")
	mustExecSQL(t, executor, "app", "alter table shared_fk_child add constraint fk_shared_b foreign key (parent_id) references shared_fk_parent_b(id)")

	mustExecSQL(t, executor, "app", "alter table shared_fk_child drop foreign key fk_shared_a")
	require.Equal(t, [][]interface{}{{"fk_shared_b"}}, mustQuerySQL(t, executor, "app", "select constraint_name from information_schema.key_column_usage where table_schema = 'app' and table_name = 'shared_fk_child' and constraint_name like 'fk_%'"))
	require.Equal(t, [][]interface{}{{"fk_shared_a"}}, mustQuerySQL(t, executor, "app", "select index_name from information_schema.statistics where table_schema = 'app' and table_name = 'shared_fk_child' and index_name = 'fk_shared_a'"))

	mustExecSQL(t, executor, "app", "alter table shared_fk_child drop foreign key fk_shared_b")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select index_name from information_schema.statistics where table_schema = 'app' and table_name = 'shared_fk_child' and index_name = 'fk_shared_a'"))
}

func TestForeignKeyChecksSessionVariableAllowsAddingForeignKeyWithExistingOrphan(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table fk_disabled_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table fk_disabled_child (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "insert into fk_disabled_child values (1, 99)")

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "set foreign_key_checks = 0")
	result := <-executor.ExecuteQuery(session, "alter table fk_disabled_child add constraint fk_disabled_parent foreign key (parent_id) references fk_disabled_parent(id)", "app")
	require.NoError(t, result.Err)
}

func TestCreateTableAcceptsAdvancedConstraintSyntax(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "inline references on delete cascade",
			sql:  "create table child (id int primary key, parent_id int references parent(id) on delete cascade)",
		},
		{
			name: "inline references on update cascade",
			sql:  "create table child (id int primary key, parent_id int references parent(id) on update cascade)",
		},
		{
			name: "check",
			sql:  "create table checked_values (id int primary key, age int check (age >= 0))",
		},
		{
			name: "fulltext",
			sql:  "create table docs (id int primary key, content text, fulltext index idx_content (content))",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executor := newTestStorageIntegratedExecutor(t, t.TempDir())
			mustExecSQL(t, executor, "", "create database app")
			if strings.Contains(strings.ToLower(tc.sql), " references ") {
				mustExecSQL(t, executor, "app", "create table parent (id int primary key)")
			}
			mustExecSQL(t, executor, "app", tc.sql)
		})
	}
}

func TestInlineForeignKeyIsEnforcedAndCascades(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table inline_fk_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table inline_fk_child (id int primary key, parent_id int references inline_fk_parent(id) on delete cascade)")
	mustExecSQL(t, executor, "app", "insert into inline_fk_parent (id) values (1)")
	mustExecSQL(t, executor, "app", "insert into inline_fk_child (id, parent_id) values (1, 1)")

	err := execSQLExpectError(t, executor, "app", "insert into inline_fk_child (id, parent_id) values (2, 99)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")

	mustExecSQL(t, executor, "app", "delete from inline_fk_parent where id = 1")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from inline_fk_child where id = 1"))
}

func TestUpdateRejectsNullForNotNullColumn(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, username varchar(50) not null)")
	mustExecSQL(t, executor, "app", "insert into users (id, username) values (1, 'alice')")

	err := execSQLExpectError(t, executor, "app", "update users set username = null where id = 1")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "not null")
}

func TestUpdateRejectsPrimaryAndUniqueKeyConflicts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sql      string
		contains string
	}{
		{
			name:     "primary key conflict",
			sql:      "update users set id = 2 where id = 1",
			contains: "primary",
		},
		{
			name:     "unique key conflict",
			sql:      "update users set email = 'bob@example.com' where id = 1",
			contains: "email",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executor := newTestStorageIntegratedExecutor(t, t.TempDir())
			mustExecSQL(t, executor, "", "create database app")
			mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(100) unique)")
			mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, 'alice@example.com')")
			mustExecSQL(t, executor, "app", "insert into users (id, email) values (2, 'bob@example.com')")

			err := execSQLExpectError(t, executor, "app", tc.sql)
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), tc.contains)
			require.Contains(t, strings.ToLower(err.Error()), "duplicate")
		})
	}
}

func TestCompositeUniqueIndexEnforcesMySQLNullSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table tenant_emails (tenant_id int, email varchar(100), unique key uq_tenant_email (tenant_id, email))")
	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	dml.SetDataDir(executor.conf.InnodbDataDir)
	dml.schemaName, dml.tableName = "app", "tenant_emails"
	meta, metaErr := dml.getTableMetadata()
	require.NoError(t, metaErr)
	require.Len(t, meta.Indices, 1)
	require.Equal(t, []string{"tenant_id", "email"}, meta.Indices[0].Columns)
	require.True(t, meta.Indices[0].Unique)
	mustExecSQL(t, executor, "app", "insert into tenant_emails (tenant_id, email) values (7, 'alice@example.com')")

	err := execSQLExpectError(t, executor, "app", "insert into tenant_emails (tenant_id, email) values (7, 'alice@example.com')")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "duplicate")

	// The same secondary unique key may be reused in another tenant.
	mustExecSQL(t, executor, "app", "insert into tenant_emails (tenant_id, email) values (8, 'alice@example.com')")
	// MySQL UNIQUE indexes permit more than one NULL key component.
	mustExecSQL(t, executor, "app", "insert into tenant_emails (tenant_id, email) values (7, null), (7, null)")
}

func TestUpdateWithoutPrimaryKeyUsesHiddenRowID(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (email varchar(100), name varchar(50))")
	mustExecSQL(t, executor, "app", "insert into users (email, name) values ('alice@example.com', 'Alice')")

	mustExecSQL(t, executor, "app", "update users set name = 'Alice2' where email = 'alice@example.com'")
	require.Equal(t, [][]interface{}{{"Alice2"}}, mustQuerySQL(t, executor, "app", "select name from users where email = 'alice@example.com'"))
}

func TestInsertPlainTableWithoutPrimaryKeyStillWorks(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (email varchar(100), name varchar(50))")

	mustExecSQL(t, executor, "app", "insert into users (email, name) values ('alice@example.com', 'Alice')")
}

func TestInsertIndexedTableWithoutPrimaryKeyUsesHiddenRowID(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int, email varchar(100) unique)")

	mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, 'alice@example.com')")
	err := execSQLExpectError(t, executor, "app", "insert into users (id, email) values (2, 'alice@example.com')")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "duplicate")
}

func TestInsertSecondaryIndexedTableWithoutPrimaryKeyUsesHiddenRowID(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (email varchar(100), index idx_email (email))")

	mustExecSQL(t, executor, "app", "insert into users (email) values ('alice@example.com')")
	require.Equal(t, [][]interface{}{{"alice@example.com"}}, mustQuerySQL(t, executor, "app", "select email from users where email = 'alice@example.com'"))
}

func TestCompositePrimaryKeyUpdateDeleteUseEncodedKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "update",
			sql:  "update memberships set score = 30 where user_id = 1 and group_id = 10",
		},
		{
			name: "delete",
			sql:  "delete from memberships where user_id = 1 and group_id = 10",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executor := newTestStorageIntegratedExecutor(t, t.TempDir())
			mustExecSQL(t, executor, "", "create database app")
			mustExecSQL(t, executor, "app", "create table memberships (user_id int, group_id int, score int, primary key (user_id, group_id))")
			mustExecSQL(t, executor, "app", "insert into memberships (user_id, group_id, score) values (1, 10, 10)")
			mustExecSQL(t, executor, "app", "insert into memberships (user_id, group_id, score) values (1, 20, 20)")

			mustExecSQL(t, executor, "app", tc.sql)
		})
	}
}

func TestCrossDatabaseForeignKeyEnforcesReferencedRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database parent_db")
	mustExecSQL(t, executor, "", "create database child_db")
	mustExecSQL(t, executor, "parent_db", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "child_db", "create table children (id int primary key, parent_id int, constraint fk_parent foreign key (parent_id) references parent_db.parents(id) on delete cascade on update cascade)")
	mustExecSQL(t, executor, "parent_db", "insert into parents values (7)")

	mustExecSQL(t, executor, "child_db", "insert into children values (1, 7)")
	require.Equal(t, [][]interface{}{{"parent_db"}}, mustQuerySQL(t, executor, "child_db", "select referenced_table_schema from information_schema.key_column_usage where table_schema = 'child_db' and table_name = 'children' and constraint_name = 'fk_parent'"))
	require.Equal(t, [][]interface{}{{"parent_db"}}, mustQuerySQL(t, executor, "child_db", "select unique_constraint_schema from information_schema.referential_constraints where constraint_schema = 'child_db' and table_name = 'children' and constraint_name = 'fk_parent'"))
	err := execSQLExpectError(t, executor, "child_db", "insert into children values (2, 99)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
	mustExecSQL(t, executor, "parent_db", "update parents set id = 8 where id = 7")
	require.Equal(t, [][]interface{}{{"8"}}, mustQuerySQL(t, executor, "child_db", "select parent_id from children where id = 1"))
	mustExecSQL(t, executor, "parent_db", "delete from parents where id = 8")
	require.Empty(t, mustQuerySQL(t, executor, "child_db", "select id from children"))
}

func TestCrossDatabaseForeignKeyCanBeAddedByAlter(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database parent_db")
	mustExecSQL(t, executor, "", "create database child_db")
	mustExecSQL(t, executor, "parent_db", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "parent_db", "insert into parents values (7)")
	mustExecSQL(t, executor, "child_db", "create table children (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "child_db", "insert into children values (1, 7)")

	mustExecSQL(t, executor, "child_db", "alter table children add constraint fk_parent foreign key (parent_id) references parent_db.parents(id)")
	err := execSQLExpectError(t, executor, "parent_db", "drop table parents")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
	err = execSQLExpectError(t, executor, "child_db", "insert into children values (2, 99)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
}

func execSQLExpectError(t *testing.T, executor *XMySQLEngine, databaseName, sql string) error {
	t.Helper()
	got := <-executor.ExecuteQuery(nil, sql, databaseName)
	return got.Err
}
