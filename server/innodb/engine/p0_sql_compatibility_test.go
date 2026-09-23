package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestP0InsertOnDuplicateKeyUpdate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, username varchar(50) unique, age int)")
	mustExecSQL(t, executor, "app", "insert into users (id, username, age) values (1, 'alice', 20)")

	mustExecSQL(t, executor, "app", "insert into users (id, username, age) values (1, 'alice', 21) on duplicate key update age = 22")

	require.Equal(t, [][]interface{}{{"alice", "22"}}, mustQuerySQL(t, executor, "app", "select username, age from users where id = 1"))
	require.Equal(t, [][]interface{}{{"\x00\x00\x00\x00\x00\x00\x00\x01"}}, mustQuerySQL(t, executor, "app", "select count(*) from users"))
}

func TestP0DMLResultReportsMySQLAffectedRowsAndWarnings(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key auto_increment, username varchar(3) unique, age int)")

	inserted := <-executor.ExecuteQuery(nil, "insert into users (id, username, age) values (1, 'bob', 20)", "app")
	require.NoError(t, inserted.Err)
	require.Equal(t, 1, inserted.AffectedRows)
	require.NotZero(t, inserted.LastInsertID)

	changed := <-executor.ExecuteQuery(nil, "insert into users (id, username, age) values (1, 'bob', 21) on duplicate key update age = 22", "app")
	require.NoError(t, changed.Err)
	require.Equal(t, 2, changed.AffectedRows)

	noOp := <-executor.ExecuteQuery(nil, "insert into users (id, username, age) values (1, 'bob', 22) on duplicate key update age = 22", "app")
	require.NoError(t, noOp.Err)
	require.Equal(t, 0, noOp.AffectedRows)

	replaced := <-executor.ExecuteQuery(nil, "replace into users (id, username, age) values (1, 'joe', 30)", "app")
	require.NoError(t, replaced.Err)
	require.Equal(t, 2, replaced.AffectedRows)

	ignored := <-executor.ExecuteQuery(nil, "insert ignore into users (id, username, age) values (2, 'long-name', 40)", "app")
	require.NoError(t, ignored.Err)
	require.Equal(t, 1, ignored.AffectedRows)
	require.Len(t, ignored.Warnings, 1)
	require.Equal(t, uint16(1265), ignored.Warnings[0].Code)
	require.Equal(t, [][]interface{}{{"lon"}}, mustQuerySQL(t, executor, "app", "select username from users where id = 2"))
}

func TestP0InsertIgnoreWarningsRemainVisibleToShowWarnings(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table users (id int primary key, name varchar(3))")
	result := <-executor.ExecuteQuery(session, "insert ignore into users values (1, 'long-name')", "app")
	require.NoError(t, result.Err)
	require.Len(t, result.Warnings, 1)

	warnings := <-executor.ExecuteQuery(session, "show warnings", "app")
	require.NoError(t, warnings.Err)
	data, ok := warnings.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"Warning", uint16(1265), "Data truncated for column 'name'"}}, rows)
}

func TestP0InsertIgnoreInvalidIntegerProducesWarningAndZero(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table numeric_values (id int primary key, amount int)")
	result := <-executor.ExecuteQuery(session, "insert ignore into numeric_values values (1, 'not-a-number')", "app")
	require.NoError(t, result.Err)
	require.Len(t, result.Warnings, 1)
	require.Equal(t, uint16(1366), result.Warnings[0].Code)
	require.Equal(t, [][]interface{}{{"0"}}, mustQuerySessionSQL(t, executor, session, "app", "select amount from numeric_values"))
}

func TestP0UpdateInvalidIntegerIsRejected(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table numeric_values (id int primary key, amount int)")
	mustExecSessionSQL(t, executor, session, "app", "insert into numeric_values values (1, 7)")
	result := <-executor.ExecuteQuery(session, "update numeric_values set amount = 'not-a-number' where id = 1", "app")
	require.Error(t, result.Err)
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySessionSQL(t, executor, session, "app", "select amount from numeric_values"))
}

func TestP0UpdateWithoutWhereUpdatesAllRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table numeric_values (id int primary key, amount int)")
	mustExecSQL(t, executor, "app", "insert into numeric_values values (1, 7), (2, 8)")
	mustExecSQL(t, executor, "app", "update numeric_values set amount = amount + 1")
	require.Equal(t, [][]interface{}{{"1", "8"}, {"2", "9"}}, mustQuerySQL(t, executor, "app", "select id, amount from numeric_values order by id"))
}

func TestP0InsertIgnoreInvalidDecimalProducesWarningAndZero(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table decimal_values (id int primary key, amount decimal(10,2))")
	result := <-executor.ExecuteQuery(session, "insert ignore into decimal_values values (1, 'not-a-number'), (2, '12.50')", "app")
	require.NoError(t, result.Err)
	require.Len(t, result.Warnings, 1)
	require.Equal(t, uint16(1366), result.Warnings[0].Code)
	require.Equal(t, [][]interface{}{{"1", "0"}, {"2", "12.5"}}, mustQuerySessionSQL(t, executor, session, "app", "select id, amount from decimal_values order by id"))
}

func TestP0InsertIgnoreInvalidDateProducesWarningAndZeroDate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table date_values (id int primary key, happened date)")
	result := <-executor.ExecuteQuery(session, "insert ignore into date_values values (1, 'bad-date'), (2, '2026-08-23')", "app")
	require.NoError(t, result.Err)
	require.Len(t, result.Warnings, 1)
	require.Equal(t, uint16(1292), result.Warnings[0].Code)
	require.Equal(t, [][]interface{}{{"1", "0000-00-00"}, {"2", "2026-08-23"}}, mustQuerySessionSQL(t, executor, session, "app", "select id, happened from date_values order by id"))
}

func TestP0YearAcceptsNumericLiteral(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table year_values (id int primary key, year_value year)")
	mustExecSQL(t, executor, "app", "insert into year_values values (1, 2024)")
	require.Equal(t, [][]interface{}{{"2024"}}, mustQuerySQL(t, executor, "app", "select year_value from year_values"))
}

func TestP0CharAndBinaryColumnsKeepFixedLengthSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table fixed_values (id int primary key, label char(5), token binary(4), name varchar(5), raw varbinary(4))")
	mustExecSQL(t, executor, "app", "insert into fixed_values values (1, 'ab  ', 'xy', 'ab', 'xy')")
	record := mustQuerySQL(t, executor, "app", "select label, token, name, raw from fixed_values")
	require.Equal(t, "ab", record[0][0])
	require.Equal(t, "xy\x00\x00", record[0][1])
	require.Equal(t, "ab", record[0][2])
	require.Equal(t, "xy", record[0][3])
}

func TestP0EnumAndSetValuesValidateAndCanonicalize(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table enum_values (id int primary key, state enum('new','done'), tags set('a','b'))")
	mustExecSessionSQL(t, executor, session, "app", "insert into enum_values values (1, 'done', 'b,a')")
	require.Equal(t, [][]interface{}{{"done", "a,b"}}, mustQuerySessionSQL(t, executor, session, "app", "select state, tags from enum_values"))
	invalid := <-executor.ExecuteQuery(session, "insert into enum_values values (2, 'invalid', 'a')", "app")
	require.Error(t, invalid.Err)
}

func TestP0InsertIgnoreEnumAndSetProducesWarning(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table enum_values (id int primary key, state enum('new','done'), tags set('a','b'))")
	result := <-executor.ExecuteQuery(session, "insert ignore into enum_values values (1, 'invalid', 'a,c')", "app")
	require.NoError(t, result.Err)
	require.Len(t, result.Warnings, 2)
	require.Equal(t, [][]interface{}{{"", "a"}}, mustQuerySessionSQL(t, executor, session, "app", "select state, tags from enum_values"))
}

func TestP0NumericOverflowIsRejectedInStrictMode(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table bounded_values (id int primary key, unsigned_value tinyint unsigned, signed_value tinyint, amount decimal(5,2))")
	result := <-executor.ExecuteQuery(session, "insert into bounded_values values (1, 256, -129, 1000.00)", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "Out of range")
}

func TestP0InsertIgnoreNumericOverflowClampsAndWarns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table bounded_values (id int primary key, unsigned_value tinyint unsigned, signed_value tinyint, amount decimal(5,2))")
	result := <-executor.ExecuteQuery(session, "insert ignore into bounded_values values (1, 256, -129, 1000.00)", "app")
	require.NoError(t, result.Err)
	require.Len(t, result.Warnings, 3)
	require.Equal(t, [][]interface{}{{"255", "-128", "999.99"}}, mustQuerySessionSQL(t, executor, session, "app", "select unsigned_value, signed_value, amount from bounded_values"))
}

func TestP0NonStrictSQLModeConvertsInvalidIntegerToWarning(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("sql_mode", "")
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table mode_values (id int primary key, amount int)")
	result := <-executor.ExecuteQuery(session, "insert into mode_values values (1, 'not-a-number')", "app")
	require.NoError(t, result.Err)
	require.Len(t, result.Warnings, 1)
	require.Equal(t, uint16(1366), result.Warnings[0].Code)
	require.Equal(t, [][]interface{}{{"0"}}, mustQuerySessionSQL(t, executor, session, "app", "select amount from mode_values"))
}

func TestP0SQLModeControlsZeroDateConversion(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table dates (id int primary key, happened date)")
	session.SetParamByName("sql_mode", "STRICT_TRANS_TABLES")
	mustExecSessionSQL(t, executor, session, "app", "insert into dates values (1, '0000-00-00')")
	require.Equal(t, [][]interface{}{{"0000-00-00"}}, mustQuerySessionSQL(t, executor, session, "app", "select happened from dates"))

	session.SetParamByName("sql_mode", "STRICT_TRANS_TABLES,NO_ZERO_DATE")
	result := <-executor.ExecuteQuery(session, "insert into dates values (2, '0000-00-00')", "app")
	require.Error(t, result.Err)

	session.SetParamByName("sql_mode", "STRICT_TRANS_TABLES")
	mustExecSessionSQL(t, executor, session, "app", "update dates set happened = '0000-00-00' where id = 1")
	mustExecSessionSQL(t, executor, session, "app", "insert into dates values (3, '2024-00-01')")
	session.SetParamByName("sql_mode", "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE")
	result = <-executor.ExecuteQuery(session, "insert into dates values (4, '2024-00-01')", "app")
	require.Error(t, result.Err)
}

func TestP0InsertOnDuplicateMixedBatchUpdatesAndInserts(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, username varchar(50) unique, age int)")
	mustExecSQL(t, executor, "app", "insert into users (id, username, age) values (1, 'alice', 20)")

	mustExecSQL(t, executor, "app", "insert into users (id, username, age) values (1, 'alice', 21), (2, 'bob', 30) on duplicate key update age = 22")

	require.Equal(t, [][]interface{}{{"1", "alice", "22"}, {"2", "bob", "30"}}, mustQuerySQL(t, executor, "app", "select id, username, age from users order by id"))
}

func TestP0ReplaceIntoReplacesExistingPrimaryKey(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, username varchar(50), age int)")
	mustExecSQL(t, executor, "app", "insert into users (id, username, age) values (1, 'alice', 20)")

	mustExecSQL(t, executor, "app", "replace into users (id, username, age) values (1, 'alice2', 30)")

	require.Equal(t, [][]interface{}{{"alice2", "30"}}, mustQuerySQL(t, executor, "app", "select username, age from users where id = 1"))
	require.Equal(t, [][]interface{}{{"\x00\x00\x00\x00\x00\x00\x00\x01"}}, mustQuerySQL(t, executor, "app", "select count(*) from users"))
}

func TestP0InsertSelectCopiesRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_users (id int primary key, username varchar(50), age int)")
	mustExecSQL(t, executor, "app", "create table copied_users (id int primary key, username varchar(50), age int)")
	mustExecSQL(t, executor, "app", "insert into source_users (id, username, age) values (1, 'alice', 20), (2, 'bob', 30)")

	mustExecSQL(t, executor, "app", "insert into copied_users (id, username, age) select id, username, age from source_users where age >= 20")

	require.Equal(t, [][]interface{}{{"1", "alice", "20"}, {"2", "bob", "30"}}, mustQuerySQL(t, executor, "app", "select id, username, age from copied_users order by id"))
}

func TestP0InsertSelectEvaluatesRowExpressions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_users (id int primary key, username varchar(50), age int)")
	mustExecSQL(t, executor, "app", "create table copied_users (id int primary key, username varchar(50), age int)")
	mustExecSQL(t, executor, "app", "insert into source_users (id, username, age) values (1, 'alice', 20), (2, 'bob', 30)")

	mustExecSQL(t, executor, "app", "insert into copied_users (id, username, age) select u.id + 10, upper(u.username), u.age + 1 from source_users as u where u.age >= 20")

	require.Equal(t, [][]interface{}{{"11", "ALICE", "21"}, {"12", "BOB", "31"}}, mustQuerySQL(t, executor, "app", "select id, username, age from copied_users order by id"))
}
