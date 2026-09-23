package engine

import "testing"

func TestP1InsertSelectSupportsJoinProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(50))")
	mustExecSQL(t, executor, "app", "create table scores (user_id int primary key, score int)")
	mustExecSQL(t, executor, "app", "create table leaderboard (id int primary key, label varchar(50), score int)")
	mustExecSQL(t, executor, "app", "insert into users values (1, 'alice'), (2, 'bob')")
	mustExecSQL(t, executor, "app", "insert into scores values (1, 95), (2, 80)")

	mustExecSQL(t, executor, "app", "insert into leaderboard (id, label, score) select u.id, concat(u.name, ':rank'), s.score from users as u join scores as s on s.user_id = u.id where s.score >= 90")

	if got := mustQuerySQL(t, executor, "app", "select id, label, score from leaderboard order by id"); len(got) != 1 || got[0][0] != "1" || got[0][1] != "alice:rank" || got[0][2] != "95" {
		t.Fatalf("unexpected leaderboard rows: %#v", got)
	}
}

func TestP1InsertSelectSupportsDerivedTableSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_users (id int primary key, username varchar(50), age int)")
	mustExecSQL(t, executor, "app", "create table selected_users (id int primary key, username varchar(50))")
	mustExecSQL(t, executor, "app", "insert into source_users values (1, 'alice', 20), (2, 'bob', 16)")

	mustExecSQL(t, executor, "app", "insert into selected_users (id, username) select filtered.id, filtered.username from (select id, username from source_users where age >= 18) as filtered")

	if got := mustQuerySQL(t, executor, "app", "select id, username from selected_users order by id"); len(got) != 1 || got[0][0] != "1" || got[0][1] != "alice" {
		t.Fatalf("unexpected selected users: %#v", got)
	}
}

func TestP1InsertSelectSupportsGroupedAggregateSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_scores (user_id int, score int)")
	mustExecSQL(t, executor, "app", "create table score_totals (user_id int primary key, total int)")
	mustExecSQL(t, executor, "app", "insert into source_scores values (1, 10), (1, 20), (2, 5)")

	mustExecSQL(t, executor, "app", "insert into score_totals (user_id, total) select user_id, sum(score) from source_scores group by user_id order by user_id")

	if got := mustQuerySQL(t, executor, "app", "select user_id, total from score_totals order by user_id"); len(got) != 2 || got[0][0] != "1" || got[0][1] != "30" || got[1][0] != "2" || got[1][1] != "5" {
		t.Fatalf("unexpected score totals: %#v", got)
	}
}

func TestP1InsertSelectSupportsCommaJoinSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_users (id int primary key, username varchar(50))")
	mustExecSQL(t, executor, "app", "create table labels (user_id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "create table user_labels (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "insert into source_users values (1, 'alice'), (2, 'bob')")
	mustExecSQL(t, executor, "app", "insert into labels values (1, 'admin'), (3, 'orphan')")

	mustExecSQL(t, executor, "app", "insert into user_labels (id, label) select u.id, l.label from source_users as u, labels as l where u.id = l.user_id")

	if got := mustQuerySQL(t, executor, "app", "select id, label from user_labels order by id"); len(got) != 1 || got[0][0] != "1" || got[0][1] != "admin" {
		t.Fatalf("unexpected user labels: %#v", got)
	}
}

func TestP1InsertSelectSupportsUsingJoinSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_users (id int primary key, username varchar(50))")
	mustExecSQL(t, executor, "app", "create table user_labels (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "create table selected_labels (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "insert into source_users values (1, 'alice'), (2, 'bob')")
	mustExecSQL(t, executor, "app", "insert into user_labels values (1, 'admin'), (3, 'orphan')")

	mustExecSQL(t, executor, "app", "insert into selected_labels (id, label) select u.id, l.label from source_users as u join user_labels as l using (id)")

	if got := mustQuerySQL(t, executor, "app", "select id, label from selected_labels order by id"); len(got) != 1 || got[0][0] != "1" || got[0][1] != "admin" {
		t.Fatalf("unexpected selected labels: %#v", got)
	}
}

func TestP1InsertSelectSupportsUnionSource(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table first_source (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "create table second_source (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "create table union_target (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "insert into first_source values (1, 'first')")
	mustExecSQL(t, executor, "app", "insert into second_source values (2, 'second')")

	mustExecSQL(t, executor, "app", "insert into union_target (id, label) select id, label from first_source union all select id, label from second_source")

	if got := mustQuerySQL(t, executor, "app", "select id, label from union_target order by id"); len(got) != 2 || got[0][0] != "1" || got[0][1] != "first" || got[1][0] != "2" || got[1][1] != "second" {
		t.Fatalf("unexpected union target: %#v", got)
	}
}

func TestP1InsertSelectUnionSourceRemovesDuplicatesByDefault(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table first_source (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "create table second_source (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "create table union_target (id int primary key, label varchar(50))")
	mustExecSQL(t, executor, "app", "insert into first_source values (1, 'same')")
	mustExecSQL(t, executor, "app", "insert into second_source values (1, 'same')")

	mustExecSQL(t, executor, "app", "insert into union_target (id, label) select id, label from first_source union select id, label from second_source")

	if got := mustQuerySQL(t, executor, "app", "select id, label from union_target"); len(got) != 1 || got[0][0] != "1" || got[0][1] != "same" {
		t.Fatalf("unexpected distinct union target: %#v", got)
	}
}

func TestP1UpdateJoinOnlyUpdatesMatchingTargetRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key, user_id int, state varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users values (1, 1), (2, 0)")
	mustExecSQL(t, executor, "app", "insert into accounts values (10, 1, 'pending'), (20, 2, 'pending')")

	mustExecSQL(t, executor, "app", "update accounts as a join users as u on u.id = a.user_id set a.state = 'active' where u.active = 1")

	if got := mustQuerySQL(t, executor, "app", "select id, state from accounts order by id"); len(got) != 2 || got[0][0] != "10" || got[0][1] != "active" || got[1][0] != "20" || got[1][1] != "pending" {
		t.Fatalf("unexpected update join rows: %#v", got)
	}
}

func TestP1UpdateJoinCanProjectJoinedSourceValues(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, display_name varchar(50), active int)")
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key, user_id int, state varchar(50))")
	mustExecSQL(t, executor, "app", "insert into users values (1, 'Alice', 1), (2, 'Bob', 0)")
	mustExecSQL(t, executor, "app", "insert into accounts values (10, 1, 'pending'), (20, 2, 'pending')")

	mustExecSQL(t, executor, "app", "update accounts as a join users as u on u.id = a.user_id set a.state = u.display_name where u.active = 1")

	if got := mustQuerySQL(t, executor, "app", "select id, state from accounts order by id"); len(got) != 2 || got[0][0] != "10" || got[0][1] != "Alice" || got[1][0] != "20" || got[1][1] != "pending" {
		t.Fatalf("unexpected source-value update join rows: %#v", got)
	}
}

func TestP1DeleteJoinOnlyDeletesMatchingTargetRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key, user_id int, state varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users values (1, 1), (2, 0)")
	mustExecSQL(t, executor, "app", "insert into accounts values (10, 1, 'pending'), (20, 2, 'pending')")

	mustExecSQL(t, executor, "app", "delete a from accounts as a join users as u on u.id = a.user_id where u.active = 1")

	if got := mustQuerySQL(t, executor, "app", "select id, state from accounts order by id"); len(got) != 1 || got[0][0] != "20" || got[0][1] != "pending" {
		t.Fatalf("unexpected delete join rows: %#v", got)
	}
}

func TestP1DeleteUsingOnlyDeletesMatchingTargetRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key, user_id int, state varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users values (1, 1), (2, 0)")
	mustExecSQL(t, executor, "app", "insert into accounts values (10, 1, 'pending'), (20, 2, 'pending')")

	mustExecSQL(t, executor, "app", "delete from accounts using accounts as a join users as u on u.id = a.user_id where u.active = 1")

	if got := mustQuerySQL(t, executor, "app", "select id, state from accounts order by id"); len(got) != 1 || got[0][0] != "20" || got[0][1] != "pending" {
		t.Fatalf("unexpected delete using rows: %#v", got)
	}
}

func TestP1CreateTableAsSelectHonorsExplicitColumnDefinitions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_users (id int primary key, username varchar(50))")
	mustExecSQL(t, executor, "app", "insert into source_users values (1, 'alice')")

	mustExecSQL(t, executor, "app", "create table copied_users (new_id bigint, label varchar(20)) as select id, username from source_users")

	if got := mustQuerySQL(t, executor, "app", "select new_id, label from copied_users"); len(got) != 1 || got[0][0] != "1" || got[0][1] != "alice" {
		t.Fatalf("unexpected explicit CTAS rows: %#v", got)
	}
}

func TestP1UpdateJoinDeduplicatesCompositeTargetKeysAndHonorsLimit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table accounts (tenant_id int, account_id int, state varchar(20), primary key (tenant_id, account_id))")
	mustExecSQL(t, executor, "app", "create table flags (tenant_id int, account_id int, enabled int)")
	mustExecSQL(t, executor, "app", "insert into accounts values (1, 10, 'pending'), (1, 20, 'pending')")
	mustExecSQL(t, executor, "app", "insert into flags values (1, 10, 1), (1, 10, 1), (1, 20, 1)")

	mustExecSQL(t, executor, "app", "update accounts as a join flags as f on f.tenant_id = a.tenant_id and f.account_id = a.account_id set a.state = 'enabled' where f.enabled = 1 order by a.account_id desc limit 1")

	if got := mustQuerySQL(t, executor, "app", "select tenant_id, account_id, state from accounts order by account_id"); len(got) != 2 || got[0][2] != "pending" || got[1][2] != "enabled" {
		t.Fatalf("unexpected composite update join rows: %#v", got)
	}
}

func TestP1UpdateJoinSupportsTargetWithoutPrimaryKey(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table accounts (user_id int, state varchar(20))")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "insert into accounts values (1, null), (2, 'pending')")
	mustExecSQL(t, executor, "app", "insert into users values (1, 1), (2, 0)")

	mustExecSQL(t, executor, "app", "update accounts as a join users as u on u.id = a.user_id set a.state = 'active' where u.active = 1")

	if got := mustQuerySQL(t, executor, "app", "select user_id, state from accounts order by user_id"); len(got) != 2 || got[0][0] != "1" || got[0][1] != "active" || got[1][0] != "2" || got[1][1] != "pending" {
		t.Fatalf("unexpected no-primary-key update join rows: %#v", got)
	}
}

func TestP1DeleteJoinSupportsTargetWithoutPrimaryKey(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table accounts (user_id int, state varchar(20))")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, active int)")
	mustExecSQL(t, executor, "app", "insert into accounts values (1, 'pending'), (2, 'pending')")
	mustExecSQL(t, executor, "app", "insert into users values (1, 1), (2, 0)")

	mustExecSQL(t, executor, "app", "delete a from accounts as a join users as u on u.id = a.user_id where u.active = 1")

	if got := mustQuerySQL(t, executor, "app", "select user_id, state from accounts order by user_id"); len(got) != 1 || got[0][0] != "2" || got[0][1] != "pending" {
		t.Fatalf("unexpected no-primary-key delete join rows: %#v", got)
	}
}
