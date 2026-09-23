package engine

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestP0SelectUsesDurableSecondaryIndexAfterRestart(t *testing.T) {
	tmp := t.TempDir()
	first := newP0SecondaryIndexTestEngine(tmp)
	mustExecSQL(t, first, "", "create database app")
	mustExecSQL(t, first, "app", "create table users (id int primary key, email varchar(100), name varchar(50), index idx_email (email))")
	mustExecSQL(t, first, "app", "insert into users (id, email, name) values (1, 'a@example.com', 'alice')")
	mustExecSQL(t, first, "app", "insert into users (id, email, name) values (2, 'b@example.com', 'bob')")
	require.NoError(t, first.Close())

	second := newP0SecondaryIndexTestEngine(tmp)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, second, tmp)
	rows := mustQueryRows(t, selectExecutor, "app", "select id, name from users where email = 'b@example.com'")
	require.Equal(t, [][]interface{}{{int64(2), "bob"}}, rows)
	require.True(t, selectExecutor.lastAccessPathWasSecondaryIndex("idx_email"))
}

func TestP0SecondaryIndexUpdateRemovesStaleEntry(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(100), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, 'old@example.com')")
	mustExecSQL(t, executor, "app", "update users set email = 'new@example.com' where id = 1")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	require.Empty(t, mustQueryRows(t, selectExecutor, "app", "select id from users where email = 'old@example.com'"))
	require.True(t, selectExecutor.lastAccessPathWasSecondaryIndex("idx_email"))

	require.Equal(t, [][]interface{}{{int64(1)}}, mustQueryRows(t, selectExecutor, "app", "select id from users where email = 'new@example.com'"))
	require.True(t, selectExecutor.lastAccessPathWasSecondaryIndex("idx_email"))
}

func TestP0SecondaryIndexDeleteRemovesStaleEntry(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(100), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, 'alice@example.com')")
	mustExecSQL(t, executor, "app", "delete from users where id = 1")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	require.Empty(t, mustQueryRows(t, selectExecutor, "app", "select id from users where email = 'alice@example.com'"))
	require.True(t, selectExecutor.lastAccessPathWasSecondaryIndex("idx_email"))
}

func TestP0SelectUsesSecondaryIndexForRangePredicates(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table products (id int primary key, price int, index idx_price (price))")
	mustExecSQL(t, executor, "app", "insert into products (id, price) values (1, 10)")
	mustExecSQL(t, executor, "app", "insert into products (id, price) values (2, 20)")
	mustExecSQL(t, executor, "app", "insert into products (id, price) values (3, 30)")

	for _, testCase := range []struct {
		query string
		want  [][]interface{}
	}{
		{query: "select id from products where price >= 20", want: [][]interface{}{{int64(2)}, {int64(3)}}},
		{query: "select id from products where price <= 20", want: [][]interface{}{{int64(1)}, {int64(2)}}},
		{query: "select id from products where price between 15 and 25", want: [][]interface{}{{int64(2)}}},
		{query: "select id from products where price not between 15 and 25", want: [][]interface{}{{int64(1)}, {int64(3)}}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
			require.ElementsMatch(t, testCase.want, mustQueryRows(t, selectExecutor, "app", testCase.query))
			require.True(t, selectExecutor.lastAccessPathWasSecondaryIndex("idx_price"))
		})
	}
}

func TestP1SelectUsesCompositeSecondaryIndexPrefix(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, tenant_id int, email varchar(100), index idx_tenant_email (tenant_id, email))")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (1, 7, 'alice@example.com')")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (2, 7, 'bob@example.com')")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (3, 8, 'alice@example.com')")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where tenant_id = 7 and email = 'bob@example.com'")
	require.Equal(t, [][]interface{}{{int64(2)}}, rows)
	require.True(t, selectExecutor.lastAccessPathWasSecondaryIndex("idx_tenant_email"))
}

func TestP1SelectUsesCompositeSecondaryIndexPrefixBeforeRange(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, tenant_id int, email varchar(100), index idx_tenant_email (tenant_id, email))")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (1, 7, 'alice@example.com')")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (2, 7, 'zoe@example.com')")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (3, 8, 'zoe@example.com')")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where tenant_id = 7 and email >= 'm' order by id")
	require.Equal(t, [][]interface{}{{int64(2)}}, rows)
	require.True(t, selectExecutor.lastAccessPathWasSecondaryIndex("idx_tenant_email"))
}

func TestP1SelectMergesSimpleOrSecondaryIndexes(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, tenant_id int, email varchar(100), index idx_tenant (tenant_id), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (1, 7, 'alice@example.com')")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (2, 8, 'bob@example.com')")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (3, 9, 'alice@example.com')")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where tenant_id = 7 or email = 'alice@example.com' order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, rows)
	require.Equal(t, "index_merge_or:idx_tenant,idx_email", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesSecondaryIndexForInList(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(100), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, 'alice@example.com'), (2, 'bob@example.com'), (3, 'carol@example.com')")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where email in ('alice@example.com', 'carol@example.com') order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, rows)
	require.Equal(t, "index_merge_or:idx_email,idx_email", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndInList(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 7, 30), (4, 8, 20)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where tenant_id = 7 and score in (10, 30) order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndBetween(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 7, 30), (4, 8, 20)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where tenant_id = 7 and score between 15 and 25 order by id")
	require.Equal(t, [][]interface{}{{int64(2)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndNotBetween(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 7, 30), (4, 8, 20)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where tenant_id = 7 and score not between 15 and 25 order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndIsNull(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, null), (2, 7, 20), (3, 8, null)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where tenant_id = 7 and score is null order by id")
	require.Equal(t, [][]interface{}{{int64(1)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndIsNotNull(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, null), (2, 7, 20), (3, 8, 20)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where tenant_id = 7 and score is not null order by id")
	require.Equal(t, [][]interface{}{{int64(2)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndPrefixLike(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, tenant_id int, email varchar(100), index idx_tenant_email (tenant_id, email))")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (1, 7, 'alice@example.com'), (2, 7, 'bob@example.com'), (3, 8, 'alice@example.com')")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where tenant_id = 7 and email like 'al%' order by id")
	require.Equal(t, [][]interface{}{{int64(1)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_email", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndNotLike(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, tenant_id int, email varchar(100), index idx_tenant_email (tenant_id, email))")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (1, 7, 'alice@example.com'), (2, 7, 'bob@example.com'), (3, 8, 'alice@example.com')")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where tenant_id = 7 and email not like 'al%' order by id")
	require.Equal(t, [][]interface{}{{int64(2)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_email", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndNotIn(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 7, 30), (4, 8, 20), (5, 7, null)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where tenant_id = 7 and score not in (10, 30) order by id")
	require.Equal(t, [][]interface{}{{int64(2)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesCompositeSecondaryIndexForSharedPrefixOr(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 7, 30), (4, 8, 10)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where tenant_id = 7 and (score = 10 or score = 30) order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesSecondaryIndexForNotInWithResidualFiltering(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(100), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, 'alice@example.com'), (2, 'bob@example.com'), (3, 'carol@example.com'), (4, null)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where email not in ('alice@example.com', 'carol@example.com') order by id")
	require.Equal(t, [][]interface{}{{int64(2)}}, rows)
	require.Equal(t, "secondary_index:idx_email", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesSecondaryIndexForPrefixLike(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(100), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, 'alice@example.com'), (2, 'bob@example.com'), (3, 'alex@example.com')")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where email like 'al%' order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_email", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesSecondaryIndexForNotLikeWithResidualFiltering(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(100), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, 'alice@example.com'), (2, 'bob@example.com'), (3, 'carol@example.com'), (4, null)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where email not like 'al%' order by id")
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_email", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesSecondaryIndexForIsNull(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(100), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, null), (2, 'bob@example.com'), (3, null)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where email is null order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_email", selectExecutor.lastAccessPath)
}

func TestP1SelectIntersectsIndependentSecondaryIndexRanges(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant (tenant_id), index idx_score (score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 8, 20)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where tenant_id >= 7 and score <= 15 order by id")
	require.Equal(t, [][]interface{}{{int64(1)}}, rows)
	require.Equal(t, "index_merge_and:idx_score,idx_tenant", selectExecutor.lastAccessPath)
}

func TestP1SelectUsesSecondaryIndexForIsNotNull(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(100), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, null), (2, 'bob@example.com'), (3, 'carol@example.com')")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where email is not null order by id")
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_email", selectExecutor.lastAccessPath)
}

func TestP1SelectIntersectsIndependentSecondaryIndexes(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, tenant_id int, email varchar(100), index idx_tenant (tenant_id), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (1, 7, 'alice@example.com')")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (2, 7, 'bob@example.com')")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (3, 8, 'bob@example.com')")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where tenant_id = 7 and email = 'bob@example.com'")
	require.Equal(t, [][]interface{}{{int64(2)}}, rows)
	require.Equal(t, "index_merge_and:idx_email,idx_tenant", selectExecutor.lastAccessPath)
}

func TestP1SelectMergesRangeOrSecondaryIndexesWithResidualFiltering(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, tenant_id int, email varchar(100), index idx_tenant (tenant_id), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (1, 7, 'alice@example.com')")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (2, 8, 'bob@example.com')")
	mustExecSQL(t, executor, "app", "insert into users (id, tenant_id, email) values (3, 9, 'carol@example.com')")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where tenant_id >= 8 or email = 'alice@example.com' order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, rows)
	require.Equal(t, "index_merge_or:idx_tenant,idx_email", selectExecutor.lastAccessPath)
}

func TestP1AlterAddIndexBuildsEntriesForExistingRows(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(100))")
	mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, 'alice@example.com'), (2, 'bob@example.com')")
	mustExecSQL(t, executor, "app", "alter table users add index idx_email (email)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from users where email = 'bob@example.com'")
	require.Equal(t, [][]interface{}{{int64(2)}}, rows)
	require.True(t, selectExecutor.lastAccessPathWasSecondaryIndex("idx_email"))
	mustExecSQL(t, executor, "app", "alter table users drop index idx_email")
	selectExecutor = newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows = mustQueryRows(t, selectExecutor, "app", "select id from users where email = 'bob@example.com'")
	require.Equal(t, [][]interface{}{{int64(2)}}, rows)
	require.Equal(t, "table_scan", selectExecutor.lastAccessPath)
}

func TestP0SelectRejectsInvalidDurableSecondaryIndexValue(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(100), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, 'alice@example.com')")

	indexManager := executor.QueryExecutor.storageManager.GetIndexManager()
	indexes := indexManager.ListIndexes(manager.SecondaryIndexTableID("app", "users"))
	require.Len(t, indexes, 1)
	index := indexes[0]
	indexKey, err := manager.EncodeSecondaryIndexKey(
		manager.SecondaryIndexTableID("app", "users"),
		metadata.IndexMeta{Name: index.Name, Columns: indexColumnNames(index), Unique: index.IsUnique},
		map[string]interface{}{"email": "alice@example.com"},
		[]byte("invalid-primary-key"),
	)
	require.NoError(t, err)
	require.NoError(t, indexManager.InsertKey(index.IndexID, indexKey, []byte("invalid-secondary-value")))

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	stmt, err := sqlparser.Parse("select id from users where email = 'alice@example.com'")
	require.NoError(t, err)
	result, err := selectExecutor.ExecuteSelect(context.Background(), stmt.(*sqlparser.Select), "app")
	require.Nil(t, result)
	require.ErrorContains(t, err, "invalid secondary index value")
}

func TestSecondaryIndexPredicateRejectsCompoundCondition(t *testing.T) {
	_, _, _, ok := secondaryIndexPredicate([]string{"last_name = 'Doe' and first_name = 'John'"})
	require.False(t, ok)
}

func newP0SecondaryIndexTestEngine(dataDir string) *XMySQLEngine {
	return NewXMySQLEngine(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
}

func newP0SecondaryIndexTestSelectExecutor(t *testing.T, engine *XMySQLEngine, dataDir string) *SelectExecutor {
	t.Helper()
	queryExecutor := engine.QueryExecutor
	optimizerManager, _ := queryExecutor.optimizerManager.(*manager.OptimizerManager)
	bufferPoolManager, _ := queryExecutor.bufferPoolManager.(*manager.OptimizedBufferPoolManager)
	btreeManager, _ := queryExecutor.btreeManager.(basic.BPlusTreeManager)
	tableManager, _ := queryExecutor.tableManager.(*manager.TableManager)

	return NewSelectExecutor(
		optimizerManager,
		bufferPoolManager,
		btreeManager,
		queryExecutor.storageManager,
		tableManager,
		dataDir,
	)
}

func mustQueryRows(t *testing.T, executor *SelectExecutor, schemaName, query string) [][]interface{} {
	t.Helper()
	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)
	selectStmt, ok := stmt.(*sqlparser.Select)
	require.True(t, ok, "expected SELECT statement, got %T", stmt)

	result, err := executor.ExecuteSelect(context.Background(), selectStmt, schemaName)
	require.NoError(t, err)
	rows := make([][]interface{}, len(result.Records))
	for rowIndex, record := range result.Records {
		values := record.GetValues()
		rows[rowIndex] = make([]interface{}, len(values))
		for valueIndex, value := range values {
			if bytes, ok := value.Raw().([]byte); ok {
				if integer, err := strconv.ParseInt(string(bytes), 10, 64); err == nil {
					rows[rowIndex][valueIndex] = integer
					continue
				}
				rows[rowIndex][valueIndex] = string(bytes)
				continue
			}
			rows[rowIndex][valueIndex] = value.Raw()
		}
	}
	return rows
}

func (se *SelectExecutor) lastAccessPathWasSecondaryIndex(indexName string) bool {
	return se.lastAccessPath == "secondary_index:"+indexName
}
