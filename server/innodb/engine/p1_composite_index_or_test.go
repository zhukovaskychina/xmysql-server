package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestP1SelectMergesCompositeSecondaryIndexOrBranches(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 8, 20), (4, 8, 30)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where (tenant_id = 7 and score = 10) or (tenant_id = 8 and score = 20) order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectMergesCompositeSecondaryIndexOrRangeBranches(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 8, 20), (4, 8, 30)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where (tenant_id = 7 and score >= 20) or (tenant_id = 8 and score >= 30) order by id")
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(4)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectMergesCompositeSecondaryIndexOrBetweenBranches(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 8, 20), (4, 8, 30)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where (tenant_id = 7 and score between 15 and 25) or (tenant_id = 8 and score between 25 and 35) order by id")
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(4)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectMergesCompositeSecondaryIndexOrNullBranches(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, null), (2, 7, 20), (3, 8, null), (4, 8, 30)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where (tenant_id = 7 and score is null) or (tenant_id = 8 and score is null) order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectMergesCompositeSecondaryIndexOrLikeBranches(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, tenant_id int, label varchar(32), index idx_tenant_label (tenant_id, label))")
	mustExecSQL(t, executor, "app", "insert into labels (id, tenant_id, label) values (1, 7, 'alpha'), (2, 7, 'beta'), (3, 8, 'apple'), (4, 8, 'berry')")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from labels where (tenant_id = 7 and label like 'a%') or (tenant_id = 8 and label like 'a%') order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_label", selectExecutor.lastAccessPath)
}

func TestP1SelectMergesCompositeSecondaryIndexOrNotInBranches(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table labels (id int primary key, tenant_id int, label varchar(32), index idx_tenant_label (tenant_id, label))")
	mustExecSQL(t, executor, "app", "insert into labels (id, tenant_id, label) values (1, 7, 'alpha'), (2, 7, 'beta'), (3, 8, 'apple'), (4, 8, 'berry')")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from labels where (tenant_id = 7 and label not in ('alpha')) or (tenant_id = 8 and label not in ('berry')) order by id")
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_label", selectExecutor.lastAccessPath)
}

func TestP1SelectMergesCompositeSecondaryIndexOrInBranches(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 7, 30), (4, 8, 20), (5, 8, 40)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where (tenant_id = 7 and score in (10, 30)) or (tenant_id = 8 and score in (40)) order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}, {int64(5)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectMergesCompositeSecondaryIndexOrNotEqualBranches(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 8, 20), (4, 8, 30)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where (tenant_id = 7 and score <> 10) or (tenant_id = 8 and score != 30) order by id")
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectMergesCompositeSecondaryIndexOrNullSafeEqualityBranches(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, null), (2, 7, 20), (3, 8, 30), (4, 8, 40)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where (tenant_id = 7 and score <=> null) or (tenant_id = 8 and score <=> 30) order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP2SelectUsesCompositeSecondaryIndexSkipScanForTrailingEquality(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 8, 20), (4, 8, 30)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where score = 20 order by id")
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score_skip_scan", selectExecutor.lastAccessPath)
}

func TestP2SelectUsesCompositeSecondaryIndexSkipScanAcrossMultipleLeadingColumns(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, region int, tenant_id int, score int, index idx_region_tenant_score (region, tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, region, tenant_id, score) values (1, 1, 7, 10), (2, 1, 8, 20), (3, 2, 7, 20), (4, 2, 9, 30), (5, 1, 8, 40)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where score = 20 order by id")
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_region_tenant_score_skip_scan", selectExecutor.lastAccessPath)
}

func TestP2SelectUsesCompositeSecondaryIndexSkipScanForTrailingRange(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, 10), (2, 7, 20), (3, 8, 20), (4, 8, 30)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where score >= 20 order by id")
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(3)}, {int64(4)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score_skip_scan", selectExecutor.lastAccessPath)
}

func TestP2SelectUsesCompositeSecondaryIndexSkipScanForTrailingNull(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, null), (2, 7, 20), (3, 8, null), (4, 8, 30)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where score is null order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score_skip_scan", selectExecutor.lastAccessPath)
}

func TestP2SelectUsesCompositeSecondaryIndexSkipScanForTrailingNotNull(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 7, null), (2, 7, 20), (3, 8, null), (4, 8, 30)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where score is not null order by id")
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(4)}}, rows)
	require.Equal(t, "secondary_index:idx_tenant_score_skip_scan", selectExecutor.lastAccessPath)
}

func TestP2SkipScanFallsBackWhenLeadingPrefixCostIsNotSelective(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metrics (id int primary key, tenant_id int, score int, index idx_tenant_score (tenant_id, score))")
	mustExecSQL(t, executor, "app", "insert into metrics (id, tenant_id, score) values (1, 1, 20), (2, 2, 20), (3, 3, 20), (4, 4, 20)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from metrics where score = 20 order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}, rows)
	require.Equal(t, "table_scan", selectExecutor.lastAccessPath)
}

func TestP1SelectIntersectsCompositeSecondaryIndexLeadingEqualityBranches(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table orders (id int primary key, tenant_id int, score int, order_state varchar(16), region_id int, index idx_tenant_score (tenant_id, score), index idx_state_region (order_state, region_id))")
	mustExecSQL(t, executor, "app", "insert into orders (id, tenant_id, score, order_state, region_id) values (1, 7, 10, 'active', 1), (2, 7, 20, 'inactive', 1), (3, 8, 20, 'active', 2), (4, 7, 30, 'active', 2)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from orders where tenant_id = 7 and order_state = 'active' order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(4)}}, rows)
	require.Equal(t, "index_merge_and:idx_state_region,idx_tenant_score", selectExecutor.lastAccessPath)
}

func TestP1SelectIntersectsCompositeSecondaryIndexLeadingRangeBranches(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table orders (id int primary key, tenant_id int, score int, order_state varchar(16), region_id int, index idx_tenant_score (tenant_id, score), index idx_state_region (order_state, region_id))")
	mustExecSQL(t, executor, "app", "insert into orders (id, tenant_id, score, order_state, region_id) values (1, 7, 10, 'active', 1), (2, 7, 20, 'inactive', 1), (3, 8, 20, 'active', 2), (4, 9, 30, 'active', 2), (5, 6, 40, 'active', 3)")

	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, executor, dataDir)
	rows := mustQueryRows(t, selectExecutor, "app", "select id from orders where tenant_id >= 7 and order_state = 'active' order by id")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}, {int64(4)}}, rows)
	require.Equal(t, "index_merge_and:idx_state_region,idx_tenant_score", selectExecutor.lastAccessPath)
}
