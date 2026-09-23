package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestAlterOperationsAreParsedAndAppliedAtomically(t *testing.T) {
	table, operations, handled, err := parseAlterOperations("alter table users add column nickname varchar(20), add unique index idx_nickname (nickname)")
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, "users", table)
	require.Len(t, operations, 2)

	metadata := map[string]interface{}{
		"columns": []interface{}{map[string]interface{}{"name": "id", "type": "int"}},
		"indexes": []interface{}{},
	}
	require.NoError(t, applyAlterOperations(metadata, operations))
	require.Len(t, metadata["columns"], 2)
	require.Len(t, metadata["indexes"], 1)
}

func TestAlterOperationsSupportIfExistsAndIfNotExists(t *testing.T) {
	_, single, handled, err := parseAlterOperations("alter table users add column if not exists nickname varchar(20)")
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, single, 1)
	require.True(t, single[0].IfNotExists)

	_, operations, handled, err := parseAlterOperations("alter table users add column if not exists nickname varchar(20), add index if not exists idx_nickname (nickname)")
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, operations, 2)
	require.Equal(t, AlterAddColumn, operations[0].Kind)
	require.Equal(t, "nickname", operations[0].NewName)
	require.Equal(t, AlterAddIndex, operations[1].Kind)
	require.Equal(t, "idx_nickname", operations[1].IndexName)

	metadata := map[string]interface{}{
		"columns": []interface{}{map[string]interface{}{"name": "nickname", "type": "varchar"}},
		"indexes": []interface{}{map[string]interface{}{"name": "idx_nickname", "columns": []interface{}{"nickname"}}},
	}
	require.NoError(t, applyAlterOperations(metadata, operations))
	require.Len(t, metadata["columns"], 1)
	require.Len(t, metadata["indexes"], 1)

	_, operations, handled, err = parseAlterOperations("alter table users drop column if exists missing, drop index if exists missing_idx")
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, operations, 2)
	require.Equal(t, AlterDropColumn, operations[0].Kind)
	require.Equal(t, "missing", operations[0].OldName)
	require.Equal(t, AlterDropIndex, operations[1].Kind)
	require.Equal(t, "missing_idx", operations[1].IndexName)
	require.NoError(t, applyAlterOperations(metadata, operations))
}

func TestAlterOperationsRejectDuplicateRenameTargetsAtomically(t *testing.T) {
	columns := []interface{}{
		map[string]interface{}{"name": "id", "type": "int"},
		map[string]interface{}{"name": "name", "type": "varchar"},
	}
	indexes := []interface{}{
		map[string]interface{}{"name": "idx_name", "columns": []interface{}{"name"}},
		map[string]interface{}{"name": "idx_id", "columns": []interface{}{"id"}},
	}
	metadata := map[string]interface{}{"columns": columns, "indexes": indexes}

	err := applyAlterOperations(metadata, []AlterOperation{{
		Kind:    AlterRenameColumn,
		OldName: "name",
		NewName: "id",
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate column")
	require.Equal(t, "name", columns[1].(map[string]interface{})["name"])

	err = applyAlterOperations(metadata, []AlterOperation{{
		Kind:      AlterRenameIndex,
		OldName:   "idx_name",
		NewName:   "idx_id",
		IndexName: "idx_name",
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate key")
	require.Equal(t, "idx_name", indexes[0].(map[string]interface{})["name"])

	metadata = map[string]interface{}{
		"columns": []interface{}{
			map[string]interface{}{"name": "id", "type": "int"},
			map[string]interface{}{"name": "name", "type": "varchar"},
		},
		"indexes": []interface{}{},
	}
	err = applyAlterOperations(metadata, []AlterOperation{{
		Kind:       AlterChangeColumn,
		OldName:    "name",
		NewName:    "id",
		Definition: "varchar(20)",
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate column")
}

func TestAlterOperationsRollBackEarlierMetadataChangesWhenLaterClauseFails(t *testing.T) {
	columns := []interface{}{
		map[string]interface{}{"name": "id", "type": "int"},
		map[string]interface{}{"name": "name", "type": "varchar"},
	}
	metadata := map[string]interface{}{
		"columns": columns,
		"indexes": []interface{}{},
	}

	err := applyAlterOperations(metadata, []AlterOperation{
		{Kind: AlterRenameColumn, OldName: "name", NewName: "display_name"},
		{Kind: AlterDropColumn, OldName: "missing"},
	})
	require.Error(t, err)
	require.Equal(t, "name", columns[1].(map[string]interface{})["name"])
	require.Equal(t, "name", metadata["columns"].([]interface{})[1].(map[string]interface{})["name"])
}

func TestAlterAddColumnHonorsFirstAndAfterPositions(t *testing.T) {
	_, single, handled, err := parseAlterOperations("alter table users add column first_col int first")
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, single, 1)
	require.True(t, single[0].First)

	_, operations, handled, err := parseAlterOperations("alter table users add column first_col int first, add column after_id int after id")
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, operations, 2)

	metadata := map[string]interface{}{
		"columns": []interface{}{
			map[string]interface{}{"name": "id", "type": "int"},
			map[string]interface{}{"name": "name", "type": "varchar"},
		},
		"indexes": []interface{}{},
	}
	require.NoError(t, applyAlterOperations(metadata, operations))
	columns := metadata["columns"].([]interface{})
	require.Equal(t, "first_col", columns[0].(map[string]interface{})["name"])
	require.Equal(t, "id", columns[1].(map[string]interface{})["name"])
	require.Equal(t, "after_id", columns[2].(map[string]interface{})["name"])
	require.Equal(t, "name", columns[3].(map[string]interface{})["name"])
}

func TestAlterModifyAndChangeColumnHonorPositions(t *testing.T) {
	_, operations, handled, err := parseAlterOperations("alter table users modify column name varchar(40) first")
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, operations, 1)
	require.True(t, operations[0].First)

	metadata := map[string]interface{}{
		"columns": []interface{}{
			map[string]interface{}{"name": "id", "type": "int"},
			map[string]interface{}{"name": "name", "type": "varchar"},
			map[string]interface{}{"name": "note", "type": "varchar"},
		},
		"indexes": []interface{}{},
	}
	require.NoError(t, applyAlterOperations(metadata, operations))
	columns := metadata["columns"].([]interface{})
	require.Equal(t, "name", columns[0].(map[string]interface{})["name"])

	_, operations, handled, err = parseAlterOperations("alter table users change column name display_name varchar(40) after id")
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, operations, 1)
	require.Equal(t, "id", operations[0].AfterColumn)
	require.NoError(t, applyAlterOperations(metadata, operations))
	columns = metadata["columns"].([]interface{})
	require.Equal(t, []string{"id", "display_name", "note"}, []string{
		columns[0].(map[string]interface{})["name"].(string),
		columns[1].(map[string]interface{})["name"].(string),
		columns[2].(map[string]interface{})["name"].(string),
	})
}

func TestAlterOperationsAcceptAlgorithmAndLockOptions(t *testing.T) {
	_, operations, handled, err := parseAlterOperations("alter table users add column note varchar(20), algorithm=inplace, lock=none")
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, operations, 1)
	require.Equal(t, AlterAddColumn, operations[0].Kind)

	_, operations, handled, err = parseAlterOperations("alter table users algorithm=instant, lock=none")
	require.True(t, handled)
	require.NoError(t, err)
	require.Empty(t, operations)
}

func TestAlterOperationsAcceptForceAsCompatibilityNoOp(t *testing.T) {
	_, operations, handled, err := parseAlterOperations("alter table users force")
	require.True(t, handled)
	require.NoError(t, err)
	require.Empty(t, operations)
}

func TestAlterIndexVisibilityIsParsedAndPersisted(t *testing.T) {
	_, operations, handled, err := parseAlterOperations("alter table users alter index idx_name invisible")
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, operations, 1)
	require.Equal(t, AlterAlterIndexVisibility, operations[0].Kind)
	require.Equal(t, "idx_name", operations[0].IndexName)
	require.False(t, operations[0].Visible)

	metadata := map[string]interface{}{
		"columns": []interface{}{},
		"indexes": []interface{}{map[string]interface{}{"name": "idx_name", "columns": []interface{}{"name"}}},
	}
	require.NoError(t, applyAlterOperations(metadata, operations))
	index := metadata["indexes"].([]interface{})[0].(map[string]interface{})
	require.False(t, index["visible"].(bool))
	require.True(t, index["visibility_set"].(bool))

	_, operations, handled, err = parseAlterOperations("alter table users alter index idx_name visible")
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, operations, 1)
	require.True(t, operations[0].Visible)
	require.NoError(t, applyAlterOperations(metadata, operations))
	require.True(t, metadata["indexes"].([]interface{})[0].(map[string]interface{})["visible"].(bool))

	_, operations, handled, err = parseAlterOperations("alter table users add index idx_email (email) invisible")
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, operations, 1)
	require.Equal(t, AlterAddIndex, operations[0].Kind)
	require.True(t, operations[0].VisibilitySet)
	require.False(t, operations[0].Visible)
	require.NoError(t, applyAlterOperations(metadata, operations))
	added := metadata["indexes"].([]interface{})[1].(map[string]interface{})
	require.False(t, added["visible"].(bool))
}

func TestAlterOperationsRejectInvalidAlgorithmAndLockOptions(t *testing.T) {
	for _, query := range []string{
		"alter table users add column note varchar(20), algorithm=fast",
		"alter table users add column note varchar(20), lock=metadata_only",
		"alter table users add column note varchar(20), algorithm=instant, algorithm=inplace",
	} {
		_, _, handled, err := parseAlterOperations(query)
		require.True(t, handled, query)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "ALTER TABLE option", query)
	}
}

func TestAlterTableEngineInnoDBIsAcceptedAsCompatibilityNoOp(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users values (1)")
	mustExecSQL(t, executor, "app", "alter table users engine=innodb")
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select id from users"))

	unsupported := <-executor.ExecuteQuery(nil, "alter table users engine=myisam", "app")
	require.Error(t, unsupported.Err)
	require.Contains(t, unsupported.Err.Error(), "ENGINE")
}

func TestMultiOperationAlterUpdatesMetadataAndQueryPath(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1)")
	mustExecSQL(t, executor, "app", "alter table users add column nickname varchar(20), add index idx_nickname (nickname)")
	rows := mustQuerySQL(t, executor, "app", "select id, nickname from users")
	require.Equal(t, [][]interface{}{{"1", nil}}, rows)
	mustExecSQL(t, executor, "app", "alter table users add column note varchar(20), algorithm=inplace, lock=none")
	require.Equal(t, [][]interface{}{{nil}}, mustQuerySQL(t, executor, "app", "select note from users"))
	show := <-executor.ExecuteQuery(nil, "show index from users", "app")
	require.NoError(t, show.Err)
}

func TestRenameColumnUpdatesMetadataAndIndex(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, nickname varchar(20), index idx_nickname (nickname))")
	mustExecSQL(t, executor, "app", "insert into users (id, nickname) values (1, 'alice')")
	mustExecSQL(t, executor, "app", "alter table users rename column nickname to display_name")
	require.Equal(t, [][]interface{}{{"alice"}}, mustQuerySQL(t, executor, "app", "select display_name from users"))
	indexRows := mustQuerySQL(t, executor, "app", "select column_name from information_schema.statistics where table_schema = 'app' and table_name = 'users' and index_name = 'idx_nickname'")
	require.Contains(t, indexRows, []interface{}{"display_name"})
}

func TestRenameTableMovesTableAcrossSchemas(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database source_db")
	mustExecSQL(t, executor, "", "create database target_db")
	mustExecSQL(t, executor, "source_db", "create table moved_rows (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "source_db", "insert into moved_rows values (1, 'kept')")
	mustExecSQL(t, executor, "source_db", "rename table source_db.moved_rows to target_db.moved_rows")
	require.FileExists(t, filepath.Join(executor.GetDataDir(), "target_db", "moved_rows.ibd"))
	require.NoFileExists(t, filepath.Join(executor.GetDataDir(), "source_db", "moved_rows.ibd"))
	require.Equal(t, [][]interface{}{{"1", "kept"}}, mustQuerySQL(t, executor, "target_db", "select id, label from moved_rows"))
	got := <-executor.ExecuteQuery(nil, "select id from moved_rows", "source_db")
	if got.Err == nil {
		result, ok := got.Data.(*SelectResult)
		require.True(t, ok)
		require.Empty(t, result.Records)
	}
}

func TestRenameTableMultipleTargetsValidateAtomically(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table first_row (id int primary key)")
	mustExecSQL(t, executor, "app", "create table second_row (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into first_row values (1)")
	mustExecSQL(t, executor, "app", "insert into second_row values (2)")
	mustExecSQL(t, executor, "app", "create table occupied (id int primary key)")

	got := <-executor.ExecuteQuery(nil, "rename table first_row to first_row_new, second_row to occupied", "app")
	require.Error(t, got.Err)
	require.FileExists(t, filepath.Join(executor.GetDataDir(), "app", "first_row.frm"))
	require.NoFileExists(t, filepath.Join(executor.GetDataDir(), "app", "first_row_new.frm"))
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select id from first_row"))
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySQL(t, executor, "app", "select id from second_row"))
	mustExecSQL(t, executor, "app", "rename table first_row to first_row_new, second_row to second_row_new")
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select id from first_row_new"))
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySQL(t, executor, "app", "select id from second_row_new"))
}

func TestDropTableMultipleTargetsValidateAtomically(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table first_drop (id int primary key)")
	mustExecSQL(t, executor, "app", "create table second_drop (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into first_drop values (1)")
	mustExecSQL(t, executor, "app", "insert into second_drop values (2)")
	targets, ifExists, matched, parseErr := parseDropTableTargets("drop table first_drop, second_drop", "app")
	require.True(t, matched)
	require.NoError(t, parseErr)
	require.False(t, ifExists)
	require.Len(t, targets, 2)

	got := <-executor.ExecuteQuery(nil, "drop table first_drop, missing_drop", "app")
	require.Error(t, got.Err)
	require.FileExists(t, filepath.Join(executor.GetDataDir(), "app", "first_drop.frm"))
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select id from first_drop"))

	dropped := <-executor.ExecuteQuery(nil, "drop table first_drop, second_drop", "app")
	require.NoError(t, dropped.Err, dropped.Message)
	require.NoFileExists(t, filepath.Join(executor.GetDataDir(), "app", "first_drop.frm"))
	require.NoFileExists(t, filepath.Join(executor.GetDataDir(), "app", "second_drop.frm"))
	mustExecSQL(t, executor, "app", "create table qualified_drop (id int primary key)")
	mustExecSQL(t, executor, "app", "create table qualified_drop_two (id int primary key)")
	mustExecSQL(t, executor, "app", "drop table if exists app.missing_drop, app.qualified_drop, qualified_drop_two")
	require.NoFileExists(t, filepath.Join(executor.GetDataDir(), "app", "qualified_drop.frm"))
	require.NoFileExists(t, filepath.Join(executor.GetDataDir(), "app", "qualified_drop_two.frm"))
}

func TestRenameParentTableAcrossSchemasUpdatesForeignKeys(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database source_db")
	mustExecSQL(t, executor, "", "create database target_db")
	mustExecSQL(t, executor, "", "create database child_db")
	mustExecSQL(t, executor, "source_db", "create table parent_rows (id int primary key)")
	mustExecSQL(t, executor, "child_db", "create table child_rows (id int primary key, parent_id int, constraint fk_parent foreign key (parent_id) references source_db.parent_rows (id))")
	mustExecSQL(t, executor, "source_db", "insert into parent_rows values (1)")
	mustExecSQL(t, executor, "child_db", "insert into child_rows values (10, 1)")

	mustExecSQL(t, executor, "source_db", "rename table source_db.parent_rows to target_db.renamed_parents")
	mustExecSQL(t, executor, "child_db", "insert into child_rows values (11, 1)")
	rows := mustQuerySQL(t, executor, "child_db", "select referenced_table_schema, referenced_table_name from information_schema.key_column_usage where table_schema = 'child_db' and table_name = 'child_rows' and constraint_name = 'fk_parent'")
	require.Equal(t, [][]interface{}{{"target_db", "renamed_parents"}}, rows)
}

func TestAlterTableRenameToMovesAcrossSchemas(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database source_db")
	mustExecSQL(t, executor, "", "create database target_db")
	mustExecSQL(t, executor, "source_db", "create table alter_move (id int primary key)")
	mustExecSQL(t, executor, "source_db", "insert into alter_move values (7)")
	mustExecSQL(t, executor, "source_db", "alter table source_db.alter_move rename to target_db.alter_moved")
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "target_db", "select id from alter_moved"))
}

func TestRenameTableAcrossSchemasSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &conf.Cfg{DataDir: dataDir, InnodbDataDir: dataDir, InnodbBufferPoolSize: 16 * 1024 * 1024, InnodbPageSize: 16384}
	first := NewXMySQLEngine(cfg)
	mustExecSQL(t, first, "", "create database source_db")
	mustExecSQL(t, first, "", "create database target_db")
	mustExecSQL(t, first, "source_db", "create table restart_rows (id int primary key)")
	mustExecSQL(t, first, "source_db", "insert into restart_rows values (9)")
	mustExecSQL(t, first, "source_db", "rename table source_db.restart_rows to target_db.restart_rows")
	require.NoError(t, first.Close())

	second := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.Equal(t, [][]interface{}{{"9"}}, mustQuerySQL(t, second, "target_db", "select id from restart_rows"))
}

func TestRenamePartitionedTableAcrossSchemasPreservesPhysicalPartitions(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &conf.Cfg{DataDir: dataDir, InnodbDataDir: dataDir, InnodbBufferPoolSize: 16 * 1024 * 1024, InnodbPageSize: 16384}
	first := NewXMySQLEngine(cfg)
	mustExecSQL(t, first, "", "create database source_db")
	mustExecSQL(t, first, "", "create database target_db")
	mustExecSQL(t, first, "source_db", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))")
	mustExecSQL(t, first, "source_db", "insert into events values (3), (17)")
	mustExecSQL(t, first, "source_db", "rename table source_db.events to target_db.renamed_events")

	require.Equal(t, [][]interface{}{{"3"}, {"17"}}, mustQuerySQL(t, first, "target_db", "select id from renamed_events order by id"))
	spaces, err := first.GetStorageManager().ListSpaces()
	require.NoError(t, err)
	spaceNames := make(map[string]bool)
	for _, space := range spaces {
		spaceNames[space.Name] = true
	}
	require.True(t, spaceNames["target_db/renamed_events#p0"])
	require.True(t, spaceNames["target_db/renamed_events#p1"])
	require.False(t, spaceNames["source_db/events#p0"])
	require.False(t, spaceNames["source_db/events#p1"])
	storageInfo, infoErr := first.GetStorageManager().GetTableStorageManager().GetTableStorageInfo("target_db", "renamed_events")
	require.NoError(t, infoErr)
	require.Len(t, storageInfo.Partitions, 2)
	_, p1BeforeCloseErr := os.Stat(filepath.Join(first.GetDataDir(), "target_db", "renamed_events#p1.ibd"))
	require.NoError(t, p1BeforeCloseErr)
	require.NoError(t, first.Close())
	_, p1AfterCloseErr := os.Stat(filepath.Join(first.GetDataDir(), "target_db", "renamed_events#p1.ibd"))
	require.NoError(t, p1AfterCloseErr)

	second := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.Equal(t, [][]interface{}{{"3"}, {"17"}}, mustQuerySQL(t, second, "target_db", "select id from renamed_events order by id"))
}

func TestAlterTableAddAndDropPrimaryKeyUpdatesConstraintMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table primary_later (id int, note varchar(20))")
	mustExecSQL(t, executor, "app", "insert into primary_later values (1, 'first')")
	mustExecSQL(t, executor, "app", "alter table primary_later add primary key (id)")
	indexRows := mustQuerySQL(t, executor, "app", "select index_name, column_name from information_schema.statistics where table_schema = 'app' and table_name = 'primary_later' and index_name = 'PRIMARY'")
	require.Contains(t, indexRows, []interface{}{"PRIMARY", "id"})
	duplicate := <-executor.ExecuteQuery(nil, "insert into primary_later values (1, 'duplicate')", "app")
	require.Error(t, duplicate.Err)
	mustExecSQL(t, executor, "app", "alter table primary_later drop primary key")
	indexRows = mustQuerySQL(t, executor, "app", "select index_name from information_schema.statistics where table_schema = 'app' and table_name = 'primary_later' and index_name = 'PRIMARY'")
	require.Empty(t, indexRows)
	mustExecSQL(t, executor, "app", "insert into primary_later values (1, 'allowed-again')")
	mustExecSQL(t, executor, "app", "alter table primary_later alter column note set default 'fallback'")
	mustExecSQL(t, executor, "app", "insert into primary_later (id) values (2)")
	require.Equal(t, [][]interface{}{{"fallback"}}, mustQuerySQL(t, executor, "app", "select note from primary_later where id = 2"))
	mustExecSQL(t, executor, "app", "alter table primary_later alter column note drop default")
	mustExecSQL(t, executor, "app", "insert into primary_later (id) values (3)")
	require.Equal(t, [][]interface{}{{nil}}, mustQuerySQL(t, executor, "app", "select note from primary_later where id = 3"))
}

func TestAlterTableRenameToMovesPersistedTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table rename_source (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into rename_source values (1)")
	mustExecSQL(t, executor, "app", "alter table rename_source rename to rename_target")
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select id from rename_target"))
	_, err := os.Stat(filepath.Join(executor.GetDataDir(), "app", "rename_source.frm"))
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(executor.GetDataDir(), "app", "rename_target.frm"))
	require.NoError(t, err)
}

func TestAlterTableAddPrimaryKeyRejectsExistingDuplicateOrNullValues(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table duplicate_primary (id int, note varchar(20))")
	mustExecSQL(t, executor, "app", "insert into duplicate_primary values (1, 'a'), (1, 'b')")
	duplicate := <-executor.ExecuteQuery(nil, "alter table duplicate_primary add primary key (id)", "app")
	require.Error(t, duplicate.Err)
	require.Contains(t, duplicate.Err.Error(), "duplicate")

	mustExecSQL(t, executor, "app", "create table null_primary (id int, note varchar(20))")
	mustExecSQL(t, executor, "app", "insert into null_primary values (null, 'a')")
	nullValue := <-executor.ExecuteQuery(nil, "alter table null_primary add primary key (id)", "app")
	require.Error(t, nullValue.Err)
	require.Contains(t, nullValue.Err.Error(), "NULL")
}

func TestAlterTableAddAndDropForeignKeyUpdatesRuntimeConstraint(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table alter_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table alter_child (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "alter table alter_child add constraint fk_alter_parent foreign key (parent_id) references alter_parent(id) on delete cascade")
	mustExecSQL(t, executor, "app", "insert into alter_parent values (1)")
	mustExecSQL(t, executor, "app", "insert into alter_child values (10, 1)")
	invalid := <-executor.ExecuteQuery(nil, "insert into alter_child values (11, 99)", "app")
	require.Error(t, invalid.Err)
	mustExecSQL(t, executor, "app", "delete from alter_parent where id = 1")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from alter_child"))
	mustExecSQL(t, executor, "app", "alter table alter_child drop foreign key fk_alter_parent")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select index_name from information_schema.statistics where table_schema = 'app' and table_name = 'alter_child' and index_name = 'fk_alter_parent'"))
	mustExecSQL(t, executor, "app", "insert into alter_child values (12, 99)")
}

func TestAlterTableRenameForeignKeyColumnUpdatesRuntimeConstraint(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table rename_fk_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table rename_fk_child (id int primary key, parent_id int, foreign key (parent_id) references rename_fk_parent(id))")
	mustExecSQL(t, executor, "app", "alter table rename_fk_child rename column parent_id to owner_id")
	mustExecSQL(t, executor, "app", "insert into rename_fk_parent values (1)")
	mustExecSQL(t, executor, "app", "insert into rename_fk_child values (10, 1)")
	invalid := <-executor.ExecuteQuery(nil, "insert into rename_fk_child values (11, 99)", "app")
	require.Error(t, invalid.Err)
	keyColumns := mustQuerySQL(t, executor, "app", "select column_name from information_schema.key_column_usage where table_schema = 'app' and table_name = 'rename_fk_child'")
	require.Contains(t, keyColumns, []interface{}{"owner_id"})
}

func TestAlterTableRenameReferencedColumnUpdatesChildForeignKey(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table rename_ref_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table rename_ref_child (id int primary key, parent_id int, foreign key (parent_id) references rename_ref_parent(id))")
	mustExecSQL(t, executor, "app", "alter table rename_ref_parent rename column id to parent_key")
	mustExecSQL(t, executor, "app", "insert into rename_ref_parent values (1)")
	mustExecSQL(t, executor, "app", "insert into rename_ref_child values (10, 1)")
	invalid := <-executor.ExecuteQuery(nil, "insert into rename_ref_child values (11, 99)", "app")
	require.Error(t, invalid.Err)
	referencedColumns := mustQuerySQL(t, executor, "app", "select referenced_column_name from information_schema.key_column_usage where table_schema = 'app' and table_name = 'rename_ref_child' and referenced_table_name = 'rename_ref_parent'")
	require.Contains(t, referencedColumns, []interface{}{"parent_key"})
}

func TestAlterTableRenameCheckColumnUpdatesConstraintExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table rename_check (id int primary key, amount int, constraint chk_amount check (amount > 0))")
	mustExecSQL(t, executor, "app", "alter table rename_check rename column amount to score")
	mustExecSQL(t, executor, "app", "insert into rename_check values (1, 2)")
	invalid := <-executor.ExecuteQuery(nil, "insert into rename_check values (2, 0)", "app")
	require.Error(t, invalid.Err)
	checks := mustQuerySQL(t, executor, "app", "select check_clause from information_schema.check_constraints where constraint_schema = 'app' and table_name = 'rename_check'")
	require.Contains(t, checks, []interface{}{"score > 0"})
}

func TestAlterTableAddAndDropCheckUpdatesRuntimeConstraint(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table alter_checked (id int primary key, amount int)")
	mustExecSQL(t, executor, "app", "alter table alter_checked add constraint chk_positive check (amount > 0)")
	invalid := <-executor.ExecuteQuery(nil, "insert into alter_checked values (1, 0)", "app")
	require.Error(t, invalid.Err)
	mustExecSQL(t, executor, "app", "insert into alter_checked values (1, 2)")
	mustExecSQL(t, executor, "app", "alter table alter_checked drop check chk_positive")
	mustExecSQL(t, executor, "app", "insert into alter_checked values (2, 0)")
}

func TestAlterTableDropConstraintRemovesCheckConstraint(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table alter_drop_constraint (id int primary key, amount int)")
	mustExecSQL(t, executor, "app", "alter table alter_drop_constraint add constraint chk_amount check (amount > 0)")
	invalid := <-executor.ExecuteQuery(nil, "insert into alter_drop_constraint values (1, 0)", "app")
	require.Error(t, invalid.Err)

	mustExecSQL(t, executor, "app", "alter table alter_drop_constraint drop constraint chk_amount")
	mustExecSQL(t, executor, "app", "insert into alter_drop_constraint values (1, 0)")
	require.Equal(t, [][]interface{}{{"0"}}, mustQuerySQL(t, executor, "app", "select amount from alter_drop_constraint where id = 1"))
}

func TestAlterTableDropConstraintAcceptsForeignKeySymbol(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table constraint_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table constraint_child (id int primary key, parent_id int)")
	mustExecSQL(t, executor, "app", "alter table constraint_child add constraint fk_parent foreign key (parent_id) references constraint_parent(id)")
	mustExecSQL(t, executor, "app", "alter table constraint_child drop constraint fk_parent")
	mustExecSQL(t, executor, "app", "insert into constraint_child values (1, 99)")
	require.Equal(t, [][]interface{}{{"1", "99"}}, mustQuerySQL(t, executor, "app", "select id, parent_id from constraint_child"))
}

func TestAlterTableAddConstraintUniqueUpdatesIndexMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table alter_unique (id int primary key, email varchar(80))")
	mustExecSQL(t, executor, "app", "alter table alter_unique add constraint uq_email unique (email)")
	show := <-executor.ExecuteQuery(nil, "show index from alter_unique", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	var uniqueRow []interface{}
	for _, row := range rows {
		if len(row) > 2 && row[2] == "uq_email" {
			uniqueRow = row
		}
	}
	require.NotNil(t, uniqueRow)
	require.Equal(t, 0, uniqueRow[1])
	duplicate := <-executor.ExecuteQuery(nil, "insert into alter_unique values (1, 'a@example.com'), (2, 'a@example.com')", "app")
	require.Error(t, duplicate.Err)
}

func TestAlterTableRenameIndexRejectsDuplicateTargetAtomically(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table rename_index_duplicate (id int primary key, left_value int, right_value int, key idx_left (left_value), key idx_right (right_value))")

	err := execSQLExpectError(t, executor, "app", "alter table rename_index_duplicate rename index idx_left to idx_right")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "duplicate")

	rows := mustQuerySQL(t, executor, "app", "select index_name from information_schema.statistics where table_schema = 'app' and table_name = 'rename_index_duplicate' order by index_name")
	require.Equal(t, [][]interface{}{{"PRIMARY"}, {"idx_left"}, {"idx_right"}}, rows)
}

func TestAlterTableDropColumnRemovesSingleColumnCheckConstraint(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_checked_column (id int primary key, amount int constraint chk_amount check (amount > 0))")

	mustExecSQL(t, executor, "app", "alter table drop_checked_column drop column amount")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select constraint_name from information_schema.check_constraints where constraint_schema = 'app' and table_name = 'drop_checked_column'"))
	mustExecSQL(t, executor, "app", "insert into drop_checked_column (id) values (1)")
}

func TestAlterTableDropColumnRejectsMultiColumnCheckConstraint(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_multi_checked (id int primary key, amount int, minimum int, constraint chk_range check (amount >= minimum))")

	err := execSQLExpectError(t, executor, "app", "alter table drop_multi_checked drop column amount")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "check constraint")
	require.Equal(t, [][]interface{}{{"chk_range"}}, mustQuerySQL(t, executor, "app", "select constraint_name from information_schema.check_constraints where constraint_schema = 'app' and table_name = 'drop_multi_checked'"))
}

func TestAlterTableDropColumnRejectsForeignKeyColumn(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_fk_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table drop_fk_child (id int primary key, parent_id int, constraint fk_parent foreign key (parent_id) references drop_fk_parent(id))")

	err := execSQLExpectError(t, executor, "app", "alter table drop_fk_child drop column parent_id")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
	require.Equal(t, [][]interface{}{{"fk_parent"}}, mustQuerySQL(t, executor, "app", "select constraint_name from information_schema.key_column_usage where constraint_schema = 'app' and table_name = 'drop_fk_child' and constraint_name = 'fk_parent'"))
}

func TestAlterTableDropColumnRejectsReferencedParentColumn(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_referenced_parent (id int primary key, code int unique)")
	mustExecSQL(t, executor, "app", "create table drop_referenced_child (id int primary key, parent_code int, constraint fk_code foreign key (parent_code) references drop_referenced_parent(code))")

	err := execSQLExpectError(t, executor, "app", "alter table drop_referenced_parent drop column code")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
	require.Equal(t, [][]interface{}{{"fk_code"}}, mustQuerySQL(t, executor, "app", "select constraint_name from information_schema.key_column_usage where constraint_schema = 'app' and table_name = 'drop_referenced_child' and constraint_name = 'fk_code'"))
}

func TestAlterTableDropIndexRejectsRequiredForeignKeyIndex(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_fk_index_parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table drop_fk_index_child (id int primary key, parent_id int, constraint fk_drop_index foreign key (parent_id) references drop_fk_index_parent(id))")

	err := execSQLExpectError(t, executor, "app", "alter table drop_fk_index_child drop index fk_drop_index")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
	require.Equal(t, [][]interface{}{{"fk_drop_index"}}, mustQuerySQL(t, executor, "app", "select index_name from information_schema.statistics where table_schema = 'app' and table_name = 'drop_fk_index_child' and index_name = 'fk_drop_index'"))
}

func TestAlterTableDropIndexRejectsReferencedParentIndex(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_parent_index_parent (id int primary key, code int, unique key uq_drop_code (code))")
	mustExecSQL(t, executor, "app", "create table drop_parent_index_child (id int primary key, parent_code int, constraint fk_drop_code foreign key (parent_code) references drop_parent_index_parent(code))")

	err := execSQLExpectError(t, executor, "app", "alter table drop_parent_index_parent drop index uq_drop_code")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
	require.Equal(t, [][]interface{}{{"uq_drop_code"}}, mustQuerySQL(t, executor, "app", "select index_name from information_schema.statistics where table_schema = 'app' and table_name = 'drop_parent_index_parent' and index_name = 'uq_drop_code'"))
}

func TestAlterTableCompoundColumnAndConstraintOperations(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table compound_alter (id int primary key)")

	mustExecSQL(t, executor, "app", "alter table compound_alter add column amount int, add constraint chk_amount check (amount > 0), add unique index uq_amount (amount)")
	mustExecSQL(t, executor, "app", "insert into compound_alter (id, amount) values (1, 10)")
	invalid := <-executor.ExecuteQuery(nil, "insert into compound_alter (id, amount) values (2, 0)", "app")
	require.Error(t, invalid.Err)
	duplicate := <-executor.ExecuteQuery(nil, "insert into compound_alter (id, amount) values (3, 10)", "app")
	require.Error(t, duplicate.Err)
	rows := mustQuerySQL(t, executor, "app", "select amount from compound_alter where id = 1")
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 1)
	require.NotNil(t, rows[0][0])
}

func TestAlterTableQualifiedNameUsesTargetDatabase(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "", "create database other")
	mustExecSQL(t, executor, "app", "create table qualified_alter (id int primary key)")

	result := <-executor.ExecuteQuery(nil, "alter table app.qualified_alter add column note varchar(20)", "other")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"note"}}, mustQuerySQL(t, executor, "app", "select column_name from information_schema.columns where table_schema = 'app' and table_name = 'qualified_alter' and column_name = 'note'"))
}

func TestCompoundAlterQualifiedNameUsesTargetDatabase(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "", "create database other")
	mustExecSQL(t, executor, "app", "create table qualified_compound (id int primary key)")

	result := <-executor.ExecuteQuery(nil, "alter table app.qualified_compound add column note varchar(20), add unique index uq_note (note)", "other")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"note"}}, mustQuerySQL(t, executor, "app", "select column_name from information_schema.columns where table_schema = 'app' and table_name = 'qualified_compound' and column_name = 'note'"))
	require.Equal(t, [][]interface{}{{"uq_note"}}, mustQuerySQL(t, executor, "app", "select index_name from information_schema.statistics where table_schema = 'app' and table_name = 'qualified_compound' and index_name = 'uq_note'"))
}

func TestQualifiedAlterTableOptionsUseTargetDatabase(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "", "create database other")
	mustExecSQL(t, executor, "app", "create table qualified_options (id int primary key)")

	result := <-executor.ExecuteQuery(nil, "alter table app.qualified_options comment = 'target comment'", "other")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"target comment"}}, mustQuerySQL(t, executor, "app", "select table_comment from information_schema.tables where table_schema = 'app' and table_name = 'qualified_options'"))
}

func TestQualifiedAlterConstraintUsesTargetDatabase(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "", "create database other")
	mustExecSQL(t, executor, "app", "create table qualified_constraint (id int primary key, amount int)")

	result := <-executor.ExecuteQuery(nil, "alter table app.qualified_constraint add constraint chk_amount check (amount > 0)", "other")
	require.NoError(t, result.Err)
	invalid := <-executor.ExecuteQuery(nil, "insert into app.qualified_constraint (id, amount) values (1, 0)", "other")
	require.Error(t, invalid.Err)
	require.Contains(t, strings.ToLower(invalid.Err.Error()), "check")
}
