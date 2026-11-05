package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func isAlterTableTablespaceQuery(query string) bool {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return false
	}
	stmt, ok := parsed.AST.(*sqlparser.AlterTable)
	return ok && len(stmt.Operations) == 1 && stmt.Operations[0].Kind == sqlparser.AlterOperationTablespace
}

// executeRawTableTablespaceCompatibility implements the data-moving form of
// ALTER TABLE ... TABLESPACE. It is intentionally separate from standalone
// tablespace DDL because the operation changes a table's clustered-index
// ownership and must preserve rows.
func (e *XMySQLExecutor) executeRawTableTablespaceCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return false, nil
	}
	stmt, ok := parsed.AST.(*sqlparser.AlterTable)
	if !ok || len(stmt.Operations) != 1 || stmt.Operations[0].Kind != sqlparser.AlterOperationTablespace {
		return false, nil
	}
	schemaName := strings.TrimSpace(stmt.Table.Qualifier.String())
	if schemaName == "" {
		schemaName = databaseName
	}
	tableName := strings.TrimSpace(stmt.Table.Name.String())
	targetName := unquoteTablespaceIdentifier(stmt.Operations[0].Name)
	return e.executeTableTablespaceCompatibility(ctx, schemaName, tableName, targetName)
}

// executeTableTablespaceCompatibility is the typed execution core shared by
// the AST ALTER path and the legacy text adapter. It receives already
// separated identifiers and therefore does not rediscover SQL syntax.
func (e *XMySQLExecutor) executeTableTablespaceCompatibility(ctx *ExecutionContext, schemaName, tableName, targetName string) (bool, error) {
	if schemaName == "" || tableName == "" || targetName == "" {
		return true, fmt.Errorf("ALTER TABLE TABLESPACE requires schema, table, and tablespace")
	}
	if e.storageManager == nil || e.tableStorageManager == nil {
		return true, fmt.Errorf("storage managers are not available")
	}
	target, err := e.storageManager.GetTablespace(targetName)
	if err != nil {
		return true, fmt.Errorf("general tablespace %s does not exist: %w", targetName, err)
	}
	current, err := e.tableStorageManager.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return true, err
	}
	if current.SpaceID == target.SpaceID {
		return true, writeTablespaceResult(ctx, fmt.Sprintf("Table '%s.%s' already uses tablespace '%s'", schemaName, tableName, targetName))
	}

	selectStmt, err := sqlparser.Parse("select * from " + quotePartitionIdentifier(schemaName) + "." + quotePartitionIdentifier(tableName))
	if err != nil {
		return true, err
	}
	selectQuery, ok := selectStmt.(*sqlparser.Select)
	if !ok {
		return true, fmt.Errorf("ALTER TABLE TABLESPACE source is not SELECT")
	}
	result, err := e.executeSelectStatement(ctx, selectQuery, schemaName)
	if err != nil {
		return true, err
	}
	rows := make([]partitionReorganizationRow, 0, len(result.Records))
	for _, record := range result.Records {
		values := record.GetValues()
		rowValues := make([]interface{}, len(result.Columns))
		for index := range result.Columns {
			if index < len(values) && !values[index].IsNull() {
				rowValues[index] = values[index].Raw()
			}
		}
		rows = append(rows, partitionReorganizationRow{columns: append([]string(nil), result.Columns...), values: rowValues})
	}

	oldInfo := *current
	oldInfo.Partitioning = cloneTablePartitioning(current.Partitioning)
	oldInfo.Partitions = append([]manager.PartitionStorageInfo(nil), current.Partitions...)
	oldMetadata, err := readTableMetadataMap(fmt.Sprintf("%s/%s/%s.frm", e.getDataDir(), schemaName, tableName))
	if err != nil {
		return true, err
	}
	if err := e.tableStorageManager.UnregisterTable(schemaName, tableName); err != nil {
		return true, err
	}
	newInfo := oldInfo
	newInfo.SpaceID = target.SpaceID
	newInfo.RootPageNo = 0
	newInfo.IndexPageNo = 0
	newInfo.DataSegmentID = target.DataSegmentID
	newInfo.TablespaceName = targetName
	newInfo.OwnsTablespace = false
	if len(oldInfo.Partitions) > 0 {
		newInfo.Partitions = make([]manager.PartitionStorageInfo, len(oldInfo.Partitions))
		for index, partition := range oldInfo.Partitions {
			partition.SpaceID = target.SpaceID
			partition.RootPageNo = 0
			partition.DataSegmentID = target.DataSegmentID
			partition.TablespaceName = targetName
			partition.OwnsTablespace = false
			newInfo.Partitions[index] = partition
		}
	}
	if err := e.tableStorageManager.RegisterTable(ctxContext(ctx), &newInfo); err != nil {
		_ = e.tableStorageManager.RegisterTable(ctxContext(ctx), &oldInfo)
		return true, err
	}
	if err := persistTableStorageIdentity(e.getDataDir(), &newInfo); err != nil {
		_ = e.tableStorageManager.UnregisterTable(schemaName, tableName)
		_ = e.tableStorageManager.RegisterTable(ctxContext(ctx), &oldInfo)
		_ = writeTableMetadataMapAtomic(fmt.Sprintf("%s/%s/%s.frm", e.getDataDir(), schemaName, tableName), oldMetadata)
		return true, err
	}
	if err := e.restorePartitionReorganizationRows(ctx, schemaName, tableName, rows); err != nil {
		_ = e.tableStorageManager.UnregisterTable(schemaName, tableName)
		_ = e.tableStorageManager.RegisterTable(ctxContext(ctx), &oldInfo)
		_ = writeTableMetadataMapAtomic(fmt.Sprintf("%s/%s/%s.frm", e.getDataDir(), schemaName, tableName), oldMetadata)
		return true, err
	}
	if oldInfo.OwnsTablespace {
		_ = clearBTreeSidecarForSpace(e.getDataDir(), oldInfo.SpaceID)
		if err := e.storageManager.DeleteSpace(oldInfo.SpaceID); err != nil {
			return true, fmt.Errorf("drop old tablespace after move failed: %w", err)
		}
	}
	if migrated, err := e.tableStorageManager.GetTableStorageInfo(schemaName, tableName); err == nil {
		if err := persistTableStorageIdentity(e.getDataDir(), migrated); err != nil {
			return true, err
		}
		if len(migrated.Partitions) > 0 {
			if err := persistPartitionStorageRoots(e.getDataDir(), schemaName, tableName, e.tableStorageManager); err != nil {
				return true, err
			}
		}
	}
	if ctx != nil && ctx.Results != nil {
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Table '%s.%s' moved to tablespace '%s'", schemaName, tableName, targetName)}
	}
	return true, nil
}

func ctxContext(ctx *ExecutionContext) context.Context {
	if ctx != nil && ctx.Context != nil {
		return ctx.Context
	}
	return context.Background()
}
