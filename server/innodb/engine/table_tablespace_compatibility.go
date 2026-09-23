package engine

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

var alterTableTablespacePattern = regexp.MustCompile(`(?is)^\s*alter\s+table\s+(` + compatibilityIdentifierPattern + `(?:\s*\.\s*` + compatibilityIdentifierPattern + `)?)\s+tablespace\s+(` + compatibilityIdentifierPattern + `)(?:\s+storage\s+disk)?\s*$`)

// executeRawTableTablespaceCompatibility implements the data-moving form of
// ALTER TABLE ... TABLESPACE. It is intentionally separate from standalone
// tablespace DDL because the operation changes a table's clustered-index
// ownership and must preserve rows.
func (e *XMySQLExecutor) executeRawTableTablespaceCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	match := alterTableTablespacePattern.FindStringSubmatch(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if len(match) != 3 {
		return false, nil
	}
	parts := strings.Split(match[1], ".")
	schemaName := strings.Trim(parts[0], "` ")
	tableName := strings.Trim(parts[len(parts)-1], "` ")
	if len(parts) == 1 {
		schemaName = databaseName
	}
	targetName := unquoteTablespaceIdentifier(match[2])
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
