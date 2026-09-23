package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

var alterTableTablespaceTransferPattern = regexp.MustCompile(`(?is)^\s*alter\s+table\s+(` + compatibilityIdentifierPattern + `(?:\s*\.\s*` + compatibilityIdentifierPattern + `)?)\s+(discard|import)\s+tablespace\s*$`)

func (e *XMySQLExecutor) executeRawTableTablespaceTransfer(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	match := alterTableTablespaceTransferPattern.FindStringSubmatch(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if len(match) != 3 {
		return false, nil
	}
	parts := strings.Split(match[1], ".")
	schemaName := strings.Trim(parts[0], "` ")
	tableName := strings.Trim(parts[len(parts)-1], "` ")
	if len(parts) == 1 {
		schemaName = databaseName
	}
	if schemaName == "" || tableName == "" {
		return true, fmt.Errorf("ALTER TABLE %s TABLESPACE requires schema and table", strings.ToUpper(match[2]))
	}
	if e.storageManager == nil || e.tableStorageManager == nil {
		return true, fmt.Errorf("storage managers are not available")
	}
	if strings.EqualFold(match[2], "discard") {
		return true, e.discardFilePerTableTablespace(ctx, schemaName, tableName)
	}
	return true, e.importFilePerTableTablespace(ctx, schemaName, tableName)
}

func (e *XMySQLExecutor) discardFilePerTableTablespace(ctx *ExecutionContext, schemaName, tableName string) error {
	info, err := e.tableStorageManager.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return err
	}
	defaultName := filepath.ToSlash(filepath.Join(schemaName, tableName))
	if len(info.Partitions) == 0 && (!info.OwnsTablespace || !strings.EqualFold(strings.TrimSpace(info.TablespaceName), defaultName)) {
		return fmt.Errorf("cannot DISCARD TABLESPACE for table %s.%s in a shared or general tablespace", schemaName, tableName)
	}
	for _, partition := range info.Partitions {
		partitionDefault := filepath.ToSlash(filepath.Join(schemaName, tableName+"#"+partition.Name))
		if !partition.OwnsTablespace || !strings.EqualFold(strings.TrimSpace(partition.TablespaceName), partitionDefault) {
			return fmt.Errorf("cannot DISCARD TABLESPACE for partitioned table %s.%s in a shared or general tablespace", schemaName, tableName)
		}
	}
	frmPath := filepath.Join(e.getDataDir(), schemaName, tableName+".frm")
	original, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	discarded := cloneStringInterfaceMap(original)
	discarded["tablespace_discarded"] = true
	discarded["discarded_space_id"] = info.SpaceID
	discarded["discarded_root_page"] = info.RootPageNo
	discarded["storage_space_id"] = 0
	discarded["storage_root_page"] = 0
	if err := writeTableMetadataMapAtomic(frmPath, discarded); err != nil {
		return err
	}
	if err := e.tableStorageManager.UnregisterTable(schemaName, tableName); err != nil {
		_ = writeTableMetadataMapAtomic(frmPath, original)
		return err
	}
	if err := clearBTreeSidecarForSpace(e.getDataDir(), info.SpaceID); err != nil {
		_ = writeTableMetadataMapAtomic(frmPath, original)
		_ = e.tableStorageManager.RegisterTable(ctxContext(ctx), info)
		return fmt.Errorf("clear discarded tablespace sidecar failed: %w", err)
	}
	for _, partition := range info.Partitions {
		if err := clearBTreeSidecarForSpace(e.getDataDir(), partition.SpaceID); err != nil {
			_ = writeTableMetadataMapAtomic(frmPath, original)
			_ = e.tableStorageManager.RegisterTable(ctxContext(ctx), info)
			return fmt.Errorf("clear discarded partition sidecar failed: %w", err)
		}
	}
	if len(info.Partitions) == 0 {
		if err := e.storageManager.DeleteSpace(info.SpaceID); err != nil {
			_ = writeTableMetadataMapAtomic(frmPath, original)
			_ = e.tableStorageManager.RegisterTable(ctxContext(ctx), info)
			return fmt.Errorf("discard tablespace failed: %w", err)
		}
	}
	return writeTablespaceResult(ctx, fmt.Sprintf("Table '%s.%s' discarded its tablespace", schemaName, tableName))
}

func (e *XMySQLExecutor) importFilePerTableTablespace(ctx *ExecutionContext, schemaName, tableName string) error {
	frmPath := filepath.Join(e.getDataDir(), schemaName, tableName+".frm")
	definition, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	if discarded, _ := definition["tablespace_discarded"].(bool); !discarded {
		return fmt.Errorf("table %s.%s does not have a discarded tablespace", schemaName, tableName)
	}
	spaceID, ok := metadataUint32(definition["discarded_space_id"])
	if !ok || spaceID == 0 {
		return fmt.Errorf("discarded tablespace identity is missing for %s.%s", schemaName, tableName)
	}
	rootPage, _ := metadataUint32(definition["discarded_root_page"])
	spaceName := filepath.ToSlash(filepath.Join(schemaName, tableName))
	if persistedName, ok := definition["tablespace_name"].(string); ok && strings.TrimSpace(persistedName) != "" {
		spaceName = strings.TrimSpace(persistedName)
	}
	if !strings.EqualFold(spaceName, filepath.ToSlash(filepath.Join(schemaName, tableName))) {
		return fmt.Errorf("IMPORT TABLESPACE only supports file-per-table storage")
	}
	ibdPath := filepath.Join(e.getDataDir(), filepath.FromSlash(spaceName)+".ibd")
	if _, err := os.Stat(ibdPath); err != nil {
		return fmt.Errorf("import tablespace file %s is not present: %w", ibdPath, err)
	}
	partitioning, _ := definition["partitioning"].(map[string]interface{})
	partitionDefinitions := persistedPartitionDefinitions(partitioning)
	if len(partitionDefinitions) == 0 {
		if _, err := e.storageManager.ImportTablespace(spaceName, spaceID); err != nil {
			return err
		}
	} else {
		for _, partition := range partitionDefinitions {
			partitionName, _ := partition["name"].(string)
			partitionSpaceID, ok := metadataUint32(partition["storage_space_id"])
			if strings.TrimSpace(partitionName) == "" || !ok || partitionSpaceID == 0 {
				return fmt.Errorf("discarded partition identity is missing for %s.%s", schemaName, tableName)
			}
			partitionSpaceName := filepath.ToSlash(filepath.Join(schemaName, tableName+"#"+partitionName))
			if persistedName, ok := partition["tablespace_name"].(string); ok && strings.TrimSpace(persistedName) != "" {
				partitionSpaceName = strings.TrimSpace(persistedName)
			}
			partitionPath := filepath.Join(e.getDataDir(), filepath.FromSlash(partitionSpaceName)+".ibd")
			if _, err := os.Stat(partitionPath); err != nil {
				return fmt.Errorf("import partition tablespace file %s is not present: %w", partitionPath, err)
			}
			if _, err := e.storageManager.ImportTablespace(partitionSpaceName, partitionSpaceID); err != nil {
				return err
			}
		}
	}
	handle, _ := e.storageManager.GetTablespace(spaceName)
	if handle == nil && len(partitionDefinitions) == 0 {
		return fmt.Errorf("imported tablespace %s is not attached", spaceName)
	}
	info := &manager.TableStorageInfo{
		SchemaName:     schemaName,
		TableName:      tableName,
		SpaceID:        spaceID,
		RootPageNo:     rootPage,
		IndexPageNo:    rootPage,
		DataSegmentID:  uint64(spaceID),
		Type:           manager.TableTypeUser,
		TablespaceName: spaceName,
		OwnsTablespace: true,
	}
	if err := e.tableStorageManager.RegisterTable(ctxContext(ctx), info); err != nil {
		return err
	}
	if partitioning != nil {
		if err := e.tableStorageManager.SetPartitioning(schemaName, tableName, partitioning); err != nil {
			_ = e.tableStorageManager.UnregisterTable(schemaName, tableName)
			return err
		}
	}
	restored := cloneStringInterfaceMap(definition)
	delete(restored, "tablespace_discarded")
	delete(restored, "discarded_space_id")
	delete(restored, "discarded_root_page")
	restored["storage_space_id"] = spaceID
	restored["storage_root_page"] = rootPage
	restored["owns_tablespace"] = true
	if err := writeTableMetadataMapAtomic(frmPath, restored); err != nil {
		_ = e.tableStorageManager.UnregisterTable(schemaName, tableName)
		return err
	}
	return writeTablespaceResult(ctx, fmt.Sprintf("Table '%s.%s' imported its tablespace", schemaName, tableName))
}

func persistedPartitionDefinitions(partitioning map[string]interface{}) []map[string]interface{} {
	if partitioning == nil {
		return nil
	}
	var result []map[string]interface{}
	switch values := partitioning["partitions"].(type) {
	case []interface{}:
		for _, value := range values {
			if partition, ok := value.(map[string]interface{}); ok {
				result = append(result, partition)
			}
		}
	case []map[string]interface{}:
		result = append(result, values...)
	}
	return result
}

func cloneStringInterfaceMap(input map[string]interface{}) map[string]interface{} {
	clone := make(map[string]interface{}, len(input))
	for key, value := range input {
		clone[key] = value
	}
	return clone
}

func metadataUint32(value interface{}) (uint32, bool) {
	switch typed := value.(type) {
	case uint32:
		return typed, true
	case int:
		return uint32(typed), typed >= 0
	case float64:
		return uint32(typed), typed >= 0 && uint32(typed) == uint32(typed)
	default:
		return 0, false
	}
}
