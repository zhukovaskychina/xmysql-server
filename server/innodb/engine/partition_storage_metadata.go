package engine

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// persistPartitionStorageRoots records the physical identity needed to reopen
// partition clustered indexes after restart.  The logical partition
// descriptor remains the source of truth; these fields are only its durable
// storage locator extension.
func persistPartitionStorageRoots(dataDir, schemaName, tableName string, tableStorageManager *manager.TableStorageManager) error {
	if dataDir == "" || tableStorageManager == nil {
		return nil
	}
	storageInfo, err := tableStorageManager.GetTableStorageInfo(schemaName, tableName)
	if err != nil || len(storageInfo.Partitions) == 0 {
		return err
	}
	frmPath := filepath.Join(dataDir, schemaName, tableName+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	partitioning, ok := tableInfo["partitioning"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("partition metadata for %s.%s is missing", schemaName, tableName)
	}
	definitions, ok := partitioning["partitions"].([]interface{})
	if !ok {
		return fmt.Errorf("partition metadata for %s.%s is invalid", schemaName, tableName)
	}
	byName := make(map[string]manager.PartitionStorageInfo, len(storageInfo.Partitions))
	for _, partition := range storageInfo.Partitions {
		byName[strings.ToLower(strings.TrimSpace(partition.Name))] = partition
	}
	for _, raw := range definitions {
		definition, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := definition["name"].(string)
		partition, exists := byName[strings.ToLower(strings.TrimSpace(name))]
		if !exists {
			continue
		}
		definition["storage_space_id"] = partition.SpaceID
		definition["storage_root_page"] = partition.RootPageNo
		definition["data_segment_id"] = partition.DataSegmentID
		if strings.TrimSpace(partition.TablespaceName) != "" {
			definition["tablespace_name"] = partition.TablespaceName
		}
		definition["owns_tablespace"] = partition.OwnsTablespace
	}
	tableInfo["partitioning"] = partitioning
	return writeTableMetadataMapAtomic(frmPath, tableInfo)
}
