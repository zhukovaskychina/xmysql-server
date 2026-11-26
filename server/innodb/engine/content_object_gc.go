package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// ReclaimOrphanedContentObjects marks references from every registered user
// table and sweeps published objects absent from that inventory. It is only
// safe during exclusive startup after transaction recovery has completed.
// A retained XA manifest causes a conservative skip because prepared row
// images may still depend on content objects outside the visible table state.
func (e *XMySQLExecutor) ReclaimOrphanedContentObjects(ctx context.Context, store *content.SegmentStore) (reclaimed int, skipped bool, err error) {
	if e == nil || store == nil || e.tableStorageManager == nil {
		return 0, true, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	hasRetainedXA, err := e.hasRetainedXAManifests()
	if err != nil {
		return 0, false, err
	}
	if hasRetainedXA {
		return 0, true, nil
	}

	dml := &StorageIntegratedDMLExecutor{
		dataDir:             e.getDataDir(),
		tableStorageManager: e.tableStorageManager,
	}
	complete, err := e.contentTableMappingsComplete(dml)
	if err != nil {
		return 0, false, err
	}
	if !complete {
		return 0, true, nil
	}
	tables := e.tableStorageManager.ListAllTables()
	tableKeys := make([]string, 0, len(tables))
	for key := range tables {
		tableKeys = append(tableKeys, key)
	}
	sort.Strings(tableKeys)
	live := make(map[string]content.ObjectRef)
	for _, tableKey := range tableKeys {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		storageInfo := tables[tableKey]
		if storageInfo == nil || storageInfo.Type != manager.TableTypeUser {
			continue
		}
		schemaName, tableName := storageInfo.SchemaName, storageInfo.TableName
		if isVirtualSystemSchema(schemaName) {
			// These schemas are managed by server registries and may appear in
			// the storage mapping without a user-table .frm/storage pair. They
			// cannot own user CONTENT references, so exclude them from the
			// physical user-table inventory used for safe reclamation.
			continue
		}
		if strings.TrimSpace(schemaName) == "" || strings.TrimSpace(tableName) == "" {
			return 0, false, fmt.Errorf("invalid table identity in content reference inventory %q", tableKey)
		}
		tableMeta, _, btree, err := dml.tableAccess(ctx, schemaName, tableName)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// A stale/internal storage mapping means the reference inventory
				// is incomplete. Skip the sweep so startup can continue without
				// risking reclamation of an object referenced by an unscannable row.
				return 0, true, nil
			}
			return 0, false, fmt.Errorf("open %s.%s for CONTENT reference inventory: %w", schemaName, tableName, err)
		}
		contentColumns := make([]string, 0)
		for _, column := range tableMeta.Columns {
			if columnHasDataType(column, metadata.TypeContent) {
				contentColumns = append(contentColumns, column.Name)
			}
		}
		if len(contentColumns) == 0 {
			continue
		}
		scanner := NewClusteredIndexScanner(btree, tableMeta)
		err = scanner.VisitProjected(ctx, nil, contentColumns, func(row *InsertRowData, _ interface{}, _ uint32) error {
			for _, column := range contentColumns {
				value, exists := dmlContentValue(row.ColumnValues, column)
				if !exists || value.ExternalRef == nil {
					continue
				}
				ref := *value.ExternalRef
				if existing, duplicate := live[ref.ID]; duplicate && existing != ref {
					return fmt.Errorf("CONTENT object %s has conflicting row references", ref.ID)
				}
				live[ref.ID] = ref
			}
			return nil
		})
		if err != nil {
			return 0, false, fmt.Errorf("scan %s.%s for CONTENT references: %w", schemaName, tableName, err)
		}
	}
	queue, err := store.OpenMaintenanceQueue()
	if err != nil {
		return 0, false, fmt.Errorf("open CONTENT maintenance queue for object inventory: %w", err)
	}
	jobs, err := queue.List()
	if err != nil {
		return 0, true, nil
	}
	for _, job := range jobs {
		if job.Status != content.MaintenanceReady || !job.ResultStored {
			continue
		}
		encoded, found, err := queue.ReadResult(job.ID)
		if err != nil || !found {
			return 0, true, nil
		}
		manifest, err := decodeContentMaintenanceResultManifest(encoded)
		if err != nil || manifest.Version != 1 || manifest.JobID != job.ID || !manifest.Source.Matches(job.Source) {
			return 0, true, nil
		}
		if len(manifest.Blocks) == 0 {
			continue
		}
		ref := manifest.Object
		if ref.ID == "" || ref.MIMEType != content.BlockMIMEType || ref.Revision != 1 {
			return 0, true, nil
		}
		if existing, duplicate := live[ref.ID]; duplicate && existing != ref {
			return 0, true, nil
		}
		live[ref.ID] = ref
	}

	reclaimed, err = store.ReclaimUnreferenced(ctx, live)
	if err != nil {
		return 0, false, err
	}
	return reclaimed, false, nil
}

func isVirtualSystemSchema(schemaName string) bool {
	switch strings.ToLower(strings.TrimSpace(schemaName)) {
	case "mysql", "sys", "information_schema", "performance_schema":
		return true
	default:
		return false
	}
}

func (e *XMySQLExecutor) contentTableMappingsComplete(dml *StorageIntegratedDMLExecutor) (bool, error) {
	dataDir := e.getDataDir()
	schemas, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("scan database schemas before CONTENT reclamation: %w", err)
	}
	for _, schemaEntry := range schemas {
		if !schemaEntry.IsDir() || strings.HasPrefix(schemaEntry.Name(), ".") ||
			strings.HasPrefix(schemaEntry.Name(), "#") || strings.HasPrefix(schemaEntry.Name(), "@") ||
			strings.EqualFold(schemaEntry.Name(), "mysql") ||
			strings.EqualFold(schemaEntry.Name(), "sys") || strings.EqualFold(schemaEntry.Name(), "information_schema") ||
			strings.EqualFold(schemaEntry.Name(), "performance_schema") {
			continue
		}
		schemaName := schemaEntry.Name()
		files, err := os.ReadDir(filepath.Join(dataDir, schemaName))
		if err != nil {
			return false, fmt.Errorf("scan persisted tables in schema %s before CONTENT reclamation: %w", schemaName, err)
		}
		for _, file := range files {
			if file.IsDir() || !strings.EqualFold(filepath.Ext(file.Name()), ".frm") {
				continue
			}
			tableName := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))
			tableMeta, err := (&SelectExecutor{}).loadTableMetaFromFrm(dataDir, schemaName, tableName)
			if err != nil {
				return false, fmt.Errorf("read persisted metadata for %s.%s before CONTENT reclamation: %w", schemaName, tableName, err)
			}
			hasContent := false
			for _, column := range tableMeta.Columns {
				if columnHasDataType(column, metadata.TypeContent) {
					hasContent = true
					break
				}
			}
			if !hasContent {
				continue
			}
			info, err := dml.tableStorageManager.GetTableStorageInfo(schemaName, tableName)
			if err != nil || info == nil || info.Type != manager.TableTypeUser {
				return false, nil
			}
		}
	}
	return true, nil
}

func (e *XMySQLExecutor) hasRetainedXAManifests() (bool, error) {
	dataDir := e.getDataDir()
	for _, directory := range []string{preparedXAManifestDir(dataDir), suspendedXAManifestDir(dataDir)} {
		entries, err := os.ReadDir(directory)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("inspect retained XA manifests in %s: %w", directory, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
				continue
			}
			info, err := os.Lstat(filepath.Join(directory, entry.Name()))
			if err != nil {
				return false, fmt.Errorf("inspect retained XA manifest %s: %w", entry.Name(), err)
			}
			if info.Mode().IsRegular() {
				return true, nil
			}
			return false, fmt.Errorf("retained XA manifest %s is not a regular file", entry.Name())
		}
	}
	return false, nil
}
