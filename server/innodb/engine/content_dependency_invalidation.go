package engine

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
)

type contentSourceMutation struct {
	before content.SourceRef
	after  *content.SourceRef
}

// invalidateDerivedContentChanges marks READY content blocks stale when a
// source CONTENT value is changed or deleted. The scan and block writes use
// the caller's existing storage transaction, so a failed invalidation aborts
// the source statement instead of publishing a partially updated generation.
//
// The reverse directory locates rows known to contain matching derived
// references and re-reads those rows through the clustered B+Tree. The
// directory is rebuilt from committed rows after process restart; shared
// explicit transactions and same-transaction writes use the full-table
// streaming fallback to include uncommitted derived writes.
func (dml *StorageIntegratedDMLExecutor) invalidateDerivedContentChanges(
	ctx context.Context,
	txn interface{},
	changes []transactionDMLChange,
) ([]transactionDMLChange, error) {
	if dml == nil || dml.tableStorageManager == nil || len(changes) == 0 {
		return nil, nil
	}
	mutations, err := dml.contentSourceMutationsForChanges(ctx, changes)
	if err != nil || len(mutations) == 0 {
		return nil, err
	}
	changedTables := make(map[string]struct{})
	if transaction, ok := txn.(*StorageTransactionContext); !ok || transaction.Session == nil {
		for _, change := range changes {
			if len(readyDerivedSourceRefs(change.before))+len(readyDerivedSourceRefs(change.after)) > 0 {
				changedTables[strings.ToLower(change.tableName)] = struct{}{}
			}
		}
	}

	originalSchema, originalTable := dml.schemaName, dml.tableName
	defer func() {
		dml.schemaName, dml.tableName = originalSchema, originalTable
	}()

	allInvalidations := make([]transactionDMLChange, 0)
	for generation := 0; len(mutations) > 0; generation++ {
		if generation >= 64 {
			return nil, fmt.Errorf("derived CONTENT invalidation exceeded 64 dependency generations")
		}
		invalidations, err := dml.invalidateDerivedContentGeneration(ctx, txn, mutations, changedTables)
		if err != nil {
			return nil, err
		}
		if len(invalidations) == 0 {
			break
		}
		allInvalidations = append(allInvalidations, invalidations...)
		mutations, err = dml.contentSourceMutationsForChanges(ctx, invalidations)
		if err != nil {
			return nil, err
		}
	}
	return allInvalidations, nil
}

func (dml *StorageIntegratedDMLExecutor) contentSourceMutationsForChanges(
	ctx context.Context,
	changes []transactionDMLChange,
) ([]contentSourceMutation, error) {
	mutations := make([]contentSourceMutation, 0)
	for _, change := range changes {
		if change.kind != "update" && change.kind != "delete" || len(change.before) == 0 {
			continue
		}
		if change.columnTypes != nil && !transactionChangeHasContentColumn(change.columnTypes) {
			continue
		}
		schemaName, tableName, ok := strings.Cut(change.tableName, ".")
		if !ok || schemaName == "" || tableName == "" {
			return nil, fmt.Errorf("invalid table identity in transaction change %q", change.tableName)
		}
		tableMeta, _, _, err := dml.tableAccess(ctx, schemaName, tableName)
		if err != nil {
			return nil, fmt.Errorf("load metadata for CONTENT dependency source %s.%s: %w", schemaName, tableName, err)
		}
		beforeKey, beforeKeyExists, err := contentSourcePrimaryKey(change.before, tableMeta)
		if err != nil {
			return nil, fmt.Errorf("build CONTENT dependency source key for %s.%s: %w", schemaName, tableName, err)
		}
		if !beforeKeyExists {
			continue
		}
		afterKey, afterKeyExists := beforeKey, true
		if change.kind == "delete" {
			afterKeyExists = false
		} else {
			afterKey, afterKeyExists, err = contentSourcePrimaryKey(change.after, tableMeta)
			if err != nil {
				return nil, fmt.Errorf("build updated CONTENT dependency source key for %s.%s: %w", schemaName, tableName, err)
			}
		}

		for _, column := range tableMeta.Columns {
			if !columnHasDataType(column, metadata.TypeContent) {
				continue
			}
			beforeValue, exists := dmlContentValue(change.before, column.Name)
			if !exists {
				continue
			}
			beforeRef, err := content.NewSourceRef(schemaName, tableName, column.Name, beforeKey, beforeValue)
			if err != nil {
				return nil, fmt.Errorf("build CONTENT dependency source identity for %s.%s.%s: %w", schemaName, tableName, column.Name, err)
			}
			var afterRef *content.SourceRef
			if afterKeyExists {
				if afterValue, exists := dmlContentValue(change.after, column.Name); exists {
					candidate, sourceErr := content.NewSourceRef(schemaName, tableName, column.Name, afterKey, afterValue)
					if sourceErr == nil {
						afterRef = &candidate
					} else {
						return nil, fmt.Errorf("build updated CONTENT dependency source identity for %s.%s.%s: %w", schemaName, tableName, column.Name, sourceErr)
					}
				}
			}
			if afterRef != nil && beforeRef.Matches(*afterRef) {
				continue
			}
			mutations = append(mutations, contentSourceMutation{before: beforeRef, after: afterRef})
		}
	}
	return mutations, nil
}

func transactionChangeHasContentColumn(columnTypes map[string]string) bool {
	for _, columnType := range columnTypes {
		if strings.EqualFold(strings.TrimSpace(columnType), string(metadata.TypeContent)) {
			return true
		}
	}
	return false
}

func (dml *StorageIntegratedDMLExecutor) invalidateDerivedContentGeneration(
	ctx context.Context,
	txn interface{},
	mutations []contentSourceMutation,
	changedTables map[string]struct{},
) ([]transactionDMLChange, error) {
	invalidatedSources := make(map[string][]content.SourceRef, len(mutations))
	for _, mutation := range mutations {
		if mutation.after != nil && mutation.before.Matches(*mutation.after) {
			continue
		}
		key := sourceVersionLookupKey(mutation.before)
		invalidatedSources[key] = append(invalidatedSources[key], mutation.before)
	}
	if len(invalidatedSources) == 0 {
		return nil, nil
	}
	sharedStorageTransaction := false
	if transaction, ok := txn.(*StorageTransactionContext); ok && transaction.Session != nil {
		sharedStorageTransaction = true
	}
	var dependencyIndex *contentDependencyIndex
	if !sharedStorageTransaction {
		var indexErr error
		dependencyIndex, indexErr = dml.rebuildContentDependencyIndex(ctx)
		if indexErr != nil {
			logger.Warnf("CONTENT dependency index unavailable; using table scan fallback: %v", indexErr)
		}
	}
	tableMap := dml.tableStorageManager.ListAllTables()
	var candidateTables map[string]struct{}
	var dependencyRows map[string][]contentDependencyRowLocator
	indexed := false
	if dependencyIndex != nil {
		dependencyRows, indexed = dependencyIndex.candidateRows(mutations, tableMap)
	}
	if indexed {
		candidateTables = make(map[string]struct{}, len(dependencyRows)+len(changedTables))
		for table := range dependencyRows {
			candidateTables[table] = struct{}{}
		}
		for table := range changedTables {
			candidateTables[table] = struct{}{}
		}
	}
	tableKeys := make([]string, 0, len(tableMap))
	for key := range tableMap {
		tableKeys = append(tableKeys, key)
	}
	sort.Strings(tableKeys)

	invalidations := make([]transactionDMLChange, 0)
	for _, tableKey := range tableKeys {
		storageInfo := tableMap[tableKey]
		if storageInfo == nil || storageInfo.Type != manager.TableTypeUser {
			continue
		}
		schemaName, tableName := storageInfo.SchemaName, storageInfo.TableName
		if strings.TrimSpace(schemaName) == "" || strings.TrimSpace(tableName) == "" {
			continue
		}
		if indexed {
			if _, candidate := candidateTables[strings.ToLower(schemaName+"."+tableName)]; !candidate {
				continue
			}
		}
		tableMeta, tableStorageInfo, btreeManager, err := dml.tableAccess(ctx, schemaName, tableName)
		if err != nil {
			return nil, fmt.Errorf("open table %s.%s while invalidating derived CONTENT: %w", schemaName, tableName, err)
		}
		blockColumns := make([]*metadata.ColumnMeta, 0)
		vectorColumns := make([]*metadata.ColumnMeta, 0)
		for _, column := range tableMeta.Columns {
			if column == nil {
				continue
			}
			switch metadata.DataType(strings.ToUpper(string(column.Type))) {
			case metadata.TypeContent:
				blockColumns = append(blockColumns, column)
			case metadata.TypeVector:
				vectorColumns = append(vectorColumns, column)
			}
		}
		if len(blockColumns) == 0 && len(vectorColumns) == 0 {
			continue
		}
		// Stream the table scan and retain only rows whose bound provenance can
		// match a changed source. The previous helper materialized every row in
		// the table before this filter, making one CONTENT update consume memory
		// proportional to the entire derived table. When the committed reverse
		// directory has exact row locators, read only those candidate rows through
		// the clustered B+Tree; same-transaction writes and missing locators use
		// the streaming fallback to preserve visibility.
		scanner := NewClusteredIndexScanner(btreeManager, tableMeta)
		rows := make([]*RowUpdateInfo, 0)
		scanSlot := 0
		considerRow := func(values *InsertRowData, storageKey interface{}, pageNumber uint32, slot int) error {
			if values == nil {
				return nil
			}
			matches, matchErr := derivedRowHasInvalidatedSource(values.ColumnValues, blockColumns, vectorColumns, invalidatedSources)
			if matchErr != nil {
				return fmt.Errorf("inspect derived CONTENT row in %s.%s: %w", schemaName, tableName, matchErr)
			}
			if !matches {
				return nil
			}
			pageNo := tableStorageInfo.RootPageNo
			if pageNumber != 0 {
				pageNo = pageNumber
			}
			rows = append(rows, &RowUpdateInfo{
				RowId: dml.rowIDFromRowData(values, tableMeta), StorageKey: storageKey,
				PageNum: pageNo, SlotIndex: slot, SchemaName: schemaName,
				TableName: tableName, OldValues: values.ColumnValues,
			})
			return nil
		}
		locatorKey := strings.ToLower(schemaName + "." + tableName)
		_, sameTransactionWrite := changedTables[locatorKey]
		locators := dependencyRows[locatorKey]
		canPointRead := indexed && !sameTransactionWrite && len(locators) > 0
		if canPointRead {
			for _, locator := range locators {
				if locator.lookupKey == nil || locator.storageKey == nil {
					canPointRead = false
					break
				}
			}
		}
		if canPointRead {
			for _, locator := range locators {
				values, pageNumber, found, lookupErr := scanner.FindByKey(ctx, locator.lookupKey)
				if lookupErr != nil {
					return nil, fmt.Errorf("lookup candidate row in %s.%s while invalidating derived CONTENT: %w", schemaName, tableName, lookupErr)
				}
				if !found {
					continue
				}
				if err := considerRow(values, locator.storageKey, pageNumber, scanSlot); err != nil {
					return nil, fmt.Errorf("inspect indexed row in %s.%s while invalidating derived CONTENT: %w", schemaName, tableName, err)
				}
				scanSlot++
			}
		} else {
			err = scanner.VisitProjected(ctx, nil, nil, func(values *InsertRowData, storageKey interface{}, pageNumber uint32) error {
				err := considerRow(values, storageKey, pageNumber, scanSlot)
				scanSlot++
				return err
			})
		}
		if err != nil {
			return nil, fmt.Errorf("scan table %s.%s while invalidating derived CONTENT: %w", schemaName, tableName, err)
		}

		for _, row := range rows {
			if row == nil {
				continue
			}
			updates := make([]*UpdateExpression, 0)
			for _, column := range blockColumns {
				blockValue, exists := dmlContentValue(row.OldValues, column.Name)
				if !exists || blockValue.MIMEType != content.BlockMIMEType {
					continue
				}
				block, err := content.BlockFromValue(blockValue)
				if err != nil {
					return nil, fmt.Errorf("decode derived CONTENT block at %s.%s.%s: %w", schemaName, tableName, column.Name, err)
				}
				if block.State != content.BlockStateReady || !sourceIsInvalidated(block.Source, invalidatedSources) {
					continue
				}
				block.State = content.BlockStateStale
				encoded, err := block.MarshalBinary()
				if err != nil {
					return nil, fmt.Errorf("encode stale derived CONTENT block at %s.%s.%s: %w", schemaName, tableName, column.Name, err)
				}
				if blockValue.Revision == ^uint64(0) {
					return nil, fmt.Errorf("derived CONTENT block revision overflow at %s.%s.%s", schemaName, tableName, column.Name)
				}
				staleValue, err := content.NewValue(content.BlockMIMEType, blockValue.Revision, encoded)
				if err != nil {
					return nil, fmt.Errorf("prepare stale derived CONTENT block at %s.%s.%s: %w", schemaName, tableName, column.Name, err)
				}
				updates = append(updates, &UpdateExpression{ColumnName: column.Name, NewValue: staleValue, ColumnType: column.Type})
			}
			for _, column := range vectorColumns {
				value, exists := dmlVectorValue(row.OldValues, column.Name)
				if !exists || value.State != vector.StateReady || value.Source == nil || !sourceIsInvalidated(*value.Source, invalidatedSources) {
					continue
				}
				value.State = vector.StateStale
				updates = append(updates, &UpdateExpression{ColumnName: column.Name, NewValue: value, ColumnType: column.Type})
			}
			if len(updates) == 0 {
				continue
			}
			previousSchema, previousTable := dml.schemaName, dml.tableName
			if err := dml.setTransactionTable(schemaName + "." + tableName); err != nil {
				dml.schemaName, dml.tableName = previousSchema, previousTable
				return nil, err
			}
			updatedRow, err := dml.applyUpdateExpressions(&InsertRowData{
				ColumnValues: cloneTransactionRow(row.OldValues),
				ColumnTypes:  make(map[string]metadata.DataType),
			}, updates, tableMeta)
			if err != nil {
				dml.schemaName, dml.tableName = previousSchema, previousTable
				return nil, fmt.Errorf("prepare stale derived CONTENT row %s.%s: %w", schemaName, tableName, err)
			}
			if err := dml.updateRowInStorage(ctx, txn, row, updates, tableMeta, tableStorageInfo, btreeManager); err != nil {
				dml.schemaName, dml.tableName = previousSchema, previousTable
				return nil, fmt.Errorf("write stale derived CONTENT row %s.%s: %w", schemaName, tableName, err)
			}
			if err := dml.updateIndexesForUpdate(ctx, txn, []*RowUpdateInfo{row}, updates, tableMeta, tableStorageInfo); err != nil {
				dml.schemaName, dml.tableName = previousSchema, previousTable
				return nil, fmt.Errorf("update stale derived CONTENT indexes %s.%s: %w", schemaName, tableName, err)
			}
			newStorageKey, err := dml.storageKeyForUpdatedRow(row, updatedRow, tableMeta)
			dml.schemaName, dml.tableName = previousSchema, previousTable
			if err != nil {
				return nil, fmt.Errorf("build stale derived CONTENT key %s.%s: %w", schemaName, tableName, err)
			}
			invalidations = append(invalidations, transactionDMLChange{
				tableName:     schemaName + "." + tableName,
				kind:          "update",
				rowID:         row.RowId,
				storageKey:    row.StorageKey,
				newStorageKey: newStorageKey,
				before:        cloneTransactionRow(row.OldValues),
				after:         cloneTransactionRow(updatedRow.ColumnValues),
				columnTypes:   cloneTransactionColumnTypes(tableMeta),
			})
		}
	}
	return invalidations, nil
}

func sourceIsInvalidated(source content.SourceRef, invalidated map[string][]content.SourceRef) bool {
	for _, candidate := range invalidated[sourceVersionLookupKey(source)] {
		if candidate.Matches(source) {
			return true
		}
	}
	return false
}

func derivedRowHasInvalidatedSource(
	values map[string]interface{},
	blockColumns, vectorColumns []*metadata.ColumnMeta,
	invalidated map[string][]content.SourceRef,
) (bool, error) {
	for _, column := range blockColumns {
		value, exists := dmlContentValue(values, column.Name)
		if !exists || value.MIMEType != content.BlockMIMEType {
			continue
		}
		block, err := content.BlockFromValue(value)
		if err != nil {
			return false, fmt.Errorf("decode CONTENT block column %s: %w", column.Name, err)
		}
		if block.State == content.BlockStateReady && sourceIsInvalidated(block.Source, invalidated) {
			return true, nil
		}
	}
	for _, column := range vectorColumns {
		value, exists := dmlVectorValue(values, column.Name)
		if exists && value.State == vector.StateReady && value.Source != nil && sourceIsInvalidated(*value.Source, invalidated) {
			return true, nil
		}
	}
	return false, nil
}

func sourceVersionLookupKey(source content.SourceRef) string {
	key := make([]byte, 8+len(source.PrimaryKey)+8+sha256.Size)
	binary.BigEndian.PutUint64(key[:8], uint64(len(source.PrimaryKey)))
	copy(key[8:], source.PrimaryKey)
	revisionOffset := 8 + len(source.PrimaryKey)
	binary.BigEndian.PutUint64(key[revisionOffset:revisionOffset+8], source.Revision)
	copy(key[revisionOffset+8:], source.Digest[:])
	return string(key)
}

func dmlVectorValue(row map[string]interface{}, column string) (vector.Value, bool) {
	value, exists := lookupDMLRowValue(row, column)
	if !exists {
		return vector.Value{}, false
	}
	switch typed := value.(type) {
	case vector.Value:
		return typed, true
	case *vector.Value:
		if typed != nil {
			return *typed, true
		}
	}
	return vector.Value{}, false
}
