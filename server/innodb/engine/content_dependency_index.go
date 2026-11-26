package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
)

const contentDependencyIndexVersion = 1

type contentDependencyIndex struct {
	mu             sync.RWMutex
	dataDir        string
	ready          bool
	dirty          bool
	tableSignature string
	byKey          map[string]map[string]*contentDependencyEntry
}

type contentDependencyEntry struct {
	source content.SourceRef
	table  string
	count  uint64
	rows   []contentDependencyRowLocator
}

type contentDependencyRowLocator struct {
	lookupKey  interface{}
	storageKey interface{}
}

type contentDependencySnapshot struct {
	Version int                            `json:"version"`
	Entries []contentDependencySnapshotRow `json:"entries"`
	Digest  string                         `json:"sha256"`
}

type contentDependencySnapshotRow struct {
	Source content.SourceRef `json:"source"`
	Table  string            `json:"table"`
	Count  uint64            `json:"count"`
}

var contentDependencyIndexes = struct {
	sync.Mutex
	items map[string]*contentDependencyIndex
}{items: make(map[string]*contentDependencyIndex)}

func dependencyIndexForDataDir(dataDir string) *contentDependencyIndex {
	key := canonicalDependencyDataDir(dataDir)
	if key == "" {
		return nil
	}
	contentDependencyIndexes.Lock()
	defer contentDependencyIndexes.Unlock()
	if index := contentDependencyIndexes.items[key]; index != nil {
		return index
	}
	index := &contentDependencyIndex{dataDir: key, byKey: make(map[string]map[string]*contentDependencyEntry)}
	contentDependencyIndexes.items[key] = index
	return index
}

func canonicalDependencyDataDir(dataDir string) string {
	dataDir = strings.TrimSpace(dataDir)
	if dataDir == "" {
		return ""
	}
	absolute, err := filepath.Abs(dataDir)
	if err != nil {
		absolute = filepath.Clean(dataDir)
	}
	return filepath.Clean(absolute)
}

// ensureContentDependencyIndex rebuilds its in-memory reverse map from the
// committed CONTENT_BLOCK and VECTOR values. It deliberately does not load a
// prior snapshot as authoritative: a process may have stopped after a storage
// commit but before publishing the latest snapshot. The durable snapshot is
// therefore a checkpoint and diagnostic artifact, while the row store is the
// source of truth after restart.
func (index *contentDependencyIndex) ensureContentDependencyIndex(ctx context.Context, dml *StorageIntegratedDMLExecutor) error {
	if index == nil || dml == nil || dml.tableStorageManager == nil {
		return fmt.Errorf("CONTENT dependency index requires a database executor and table catalog")
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	if index.ready && !index.dirty {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	originalSchema, originalTable := dml.schemaName, dml.tableName
	defer func() { dml.schemaName, dml.tableName = originalSchema, originalTable }()

	entries := make(map[string]map[string]*contentDependencyEntry)
	tableMap := dml.tableStorageManager.ListAllTables()
	initialTableSignature := contentDependencyTableSignature(tableMap)
	tableKeys := make([]string, 0, len(tableMap))
	for key := range tableMap {
		tableKeys = append(tableKeys, key)
	}
	sort.Strings(tableKeys)
	for _, tableKey := range tableKeys {
		if err := ctx.Err(); err != nil {
			return err
		}
		storageInfo := tableMap[tableKey]
		if storageInfo == nil || storageInfo.Type != manager.TableTypeUser {
			continue
		}
		schemaName, tableName := storageInfo.SchemaName, storageInfo.TableName
		if strings.TrimSpace(schemaName) == "" || strings.TrimSpace(tableName) == "" {
			return fmt.Errorf("invalid table identity %q while building CONTENT dependency index", tableKey)
		}
		tableMeta, _, btree, err := dml.tableAccess(ctx, schemaName, tableName)
		if err != nil {
			return fmt.Errorf("open %s.%s while building CONTENT dependency index: %w", schemaName, tableName, err)
		}
		hasDerivedColumns := false
		for _, column := range tableMeta.Columns {
			if columnHasDataType(column, metadata.TypeContent) || columnHasDataType(column, metadata.TypeVector) {
				hasDerivedColumns = true
				break
			}
		}
		if !hasDerivedColumns {
			continue
		}
		scanner := NewClusteredIndexScanner(btree, tableMeta)
		err = scanner.VisitProjectedWithKeys(ctx, nil, nil, func(row *InsertRowData, lookupKey, storageKey interface{}, _ uint32) error {
			if row == nil {
				return nil
			}
			locator := contentDependencyRowLocator{
				lookupKey:  cloneDependencyIndexKey(lookupKey),
				storageKey: cloneDependencyIndexKey(storageKey),
			}
			for _, source := range readyDerivedSourceRefs(row.ColumnValues) {
				addContentDependencyEntry(entries, source, schemaName+"."+tableName, locator)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("scan %s.%s while building CONTENT dependency index: %w", schemaName, tableName, err)
		}
	}
	if currentSignature := contentDependencyTableSignature(dml.tableStorageManager.ListAllTables()); currentSignature != initialTableSignature {
		index.ready = false
		index.dirty = true
		return fmt.Errorf("table catalog changed while rebuilding CONTENT dependency index")
	}
	index.byKey = entries
	index.tableSignature = initialTableSignature
	index.ready = true
	index.dirty = false
	if err := index.persistLocked(); err != nil {
		index.ready = false
		return fmt.Errorf("persist CONTENT dependency index checkpoint: %w", err)
	}
	return nil
}

func readyDerivedSourceRefs(values map[string]interface{}) []content.SourceRef {
	refs := make([]content.SourceRef, 0)
	for _, raw := range values {
		value, ok := contentValueFromAny(raw)
		if !ok || value.MIMEType != content.BlockMIMEType {
			continue
		}
		block, err := content.BlockFromValue(value)
		if err == nil && block.State == content.BlockStateReady {
			refs = append(refs, block.Source)
		}
	}
	for _, raw := range values {
		var value vector.Value
		switch typed := raw.(type) {
		case vector.Value:
			value = typed
		case *vector.Value:
			if typed == nil {
				continue
			}
			value = *typed
		default:
			continue
		}
		if value.State == vector.StateReady && value.Source != nil {
			refs = append(refs, *value.Source)
		}
	}
	return refs
}

func contentValueFromAny(raw interface{}) (content.Value, bool) {
	switch typed := raw.(type) {
	case content.Value:
		return typed, true
	case *content.Value:
		if typed != nil {
			return *typed, true
		}
	}
	return content.Value{}, false
}

func addContentDependencyEntry(entries map[string]map[string]*contentDependencyEntry, source content.SourceRef, table string, locator contentDependencyRowLocator) {
	key := sourceVersionLookupKey(source)
	tables := entries[key]
	if tables == nil {
		tables = make(map[string]*contentDependencyEntry)
		entries[key] = tables
	}
	for _, entry := range tables {
		if strings.EqualFold(entry.table, table) && entry.source.Matches(source) {
			entry.count++
			appendDependencyRowLocator(entry, locator)
			return
		}
	}
	copySource := source
	copySource.PrimaryKey = append([]byte(nil), source.PrimaryKey...)
	tables[contentDependencyEntryKey(source, table)] = &contentDependencyEntry{
		source: copySource, table: table, count: 1, rows: []contentDependencyRowLocator{locator},
	}
}

func appendDependencyRowLocator(entry *contentDependencyEntry, locator contentDependencyRowLocator) {
	if entry == nil || locator.lookupKey == nil || locator.storageKey == nil {
		return
	}
	keyBytes, ok := storageKeyToBytes(locator.storageKey)
	if !ok {
		return
	}
	for _, existing := range entry.rows {
		existingBytes, existingOK := storageKeyToBytes(existing.storageKey)
		if existingOK && string(existingBytes) == string(keyBytes) {
			return
		}
	}
	entry.rows = append(entry.rows, contentDependencyRowLocator{
		lookupKey: cloneDependencyIndexKey(locator.lookupKey), storageKey: cloneDependencyIndexKey(locator.storageKey),
	})
}

func cloneDependencyIndexKey(key interface{}) interface{} {
	if raw, ok := key.([]byte); ok {
		return append([]byte(nil), raw...)
	}
	return key
}

func contentDependencyEntryKey(source content.SourceRef, table string) string {
	return strings.ToLower(strings.TrimSpace(source.Schema)) + "\x00" +
		strings.ToLower(strings.TrimSpace(source.Table)) + "\x00" +
		strings.ToLower(strings.TrimSpace(source.Column)) + "\x00" +
		strings.ToLower(strings.TrimSpace(table)) + "\x00" + sourceVersionLookupKey(source)
}

func contentDependencyTableSignature(tables map[string]*manager.TableStorageInfo) string {
	type tableEntry struct {
		key  string
		info *manager.TableStorageInfo
	}
	items := make([]tableEntry, 0, len(tables))
	for key, info := range tables {
		items = append(items, tableEntry{key: strings.ToLower(strings.TrimSpace(key)), info: info})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].key < items[j].key })
	var builder strings.Builder
	for _, item := range items {
		builder.WriteString(item.key)
		builder.WriteByte('\x00')
		if item.info != nil {
			builder.WriteString(strings.ToLower(item.info.SchemaName))
			builder.WriteByte('\x00')
			builder.WriteString(strings.ToLower(item.info.TableName))
			builder.WriteByte('\x00')
			builder.WriteString(fmt.Sprint(item.info.Type))
		}
		builder.WriteByte('\n')
	}
	return builder.String()
}

func (index *contentDependencyIndex) candidateRows(mutations []contentSourceMutation, currentTables map[string]*manager.TableStorageInfo) (map[string][]contentDependencyRowLocator, bool) {
	if index == nil {
		return nil, false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	if !index.ready || index.dirty {
		return nil, false
	}
	if contentDependencyTableSignature(currentTables) != index.tableSignature {
		index.dirty = true
		return nil, false
	}
	rows := make(map[string][]contentDependencyRowLocator)
	seenLocators := make(map[string]map[string]struct{})
	for _, mutation := range mutations {
		if mutation.after != nil && mutation.before.Matches(*mutation.after) {
			continue
		}
		for _, entry := range index.byKey[sourceVersionLookupKey(mutation.before)] {
			if entry.count > 0 && entry.source.Matches(mutation.before) {
				key := strings.ToLower(entry.table)
				if _, exists := rows[key]; !exists {
					rows[key] = make([]contentDependencyRowLocator, 0, len(entry.rows))
				}
				if seenLocators[key] == nil {
					seenLocators[key] = make(map[string]struct{})
				}
				for _, locator := range entry.rows {
					storageBytes, storageOK := storageKeyToBytes(locator.storageKey)
					if !storageOK {
						continue
					}
					locatorKey := string(storageBytes)
					if _, exists := seenLocators[key][locatorKey]; exists {
						continue
					}
					seenLocators[key][locatorKey] = struct{}{}
					rows[key] = append(rows[key], contentDependencyRowLocator{
						lookupKey: cloneDependencyIndexKey(locator.lookupKey), storageKey: cloneDependencyIndexKey(locator.storageKey),
					})
				}
			}
		}
	}
	return rows, true
}

func (index *contentDependencyIndex) applyCommittedChanges(changes []transactionDMLChange) {
	if index == nil || len(changes) == 0 {
		return
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	if !index.ready || index.dirty {
		index.dirty = true
		return
	}
	changed := false
	for _, change := range changes {
		schemaName, tableName, ok := strings.Cut(change.tableName, ".")
		if !ok || strings.TrimSpace(schemaName) == "" || strings.TrimSpace(tableName) == "" {
			index.ready = false
			return
		}
		if len(readyDerivedSourceRefs(change.before)) == 0 && len(readyDerivedSourceRefs(change.after)) == 0 {
			continue
		}
		changed = true
	}
	if !changed {
		return
	}
	// Exact row locators cannot be patched safely from a post-commit summary
	// without the table's key codec. Force a rebuild from committed rows before
	// the next invalidation instead of retaining a stale candidate set.
	index.dirty = true
}

func (index *contentDependencyIndex) persistLocked() error {
	if index == nil {
		return nil
	}
	rows := make([]contentDependencySnapshotRow, 0)
	for _, tables := range index.byKey {
		for _, entry := range tables {
			if entry == nil || entry.count == 0 {
				continue
			}
			rows = append(rows, contentDependencySnapshotRow{Source: entry.source, Table: entry.table, Count: entry.count})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		left, right := rows[i], rows[j]
		if left.Source.Schema != right.Source.Schema {
			return left.Source.Schema < right.Source.Schema
		}
		if left.Source.Table != right.Source.Table {
			return left.Source.Table < right.Source.Table
		}
		if left.Source.Column != right.Source.Column {
			return left.Source.Column < right.Source.Column
		}
		if string(left.Source.PrimaryKey) != string(right.Source.PrimaryKey) {
			return string(left.Source.PrimaryKey) < string(right.Source.PrimaryKey)
		}
		if left.Source.Revision != right.Source.Revision {
			return left.Source.Revision < right.Source.Revision
		}
		return left.Table < right.Table
	})
	canonical, err := json.Marshal(struct {
		Version int                            `json:"version"`
		Entries []contentDependencySnapshotRow `json:"entries"`
	}{Version: contentDependencyIndexVersion, Entries: rows})
	if err != nil {
		return err
	}
	digest := sha256.Sum256(canonical)
	snapshot := contentDependencySnapshot{
		Version: contentDependencyIndexVersion,
		Entries: rows,
		Digest:  hex.EncodeToString(digest[:]),
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	directory := filepath.Join(index.dataDir, ".xmysql-content")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	return writeMetadataFileAtomic(filepath.Join(directory, "dependencies.json"), encoded)
}

func applyCommittedContentDependencyChanges(dataDir string, changes []transactionDMLChange) {
	key := canonicalDependencyDataDir(dataDir)
	if key == "" {
		return
	}
	contentDependencyIndexes.Lock()
	index := contentDependencyIndexes.items[key]
	contentDependencyIndexes.Unlock()
	if index != nil {
		index.applyCommittedChanges(changes)
	}
}

func (dml *StorageIntegratedDMLExecutor) rebuildContentDependencyIndex(ctx context.Context) (*contentDependencyIndex, error) {
	index := dependencyIndexForDataDir(dml.dataDir)
	if err := index.ensureContentDependencyIndex(ctx, dml); err != nil {
		return index, err
	}
	return index, nil
}
