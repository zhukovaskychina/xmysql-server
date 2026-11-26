package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
)

// maintainCommittedVectorIndexes publishes an ordered, source-bound delta for
// each READY index affected by the committed row images. Any ambiguous image,
// missing commit sequence, or publication conflict falls back to DIRTY so a
// query can never observe a stale base graph as current.
func (e *XMySQLExecutor) maintainCommittedVectorIndexes(changes []transactionDMLChange, commitSequence uint64) error {
	if e == nil || e.vectorIndexCatalog == nil || len(changes) == 0 {
		return nil
	}
	tables := make(map[string][2]string)
	for _, change := range changes {
		containsVector := false
		for _, typeName := range change.columnTypes {
			if strings.EqualFold(strings.TrimSpace(typeName), "VECTOR") {
				containsVector = true
				break
			}
		}
		if !containsVector {
			continue
		}
		schema, table, ok := strings.Cut(strings.TrimSpace(change.tableName), ".")
		if !ok || strings.TrimSpace(schema) == "" || strings.TrimSpace(table) == "" {
			continue
		}
		key := strings.ToLower(schema + "\x00" + table)
		tables[key] = [2]string{schema, table}
	}
	keys := make([]string, 0, len(tables))
	for key := range tables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	manifests, err := e.vectorIndexCatalog.ListLatest()
	if err != nil {
		return fmt.Errorf("list vector indexes for committed DML: %w", err)
	}
tableLoop:
	for _, key := range keys {
		identity := tables[key]
		for _, manifest := range manifests {
			if !strings.EqualFold(manifest.Schema, identity[0]) || !strings.EqualFold(manifest.Table, identity[1]) || manifest.Freshness != vector.IndexFreshnessReady {
				continue
			}
			mutations := make([]vector.DeltaMutation, 0)
			for _, change := range changes {
				changeSchema, changeTable, ok := strings.Cut(strings.TrimSpace(change.tableName), ".")
				if !ok || !strings.EqualFold(changeSchema, manifest.Schema) || !strings.EqualFold(changeTable, manifest.Table) {
					continue
				}
				changeMutations, err := vectorMutationsForChange(manifest, change)
				if err != nil {
					if dirtyErr := e.dirtyVectorTableAfterCommit(manifest.Schema, manifest.Table, err); dirtyErr != nil {
						return dirtyErr
					}
					continue tableLoop
				}
				mutations = append(mutations, changeMutations...)
			}
			if len(mutations) == 0 {
				continue
			}
			if commitSequence == 0 {
				if err := e.dirtyVectorTableAfterCommit(manifest.Schema, manifest.Table, fmt.Errorf("durable commit sequence is unavailable")); err != nil {
					return err
				}
				continue tableLoop
			}
			batch := vector.DeltaBatch{
				IndexID: manifest.IndexID, TransactionID: fmt.Sprintf("wal-%d", commitSequence),
				CommitSequence: commitSequence, SourceSnapshotID: manifest.SourceSnapshotID, Mutations: mutations,
			}
			if _, err := e.vectorIndexCatalog.AppendDelta(manifest.Generation, batch); err != nil {
				if dirtyErr := e.dirtyVectorTableAfterCommit(manifest.Schema, manifest.Table, err); dirtyErr != nil {
					return dirtyErr
				}
				continue tableLoop
			}
		}
	}
	return nil
}

// markCommittedVectorIndexesDirty is the recovery path. Startup cannot assume
// that a derived delta publication completed after the WAL commit, so it fences
// every affected table until an explicit rebuild reconstructs a source snapshot.
func (e *XMySQLExecutor) markCommittedVectorIndexesDirty(changes []transactionDMLChange) error {
	if e == nil || e.vectorIndexCatalog == nil || len(changes) == 0 {
		return nil
	}
	tables := make(map[string][2]string)
	for _, change := range changes {
		containsVector := false
		for _, typeName := range change.columnTypes {
			if strings.EqualFold(strings.TrimSpace(typeName), "VECTOR") {
				containsVector = true
				break
			}
		}
		if !containsVector {
			continue
		}
		schema, table, ok := strings.Cut(strings.TrimSpace(change.tableName), ".")
		if !ok || strings.TrimSpace(schema) == "" || strings.TrimSpace(table) == "" {
			continue
		}
		tables[strings.ToLower(schema+"\x00"+table)] = [2]string{schema, table}
	}
	keys := make([]string, 0, len(tables))
	for key := range tables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		identity := tables[key]
		if _, err := e.vectorIndexCatalog.MarkTableDirty(identity[0], identity[1]); err != nil {
			return fmt.Errorf("invalidate vector indexes for %s.%s during recovery: %w", identity[0], identity[1], err)
		}
	}
	return nil
}

func (e *XMySQLExecutor) acquireVectorCommitFence(changes []transactionDMLChange) func() {
	if e == nil || e.vectorIndexCatalog == nil {
		return func() {}
	}
	for _, change := range changes {
		for _, typeName := range change.columnTypes {
			if strings.EqualFold(strings.TrimSpace(typeName), "VECTOR") {
				return e.vectorIndexCatalog.AcquireCommitFence()
			}
		}
	}
	return func() {}
}

func vectorMutationsForChange(manifest vector.IndexManifest, change transactionDMLChange) ([]vector.DeltaMutation, error) {
	mutations := make([]vector.DeltaMutation, 0, 2)
	if previous, ok := rowVectorValue(change.before, manifest.Column); ok && previous.Source != nil {
		entryID := vectorIndexEntryID(manifest.IndexID, *previous.Source, previous.SourceBlockID)
		mutations = append(mutations, vector.DeltaMutation{EntryID: entryID, Delete: true, Source: *previous.Source})
	}
	current, ok := rowVectorValue(change.after, manifest.Column)
	if !ok || current.State != vector.StateReady || current.Source == nil {
		return mutations, nil
	}
	if err := current.Validate(); err != nil {
		return nil, fmt.Errorf("validate committed VECTOR value for %s.%s.%s: %w", manifest.Schema, manifest.Table, manifest.Column, err)
	}
	sourceColumn := rowContentValue(change.after, current.Source.Column)
	if sourceColumn == nil {
		// A missing or replaced source cannot be trusted as an upsert. The old
		// source entry remains covered by the tombstone above.
		return mutations, nil
	}
	currentSource, err := content.NewSourceRef(current.Source.Schema, current.Source.Table, current.Source.Column, current.Source.PrimaryKey, *sourceColumn)
	if err != nil {
		return mutations, nil
	}
	if !current.Source.Matches(currentSource) || !strings.EqualFold(current.Source.Schema, manifest.Schema) ||
		!strings.EqualFold(current.Source.Table, manifest.Table) {
		return mutations, nil
	}
	entryID := vectorIndexEntryID(manifest.IndexID, *current.Source, current.SourceBlockID)
	value := current.Clone()
	mutations = append(mutations, vector.DeltaMutation{EntryID: entryID, Source: *current.Source, Value: &value})
	return mutations, nil
}

func rowVectorValue(row map[string]interface{}, column string) (vector.Value, bool) {
	for name, value := range row {
		if !strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(column)) {
			continue
		}
		return vectorArgument(value)
	}
	return vector.Value{}, false
}

func rowContentValue(row map[string]interface{}, column string) *content.Value {
	for name, value := range row {
		if !strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(column)) {
			continue
		}
		switch typed := value.(type) {
		case content.Value:
			copyValue := typed.Clone()
			return &copyValue
		case *content.Value:
			if typed != nil {
				copyValue := typed.Clone()
				return &copyValue
			}
		}
	}
	return nil
}

func (e *XMySQLExecutor) dirtyVectorTableAfterCommit(schema, table string, cause error) error {
	if e == nil || e.vectorIndexCatalog == nil {
		return fmt.Errorf("vector index maintenance failed after commit: %w", cause)
	}
	if _, err := e.vectorIndexCatalog.MarkTableDirty(schema, table); err != nil {
		e.vectorIndexCatalogUsable.Store(false)
		return fmt.Errorf("vector index maintenance failed after commit (%v), and invalidation of %s.%s failed: %w", cause, schema, table, err)
	}
	logger.Warnf("vector index maintenance for %s.%s fell back to DIRTY after committed DML: %v", schema, table, cause)
	return nil
}

// invalidateCommittedVectorIndexes keeps derived index failures from changing
// the outcome of an already durable row commit. It disables indexed reads for
// this process if publication fails; the durable journal is replayed during
// startup recovery before the engine is marked ready.
func (e *XMySQLExecutor) invalidateCommittedVectorIndexes(changes []transactionDMLChange, commitSequence uint64) {
	if e == nil {
		return
	}
	if err := e.maintainCommittedVectorIndexes(changes, commitSequence); err != nil {
		e.vectorIndexCatalogUsable.Store(false)
		logger.Errorf("committed rows are durable but vector index maintenance failed; indexed reads disabled until recovery: %v", err)
	}
}
