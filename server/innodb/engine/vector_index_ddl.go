package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
)

const maxInitialVectorIndexEntries = 1 << 20

func vectorIndexID(schema, table, name string) string {
	identity := strings.ToLower(strings.TrimSpace(schema)) + "\x00" + strings.ToLower(strings.TrimSpace(table)) + "\x00" + strings.ToLower(strings.TrimSpace(name))
	digest := sha256.Sum256([]byte(identity))
	return "vi_" + hex.EncodeToString(digest[:])
}

func columnHasDataType(column *metadata.ColumnMeta, expected metadata.DataType) bool {
	return column != nil && metadata.DataType(strings.ToUpper(strings.TrimSpace(string(column.Type)))) == expected
}

func (e *XMySQLExecutor) createVectorIndex(ctx *ExecutionContext, statement *sqlparser.IndexDDLStatement, databaseName string) error {
	if e == nil || statement == nil || e.vectorIndexCatalog == nil {
		return fmt.Errorf("native vector index catalog is unavailable")
	}
	if !e.VectorIndexCatalogUsable() {
		return fmt.Errorf("native vector index catalog is unavailable until committed index maintenance is recovered")
	}
	if len(statement.Columns) != 1 {
		return fmt.Errorf("CREATE VECTOR INDEX requires exactly one VECTOR column")
	}
	schema := trimIndexIdentifier(statement.Table.Qualifier.String())
	if schema == "" {
		schema = strings.TrimSpace(databaseName)
	}
	table := trimIndexIdentifier(statement.Table.Name.String())
	name := trimIndexIdentifier(statement.Name)
	columnName := trimIndexIdentifier(statement.Columns[0])
	if schema == "" || table == "" || name == "" || columnName == "" {
		return fmt.Errorf("CREATE VECTOR INDEX requires database, table, index name and vector column")
	}
	if ctx == nil {
		ctx = &ExecutionContext{Context: context.Background(), DatabaseName: schema}
	}
	if err := e.checkTablePrivilege(ctx, schema, table, "INDEX"); err != nil {
		return err
	}
	indexID := vectorIndexID(schema, table, name)
	expectedGeneration := uint64(0)
	rebuilt := statement.Rebuild
	if existing, found, err := e.vectorIndexCatalog.ReadLatest(indexID); err != nil {
		return err
	} else if statement.Rebuild {
		if !found {
			return fmt.Errorf("VECTOR index %s does not exist on %s.%s", name, schema, table)
		}
		if !strings.EqualFold(existing.Schema, schema) || !strings.EqualFold(existing.Table, table) {
			return fmt.Errorf("VECTOR index identity does not match requested table")
		}
		statement.Columns = []string{existing.Column}
		expectedGeneration = existing.Generation
	} else if found {
		if existing.Freshness == vector.IndexFreshnessDropped {
			expectedGeneration = existing.Generation
			rebuilt = true
		} else if statement.IfNotExists && strings.EqualFold(existing.Schema, schema) && strings.EqualFold(existing.Table, table) && strings.EqualFold(existing.Column, columnName) {
			return nil
		} else {
			return fmt.Errorf("duplicate VECTOR index %s on %s.%s", name, schema, table)
		}
	}
	dml, err := e.newStorageIntegratedDMLExecutor()
	if err != nil {
		return err
	}
	lockKey := strings.ToLower(schema + "." + table)
	release, err := e.acquireDDLWriteLock(ctx, lockKey, false)
	if err != nil {
		return fmt.Errorf("acquire VECTOR index build lock for %s: %w", lockKey, err)
	}
	defer release()
	if existing, found, err := e.vectorIndexCatalog.ReadLatest(indexID); err != nil {
		return err
	} else if statement.Rebuild {
		if !found {
			return fmt.Errorf("VECTOR index %s does not exist on %s.%s", name, schema, table)
		}
		if !strings.EqualFold(existing.Schema, schema) || !strings.EqualFold(existing.Table, table) || !strings.EqualFold(existing.Column, columnName) {
			return fmt.Errorf("VECTOR index identity changed while rebuilding")
		}
		expectedGeneration = existing.Generation
	} else if found {
		if existing.Freshness == vector.IndexFreshnessDropped {
			expectedGeneration = existing.Generation
		} else if statement.IfNotExists && strings.EqualFold(existing.Schema, schema) && strings.EqualFold(existing.Table, table) && strings.EqualFold(existing.Column, columnName) {
			return nil
		} else {
			return fmt.Errorf("duplicate VECTOR index %s on %s.%s", name, schema, table)
		}
	}
	tableMeta, _, btree, err := dml.tableAccess(ctxContext(ctx), schema, table)
	if err != nil {
		return err
	}
	var vectorColumn *metadata.ColumnMeta
	columns := make(map[string]*metadata.ColumnMeta, len(tableMeta.Columns))
	for _, column := range tableMeta.Columns {
		if column == nil {
			continue
		}
		columns[strings.ToLower(strings.TrimSpace(column.Name))] = column
		if strings.EqualFold(column.Name, columnName) {
			vectorColumn = column
		}
	}
	if vectorColumn == nil {
		return fmt.Errorf("VECTOR column %s.%s.%s does not exist", schema, table, columnName)
	}
	if !columnHasDataType(vectorColumn, metadata.TypeVector) {
		return fmt.Errorf("CREATE VECTOR INDEX column %s.%s.%s has type %s, expected VECTOR", schema, table, columnName, vectorColumn.Type)
	}
	entries := make([]vector.HNSWEntry, 0)
	snapshotHash := sha256.New()
	seenEntryIDs := make(map[string]struct{})
	scanner := NewClusteredIndexScanner(btree, tableMeta)
	err = scanner.VisitProjected(ctxContext(ctx), nil, []string{vectorColumn.Name}, func(row *InsertRowData, _ interface{}, _ uint32) error {
		if row == nil {
			return nil
		}
		raw, exists := row.ColumnValues[vectorColumn.Name]
		if !exists || raw == nil {
			return nil
		}
		value, ok := vectorArgument(raw)
		if !ok {
			return fmt.Errorf("VECTOR column %s.%s.%s contains unsupported value %T", schema, table, vectorColumn.Name, raw)
		}
		if err := value.Validate(); err != nil {
			return fmt.Errorf("VECTOR column %s.%s.%s contains an invalid value: %w", schema, table, vectorColumn.Name, err)
		}
		if value.State != vector.StateReady || value.Source == nil {
			return fmt.Errorf("VECTOR index %s requires READY source-bound values; found %s", name, value.State)
		}
		source := value.Source
		if !strings.EqualFold(source.Schema, schema) || !strings.EqualFold(source.Table, table) {
			return fmt.Errorf("VECTOR index %s requires source-bound values from the same table", name)
		}
		sourceColumn := columns[strings.ToLower(strings.TrimSpace(source.Column))]
		if !columnHasDataType(sourceColumn, metadata.TypeContent) {
			return fmt.Errorf("VECTOR index %s source column %s is not a CONTENT column", name, source.Column)
		}
		entryID := vectorIndexEntryID(indexID, *source, value.SourceBlockID)
		if _, duplicate := seenEntryIDs[entryID]; duplicate {
			return fmt.Errorf("VECTOR index %s contains duplicate source/block identity", name)
		}
		seenEntryIDs[entryID] = struct{}{}
		if len(entries) >= maxInitialVectorIndexEntries {
			return fmt.Errorf("VECTOR index %s exceeds the initial build limit of %d entries", name, maxInitialVectorIndexEntries)
		}
		entries = append(entries, vector.HNSWEntry{ID: entryID, Value: value.Clone()})
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan VECTOR index source %s.%s.%s: %w", schema, table, columnName, err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("CREATE VECTOR INDEX requires at least one non-NULL READY source-bound vector")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	for _, entry := range entries {
		encoded, err := entry.Value.MarshalBinary()
		if err != nil {
			return err
		}
		_, _ = snapshotHash.Write([]byte(entry.ID))
		_, _ = snapshotHash.Write(encoded)
	}
	first := entries[0].Value
	for _, entry := range entries[1:] {
		if entry.Value.Space.ID != first.Space.ID || entry.Value.Dimension() != first.Dimension() {
			return fmt.Errorf("VECTOR index %s contains incompatible vector spaces or dimensions", name)
		}
	}
	segment, err := vector.BuildHNSWSegment(entries, vector.MetricCosine, vector.HNSWConfig{M: 16, EFConstruction: 200})
	if err != nil {
		return err
	}
	segmentRef, err := e.vectorIndexCatalog.SegmentStore().Put(segment)
	if err != nil {
		return err
	}
	manifest := vector.IndexManifest{
		IndexID: indexID, IndexName: name, Generation: expectedGeneration + 1, Schema: schema, Table: table, Column: vectorColumn.Name,
		SpaceID: first.Space.ID, Dimension: uint32(first.Dimension()), Metric: vector.MetricCosine,
		Freshness: vector.IndexFreshnessReady, SourceSnapshotID: hex.EncodeToString(snapshotHash.Sum(nil)),
		Segments: []vector.SegmentRef{segmentRef},
	}
	if rebuilt {
		_, err = e.vectorIndexCatalog.PublishRebuilt(expectedGeneration, manifest)
	} else {
		_, err = e.vectorIndexCatalog.Publish(expectedGeneration, manifest)
	}
	if err != nil {
		return err
	}
	return nil
}

// RebuildDirtyVectorIndexes performs a best-effort source rescan for indexes
// fenced during transaction-journal recovery. Regular SQL remains available if
// an index cannot be rebuilt; that manifest stays DIRTY and VECTOR_SEARCH
// continues to fail closed until an operator runs REBUILD VECTOR INDEX.
func (e *XMySQLExecutor) RebuildDirtyVectorIndexes(ctx *ExecutionContext) {
	if e == nil || e.vectorIndexCatalog == nil || !e.VectorIndexCatalogUsable() {
		return
	}
	manifests, err := e.vectorIndexCatalog.ListLatest()
	if err != nil {
		e.vectorIndexCatalogUsable.Store(false)
		logger.Warnf("vector index recovery scan unavailable; vector searches disabled: %v", err)
		return
	}
	for _, manifest := range manifests {
		if manifest.Freshness != vector.IndexFreshnessDirty {
			continue
		}
		if strings.TrimSpace(manifest.IndexName) == "" {
			logger.Warnf("vector index %s remains DIRTY because its durable manifest has no index name; run REBUILD VECTOR INDEX explicitly", manifest.IndexID)
			continue
		}
		statement := &sqlparser.IndexDDLStatement{
			Action: "create", Name: manifest.IndexName,
			Table:   sqlparser.TableName{Name: sqlparser.NewTableIdent(manifest.Table), Qualifier: sqlparser.NewTableIdent(manifest.Schema)},
			Columns: []string{manifest.Column}, Vector: true, Rebuild: true,
		}
		if err := e.rebuildVectorIndex(ctx, statement, manifest.Schema); err != nil {
			logger.Warnf("vector index %s recovery rebuild failed and remains DIRTY: %v", manifest.IndexID, err)
		}
	}
}

func (e *XMySQLExecutor) rebuildVectorIndex(ctx *ExecutionContext, statement *sqlparser.IndexDDLStatement, databaseName string) error {
	if e == nil || statement == nil || !statement.Vector || !statement.Rebuild || e.vectorIndexCatalog == nil {
		return fmt.Errorf("invalid REBUILD VECTOR INDEX statement")
	}
	schema := trimIndexIdentifier(statement.Table.Qualifier.String())
	if schema == "" {
		schema = strings.TrimSpace(databaseName)
	}
	table := trimIndexIdentifier(statement.Table.Name.String())
	name := trimIndexIdentifier(statement.Name)
	manifest, found, err := e.vectorIndexCatalog.ReadLatest(vectorIndexID(schema, table, name))
	if err != nil {
		return err
	}
	if !found || manifest.Freshness == vector.IndexFreshnessDropped || !strings.EqualFold(manifest.Schema, schema) || !strings.EqualFold(manifest.Table, table) {
		return fmt.Errorf("VECTOR index %s does not exist on %s.%s", name, schema, table)
	}
	statement.Columns = []string{manifest.Column}
	statement.Action = "create"
	return e.createVectorIndex(ctx, statement, databaseName)
}

func (e *XMySQLExecutor) dropVectorIndex(ctx *ExecutionContext, statement *sqlparser.IndexDDLStatement, databaseName string) error {
	if e == nil || statement == nil || !statement.Vector || !strings.EqualFold(statement.Action, "drop") || e.vectorIndexCatalog == nil {
		return fmt.Errorf("invalid DROP VECTOR INDEX statement")
	}
	schema := trimIndexIdentifier(statement.Table.Qualifier.String())
	if schema == "" {
		schema = strings.TrimSpace(databaseName)
	}
	table := trimIndexIdentifier(statement.Table.Name.String())
	name := trimIndexIdentifier(statement.Name)
	if schema == "" || table == "" || name == "" {
		return fmt.Errorf("DROP VECTOR INDEX requires database, table and index name")
	}
	if ctx == nil {
		ctx = &ExecutionContext{Context: context.Background(), DatabaseName: schema}
	}
	if err := e.checkTablePrivilege(ctx, schema, table, "INDEX"); err != nil {
		return err
	}
	lockKey := strings.ToLower(schema + "." + table)
	release, err := e.acquireDDLWriteLock(ctx, lockKey, false)
	if err != nil {
		return fmt.Errorf("acquire VECTOR index drop lock for %s: %w", lockKey, err)
	}
	defer release()
	indexID := vectorIndexID(schema, table, name)
	manifest, found, err := e.vectorIndexCatalog.ReadLatest(indexID)
	if err != nil {
		return err
	}
	if !found || !strings.EqualFold(manifest.Schema, schema) || !strings.EqualFold(manifest.Table, table) {
		if statement.IfExists {
			return nil
		}
		return fmt.Errorf("VECTOR index %s does not exist on %s.%s", name, schema, table)
	}
	_, err = e.vectorIndexCatalog.MarkDropped(indexID)
	return err
}

func vectorIndexEntryID(indexID string, source content.SourceRef, blockID string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(indexID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(strings.ToLower(source.Column)))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(source.PrimaryKey)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(strings.ToLower(strings.TrimSpace(blockID))))
	return hex.EncodeToString(hash.Sum(nil))
}
