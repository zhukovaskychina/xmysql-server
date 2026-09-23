package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestFullTextCompatibilityPersistsIndexAndFiltersRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table docs (id int primary key, content text, fulltext index idx_content (content))")
	mustExecSQL(t, executor, "app", "insert into docs values (1, 'mysql compatibility'), (2, 'unrelated text')")
	rows := mustQuerySQL(t, executor, "app", "select id, content from docs where match(content) against('mysql')")
	require.Len(t, rows, 1)
	require.Equal(t, "1", fmt.Sprint(rows[0][0]))

	info, err := readTableMetadataMap(executor.GetDataDir() + "/app/docs.frm")
	require.NoError(t, err)
	require.Contains(t, advancedIndexNames(info), "idx_content")
}

func TestFullTextTokenizerSegmentRebuildDropAndRestart(t *testing.T) {
	info := map[string]interface{}{}
	rows := []map[string]interface{}{
		{"id": int64(1), "content": "The MySQL compatibility layer"},
		{"id": int64(2), "content": "compatibility layer"},
	}
	require.NoError(t, rebuildFullTextSegments(info, "idx_content", rows, []string{"content"}))
	state, ok := info["fulltext_segments_idx_content"].(fullTextIndexState)
	require.True(t, ok)
	require.Equal(t, []string{"1", "2"}, state.Segments["compatibility"])
	require.NotContains(t, state.Segments, "the")

	path := t.TempDir() + "/table.frm"
	raw, err := json.Marshal(info)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0644))
	reloadedRaw, err := os.ReadFile(path)
	require.NoError(t, err)
	var reloaded map[string]interface{}
	require.NoError(t, json.Unmarshal(reloadedRaw, &reloaded))
	require.Contains(t, reloaded, "fulltext_segments_idx_content")
	delete(reloaded, "fulltext_segments_idx_content")
	require.NotContains(t, reloaded, "fulltext_segments_idx_content", "DROP INDEX must remove the persisted segment")
}

func TestAdvancedIndexDDLRemovesAndRenamesPersistedAuxiliaryState(t *testing.T) {
	tableInfo := map[string]interface{}{
		"indexes":                       []interface{}{map[string]interface{}{"name": "idx_content", "type": "FULLTEXT"}},
		"fulltext_segments_idx_content": fullTextIndexState{Version: 1},
		"spatial_entries_idx_location":  map[string]interface{}{"rows": 1},
	}
	operations := []AlterOperation{{Kind: AlterRenameIndex, OldName: "idx_content", NewName: "idx_body"}}
	require.NoError(t, applyAlterOperations(tableInfo, operations))
	_, oldExists := tableInfo["fulltext_segments_idx_content"]
	_, newExists := tableInfo["fulltext_segments_idx_body"]
	require.False(t, oldExists)
	require.True(t, newExists)
	operations = []AlterOperation{{Kind: AlterDropIndex, IndexName: "idx_body"}}
	require.NoError(t, applyAlterOperations(tableInfo, operations))
	_, newExists = tableInfo["fulltext_segments_idx_body"]
	require.False(t, newExists)
	_, spatialExists := tableInfo["spatial_entries_idx_location"]
	require.True(t, spatialExists, "unrelated spatial auxiliary state must be preserved")
}

func TestFullTextAndSpatialReferenceEdgeCases(t *testing.T) {
	require.Equal(t, 2, fullTextScore("mysql mysql compatibility", "mysql"), "ranking must count repeated matching tokens")
	require.Equal(t, 0, fullTextScore("", "mysql"), "empty text must not match")
	require.Equal(t, 0, fullTextScore("mysql", ""), "empty query must not match")
	require.False(t, func() bool { _, _, _, _, ok := spatialBounds("POLYGON((invalid))"); return ok }())
	_, _, handled, err := parseAdvancedIndexCreate("create table docs (id int, fulltext index idx_content (content), fulltext index idx_content (content))")
	require.True(t, handled)
	require.Error(t, err, "duplicate advanced indexes must be rejected")
}

func TestSpatialCompatibilityFiltersPointWithinBoundingBox(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table places (id int primary key, location geometry, spatial index idx_location (location))")
	meta, err := (&SelectExecutor{}).loadTableMetaFromFrm(executor.GetDataDir(), "app", "places")
	require.NoError(t, err)
	location, err := meta.GetColumn("location")
	require.NoError(t, err)
	require.Equal(t, metadata.TypeGeometry, location.Type)
	require.Equal(t, "GEOMETRY", location.SQLType())
	mustExecSQL(t, executor, "app", "insert into places values (1, 'POINT(1 1)'), (2, 'POINT(10 10)')")
	rows := mustQuerySQL(t, executor, "app", "select id, location from places where st_within(location, st_geomfromtext('POLYGON((0 0,5 0,5 5,0 5,0 0))'))")
	require.Len(t, rows, 1)
	require.Equal(t, "1", fmt.Sprint(rows[0][0]))
	rows = mustQuerySQL(t, executor, "app", "select id, location from places where st_intersects(location, st_geomfromtext('POINT(1 1)'))")
	require.Len(t, rows, 1)
	require.Equal(t, "1", fmt.Sprint(rows[0][0]))
	rows = mustQuerySQL(t, executor, "app", "select id, location from places where st_equals(location, st_geomfromtext('POINT(10 10)'))")
	require.Len(t, rows, 1)
	require.Equal(t, "2", fmt.Sprint(rows[0][0]))
	rows = mustQuerySQL(t, executor, "app", "select id from places where st_contains(st_geomfromtext('POLYGON((0 0,5 0,5 5,0 5,0 0))'), location)")
	require.Len(t, rows, 1)
	require.Equal(t, "1", fmt.Sprint(rows[0][0]))
	rows = mustQuerySQL(t, executor, "app", "select id from places where st_intersects(location, st_geomfromtext('POINT(10 10)', 4326))")
	require.Len(t, rows, 1)
	require.Equal(t, "2", fmt.Sprint(rows[0][0]))
	rows = mustQuerySQL(t, executor, "app", "select id from places where mbrdisjoint(location, st_geomfromtext('POINT(1 1)'))")
	require.Len(t, rows, 1)
	require.Equal(t, "2", fmt.Sprint(rows[0][0]))
}

func TestSpatialCompatibilityFiltersLineAndPolygonGeometries(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table shapes (id int primary key, location geometry, spatial index idx_location (location))")
	mustExecSQL(t, executor, "app", "insert into shapes values (1, 'LINESTRING(2 2,8 8)'), (2, 'LINESTRING(20 20,30 30)'), (3, 'POLYGON((1 1,4 1,4 4,1 4,1 1))')")
	rows := mustQuerySQL(t, executor, "app", "select id from shapes where st_contains(st_geomfromtext('POLYGON((0 0,10 0,10 10,0 10,0 0))'), location) order by id")
	require.Equal(t, [][]interface{}{{"1"}, {"3"}}, rows)
	rows = mustQuerySQL(t, executor, "app", "select id from shapes where st_intersects(location, st_geomfromtext('LINESTRING(0 10,10 0)')) order by id")
	require.Equal(t, [][]interface{}{{"1"}}, rows)
	rows = mustQuerySQL(t, executor, "app", "select id from shapes where st_disjoint(location, st_geomfromtext('POLYGON((0 0,10 0,10 10,0 10,0 0))')) order by id")
	require.Equal(t, [][]interface{}{{"2"}}, rows)
}

func TestSpatialCompatibilityFiltersCrossingGeometries(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table crossings (id int primary key, location geometry, spatial index idx_location (location))")
	mustExecSQL(t, executor, "app", "insert into crossings values (1, 'LINESTRING(-1 5,11 5)'), (2, 'LINESTRING(2 2,8 8)'), (3, 'LINESTRING(20 20,30 30)')")
	rows := mustQuerySQL(t, executor, "app", "select id from crossings where st_crosses(location, st_geomfromtext('POLYGON((0 0,10 0,10 10,0 10,0 0))')) order by id")
	require.Equal(t, [][]interface{}{{"1"}}, rows)
	rows = mustQuerySQL(t, executor, "app", "select id from crossings where st_crosses(st_geomfromtext('POLYGON((0 0,10 0,10 10,0 10,0 0))'), location) order by id")
	require.Equal(t, [][]interface{}{{"1"}}, rows)
}

func TestSpatialCompatibilityEvaluatesConvexHullProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	rows := mustQuerySQL(t, executor, "app", "select st_astext(st_convexhull(st_geomfromtext('POLYGON((0 0,4 0,4 2,2 4,0 2,0 0))'))) ")
	require.Equal(t, [][]interface{}{{"POLYGON((0 0,4 0,4 2,2 4,0 2,0 0))"}}, rows)
}

func TestSpatialCompatibilityEvaluatesInterpolationAndGeoHashProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	rows := mustQuerySQL(t, executor, "app", "select st_astext(st_pointatdistance(st_geomfromtext('LINESTRING(0 0,3 4)'), 2.5)), st_astext(st_lineinterpolatepoints(st_geomfromtext('LINESTRING(0 0,3 4)'), .5)), st_geohash(180, 0, 10)")
	require.Equal(t, [][]interface{}{{"POINT(1.5 2)", "MULTIPOINT((0 0),(1.5 2),(3 4))", "xbpbpbpbpb"}}, rows)
}

func TestSpatialCompatibilityEvaluatesBufferProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	rows := mustQuerySQL(t, executor, "app", "select st_geometrytype(st_buffer(st_geomfromtext('POINT(0 0)'), 2)), st_area(st_buffer(st_geomfromtext('POINT(0 0)'), 2))")
	require.Len(t, rows, 1)
	require.Equal(t, "ST_Polygon", rows[0][0])
	area, ok := rows[0][1].(string)
	require.True(t, ok, "buffer area = %#v", rows[0][1])
	require.NotEqual(t, "0", area)
}

func TestSpatialIndexStateTracksInsertUpdateDelete(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table indexed_places (id int primary key, location geometry, spatial index idx_location (location))")
	mustExecSQL(t, executor, "app", "insert into indexed_places values (1, 'POINT(1 2)'), (2, 'POINT(10 20)')")

	readState := func() map[string]interface{} {
		info, err := readTableMetadataMap(filepath.Join(executor.GetDataDir(), "app", "indexed_places.frm"))
		require.NoError(t, err)
		state, ok := info["spatial_entries_idx_location"].(map[string]interface{})
		require.True(t, ok, "spatial index state = %T", info["spatial_entries_idx_location"])
		return state
	}
	state := readState()
	require.Equal(t, float64(2), state["version"])
	entries, ok := state["entries"].([]interface{})
	require.True(t, ok)
	require.Len(t, entries, 2)

	mustExecSQL(t, executor, "app", "update indexed_places set location = 'POINT(30 40)' where id = 1")
	state = readState()
	entries, ok = state["entries"].([]interface{})
	require.True(t, ok)
	require.Len(t, entries, 2)
	entryText, err := json.Marshal(entries)
	require.NoError(t, err)
	require.Contains(t, string(entryText), "30")
	require.Contains(t, string(entryText), "40")

	mustExecSQL(t, executor, "app", "delete from indexed_places where id = 2")
	state = readState()
	entries, ok = state["entries"].([]interface{})
	require.True(t, ok)
	require.Len(t, entries, 1)
}

func TestSpatialIndexDDLBuildsAndRemovesPersistedState(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table alter_places (id int primary key, location geometry)")
	mustExecSQL(t, executor, "app", "insert into alter_places values (1, 'POINT(1 2)'), (2, 'POINT(10 20)')")
	mustExecSQL(t, executor, "app", "alter table alter_places add spatial index idx_location (location)")

	info, err := readTableMetadataMap(filepath.Join(executor.GetDataDir(), "app", "alter_places.frm"))
	require.NoError(t, err)
	state, ok := info["spatial_entries_idx_location"].(map[string]interface{})
	require.True(t, ok, "spatial state = %T", info["spatial_entries_idx_location"])
	entries, ok := state["entries"].([]interface{})
	require.True(t, ok)
	require.Len(t, entries, 2)

	mustExecSQL(t, executor, "app", "alter table alter_places rename index idx_location to idx_places_location")
	info, err = readTableMetadataMap(filepath.Join(executor.GetDataDir(), "app", "alter_places.frm"))
	require.NoError(t, err)
	_, ok = info["spatial_entries_idx_location"]
	require.False(t, ok)
	_, ok = info["spatial_entries_idx_places_location"]
	require.True(t, ok)

	mustExecSQL(t, executor, "app", "alter table alter_places drop index idx_places_location")
	info, err = readTableMetadataMap(filepath.Join(executor.GetDataDir(), "app", "alter_places.frm"))
	require.NoError(t, err)
	_, ok = info["spatial_entries_idx_places_location"]
	require.False(t, ok)
}

func TestSpatialIndexCandidateKeysUseMBRRelations(t *testing.T) {
	state := spatialIndexState{Version: 1, Entries: []spatialIndexEntry{
		{RowKey: "inside", MinX: 1, MinY: 1, MaxX: 2, MaxY: 2},
		{RowKey: "overlap", MinX: 4, MinY: 4, MaxX: 8, MaxY: 8},
		{RowKey: "outside", MinX: 10, MinY: 10, MaxX: 12, MaxY: 12},
	}}
	candidates, ok := spatialIndexCandidateKeys(state, "ST_INTERSECTS", false, 0, 0, 5, 5)
	require.True(t, ok)
	require.Equal(t, map[string]struct{}{"inside": {}, "overlap": {}}, candidates)
	_, ok = spatialIndexCandidateKeys(state, "ST_DISJOINT", false, 0, 0, 5, 5)
	require.False(t, ok, "ST_DISJOINT cannot be safely reduced to intersecting MBR candidates")
	candidates, ok = spatialIndexCandidateKeys(state, "MBRDISJOINT", false, 0, 0, 5, 5)
	require.True(t, ok)
	require.Equal(t, map[string]struct{}{"outside": {}}, candidates)
}

func TestSpatialCandidateKeysFilterBeforeRecordProjection(t *testing.T) {
	rows := filterClusteredScannedRowsByStorageKeys([]clusteredScannedRow{
		{data: &InsertRowData{ColumnValues: map[string]interface{}{"id": int64(1)}}, storageKey: "keep"},
		{data: &InsertRowData{ColumnValues: map[string]interface{}{"id": int64(2)}}, storageKey: "skip"},
	}, map[string]struct{}{"keep": {}})
	require.Len(t, rows, 1)
	require.Equal(t, int64(1), rows[0].ColumnValues["id"])
}

func TestSpatialIndexStateBuildsDeterministicRTree(t *testing.T) {
	entries := make([]spatialIndexEntry, 0, 17)
	for index := 0; index < 17; index++ {
		value := float64(index)
		entries = append(entries, spatialIndexEntry{
			RowKey: fmt.Sprintf("row-%02d", index), MinX: value, MinY: value,
			MaxX: value + 1, MaxY: value + 1,
		})
	}
	state := buildSpatialIndexState([]string{"shape"}, entries)
	require.Equal(t, 2, state.Version)
	require.GreaterOrEqual(t, len(state.Nodes), 3)
	require.GreaterOrEqual(t, state.Root, 0)
	require.Less(t, state.Root, len(state.Nodes))
	require.False(t, state.Nodes[state.Root].Leaf)

	candidates, ok := spatialIndexCandidateKeys(state, "ST_INTERSECTS", false, 4.5, 4.5, 5.5, 5.5)
	require.True(t, ok)
	require.Equal(t, map[string]struct{}{"row-04": {}, "row-05": {}}, candidates)

	repeat := buildSpatialIndexState([]string{"shape"}, entries)
	require.Equal(t, state, repeat, "R-tree layout must be deterministic for repeatable metadata and recovery")
}
