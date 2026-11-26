package vector

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
)

func testBoundVector(t *testing.T, primaryKey string, revision uint64, elements []float32) (string, Value) {
	t.Helper()
	contentValue, err := content.NewValue("text/plain", revision, []byte(fmt.Sprintf("source-%s-revision-%d", primaryKey, revision)))
	if err != nil {
		t.Fatal(err)
	}
	source, err := content.NewSourceRef("app", "documents", "body", []byte(primaryKey), contentValue)
	if err != nil {
		t.Fatal(err)
	}
	value, err := NewBlockBoundValue(
		Space{ModelID: "test-model", ModelVersion: "1", ID: "shared-text-image-space"},
		"text", elements, source, fmt.Sprintf("%064x", revision),
	)
	if err != nil {
		t.Fatal(err)
	}
	return "entry-" + primaryKey + fmt.Sprintf("-r%d", revision), value
}

func TestIndexCatalogSearchMergesDurableUpdateAndDeleteDeltas(t *testing.T) {
	root := filepath.Join(t.TempDir(), "vectors")
	catalog, err := OpenIndexCatalog(root, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	firstID, first := testBoundVector(t, "1", 1, []float32{0.8, 0.2})
	secondID, second := testBoundVector(t, "2", 1, []float32{0, 1})
	thirdID, third := testBoundVector(t, "3", 1, []float32{0.7, 0.7})
	entries := []HNSWEntry{{ID: firstID, Value: first}, {ID: secondID, Value: second}, {ID: thirdID, Value: third}}
	segment, err := BuildHNSWSegment(entries, MetricCosine, HNSWConfig{M: 4, EFConstruction: 32})
	if err != nil {
		t.Fatal(err)
	}
	segmentRef, err := catalog.SegmentStore().Put(segment)
	if err != nil {
		t.Fatal(err)
	}
	manifest := IndexManifest{
		IndexID: "documents-embedding", IndexName: "embedding_idx", Generation: 1,
		Schema: "app", Table: "documents", Column: "embedding", SpaceID: first.Space.ID,
		Dimension: uint32(first.Dimension()), Metric: MetricCosine,
		Freshness: IndexFreshnessReady, SourceSnapshotID: "snapshot-1", Segments: []SegmentRef{segmentRef},
	}
	if _, err := catalog.Publish(0, manifest); err != nil {
		t.Fatal(err)
	}
	query, err := NewValue(first.Space, "text", []float32{1, 0})
	if err != nil {
		t.Fatal(err)
	}
	exact, _, err := catalog.SearchExact(manifest.IndexID, query, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(exact) != 3 || exact[0].Entry.ID != firstID {
		t.Fatalf("initial exact search = %#v, want %q first", exact, firstID)
	}
	approximate, _, err := catalog.SearchApproximate(manifest.IndexID, query, 2, 8, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(approximate.Hits) != 2 || !approximate.Approximate || approximate.SearchBudget != 8 {
		t.Fatalf("initial ANN result = %#v", approximate)
	}

	updatedID, updated := testBoundVector(t, "2", 2, []float32{0.99, 0.01})
	updatedSource := *updated.Source
	oldSource := *second.Source
	batch := DeltaBatch{
		IndexID: manifest.IndexID, TransactionID: "tx-update-delete", CommitSequence: 1, SourceSnapshotID: "snapshot-2",
		Mutations: []DeltaMutation{
			{EntryID: secondID, Delete: true, Source: oldSource},
			{EntryID: updatedID, Value: &updated, Source: updatedSource},
			{EntryID: thirdID, Delete: true, Source: *third.Source},
		},
	}
	updatedManifest, err := catalog.AppendDelta(1, batch)
	if err != nil {
		t.Fatal(err)
	}
	if updatedManifest.Generation != 2 || updatedManifest.AppliedCommitSequence != 1 {
		t.Fatalf("updated manifest watermark = generation %d sequence %d", updatedManifest.Generation, updatedManifest.AppliedCommitSequence)
	}
	if _, err := catalog.AppendDelta(1, batch); err != nil {
		t.Fatalf("idempotent delta retry failed: %v", err)
	}
	if _, err := catalog.AppendDelta(1, DeltaBatch{
		IndexID: manifest.IndexID, TransactionID: "tx-stale", CommitSequence: 1, SourceSnapshotID: "snapshot-stale",
		Mutations: []DeltaMutation{{EntryID: firstID, Delete: true, Source: *first.Source}},
	}); err != ErrStaleDeltaBatch {
		t.Fatalf("stale delta error = %v, want %v", err, ErrStaleDeltaBatch)
	}

	current, _, err := catalog.SearchExact(manifest.IndexID, query, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 2 || current[0].Entry.ID != updatedID || current[1].Entry.ID != firstID {
		t.Fatalf("delta-merged exact search = %#v, want updated and unchanged entries", current)
	}

	reopened, err := OpenIndexCatalog(root, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	current, durableManifest, err := reopened.SearchExact(manifest.IndexID, query, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if durableManifest.Generation != 2 || len(current) != 2 || current[0].Entry.ID != updatedID {
		t.Fatalf("reopened search = %#v, manifest=%#v", current, durableManifest)
	}

	compacted, err := reopened.Compact(manifest.IndexID, durableManifest.Generation, HNSWConfig{M: 4, EFConstruction: 32})
	if err != nil {
		t.Fatal(err)
	}
	if len(compacted.Deltas) != 0 || compacted.AppliedCommitSequence != 1 || compacted.LastTransactionID != "tx-update-delete" || compacted.Generation != 3 {
		t.Fatalf("compacted manifest retained delta state: %#v", compacted)
	}
	current, _, err = reopened.SearchExact(manifest.IndexID, query, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 2 || current[0].Entry.ID != updatedID || current[1].Entry.ID != firstID {
		t.Fatalf("compacted exact search = %#v", current)
	}
}

func TestHNSWBuildAndSearchAreDeterministicAcrossInputOrder(t *testing.T) {
	firstID, first := testBoundVector(t, "1", 1, []float32{1, 0})
	secondID, second := testBoundVector(t, "2", 1, []float32{0, 1})
	thirdID, third := testBoundVector(t, "3", 1, []float32{-1, 0})
	config := HNSWConfig{M: 4, EFConstruction: 32}
	left, err := BuildHNSWSegment([]HNSWEntry{{ID: firstID, Value: first}, {ID: secondID, Value: second}, {ID: thirdID, Value: third}}, MetricCosine, config)
	if err != nil {
		t.Fatal(err)
	}
	right, err := BuildHNSWSegment([]HNSWEntry{{ID: thirdID, Value: third}, {ID: firstID, Value: first}, {ID: secondID, Value: second}}, MetricCosine, config)
	if err != nil {
		t.Fatal(err)
	}
	leftBytes, err := left.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	rightBytes, err := right.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if string(leftBytes) != string(rightBytes) {
		t.Fatal("HNSW segment bytes depend on input order")
	}
	query, err := NewValue(first.Space, "text", []float32{1, 0})
	if err != nil {
		t.Fatal(err)
	}
	result, err := left.SearchApproximate(query, 1, 16, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].Entry.ID != firstID || !result.Approximate {
		t.Fatalf("ANN result = %#v", result)
	}
}
