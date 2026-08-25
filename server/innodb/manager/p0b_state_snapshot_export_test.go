package manager

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type p0bSnapshotDocument struct {
	EvidenceType string      `json:"evidence_type"`
	GeneratedAt  string      `json:"generated_at"`
	Scope        string      `json:"scope"`
	Scenario     string      `json:"scenario"`
	Items        interface{} `json:"items"`
}

type p0bRowSnapshot struct {
	PageID uint64 `json:"page_id"`
	LSN    uint64 `json:"lsn"`
	Value  string `json:"value"`
}

type p0bPageSnapshot struct {
	PageID     uint64 `json:"page_id"`
	LSN        uint64 `json:"lsn"`
	Dirty      bool   `json:"dirty"`
	DataSHA256 string `json:"data_sha256"`
	DataPrefix string `json:"data_prefix"`
}

type p0bWalBoundarySnapshot struct {
	Operation                 string `json:"operation"`
	PageID                    uint64 `json:"page_id"`
	LSN                       uint64 `json:"lsn"`
	ExpectedApplied           bool   `json:"expected_applied"`
	ActualPageLSNAfterReplay  uint64 `json:"actual_page_lsn_after_replay"`
	ActualValueAfterReplay    string `json:"actual_value_after_replay"`
	IdempotentReplayAttempted bool   `json:"idempotent_replay_attempted"`
}

func TestP0BStateSnapshotExport(t *testing.T) {
	snapshotDir := os.Getenv("P0B_STATE_SNAPSHOT_DIR")
	if strings.TrimSpace(snapshotDir) == "" {
		t.Skip("P0B_STATE_SNAPSHOT_DIR is not set; skipping P0-B state snapshot export")
	}

	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatalf("create snapshot dir: %v", err)
	}

	redoLogManager, err := NewRedoLogManager(t.TempDir(), 100)
	if err != nil {
		t.Fatalf("create redo log manager: %v", err)
	}
	defer redoLogManager.Close()

	recovery := NewCrashRecovery(redoLogManager, nil, 0)
	bufferPool := NewMockBufferPool()
	recovery.SetBufferPoolManager(bufferPool)

	entries := []*RedoLogEntry{
		{LSN: 100, TrxID: 10, PageID: 1, Type: LOG_TYPE_INSERT, Data: []byte("p0b-row-1-inserted")},
		{LSN: 200, TrxID: 10, PageID: 2, Type: LOG_TYPE_INSERT, Data: []byte("p0b-row-2-inserted")},
		{LSN: 300, TrxID: 10, PageID: 1, Type: LOG_TYPE_UPDATE, Data: []byte("p0b-row-1-updated")},
	}

	for _, entry := range entries {
		if err := recovery.redoLogEntry(entry); err != nil {
			t.Fatalf("redo entry lsn=%d page=%d: %v", entry.LSN, entry.PageID, err)
		}
	}

	idempotentEntry := &RedoLogEntry{LSN: 200, TrxID: 10, PageID: 2, Type: LOG_TYPE_INSERT, Data: []byte("p0b-row-2-stale-replay")}
	if err := recovery.redoLogEntry(idempotentEntry); err != nil {
		t.Fatalf("redo idempotent replay: %v", err)
	}

	page1, err := bufferPool.FetchPage(1)
	if err != nil {
		t.Fatalf("fetch page 1: %v", err)
	}
	page2, err := bufferPool.FetchPage(2)
	if err != nil {
		t.Fatalf("fetch page 2: %v", err)
	}

	expectedRows := []p0bRowSnapshot{
		{PageID: 1, LSN: 300, Value: "p0b-row-1-updated"},
		{PageID: 2, LSN: 200, Value: "p0b-row-2-inserted"},
	}
	actualRows := []p0bRowSnapshot{
		{PageID: 1, LSN: page1.GetLSN(), Value: snapshotPageValue(page1)},
		{PageID: 2, LSN: page2.GetLSN(), Value: snapshotPageValue(page2)},
	}

	expectedPages := []p0bPageSnapshot{
		expectedPageSnapshot(1, 300, []byte("p0b-row-1-updated"), true),
		expectedPageSnapshot(2, 200, []byte("p0b-row-2-inserted"), true),
	}
	actualPages := []p0bPageSnapshot{
		actualPageSnapshot(page1),
		actualPageSnapshot(page2),
	}

	expectedWal := []p0bWalBoundarySnapshot{
		{Operation: "insert", PageID: 1, LSN: 100, ExpectedApplied: true, ActualPageLSNAfterReplay: 300, ActualValueAfterReplay: "p0b-row-1-updated", IdempotentReplayAttempted: false},
		{Operation: "insert", PageID: 2, LSN: 200, ExpectedApplied: true, ActualPageLSNAfterReplay: 200, ActualValueAfterReplay: "p0b-row-2-inserted", IdempotentReplayAttempted: true},
		{Operation: "update", PageID: 1, LSN: 300, ExpectedApplied: true, ActualPageLSNAfterReplay: 300, ActualValueAfterReplay: "p0b-row-1-updated", IdempotentReplayAttempted: false},
	}
	actualWal := []p0bWalBoundarySnapshot{
		{Operation: "insert", PageID: 1, LSN: 100, ExpectedApplied: true, ActualPageLSNAfterReplay: page1.GetLSN(), ActualValueAfterReplay: snapshotPageValue(page1), IdempotentReplayAttempted: false},
		{Operation: "insert", PageID: 2, LSN: 200, ExpectedApplied: true, ActualPageLSNAfterReplay: page2.GetLSN(), ActualValueAfterReplay: snapshotPageValue(page2), IdempotentReplayAttempted: true},
		{Operation: "update", PageID: 1, LSN: 300, ExpectedApplied: true, ActualPageLSNAfterReplay: page1.GetLSN(), ActualValueAfterReplay: snapshotPageValue(page1), IdempotentReplayAttempted: false},
	}

	writeP0BSnapshot(t, snapshotDir, "expected_rows.json", "row_state_diff", expectedRows)
	writeP0BSnapshot(t, snapshotDir, "actual_rows.json", "row_state_diff", actualRows)
	writeP0BSnapshot(t, snapshotDir, "expected_pages.json", "page_state_diff", expectedPages)
	writeP0BSnapshot(t, snapshotDir, "actual_pages.json", "page_state_diff", actualPages)
	writeP0BSnapshot(t, snapshotDir, "expected_wal.json", "wal_replay_diff", expectedWal)
	writeP0BSnapshot(t, snapshotDir, "actual_wal.json", "wal_replay_diff", actualWal)
}

func writeP0BSnapshot(t *testing.T, dir string, name string, scope string, items interface{}) {
	t.Helper()

	doc := p0bSnapshotDocument{
		EvidenceType: "p0b_recovery_snapshot",
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		Scope:        scope,
		Scenario:     "redo_idempotent_page_replay",
		Items:        items,
	}

	content, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatalf("marshal snapshot %s: %v", name, err)
	}

	if err := os.WriteFile(filepath.Join(dir, name), content, 0644); err != nil {
		t.Fatalf("write snapshot %s: %v", name, err)
	}
}

func actualPageSnapshot(page PageInterface) p0bPageSnapshot {
	data := page.GetData()
	return p0bPageSnapshot{
		PageID:     page.GetPageID(),
		LSN:        page.GetLSN(),
		Dirty:      page.IsDirty(),
		DataSHA256: sha256Hex(data),
		DataPrefix: snapshotPageValue(page),
	}
}

func expectedPageSnapshot(pageID uint64, lsn uint64, value []byte, dirty bool) p0bPageSnapshot {
	data := make([]byte, 16384)
	copy(data, value)
	return p0bPageSnapshot{
		PageID:     pageID,
		LSN:        lsn,
		Dirty:      dirty,
		DataSHA256: sha256Hex(data),
		DataPrefix: string(value),
	}
}

func snapshotPageValue(page PageInterface) string {
	data := page.GetData()
	end := 0
	for end < len(data) && data[end] != 0 {
		end++
	}
	return string(data[:end])
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
