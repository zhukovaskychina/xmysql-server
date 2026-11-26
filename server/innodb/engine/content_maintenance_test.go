package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
)

func TestTextMaintenanceRequestsBindCommittedContentVersion(t *testing.T) {
	value, err := content.NewValue("text/plain", 7, []byte("the source text"))
	if err != nil {
		t.Fatal(err)
	}
	change := transactionDMLChange{
		tableName: "app.documents", kind: "insert", storageKey: int64(42),
		columnTypes: map[string]string{"id": "INT", "body": "CONTENT"},
		after:       map[string]interface{}{"id": int64(42), "body": value},
	}
	requests, err := textMaintenanceRequestsForChange(change)
	if err != nil || len(requests) != 1 {
		t.Fatalf("text source requests = %#v, err=%v", requests, err)
	}
	request := requests[0]
	if request.Source.Schema != "app" || request.Source.Table != "documents" || request.Source.Column != "body" ||
		!bytes.Equal(request.Source.PrimaryKey, []byte("42")) || request.Source.Revision != value.Revision || request.Source.Digest != value.Digest {
		t.Fatalf("maintenance request lost its source identity: %#v", request)
	}

	changes := []transactionDMLChange{{tableName: change.tableName, kind: change.kind, storageKey: change.storageKey, maintenanceRequests: requests}}
	durable, err := json.Marshal(durableChanges(changes))
	if err != nil {
		t.Fatal(err)
	}
	var decoded []durableTransactionChange
	if err := json.Unmarshal(durable, &decoded); err != nil {
		t.Fatal(err)
	}
	recovered := transactionDMLChangesFromDurable(decoded)
	if len(recovered) != 1 || len(recovered[0].maintenanceRequests) != 1 ||
		recovered[0].maintenanceRequests[0].Source.Digest != request.Source.Digest ||
		!bytes.Equal(recovered[0].maintenanceRequests[0].Source.PrimaryKey, request.Source.PrimaryKey) {
		t.Fatalf("maintenance request did not survive transaction journal round trip: %#v", recovered)
	}
	recovered[0].maintenanceRequests[0].Source.PrimaryKey[0] = '9'
	if !bytes.Equal(request.Source.PrimaryKey, []byte("42")) {
		t.Fatal("recovered request aliases the original primary key bytes")
	}
}

func TestTextMaintenanceRequestsSkipDeletesAndNonTextContent(t *testing.T) {
	textValue, err := content.NewValue("text/plain", 1, []byte("text"))
	if err != nil {
		t.Fatal(err)
	}
	imageValue, err := content.NewValue("image/png", 1, []byte("png"))
	if err != nil {
		t.Fatal(err)
	}
	base := transactionDMLChange{
		tableName: "app.documents", storageKey: int64(1), columnTypes: map[string]string{"body": "CONTENT"},
		after: map[string]interface{}{"body": textValue},
	}
	base.kind = "delete"
	if requests, err := textMaintenanceRequestsForChange(base); err != nil || len(requests) != 0 {
		t.Fatalf("delete maintenance requests = %#v, err=%v", requests, err)
	}
	base.kind = "insert"
	base.after["body"] = imageValue
	if requests, err := textMaintenanceRequestsForChange(base); err != nil || len(requests) != 0 {
		t.Fatalf("non-text maintenance requests = %#v, err=%v", requests, err)
	}
	base.after["body"] = nil
	if requests, err := textMaintenanceRequestsForChange(base); err != nil || len(requests) != 0 {
		t.Fatalf("NULL maintenance requests = %#v, err=%v", requests, err)
	}
	base.after["body"] = textValue
	base.storageKey = nil
	if _, err := textMaintenanceRequestsForChange(base); err == nil {
		t.Fatal("text CONTENT without a stable row key must fail maintenance capture")
	}
}

func TestClientCommitRejectsUncapturableContentMaintenanceIntent(t *testing.T) {
	session := newTestMySQLSession()
	session.SetParamByName(contentMaintenanceCaptureErrorParam, errors.New("missing stable key"))
	executor := &XMySQLExecutor{}
	err := executor.commitClientStorageTransaction(session)
	if err == nil || !strings.Contains(err.Error(), "missing stable key") {
		t.Fatalf("commit error = %v", err)
	}
}

func TestCommittedTextMaintenanceEnqueueIsIdempotent(t *testing.T) {
	store, err := content.NewSegmentStore(t.TempDir(), content.SegmentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := store.OpenMaintenanceQueue()
	if err != nil {
		t.Fatal(err)
	}
	value, err := content.NewValue("text/plain", 1, []byte("durable text"))
	if err != nil {
		t.Fatal(err)
	}
	changes := []transactionDMLChange{{
		tableName: "app.documents", kind: "insert", storageKey: int64(8),
		columnTypes: map[string]string{"id": "INT", "body": "CONTENT"},
		after:       map[string]interface{}{"id": int64(8), "body": value},
	}}
	executor := &XMySQLExecutor{contentMaintenanceQueue: queue}
	executor.attachContentMaintenanceRequests(changes)
	if len(changes[0].maintenanceRequests) != 1 {
		t.Fatalf("attached maintenance requests = %#v", changes[0].maintenanceRequests)
	}
	if err := executor.enqueueContentMaintenance(changes); err != nil {
		t.Fatal(err)
	}
	job, created, err := queue.Enqueue(changes[0].maintenanceRequests[0], time.Now().UTC())
	if err != nil || created || job.Generation != 1 || job.Status != content.MaintenanceQueued {
		t.Fatalf("duplicate committed enqueue = %#v, created=%t, err=%v", job, created, err)
	}
}

func TestClientTransactionPublishesTextMaintenanceOnlyAfterCommit(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table queued_documents (id int primary key, body content)")
	queue := engine.QueryExecutor.contentMaintenanceQueue
	if queue == nil {
		t.Fatal("database engine did not bind its CONTENT maintenance queue")
	}
	session := newTestMySQLSession()
	claim := func(owner string) (content.MaintenanceLease, bool, error) {
		return queue.Claim(owner, time.Now().UTC().Add(time.Second), time.Minute)
	}

	mustExecSessionSQL(t, engine, session, "app", "start transaction")
	mustExecSessionSQL(t, engine, session, "app", "insert into queued_documents values (1, CONTENT_FROM_BYTES('rolled back text', 'text/plain'))")
	if _, found, err := claim("before-rollback"); err != nil || found {
		t.Fatalf("uncommitted CONTENT maintenance was visible: found=%t, err=%v", found, err)
	}
	mustExecSessionSQL(t, engine, session, "app", "rollback")
	if _, found, err := claim("after-rollback"); err != nil || found {
		t.Fatalf("rolled-back CONTENT created maintenance work: found=%t, err=%v", found, err)
	}

	mustExecSessionSQL(t, engine, session, "app", "start transaction")
	mustExecSessionSQL(t, engine, session, "app", "insert into queued_documents values (2, CONTENT_FROM_BYTES('committed text', 'text/plain'))")
	mustExecSessionSQL(t, engine, session, "app", "commit")
	lease, found, err := claim("after-commit")
	if err != nil || !found {
		t.Fatalf("committed CONTENT maintenance was not published: found=%t, err=%v", found, err)
	}
	if lease.Job.Source.Schema != "app" || lease.Job.Source.Table != "queued_documents" || lease.Job.Source.Column != "body" ||
		!bytes.Equal(lease.Job.Source.PrimaryKey, []byte("2")) || lease.Job.Source.Revision == 0 {
		t.Fatalf("queued maintenance source is not bound to the committed row: %#v", lease.Job.Source)
	}
}

func TestTextMaintenanceWorkerPersistsSourceVerifiedBlocks(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table worker_documents (id int primary key, body content)")
	text := []byte("first paragraph\n\nsecond paragraph")
	mustExecSQL(t, engine, "app", "insert into worker_documents values (7, CONTENT_FROM_BYTES('first paragraph\\n\\nsecond paragraph', 'text/plain'))")

	value, err := content.NewValue("text/plain", 1, text)
	if err != nil {
		t.Fatal(err)
	}
	source, err := content.NewSourceRef("app", "worker_documents", "body", []byte("7"), value)
	if err != nil {
		t.Fatal(err)
	}
	request, err := content.NewTextChunkMaintenanceRequest(source, content.TextChunkOptions{MaxBytes: content.DefaultTextChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	queue := engine.QueryExecutor.contentMaintenanceQueue
	seedJob, _, err := queue.Enqueue(request, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	queuedStatus := mustQuerySQL(t, engine, "app", "select content_maintenance_status(body, 4096) from worker_documents where id = 7")
	if len(queuedStatus) != 1 || queuedStatus[0][0] != string(content.MaintenanceQueued) {
		t.Fatalf("SQL queued maintenance status = %#v, want QUEUED", queuedStatus)
	}

	processed, err := engine.QueryExecutor.ProcessNextContentMaintenance(context.Background(), "integration-worker")
	if err != nil || !processed {
		t.Fatalf("process next maintenance = processed %t, err %v", processed, err)
	}
	job, found, err := queue.Get(seedJob.ID)
	if err != nil || !found || job.Status != content.MaintenanceReady || !job.ResultStored {
		t.Fatalf("completed maintenance = %#v, found=%t, err=%v", job, found, err)
	}
	result, found, err := queue.ReadResult(job.ID)
	if err != nil || !found {
		t.Fatalf("read maintenance result = found %t, err %v", found, err)
	}
	var manifest contentMaintenanceResultManifest
	if err := json.Unmarshal(result, &manifest); err != nil {
		t.Fatalf("decode maintenance manifest: %v", err)
	}
	if manifest.Version != 1 || !manifest.Source.Matches(source) || manifest.Object.ID == "" || len(manifest.Blocks) != 1 {
		t.Fatalf("maintenance manifest lost source or blocks: %#v", manifest)
	}
	store, err := contentSegmentStoreForDataDir(engine.QueryExecutor.getDataDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, skipped, err := engine.QueryExecutor.ReclaimOrphanedContentObjects(context.Background(), store); err != nil || skipped {
		t.Fatalf("CONTENT result object inventory = skipped %t, err %v", skipped, err)
	}
	encoded, err := store.ReadRange(context.Background(), manifest.Object, manifest.Blocks[0].Offset, manifest.Blocks[0].Length)
	if err != nil {
		t.Fatal(err)
	}
	var block content.Block
	if err := block.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	if block.ID != manifest.Blocks[0].ID || block.Text != string(text) || !block.Source.Matches(source) {
		t.Fatalf("persisted block = %#v, manifest entry = %#v", block, manifest.Blocks[0])
	}
	var reusedBlocks []content.Block
	reusedCount, reused, err := visitPublishedTextChunks(context.Background(), store, source, content.TextChunkOptions{MaxBytes: content.DefaultTextChunkBytes}, func(block content.Block) error {
		reusedBlocks = append(reusedBlocks, block)
		return nil
	})
	if err != nil || !reused || reusedCount != 1 || len(reusedBlocks) != 1 || reusedBlocks[0].ID != block.ID {
		t.Fatalf("published block reuse = count %d, reused %t, blocks %#v, err %v", reusedCount, reused, reusedBlocks, err)
	}
	rows := mustQuerySQL(t, engine, "app", "select chunks.ordinal, chunks.text from worker_documents d, table(content_text_chunks(d.body, 4096)) as chunks")
	if len(rows) != 1 || rows[0][0] != "0" || rows[0][1] != string(text) {
		t.Fatalf("SQL did not read the source-bound persisted text block: %#v", rows)
	}
	status := mustQuerySQL(t, engine, "app", "select content_maintenance_status(body, 4096) from worker_documents where id = 7")
	if len(status) != 1 || status[0][0] != string(content.MaintenanceReady) {
		t.Fatalf("SQL maintenance status = %#v, want READY", status)
	}
}

func TestStartedEngineRunsAndStopsContentMaintenanceWorker(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table background_documents (id int primary key, body content)")
	text := []byte("background derived text")
	mustExecSQL(t, engine, "app", "insert into background_documents values (9, CONTENT_FROM_BYTES('background derived text', 'text/plain'))")
	value, err := content.NewValue("text/plain", 1, text)
	if err != nil {
		t.Fatal(err)
	}
	source, err := content.NewSourceRef("app", "background_documents", "body", []byte("9"), value)
	if err != nil {
		t.Fatal(err)
	}
	request, err := content.NewTextChunkMaintenanceRequest(source, content.TextChunkOptions{MaxBytes: content.DefaultTextChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	queue := engine.QueryExecutor.contentMaintenanceQueue
	job, _, err := queue.Enqueue(request, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		current, found, err := queue.Get(job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if found && current.Status == content.MaintenanceReady {
			if !current.ResultStored {
				t.Fatal("background worker marked job READY without storing its result")
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	current, found, err := queue.Get(job.ID)
	if err != nil || !found || current.Status != content.MaintenanceReady {
		t.Fatalf("background maintenance job = %#v, found=%t, err=%v", current, found, err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine and stop worker: %v", err)
	}
	if engine.contentMaintenanceCancel != nil {
		t.Fatal("content maintenance worker cancellation handle remained after close")
	}
}

func TestTextMaintenanceWorkerFailsSupersededSourceVersion(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table changing_documents (id int primary key, body content)")
	oldText := []byte("old source version")
	mustExecSQL(t, engine, "app", "insert into changing_documents values (11, CONTENT_FROM_BYTES('old source version', 'text/plain'))")
	oldValue, err := content.NewValue("text/plain", 1, oldText)
	if err != nil {
		t.Fatal(err)
	}
	oldSource, err := content.NewSourceRef("app", "changing_documents", "body", []byte("11"), oldValue)
	if err != nil {
		t.Fatal(err)
	}
	oldRequest, err := content.NewTextChunkMaintenanceRequest(oldSource, content.TextChunkOptions{MaxBytes: content.DefaultTextChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	queue := engine.QueryExecutor.contentMaintenanceQueue
	oldJob, _, err := queue.Enqueue(oldRequest, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	mustExecSQL(t, engine, "app", "update changing_documents set body = CONTENT_FROM_BYTES('new source version', 'text/plain') where id = 11")
	processed, err := engine.QueryExecutor.ProcessNextContentMaintenance(context.Background(), "stale-source-worker")
	if err != nil || !processed {
		t.Fatalf("process superseded source = processed %t, err %v", processed, err)
	}
	job, found, err := queue.Get(oldJob.ID)
	if err != nil || !found || job.Status != content.MaintenanceFailed || job.ResultStored || job.ResultDigest != "" {
		t.Fatalf("superseded source job = %#v, found=%t, err=%v", job, found, err)
	}
}

func TestCommittedJournalRecoveryReplaysTextMaintenanceIntent(t *testing.T) {
	store, err := content.NewSegmentStore(t.TempDir(), content.SegmentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := store.OpenMaintenanceQueue()
	if err != nil {
		t.Fatal(err)
	}
	value, err := content.NewValue("text/plain", 5, []byte("replayed source"))
	if err != nil {
		t.Fatal(err)
	}
	change := transactionDMLChange{
		tableName: "app.documents", kind: "insert", storageKey: int64(12),
		columnTypes: map[string]string{"id": "INT", "body": "CONTENT"},
		after:       map[string]interface{}{"id": int64(12), "body": value},
	}
	requests, err := textMaintenanceRequestsForChange(change)
	if err != nil || len(requests) != 1 {
		t.Fatalf("source request = %#v, err=%v", requests, err)
	}
	durable, err := json.Marshal(durableChanges([]transactionDMLChange{{
		tableName: change.tableName, kind: change.kind, storageKey: change.storageKey, maintenanceRequests: requests,
	}})[0])
	if err != nil {
		t.Fatal(err)
	}
	commit, err := json.Marshal(durableTransactionChange{RecordType: replicationCommitJournalRecordType, TransactionID: "committed-content-tx"})
	if err != nil {
		t.Fatal(err)
	}
	raw := append(append(append([]byte(nil), durable...), '\n'), commit...)
	raw = append(raw, '\n')
	executor := &XMySQLExecutor{
		conf: &conf.Cfg{DataDir: t.TempDir()}, contentMaintenanceQueue: queue,
	}
	if err := executor.recoverPendingReplicationCommit("client-session", "committed-content-tx", nil, raw); err != nil {
		t.Fatal(err)
	}
	lease, found, err := queue.Claim("recovery", time.Now().UTC().Add(time.Second), time.Minute)
	if err != nil || !found {
		t.Fatalf("recovered committed maintenance was not queued: found=%t, err=%v", found, err)
	}
	if lease.Job.Source.Revision != 5 || lease.Job.Source.Table != "documents" || !bytes.Equal(lease.Job.Source.PrimaryKey, []byte("12")) {
		t.Fatalf("recovered job source = %#v", lease.Job.Source)
	}
}
