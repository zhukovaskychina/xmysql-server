package engine

import (
	"bytes"
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
