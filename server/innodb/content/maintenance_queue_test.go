package content

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestTextChunkMaintenanceRequestCapturesNormalizedConfiguration(t *testing.T) {
	source := SourceRef{Schema: "app", Table: "documents", Column: "body", PrimaryKey: []byte("7"), Revision: 3, Digest: sha256.Sum256([]byte("text"))}
	first, err := NewTextChunkMaintenanceRequest(source, TextChunkOptions{MaxBytes: DefaultTextChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewTextChunkMaintenanceRequest(source, TextChunkOptions{MaxBytes: DefaultTextChunkBytes, OverlapBytes: 0, MinBoundaryBytes: 0})
	if err != nil {
		t.Fatal(err)
	}
	if first.ProcessorID != textChunkerProcessorID || first.ProcessorVersion != textChunkerProcessorVersion || first.ConfigurationDigest != second.ConfigurationDigest {
		t.Fatalf("default chunk requests are not stable: %#v / %#v", first, second)
	}
	changed, err := NewTextChunkMaintenanceRequest(source, TextChunkOptions{MaxBytes: DefaultTextChunkBytes / 2})
	if err != nil {
		t.Fatal(err)
	}
	if changed.ConfigurationDigest == first.ConfigurationDigest {
		t.Fatal("different chunk settings reused the same configuration digest")
	}
}

func TestMaintenanceQueueFencesExpiredWorkersAndRecoversPublishedState(t *testing.T) {
	root := t.TempDir()
	store, err := NewSegmentStore(root, SegmentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := store.OpenMaintenanceQueue()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("source"))
	request := MaintenanceRequest{
		Source:              SourceRef{Schema: "app", Table: "documents", Column: "body", PrimaryKey: []byte{1, 2}, Revision: 7, Digest: digest},
		ProcessorID:         "text-chunker",
		ProcessorVersion:    "2",
		ConfigurationDigest: testMaintenanceConfigurationDigest(),
	}
	job, created, err := queue.Enqueue(request, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !created || job.Status != MaintenanceQueued || job.Generation != 1 {
		t.Fatalf("initial job = %#v, created=%t", job, created)
	}
	duplicate, created, err := queue.Enqueue(request, time.Unix(101, 0))
	if err != nil || created || duplicate.ID != job.ID || duplicate.Generation != job.Generation {
		t.Fatalf("duplicate enqueue = %#v, created=%t, err=%v", duplicate, created, err)
	}
	otherConfiguration := request
	otherConfigHash := sha256.Sum256([]byte("different chunk settings"))
	otherConfiguration.ConfigurationDigest = hex.EncodeToString(otherConfigHash[:])
	firstID, firstErr := maintenanceJobID(request)
	secondID, secondErr := maintenanceJobID(otherConfiguration)
	if firstErr != nil || secondErr != nil || firstID == secondID {
		t.Fatalf("different processor configuration shared a job identity: %q, %q, errors=%v/%v", firstID, secondID, firstErr, secondErr)
	}

	first, found, err := queue.Claim("worker-a", time.Unix(102, 0), time.Second)
	if err != nil || !found || first.FencingToken != 1 {
		t.Fatalf("first claim = %#v, found=%t, err=%v", first, found, err)
	}
	second, found, err := queue.Claim("worker-b", time.Unix(104, 0), time.Minute)
	if err != nil || !found || second.FencingToken != 2 {
		t.Fatalf("expired lease reclaim = %#v, found=%t, err=%v", second, found, err)
	}
	resultHash := sha256.Sum256([]byte("derived-digest"))
	resultDigest := hex.EncodeToString(resultHash[:])
	if _, err := queue.Complete(first, time.Unix(105, 0), resultDigest); !errors.Is(err, ErrMaintenanceLeaseFenced) {
		t.Fatalf("expired worker completion error = %v, want lease fenced", err)
	}
	completed, err := queue.Complete(second, time.Unix(106, 0), resultDigest)
	if err != nil || completed.Status != MaintenanceReady || completed.ResultDigest != resultDigest {
		t.Fatalf("completion = %#v, err=%v", completed, err)
	}

	reopenedStore, err := NewSegmentStore(root, SegmentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := reopenedStore.OpenMaintenanceQueue()
	if err != nil {
		t.Fatal(err)
	}
	loaded, found, err := reopened.Get(job.ID)
	if err != nil || !found || loaded.Status != MaintenanceReady || loaded.FencingToken != 2 || loaded.ResultDigest != resultDigest {
		t.Fatalf("recovered job = %#v, found=%t, err=%v", loaded, found, err)
	}
}

func TestMaintenanceQueueRetriesWithBackoffAndHeartbeatFencesOldLease(t *testing.T) {
	store, err := NewSegmentStore(t.TempDir(), SegmentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := store.OpenMaintenanceQueue()
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = queue.Enqueue(testMaintenanceRequest(), time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	first, found, err := queue.Claim("worker-a", time.Unix(201, 0), 10*time.Second)
	if err != nil || !found {
		t.Fatalf("first claim = %#v, found=%t, err=%v", first, found, err)
	}
	retry, err := queue.Retry(first, time.Unix(202, 0), 5*time.Second, fmt.Errorf("provider busy"))
	if err != nil || retry.Status != MaintenanceQueued || retry.LastError != "provider busy" || !retry.NextAttemptAt.Equal(time.Unix(207, 0)) {
		t.Fatalf("retry state = %#v, err=%v", retry, err)
	}
	if _, found, err := queue.Claim("worker-b", time.Unix(206, 0), time.Second); err != nil || found {
		t.Fatalf("retry claimed before backoff: found=%t, err=%v", found, err)
	}
	second, found, err := queue.Claim("worker-b", time.Unix(207, 0), time.Second)
	if err != nil || !found || second.FencingToken != 2 {
		t.Fatalf("retry claim = %#v, found=%t, err=%v", second, found, err)
	}
	renewed, err := queue.Heartbeat(second, time.Unix(207, 500000000), time.Minute)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	resultHash := sha256.Sum256([]byte("retry-result"))
	resultDigest := hex.EncodeToString(resultHash[:])
	if _, err := queue.Complete(second, time.Unix(208, 0), resultDigest); !errors.Is(err, ErrMaintenanceLeaseFenced) {
		t.Fatalf("pre-heartbeat lease completion error = %v, want lease fenced", err)
	}
	completed, err := queue.Complete(renewed, time.Unix(220, 0), resultDigest)
	if err != nil || completed.Status != MaintenanceReady || completed.Attempts != 2 {
		t.Fatalf("completed retry state = %#v, err=%v", completed, err)
	}
}

func TestMaintenanceQueueConcurrentHandlesGrantOneLease(t *testing.T) {
	root := t.TempDir()
	storeA, err := NewSegmentStore(root, SegmentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	storeB, err := NewSegmentStore(root, SegmentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	queueA, err := storeA.OpenMaintenanceQueue()
	if err != nil {
		t.Fatal(err)
	}
	queueB, err := storeB.OpenMaintenanceQueue()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := queueA.Enqueue(testMaintenanceRequest(), time.Unix(300, 0)); err != nil {
		t.Fatal(err)
	}
	queues := []*MaintenanceQueue{queueA, queueB}
	results := make(chan bool, len(queues))
	errorsFound := make(chan error, len(queues))
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index, queue := range queues {
		wait.Add(1)
		go func(index int, queue *MaintenanceQueue) {
			defer wait.Done()
			<-start
			_, found, err := queue.Claim(fmt.Sprintf("worker-%d", index), time.Unix(301, 0), time.Minute)
			results <- found
			errorsFound <- err
		}(index, queue)
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsFound)
	claims := 0
	for found := range results {
		if found {
			claims++
		}
	}
	for err := range errorsFound {
		if err != nil {
			t.Fatalf("concurrent claim error: %v", err)
		}
	}
	if claims != 1 {
		t.Fatalf("successful concurrent claims = %d, want exactly one", claims)
	}
}

func TestMaintenanceQueueFailureIsDurableAndNotSilentlyRequeued(t *testing.T) {
	root := t.TempDir()
	store, err := NewSegmentStore(root, SegmentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := store.OpenMaintenanceQueue()
	if err != nil {
		t.Fatal(err)
	}
	request := testMaintenanceRequest()
	job, _, err := queue.Enqueue(request, time.Unix(400, 0))
	if err != nil {
		t.Fatal(err)
	}
	lease, found, err := queue.Claim("worker", time.Unix(401, 0), time.Minute)
	if err != nil || !found {
		t.Fatalf("claim = %#v, found=%t, err=%v", lease, found, err)
	}
	failed, err := queue.Fail(lease, time.Unix(402, 0), fmt.Errorf("unsupported media format"))
	if err != nil || failed.Status != MaintenanceFailed || failed.LastError != "unsupported media format" {
		t.Fatalf("failed state = %#v, err=%v", failed, err)
	}
	reopenedStore, err := NewSegmentStore(root, SegmentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := reopenedStore.OpenMaintenanceQueue()
	if err != nil {
		t.Fatal(err)
	}
	recovered, found, err := reopened.Get(job.ID)
	if err != nil || !found || recovered.Status != MaintenanceFailed {
		t.Fatalf("recovered failure = %#v, found=%t, err=%v", recovered, found, err)
	}
	duplicate, created, err := reopened.Enqueue(request, time.Unix(500, 0))
	if err != nil || created || duplicate.Status != MaintenanceFailed {
		t.Fatalf("failed job was silently reset: %#v, created=%t, err=%v", duplicate, created, err)
	}
	if _, found, err := reopened.Claim("worker-b", time.Unix(600, 0), time.Second); err != nil || found {
		t.Fatalf("failed job became claimable: found=%t, err=%v", found, err)
	}
}

func TestMaintenanceQueueCorruptLatestGenerationFailsClosed(t *testing.T) {
	root := t.TempDir()
	store, err := NewSegmentStore(root, SegmentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := store.OpenMaintenanceQueue()
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := queue.Enqueue(testMaintenanceRequest(), time.Unix(700, 0))
	if err != nil {
		t.Fatal(err)
	}
	lease, found, err := queue.Claim("worker", time.Unix(701, 0), time.Minute)
	if err != nil || !found {
		t.Fatalf("claim = %#v, found=%t, err=%v", lease, found, err)
	}
	latest := filepath.Join(root, "maintenance", job.ID, maintenanceJobFilename(lease.Job.Generation))
	if err := os.WriteFile(latest, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if recovered, found, err := queue.Get(job.ID); err == nil || found || recovered.Generation != 0 {
		t.Fatalf("corrupt latest generation was hidden by an older snapshot: %#v, found=%t, err=%v", recovered, found, err)
	}
}

func testMaintenanceRequest() MaintenanceRequest {
	digest := sha256.Sum256([]byte("source"))
	return MaintenanceRequest{
		Source:              SourceRef{Schema: "app", Table: "documents", Column: "body", PrimaryKey: []byte{1, 2}, Revision: 7, Digest: digest},
		ProcessorID:         "text-chunker",
		ProcessorVersion:    "2",
		ConfigurationDigest: testMaintenanceConfigurationDigest(),
	}
}

func testMaintenanceConfigurationDigest() string {
	digest := sha256.Sum256([]byte("chunk_bytes=512;overlap=32"))
	return hex.EncodeToString(digest[:])
}
