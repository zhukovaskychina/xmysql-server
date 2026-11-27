package io

import (
	"errors"
	"sync"
	"testing"
)

type recordingPageIO struct {
	mu     sync.Mutex
	pages  map[ioPageKey][]byte
	writes int
}

type prefixPageCompression struct{}

func (prefixPageCompression) CompressPage(_ uint32, _ uint32, data []byte) ([]byte, error) {
	return append([]byte("compressed:"), data...), nil
}

func (prefixPageCompression) DecompressPage(_ uint32, _ uint32, data []byte) ([]byte, error) {
	return append([]byte(nil), data[len("compressed:"):]...), nil
}

func newRecordingPageIO() *recordingPageIO {
	return &recordingPageIO{pages: make(map[ioPageKey][]byte)}
}

func (r *recordingPageIO) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, ok := r.pages[makeIOPageKey(spaceID, pageNo)]
	if !ok {
		return nil, errors.New("page not found")
	}
	return append([]byte(nil), data...), nil
}

func (r *recordingPageIO) WritePage(spaceID, pageNo uint32, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pages[makeIOPageKey(spaceID, pageNo)] = append([]byte(nil), data...)
	r.writes++
	return nil
}

func (r *recordingPageIO) writeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writes
}

func TestIOOptimizerUsesConfiguredPageBackendAndSeparatesSpaces(t *testing.T) {
	backend := newRecordingPageIO()
	if err := backend.WritePage(1, 7, []byte("space-one")); err != nil {
		t.Fatal(err)
	}
	if err := backend.WritePage(2, 7, []byte("space-two")); err != nil {
		t.Fatal(err)
	}

	optimizer := NewIOOptimizer(&IOOptimizerConfig{
		PageIO:            backend,
		EnableReadCache:   true,
		EnableReadAhead:   false,
		EnableBatchWrite:  false,
		EnableIOScheduler: false,
		EnableWriteCache:  false,
	})
	defer optimizer.Stop()

	first, err := optimizer.ReadPage(1, 7)
	if err != nil || string(first) != "space-one" {
		t.Fatalf("read first space: %q, %v", first, err)
	}
	second, err := optimizer.ReadPage(2, 7)
	if err != nil || string(second) != "space-two" {
		t.Fatalf("read second space: %q, %v", second, err)
	}
}

func TestIOOptimizerBatchWritePersistsThroughBackend(t *testing.T) {
	backend := newRecordingPageIO()
	optimizer := NewIOOptimizer(&IOOptimizerConfig{
		PageIO:            backend,
		EnableReadAhead:   false,
		EnableBatchWrite:  true,
		BatchWriteSize:    2,
		FlushInterval:     1000,
		EnableIOScheduler: false,
		EnableReadCache:   false,
		EnableWriteCache:  false,
	})
	defer optimizer.Stop()

	if err := optimizer.WritePage(9, 1, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := optimizer.WritePage(9, 2, []byte("two")); err != nil {
		t.Fatal(err)
	}

	gotOne, err := backend.ReadPage(9, 1)
	if err != nil || string(gotOne) != "one" {
		t.Fatalf("flushed page one: %q, %v", gotOne, err)
	}
	gotTwo, err := backend.ReadPage(9, 2)
	if err != nil || string(gotTwo) != "two" {
		t.Fatalf("flushed page two: %q, %v", gotTwo, err)
	}
	if backend.writeCount() != 2 {
		t.Fatalf("backend writes = %d, want 2", backend.writeCount())
	}
}

func TestIOOptimizerRequiresPageBackend(t *testing.T) {
	optimizer := NewIOOptimizer(&IOOptimizerConfig{
		EnableReadAhead:   false,
		EnableBatchWrite:  false,
		EnableIOScheduler: false,
		EnableReadCache:   false,
		EnableWriteCache:  false,
	})
	defer optimizer.Stop()

	if _, err := optimizer.ReadPage(1, 1); err == nil {
		t.Fatal("ReadPage without a backend must fail explicitly")
	}
	if err := optimizer.WritePage(1, 1, []byte("data")); err == nil {
		t.Fatal("WritePage without a backend must fail explicitly")
	}
	optimizer.Stop()
}

func TestIOOptimizerAppliesPageCompressionOnlyAtDurableBoundary(t *testing.T) {
	backend := newRecordingPageIO()
	optimizer := NewIOOptimizer(&IOOptimizerConfig{
		PageIO:            backend,
		PageCompression:   prefixPageCompression{},
		EnableReadAhead:   false,
		EnableBatchWrite:  false,
		EnableIOScheduler: false,
		EnableReadCache:   true,
		EnableWriteCache:  false,
	})
	defer optimizer.Stop()

	original := []byte("durable page payload")
	if err := optimizer.WritePage(4, 8, original); err != nil {
		t.Fatal(err)
	}
	raw, err := backend.ReadPage(4, 8)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "compressed:durable page payload" {
		t.Fatalf("backend bytes = %q, want compressed payload", raw)
	}
	decoded, err := optimizer.ReadPage(4, 8)
	if err != nil || string(decoded) != string(original) {
		t.Fatalf("decoded page = %q, %v; want %q", decoded, err, original)
	}
}

func TestIOOptimizerPendingWriteIsReadableAndStopIsIdempotent(t *testing.T) {
	backend := newRecordingPageIO()
	optimizer := NewIOOptimizer(&IOOptimizerConfig{
		PageIO:            backend,
		EnableReadAhead:   false,
		EnableBatchWrite:  true,
		BatchWriteSize:    8,
		FlushInterval:     1000,
		EnableIOScheduler: false,
		EnableReadCache:   false,
		EnableWriteCache:  true,
	})

	if err := optimizer.WritePage(3, 4, []byte("pending")); err != nil {
		t.Fatal(err)
	}
	data, err := optimizer.ReadPage(3, 4)
	if err != nil || string(data) != "pending" {
		t.Fatalf("pending write readback: %q, %v", data, err)
	}
	optimizer.Stop()
	optimizer.Stop()
}
