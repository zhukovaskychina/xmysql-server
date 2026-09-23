package manager

import (
	"path/filepath"
	"testing"
)

func TestBackgroundManagersStopIsIdempotent(t *testing.T) {
	extentReuse := NewExtentReuseManager(&ExtentReuseConfig{MonitorInterval: 1})
	extentReuse.Stop()
	extentReuse.Stop()

	segmentOptimizer := NewSegmentSpaceOptimizer(nil)
	segmentOptimizer.Stop()
	segmentOptimizer.Stop()

	expansion := NewSpaceExpansionManager(nil, &ExpansionConfig{AsyncExpand: false})
	expansion.Stop()
	expansion.Stop()
}

func TestBatchCompressorCloseIsIdempotent(t *testing.T) {
	compressor := NewBatchCompressor(COMPRESS_NONE, 0, 1)
	compressor.Close()
	compressor.Close()
}

func TestBatchWriterCloseIsIdempotent(t *testing.T) {
	writer, err := NewBatchWriter(filepath.Join(t.TempDir(), "redo.log"), 1024, NewLSNManager(0))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}
