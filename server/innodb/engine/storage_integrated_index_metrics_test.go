package engine

import (
	"context"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

type indexMetricsBTreeManager struct {
	stats *manager.BTreeManagerStats
}

func (m *indexMetricsBTreeManager) Init(context.Context, uint32, uint32) error { return nil }
func (m *indexMetricsBTreeManager) GetAllLeafPages(context.Context) ([]uint32, error) {
	return nil, nil
}
func (m *indexMetricsBTreeManager) Search(context.Context, interface{}) (uint32, int, error) {
	return 0, 0, nil
}
func (m *indexMetricsBTreeManager) Insert(context.Context, interface{}, []byte) error { return nil }
func (m *indexMetricsBTreeManager) Delete(context.Context, interface{}) error         { return nil }
func (m *indexMetricsBTreeManager) RangeSearch(context.Context, interface{}, interface{}) ([]basic.Row, error) {
	return nil, nil
}
func (m *indexMetricsBTreeManager) GetFirstLeafPage(context.Context) (uint32, error) { return 0, nil }
func (m *indexMetricsBTreeManager) GetIndexCacheStats() (uint64, uint64) {
	return m.stats.IndexCacheHits, m.stats.IndexCacheMisses
}

func TestIndexPerformanceMetricsUseRuntimeCounters(t *testing.T) {
	indexManager := manager.NewIndexManager(nil, nil, nil)
	btreeManager := &indexMetricsBTreeManager{stats: &manager.BTreeManagerStats{
		IndexCacheHits:   7,
		IndexCacheMisses: 3,
	}}
	executor := NewStorageIntegratedDMLExecutor(nil, nil, btreeManager, nil, nil, indexManager, nil, nil)
	executor.stats.IndexUpdates = 4
	executor.stats.IndexUpdateTime = 40 * time.Millisecond

	metrics := executor.monitorIndexPerformance()
	if metrics.TotalIndexUpdates != 4 {
		t.Fatalf("expected 4 index updates, got %d", metrics.TotalIndexUpdates)
	}
	if metrics.AverageUpdateTime != 10*time.Millisecond {
		t.Fatalf("expected 10ms average update time, got %s", metrics.AverageUpdateTime)
	}
	if metrics.IndexCacheHitRate != 0.7 {
		t.Fatalf("expected 0.7 cache hit rate, got %v", metrics.IndexCacheHitRate)
	}
	if metrics.ActiveIndexCount != 0 {
		t.Fatalf("expected no active indexes in a newly created manager, got %d", metrics.ActiveIndexCount)
	}
}

func TestIndexPerformanceMetricsHaveDefinedEmptyState(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	metrics := executor.monitorIndexPerformance()
	if metrics.AverageUpdateTime != 0 {
		t.Fatalf("expected zero average update time without updates, got %s", metrics.AverageUpdateTime)
	}
	if metrics.IndexCacheHitRate != 0 {
		t.Fatalf("expected zero cache hit rate without observations, got %v", metrics.IndexCacheHitRate)
	}
	if metrics.ActiveIndexCount != 0 {
		t.Fatalf("expected zero active indexes without an index manager, got %d", metrics.ActiveIndexCount)
	}
}

func TestRecordIndexUpdateAccumulatesMeasuredDuration(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	executor.recordIndexUpdate(2, 25*time.Millisecond)
	executor.recordIndexUpdate(1, 5*time.Millisecond)

	if executor.stats.IndexUpdates != 3 {
		t.Fatalf("expected 3 index updates, got %d", executor.stats.IndexUpdates)
	}
	if executor.stats.IndexUpdateTime != 30*time.Millisecond {
		t.Fatalf("expected 30ms measured index update time, got %s", executor.stats.IndexUpdateTime)
	}
}
