package plan

import (
	"testing"
	"time"
)

func TestTableStats(t *testing.T) {
	builder := &StatsBuilder{
		sampleRate: 1.0,
		maxSamples: 1000,
	}

	rows := [][]interface{}{
		{int64(1), "test1", float64(1.1)},
		{int64(2), "test2", float64(2.2)},
		{int64(3), "test3", float64(3.3)},
	}

	stats := builder.BuildTableStats("test_table", rows)

	// 验证基本统计信息
	if stats.TableName != "test_table" {
		t.Errorf("TableName = %v, want %v", stats.TableName, "test_table")
	}
	if stats.RowCount != 3 {
		t.Errorf("RowCount = %v, want %v", stats.RowCount, 3)
	}
	if stats.TotalSize <= 0 {
		t.Errorf("TotalSize = %v, want > 0", stats.TotalSize)
	}
}

func TestColumnStats(t *testing.T) {
	builder := &StatsBuilder{
		sampleRate: 1.0,
		maxSamples: 1000,
	}

	values := []interface{}{
		int64(1), int64(2), int64(2), int64(3), int64(3), int64(3), nil,
	}

	stats := builder.BuildColumnStats("test_column", values)

	// 验证基本统计信息
	if stats.ColumnName != "test_column" {
		t.Errorf("ColumnName = %v, want %v", stats.ColumnName, "test_column")
	}
	if stats.NotNullCount != 6 {
		t.Errorf("NotNullCount = %v, want %v", stats.NotNullCount, 6)
	}
	if stats.NullCount != 1 {
		t.Errorf("NullCount = %v, want %v", stats.NullCount, 1)
	}
	if stats.DistinctCount != 3 {
		t.Errorf("DistinctCount = %v, want %v", stats.DistinctCount, 3)
	}

	// 验证最大最小值
	if stats.MinValue != int64(1) {
		t.Errorf("MinValue = %v, want %v", stats.MinValue, 1)
	}
	if stats.MaxValue != int64(3) {
		t.Errorf("MaxValue = %v, want %v", stats.MaxValue, 3)
	}

	// 验证直方图
	if stats.Histogram == nil {
		t.Error("Histogram is nil")
	} else {
		if stats.Histogram.TotalCount != 6 {
			t.Errorf("Histogram.TotalCount = %v, want %v", stats.Histogram.TotalCount, 6)
		}
		if stats.Histogram.NDV != 3 {
			t.Errorf("Histogram.NDV = %v, want %v", stats.Histogram.NDV, 3)
		}
	}

	// 验证TopN
	if len(stats.TopN) == 0 {
		t.Error("TopN is empty")
	} else {
		if stats.TopN[0].Value != int64(3) || stats.TopN[0].Freq != 3 {
			t.Errorf("TopN[0] = {%v, %v}, want {3, 3}", stats.TopN[0].Value, stats.TopN[0].Freq)
		}
	}
}

func TestIndexStats(t *testing.T) {
	builder := &StatsBuilder{
		sampleRate: 1.0,
		maxSamples: 1000,
	}

	keys := [][]interface{}{
		{int64(1), "a"},
		{int64(1), "b"},
		{int64(2), "a"},
		{int64(2), "b"},
		{int64(3), "c"},
	}

	stats := builder.BuildIndexStats("test_index", keys)

	// 验证基本统计信息
	if stats.IndexName != "test_index" {
		t.Errorf("IndexName = %v, want %v", stats.IndexName, "test_index")
	}
	if stats.Cardinality != 5 {
		t.Errorf("Cardinality = %v, want %v", stats.Cardinality, 5)
	}

	// 验证选择性
	expectedSelectivity := float64(5) / float64(5) // 5个不同的键 / 5个总键
	if stats.Selectivity != expectedSelectivity {
		t.Errorf("Selectivity = %v, want %v", stats.Selectivity, expectedSelectivity)
	}
}

func TestHistogram(t *testing.T) {
	values := []interface{}{
		int64(1), int64(2), int64(2), int64(3), int64(3), int64(3),
		int64(4), int64(4), int64(4), int64(4),
	}

	hist := buildHistogram(values, 5)

	// 验证基本属性
	if hist == nil {
		t.Fatal("Histogram is nil")
	}
	if hist.TotalCount != 10 {
		t.Errorf("TotalCount = %v, want %v", hist.TotalCount, 10)
	}
	if hist.NDV != 4 {
		t.Errorf("NDV = %v, want %v", hist.NDV, 4)
	}

	// 验证桶
	if len(hist.Buckets) == 0 {
		t.Error("Buckets is empty")
	} else {
		// 验证第一个桶
		firstBucket := hist.Buckets[0]
		if firstBucket.LowerBound != int64(1) {
			t.Errorf("First bucket LowerBound = %v, want %v", firstBucket.LowerBound, 1)
		}
		if firstBucket.Count <= 0 {
			t.Errorf("First bucket Count = %v, want > 0", firstBucket.Count)
		}

		// 验证最后一个桶
		lastBucket := hist.Buckets[len(hist.Buckets)-1]
		if lastBucket.UpperBound != int64(4) {
			t.Errorf("Last bucket UpperBound = %v, want %v", lastBucket.UpperBound, 4)
		}
		if lastBucket.Count <= 0 {
			t.Errorf("Last bucket Count = %v, want > 0", lastBucket.Count)
		}
	}
}

func TestValueFreq(t *testing.T) {
	freq := map[interface{}]int64{
		int64(1): 1,
		int64(2): 2,
		int64(3): 3,
		int64(4): 4,
	}

	topN := buildTopN(freq, 3)

	// 验证TopN结果
	if len(topN) != 3 {
		t.Errorf("len(topN) = %v, want %v", len(topN), 3)
	}

	// 验证排序是否正确
	for i := 1; i < len(topN); i++ {
		if topN[i-1].Freq < topN[i].Freq {
			t.Errorf("TopN not properly sorted at index %v", i)
		}
	}

	// 验证最高频值
	if topN[0].Value != int64(4) || topN[0].Freq != 4 {
		t.Errorf("Top frequency item = {%v, %v}, want {4, 4}", topN[0].Value, topN[0].Freq)
	}
}

func TestStatsBuilder(t *testing.T) {
	tests := []struct {
		name       string
		sampleRate float64
		maxSamples int64
		data       []interface{}
		wantErr    bool
	}{
		{
			name:       "FullSample",
			sampleRate: 1.0,
			maxSamples: 1000,
			data:       []interface{}{1, 2, 3, 4, 5},
			wantErr:    false,
		},
		{
			name:       "PartialSample",
			sampleRate: 0.5,
			maxSamples: 2,
			data:       []interface{}{1, 2, 3, 4, 5},
			wantErr:    false,
		},
		{
			name:       "EmptyData",
			sampleRate: 1.0,
			maxSamples: 1000,
			data:       []interface{}{},
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := &StatsBuilder{
				sampleRate: tt.sampleRate,
				maxSamples: tt.maxSamples,
			}

			stats := builder.BuildColumnStats("test", tt.data)
			if (stats == nil) != tt.wantErr {
				t.Errorf("BuildColumnStats() error = %v, wantErr %v", stats == nil, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if stats.ColumnName != "test" {
					t.Errorf("ColumnName = %v, want %v", stats.ColumnName, "test")
				}
				if len(tt.data) > 0 && stats.Histogram == nil {
					t.Error("Histogram is nil for non-empty data")
				}
			}
		})
	}
}
