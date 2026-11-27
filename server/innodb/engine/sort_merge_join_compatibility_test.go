package engine

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

type sliceJoinOperator struct {
	rows   []Record
	schema *metadata.QuerySchema
	index  int
}

func (s *sliceJoinOperator) Open(context.Context) error { s.index = 0; return nil }
func (s *sliceJoinOperator) Next(context.Context) (Record, error) {
	if s.index >= len(s.rows) {
		return nil, nil
	}
	row := s.rows[s.index]
	s.index++
	return row, nil
}
func (s *sliceJoinOperator) Close() error                  { return nil }
func (s *sliceJoinOperator) Schema() *metadata.QuerySchema { return s.schema }

func newJoinTestSchema(name string) *metadata.QuerySchema {
	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn(name, metadata.TypeInt))
	return schema
}

func newJoinTestRows(schema *metadata.QuerySchema, values ...int64) []Record {
	rows := make([]Record, 0, len(values))
	for _, value := range values {
		rows = append(rows, NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(value)}, schema))
	}
	return rows
}

func TestSortMergeJoinHandlesUnsortedInputs(t *testing.T) {
	leftSchema := newJoinTestSchema("id")
	rightSchema := newJoinTestSchema("id")
	left := &sliceJoinOperator{rows: newJoinTestRows(leftSchema, 2, 1), schema: leftSchema}
	right := &sliceJoinOperator{rows: newJoinTestRows(rightSchema, 2, 3), schema: rightSchema}

	join := NewSortMergeJoinOperator(left, right, "INNER", func(row Record) (string, bool) {
		return row.GetValueByIndex(0).ToString(), row.GetValueByIndex(0).IsNull()
	}, func(row Record) (string, bool) {
		return row.GetValueByIndex(0).ToString(), row.GetValueByIndex(0).IsNull()
	})
	if err := join.Open(context.Background()); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer join.Close()

	row, err := join.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if row == nil || row.GetValueByIndex(0).Int() != 2 || row.GetValueByIndex(1).Int() != 2 {
		t.Fatalf("joined row = %v, want [2, 2]", row)
	}
	row, err = join.Next(context.Background())
	if err != nil {
		t.Fatalf("second Next() error = %v", err)
	}
	if row != nil {
		t.Fatalf("inner join returned unexpected extra row %v", row)
	}
}

func TestSortMergeJoinPreservesLeftOuterRows(t *testing.T) {
	leftSchema := newJoinTestSchema("id")
	rightSchema := newJoinTestSchema("id")
	left := &sliceJoinOperator{rows: newJoinTestRows(leftSchema, 2, 1), schema: leftSchema}
	right := &sliceJoinOperator{rows: newJoinTestRows(rightSchema, 2), schema: rightSchema}
	join := NewSortMergeJoinOperator(left, right, "LEFT", func(row Record) (string, bool) {
		return row.GetValueByIndex(0).ToString(), row.GetValueByIndex(0).IsNull()
	}, func(row Record) (string, bool) {
		return row.GetValueByIndex(0).ToString(), row.GetValueByIndex(0).IsNull()
	})
	if err := join.Open(context.Background()); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer join.Close()

	rows := make([]Record, 0, 2)
	for {
		row, err := join.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}
	if len(rows) != 2 {
		t.Fatalf("LEFT join returned %d rows, want 2", len(rows))
	}
	if !rows[0].GetValueByIndex(1).IsNull() && !rows[1].GetValueByIndex(1).IsNull() {
		t.Fatal("LEFT join did not produce a NULL right-side value for the unmatched row")
	}
}

func TestSortMergeJoinConsumesBatchChildren(t *testing.T) {
	left := &batchAwareOperator{records: newJoinTestRows(nil, 1, 2)}
	right := &batchAwareOperator{records: newJoinTestRows(nil, 2, 3)}
	join := NewSortMergeJoinOperator(left, right, "INNER", func(row Record) (string, bool) {
		return row.GetValueByIndex(0).ToString(), row.GetValueByIndex(0).IsNull()
	}, func(row Record) (string, bool) {
		return row.GetValueByIndex(0).ToString(), row.GetValueByIndex(0).IsNull()
	})
	if err := join.Open(context.Background()); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer join.Close()
	if left.batchCalls == 0 || right.batchCalls == 0 {
		t.Fatalf("sort-merge join child batch calls = left %d, right %d; want both positive", left.batchCalls, right.batchCalls)
	}
	rows, err := join.NextBatch(context.Background(), 1)
	if err != nil {
		t.Fatalf("NextBatch() error = %v", err)
	}
	if len(rows) != 1 || rows[0].GetValueByIndex(0).Int() != 2 {
		t.Fatalf("NextBatch() rows = %v, want one joined row [2, 2]", rows)
	}
}
