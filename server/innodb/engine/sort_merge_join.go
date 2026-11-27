package engine

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

type sortMergeJoinKey func(Record) (string, bool)

type sortMergeJoinRow struct {
	record Record
	key    string
	null   bool
}

// SortMergeJoinOperator materializes and sorts both inputs, then emits equal
// key groups in merge order. It is used only for plans with an equi-join key;
// callers keep the nested-loop fallback for non-equality predicates.
type SortMergeJoinOperator struct {
	BaseOperator
	left      Operator
	right     Operator
	joinType  string
	leftKey   sortMergeJoinKey
	rightKey  sortMergeJoinKey
	rows      []Record
	index     int
	leftCols  int
	rightCols int
}

func NewSortMergeJoinOperator(left, right Operator, joinType string, leftKey, rightKey sortMergeJoinKey) *SortMergeJoinOperator {
	return &SortMergeJoinOperator{
		BaseOperator: BaseOperator{children: []Operator{left, right}},
		left:         left,
		right:        right,
		joinType:     joinType,
		leftKey:      leftKey,
		rightKey:     rightKey,
	}
}

func (s *SortMergeJoinOperator) Open(ctx context.Context) error {
	if err := s.BaseOperator.Open(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.left.Schema() != nil && s.right.Schema() != nil {
		s.schema = metadata.MergeSchemas(s.left.Schema(), s.right.Schema())
	} else if s.left.Schema() != nil {
		s.schema = s.left.Schema().Clone()
	} else if s.right.Schema() != nil {
		s.schema = s.right.Schema().Clone()
	} else {
		s.schema = metadata.NewQuerySchema()
	}
	if s.left.Schema() != nil {
		s.leftCols = s.left.Schema().ColumnCount()
	}
	if s.right.Schema() != nil {
		s.rightCols = s.right.Schema().ColumnCount()
	}

	leftRows, err := collectSortMergeRows(ctx, s.left, s.leftKey)
	if err != nil {
		return err
	}
	rightRows, err := collectSortMergeRows(ctx, s.right, s.rightKey)
	if err != nil {
		return err
	}
	if s.leftCols == 0 && len(leftRows) > 0 {
		s.leftCols = leftRows[0].record.GetColumnCount()
	}
	if s.rightCols == 0 && len(rightRows) > 0 {
		s.rightCols = rightRows[0].record.GetColumnCount()
	}
	s.rows = mergeSortedJoinRows(leftRows, rightRows, s.joinType, s.schema, s.leftCols, s.rightCols)
	s.index = 0
	return nil
}

func (s *SortMergeJoinOperator) Next(ctx context.Context) (Record, error) {
	if !s.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.index >= len(s.rows) {
		return nil, nil
	}
	row := s.rows[s.index]
	s.index++
	return row, nil
}

func (s *SortMergeJoinOperator) NextBatch(ctx context.Context, maxRows int) ([]Record, error) {
	if !s.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	rows := make([]Record, 0, maxRows)
	for len(rows) < maxRows {
		record, err := s.Next(ctx)
		if err != nil {
			return rows, err
		}
		if record == nil {
			return rows, io.EOF
		}
		rows = append(rows, record)
	}
	return rows, nil
}

func collectSortMergeRows(ctx context.Context, operator Operator, keyFn sortMergeJoinKey) ([]sortMergeJoinRow, error) {
	rows := make([]sortMergeJoinRow, 0)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := nextOperatorBatch(ctx, operator, 256)
		if err != nil && err != io.EOF {
			return nil, err
		}
		for _, record := range batch {
			key, isNull := keyFn(record)
			rows = append(rows, sortMergeJoinRow{record: record, key: key, null: isNull})
		}
		if err == io.EOF {
			break
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].null != rows[j].null {
			return rows[j].null
		}
		return rows[i].key < rows[j].key
	})
	return rows, nil
}

func mergeSortedJoinRows(left, right []sortMergeJoinRow, joinType string, schema *metadata.QuerySchema, leftCount, rightCount int) []Record {
	result := make([]Record, 0)
	leftOuter := strings.EqualFold(joinType, "LEFT") || strings.EqualFold(joinType, "LEFT OUTER") || strings.EqualFold(joinType, "FULL") || strings.EqualFold(joinType, "FULL OUTER")
	rightOuter := strings.EqualFold(joinType, "RIGHT") || strings.EqualFold(joinType, "RIGHT OUTER") || strings.EqualFold(joinType, "FULL") || strings.EqualFold(joinType, "FULL OUTER")

	for i, j := 0, 0; i < len(left) || j < len(right); {
		if i >= len(left) {
			if rightOuter {
				result = append(result, mergeSortMergeNullLeft(right[j].record, leftCount, schema))
			}
			j++
			continue
		}
		if j >= len(right) {
			if leftOuter {
				result = append(result, mergeSortMergeNullRight(left[i].record, rightCount, schema))
			}
			i++
			continue
		}
		if left[i].null {
			if leftOuter {
				result = append(result, mergeSortMergeNullRight(left[i].record, rightCount, schema))
			}
			i++
			continue
		}
		if right[j].null {
			if rightOuter {
				result = append(result, mergeSortMergeNullLeft(right[j].record, leftCount, schema))
			}
			j++
			continue
		}
		if left[i].key < right[j].key {
			if leftOuter {
				result = append(result, mergeSortMergeNullRight(left[i].record, rightCount, schema))
			}
			i++
			continue
		}
		if left[i].key > right[j].key {
			if rightOuter {
				result = append(result, mergeSortMergeNullLeft(right[j].record, leftCount, schema))
			}
			j++
			continue
		}

		leftStart, rightStart := i, j
		for i < len(left) && !left[i].null && left[i].key == left[leftStart].key {
			i++
		}
		for j < len(right) && !right[j].null && right[j].key == right[rightStart].key {
			j++
		}
		for leftIndex := leftStart; leftIndex < i; leftIndex++ {
			for rightIndex := rightStart; rightIndex < j; rightIndex++ {
				result = append(result, mergeSortMergeRecords(left[leftIndex].record, right[rightIndex].record, schema))
			}
		}
	}
	return result
}

func mergeSortMergeRecords(left, right Record, schema *metadata.QuerySchema) Record {
	values := append(append([]basic.Value(nil), left.GetValues()...), right.GetValues()...)
	return NewExecutorRecordFromValues(values, schema)
}

func mergeSortMergeNullRight(left Record, rightCount int, schema *metadata.QuerySchema) Record {
	values := append([]basic.Value(nil), left.GetValues()...)
	for i := 0; i < rightCount; i++ {
		values = append(values, basic.NewNull())
	}
	return NewExecutorRecordFromValues(values, schema)
}

func mergeSortMergeNullLeft(right Record, leftCount int, schema *metadata.QuerySchema) Record {
	values := make([]basic.Value, leftCount, leftCount+right.GetColumnCount())
	for i := 0; i < leftCount; i++ {
		values[i] = basic.NewNull()
	}
	values = append(values, right.GetValues()...)
	return NewExecutorRecordFromValues(values, schema)
}

func buildSortMergeKeyFunctions(conditions []plan.Expression) (sortMergeJoinKey, sortMergeJoinKey, bool) {
	// The operator currently carries one pair of key functions and does not
	// evaluate residual predicates. Only select the merge path when the whole
	// condition set is represented by that one equi-key.
	if len(conditions) != 1 {
		return nil, nil, false
	}
	for _, condition := range conditions {
		binary, ok := condition.(*plan.BinaryOperation)
		if !ok || binary.Op != plan.OpEQ {
			continue
		}
		left, leftOK := binary.Left.(*plan.Column)
		right, rightOK := binary.Right.(*plan.Column)
		if !leftOK || !rightOK {
			continue
		}
		return keyForJoinColumn(left.Name), keyForJoinColumn(right.Name), true
	}
	return nil, nil, false
}

func keyForJoinColumn(name string) sortMergeJoinKey {
	bare := name
	if dot := strings.LastIndex(bare, "."); dot >= 0 {
		bare = bare[dot+1:]
	}
	bare = strings.Trim(bare, "` ")
	return func(record Record) (string, bool) {
		value, err := record.GetValueByName(name)
		if err != nil {
			value, err = record.GetValueByName(bare)
		}
		if err != nil || value == nil {
			if values := record.GetValues(); len(values) > 0 {
				value = values[0]
			} else {
				return "", true
			}
		}
		if value.IsNull() {
			return "", true
		}
		return value.ToString(), false
	}
}
