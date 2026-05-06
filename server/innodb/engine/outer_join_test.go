package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// 外连接测试用：左表 (id, name)，右表 (uid, score)，ON left.id = right.uid

func TestNestedLoopJoin_LeftOuterJoin(t *testing.T) {
	ctx := context.Background()

	leftSchema := createTestSchema([]testColumn{
		{Name: "id", Type: metadata.TypeInt},
		{Name: "name", Type: metadata.TypeVarchar},
	})
	leftData := [][]basic.Value{
		{basic.NewInt64(1), basic.NewString("A")},
		{basic.NewInt64(2), basic.NewString("B")},
		{basic.NewInt64(3), basic.NewString("C")},
	}
	rightSchema := createTestSchema([]testColumn{
		{Name: "uid", Type: metadata.TypeInt},
		{Name: "score", Type: metadata.TypeInt},
	})
	rightData := [][]basic.Value{
		{basic.NewInt64(1), basic.NewInt64(10)},
		{basic.NewInt64(2), basic.NewInt64(20)},
	}
	leftOp := NewMockDataOperator(leftData, leftSchema)
	rightOp := NewMockDataOperator(rightData, rightSchema)

	condition := func(l, r Record) bool {
		if l == nil || r == nil {
			return false
		}
		lv, rv := l.GetValues(), r.GetValues()
		if len(lv) < 1 || len(rv) < 1 {
			return false
		}
		return lv[0].Int() == rv[0].Int()
	}

	nlj := NewNestedLoopJoinOperator(leftOp, rightOp, "LEFT", condition)
	err := nlj.Open(ctx)
	assert.NoError(t, err)

	var results []Record
	for {
		rec, err := nlj.Next(ctx)
		assert.NoError(t, err)
		if rec == nil {
			break
		}
		results = append(results, rec)
	}

	// LEFT: 左表 3 行都要出现。左1 匹配右1、左2 匹配右2、左3 无匹配 -> 共 3 行
	assert.Len(t, results, 3)

	// 检查第三条应为 (3, C, NULL, NULL)
	vals3 := results[2].GetValues()
	assert.Len(t, vals3, 4)
	assert.Equal(t, int64(3), vals3[0].Int())
	assert.Equal(t, "C", vals3[1].String())
	assert.True(t, vals3[2].IsNull())
	assert.True(t, vals3[3].IsNull())

	_ = nlj.Close()
}

func TestNestedLoopJoin_RightOuterJoin(t *testing.T) {
	ctx := context.Background()

	leftSchema := createTestSchema([]testColumn{
		{Name: "id", Type: metadata.TypeInt},
		{Name: "name", Type: metadata.TypeVarchar},
	})
	leftData := [][]basic.Value{
		{basic.NewInt64(1), basic.NewString("A")},
		{basic.NewInt64(2), basic.NewString("B")},
	}
	rightSchema := createTestSchema([]testColumn{
		{Name: "uid", Type: metadata.TypeInt},
		{Name: "score", Type: metadata.TypeInt},
	})
	rightData := [][]basic.Value{
		{basic.NewInt64(1), basic.NewInt64(10)},
		{basic.NewInt64(2), basic.NewInt64(20)},
		{basic.NewInt64(3), basic.NewInt64(30)},
	}
	leftOp := NewMockDataOperator(leftData, leftSchema)
	rightOp := NewMockDataOperator(rightData, rightSchema)

	condition := func(l, r Record) bool {
		if l == nil || r == nil {
			return false
		}
		lv, rv := l.GetValues(), r.GetValues()
		return len(lv) >= 1 && len(rv) >= 1 && lv[0].Int() == rv[0].Int()
	}

	nlj := NewNestedLoopJoinOperator(leftOp, rightOp, "RIGHT", condition)
	err := nlj.Open(ctx)
	assert.NoError(t, err)

	var results []Record
	for {
		rec, err := nlj.Next(ctx)
		assert.NoError(t, err)
		if rec == nil {
			break
		}
		results = append(results, rec)
	}

	// RIGHT: 右表 3 行都要出现。uid=1,2 有匹配，uid=3 无匹配应有一行 (NULL, NULL, 3, 30)
	assert.Len(t, results, 3)
	vals3 := results[2].GetValues()
	assert.Len(t, vals3, 4)
	assert.True(t, vals3[0].IsNull())
	assert.True(t, vals3[1].IsNull())
	assert.Equal(t, int64(3), vals3[2].Int())
	assert.Equal(t, int64(30), vals3[3].Int())

	_ = nlj.Close()
}

func TestNestedLoopJoin_FullOuterJoin(t *testing.T) {
	ctx := context.Background()

	leftSchema := createTestSchema([]testColumn{
		{Name: "id", Type: metadata.TypeInt},
		{Name: "name", Type: metadata.TypeVarchar},
	})
	leftData := [][]basic.Value{
		{basic.NewInt64(1), basic.NewString("A")},
		{basic.NewInt64(2), basic.NewString("B")},
	}
	rightSchema := createTestSchema([]testColumn{
		{Name: "uid", Type: metadata.TypeInt},
		{Name: "score", Type: metadata.TypeInt},
	})
	rightData := [][]basic.Value{
		{basic.NewInt64(1), basic.NewInt64(10)},
		{basic.NewInt64(3), basic.NewInt64(30)},
	}
	leftOp := NewMockDataOperator(leftData, leftSchema)
	rightOp := NewMockDataOperator(rightData, rightSchema)

	condition := func(l, r Record) bool {
		if l == nil || r == nil {
			return false
		}
		lv, rv := l.GetValues(), r.GetValues()
		return len(lv) >= 1 && len(rv) >= 1 && lv[0].Int() == rv[0].Int()
	}

	nlj := NewNestedLoopJoinOperator(leftOp, rightOp, "FULL", condition)
	err := nlj.Open(ctx)
	assert.NoError(t, err)

	var results []Record
	for {
		rec, err := nlj.Next(ctx)
		assert.NoError(t, err)
		if rec == nil {
			break
		}
		results = append(results, rec)
	}

	// FULL: (1,A,1,10) 匹配；(2,B,NULL,NULL) 左无匹配；(NULL,NULL,3,30) 右无匹配 -> 共 3 行
	assert.Len(t, results, 3)
	// 最后一行应为右独有
	last := results[2].GetValues()
	assert.True(t, last[0].IsNull())
	assert.True(t, last[1].IsNull())
	assert.Equal(t, int64(3), last[2].Int())
	assert.Equal(t, int64(30), last[3].Int())

	_ = nlj.Close()
}

func TestHashJoin_LeftOuterJoin(t *testing.T) {
	ctx := context.Background()

	leftSchema := createTestSchema([]testColumn{
		{Name: "id", Type: metadata.TypeInt},
		{Name: "name", Type: metadata.TypeVarchar},
	})
	leftData := [][]basic.Value{
		{basic.NewInt64(1), basic.NewString("A")},
		{basic.NewInt64(2), basic.NewString("B")},
		{basic.NewInt64(3), basic.NewString("C")},
	}
	rightSchema := createTestSchema([]testColumn{
		{Name: "uid", Type: metadata.TypeInt},
		{Name: "score", Type: metadata.TypeInt},
	})
	rightData := [][]basic.Value{
		{basic.NewInt64(1), basic.NewInt64(10)},
		{basic.NewInt64(2), basic.NewInt64(20)},
	}
	leftOp := NewMockDataOperator(leftData, leftSchema)
	rightOp := NewMockDataOperator(rightData, rightSchema)

	buildKey := func(r Record) string {
		v := r.GetValues()
		if len(v) > 0 {
			return v[0].ToString()
		}
		return ""
	}
	probeKey := func(r Record) string {
		v := r.GetValues()
		if len(v) > 0 {
			return v[0].ToString()
		}
		return ""
	}
	// LEFT 时构造为 build=right, probe=left，由 buildHashJoin 内部处理；单测直接 NewHashJoinOperator(right, left, "LEFT", probeKey, buildKey)
	hashJoin := NewHashJoinOperator(rightOp, leftOp, "LEFT", probeKey, buildKey)
	err := hashJoin.Open(ctx)
	assert.NoError(t, err)

	var results []Record
	for {
		rec, err := hashJoin.Next(ctx)
		assert.NoError(t, err)
		if rec == nil {
			break
		}
		results = append(results, rec)
	}

	// 左表 3 行：1,2 有匹配，3 无匹配 -> 3 行
	assert.Len(t, results, 3)
	vals3 := results[2].GetValues()
	assert.Equal(t, int64(3), vals3[0].Int())
	assert.Equal(t, "C", vals3[1].String())
	assert.True(t, vals3[2].IsNull())
	assert.True(t, vals3[3].IsNull())

	_ = hashJoin.Close()
}
