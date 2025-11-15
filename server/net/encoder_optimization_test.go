package net

import (
	"testing"
)

// BenchmarkEncoderCreation_WithoutReuse 基准测试：每次创建新编码器（优化前）
func BenchmarkEncoderCreation_WithoutReuse(b *testing.B) {
	data := &ResultSetData{
		Columns: []string{"id", "name", "age"},
		Rows: [][]interface{}{
			{int64(1), "Alice", int64(25)},
			{int64(2), "Bob", int64(30)},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 每次都创建新的编码器（优化前的方式）
		encoder := NewMySQLProtocolEncoder()
		_ = encoder.SendResultSetPackets(data)
	}
}

// BenchmarkEncoderCreation_WithReuse 基准测试：复用编码器实例（优化后）
func BenchmarkEncoderCreation_WithReuse(b *testing.B) {
	data := &ResultSetData{
		Columns: []string{"id", "name", "age"},
		Rows: [][]interface{}{
			{int64(1), "Alice", int64(25)},
			{int64(2), "Bob", int64(30)},
		},
	}

	// 复用同一个编码器实例（优化后的方式）
	encoder := NewMySQLProtocolEncoder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = encoder.SendResultSetPackets(data)
	}
}

// BenchmarkEncoderCreation_SingleColumn 基准测试：单列结果（如 SELECT @@session.tx_read_only）
func BenchmarkEncoderCreation_SingleColumn(b *testing.B) {
	data := &ResultSetData{
		Columns: []string{"tx_read_only"},
		Rows:    [][]interface{}{{int64(0)}},
	}

	encoder := NewMySQLProtocolEncoder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = encoder.SendResultSetPackets(data)
	}
}

// BenchmarkEncoderCreation_LargeResultSet 基准测试：大结果集
func BenchmarkEncoderCreation_LargeResultSet(b *testing.B) {
	// 生成 100 行数据
	rows := make([][]interface{}, 100)
	for i := 0; i < 100; i++ {
		rows[i] = []interface{}{int64(i), "User" + string(rune(i)), int64(20 + i)}
	}

	data := &ResultSetData{
		Columns: []string{"id", "name", "age"},
		Rows:    rows,
	}

	encoder := NewMySQLProtocolEncoder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = encoder.SendResultSetPackets(data)
	}
}

// TestEncoderReuseSafety 测试编码器复用的线程安全性
func TestEncoderReuseSafety(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	data1 := &ResultSetData{
		Columns: []string{"col1"},
		Rows:    [][]interface{}{{int64(1)}},
	}

	data2 := &ResultSetData{
		Columns: []string{"col2"},
		Rows:    [][]interface{}{{int64(2)}},
	}

	// 连续使用同一个编码器编码不同的数据
	packets1 := encoder.SendResultSetPackets(data1)
	packets2 := encoder.SendResultSetPackets(data2)

	// 验证两次编码的结果不会互相影响
	if len(packets1) != 5 {
		t.Errorf("packets1 length = %d, want 5", len(packets1))
	}

	if len(packets2) != 5 {
		t.Errorf("packets2 length = %d, want 5", len(packets2))
	}

	// 验证第一个结果集包含 "col1"
	found1 := false
	for _, pkt := range packets1 {
		if containsBytes(pkt, []byte("col1")) {
			found1 = true
			break
		}
	}
	if !found1 {
		t.Error("packets1 should contain 'col1'")
	}

	// 验证第二个结果集包含 "col2"
	found2 := false
	for _, pkt := range packets2 {
		if containsBytes(pkt, []byte("col2")) {
			found2 = true
			break
		}
	}
	if !found2 {
		t.Error("packets2 should contain 'col2'")
	}
}

// containsBytes 检查字节数组是否包含子数组
func containsBytes(data, subdata []byte) bool {
	if len(subdata) == 0 {
		return true
	}
	if len(data) < len(subdata) {
		return false
	}

	for i := 0; i <= len(data)-len(subdata); i++ {
		match := true
		for j := 0; j < len(subdata); j++ {
			if data[i+j] != subdata[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// TestEncoderMemoryAllocation 测试编码器的内存分配
func TestEncoderMemoryAllocation(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	data := &ResultSetData{
		Columns: []string{"id", "name"},
		Rows: [][]interface{}{
			{int64(1), "test"},
		},
	}

	// 多次调用，确保没有内存泄漏
	// 包数量：1(column count) + 2(column defs) + 1(EOF) + 1(row) + 1(EOF) = 6
	for i := 0; i < 1000; i++ {
		packets := encoder.SendResultSetPackets(data)
		if len(packets) != 6 {
			t.Errorf("iteration %d: packet count = %d, want 6", i, len(packets))
		}
	}
}
