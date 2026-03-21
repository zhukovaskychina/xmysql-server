package protocol

import (
	"bytes"
	"testing"
)

// TestEncodeColumnDefinitionPacket 测试列定义包编码
func TestEncodeColumnDefinitionPacket(t *testing.T) {
	tests := []struct {
		name     string
		def      *ColumnDefinition
		seq      byte
		wantLen  int // 预期包长度（包括 4 字节头）
		validate func(t *testing.T, packet []byte)
	}{
		{
			name: "Simple VARCHAR column",
			def: &ColumnDefinition{
				Catalog:      "def",
				Schema:       "",
				Table:        "",
				OrgTable:     "",
				Name:         "@@version",
				OrgName:      "@@version",
				CharacterSet: 0x21, // utf8mb4
				ColumnLength: 255,
				ColumnType:   MYSQL_TYPE_VAR_STRING,
				Flags:        0,
				Decimals:     0,
			},
			seq:     1,
			wantLen: 4 + 3 + 1 + 1 + 1 + 10 + 10 + 1 + 2 + 4 + 1 + 2 + 1 + 2, // 至少 43 字节
			validate: func(t *testing.T, packet []byte) {
				if len(packet) < 43 {
					t.Errorf("Packet too short: got %d bytes, want at least 43", len(packet))
				}

				// 检查包头
				length := int(packet[0]) | int(packet[1])<<8 | int(packet[2])<<16
				seq := packet[3]
				if seq != 1 {
					t.Errorf("Wrong sequence ID: got %d, want 1", seq)
				}
				if length != len(packet)-4 {
					t.Errorf("Wrong length in header: got %d, want %d", length, len(packet)-4)
				}

				// 检查 catalog = "def"
				if packet[4] != 3 || string(packet[5:8]) != "def" {
					t.Errorf("Wrong catalog: expected 'def'")
				}

				// 检查固定长度字段标记 (0x0C)
				// 需要跳过所有 lenenc-str 字段找到它
				// 简化：只检查包中是否包含 0x0C
				found := false
				for i := 4; i < len(packet)-12; i++ {
					if packet[i] == 0x0C {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Missing 0x0C marker for fixed-length fields")
				}
			},
		},
		{
			name: "INT column with flags",
			def: &ColumnDefinition{
				Catalog:      "def",
				Schema:       "test",
				Table:        "users",
				OrgTable:     "users",
				Name:         "id",
				OrgName:      "id",
				CharacterSet: 0x3F, // binary
				ColumnLength: 11,
				ColumnType:   MYSQL_TYPE_LONG,
				Flags:        FLAG_NOT_NULL | FLAG_PRI_KEY | FLAG_AUTO_INCREMENT,
				Decimals:     0,
			},
			seq:     2,
			wantLen: 50, // 大约
			validate: func(t *testing.T, packet []byte) {
				if len(packet) < 40 {
					t.Errorf("Packet too short: got %d bytes", len(packet))
				}

				// 检查序列号
				if packet[3] != 2 {
					t.Errorf("Wrong sequence ID: got %d, want 2", packet[3])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packet := EncodeColumnDefinitionPacket(tt.def, tt.seq)

			t.Logf("Packet length: %d bytes", len(packet))
			t.Logf("Packet hex: %x", packet)

			if tt.validate != nil {
				tt.validate(t, packet)
			}
		})
	}
}

// TestEncodeRowDataPacket 测试行数据包编码
func TestEncodeRowDataPacket(t *testing.T) {
	tests := []struct {
		name     string
		row      []interface{}
		seq      byte
		validate func(t *testing.T, packet []byte)
	}{
		{
			name: "Single string value",
			row:  []interface{}{"8.0.32"},
			seq:  3,
			validate: func(t *testing.T, packet []byte) {
				// 包头 (4 bytes) + lenenc(6) + "8.0.32" (6 bytes)
				// lenenc(6) = 1 byte (0x06)
				// 总共: 4 + 1 + 6 = 11 bytes
				if len(packet) != 11 {
					t.Errorf("Wrong packet length: got %d, want 11", len(packet))
				}

				// 检查序列号
				if packet[3] != 3 {
					t.Errorf("Wrong sequence ID: got %d, want 3", packet[3])
				}

				// 检查字符串长度编码
				if packet[4] != 6 {
					t.Errorf("Wrong string length: got %d, want 6", packet[4])
				}

				// 检查字符串内容
				if string(packet[5:11]) != "8.0.32" {
					t.Errorf("Wrong string value: got %s, want '8.0.32'", string(packet[5:11]))
				}
			},
		},
		{
			name: "Multiple values with NULL",
			row:  []interface{}{"test", 123, nil, true},
			seq:  4,
			validate: func(t *testing.T, packet []byte) {
				if len(packet) < 10 {
					t.Errorf("Packet too short: got %d bytes", len(packet))
				}

				// 检查序列号
				if packet[3] != 4 {
					t.Errorf("Wrong sequence ID: got %d, want 4", packet[3])
				}

				// 检查是否包含 NULL 标记 (0xFB)
				found := false
				for i := 4; i < len(packet); i++ {
					if packet[i] == 0xFB {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Missing NULL marker (0xFB)")
				}
			},
		},
		{
			name: "Integer values",
			row:  []interface{}{int64(3309), int32(100), int16(50), int8(10)},
			seq:  5,
			validate: func(t *testing.T, packet []byte) {
				if len(packet) < 15 {
					t.Errorf("Packet too short: got %d bytes", len(packet))
				}

				// 检查序列号
				if packet[3] != 5 {
					t.Errorf("Wrong sequence ID: got %d, want 5", packet[3])
				}
			},
		},
		{
			name: "Float values",
			row:  []interface{}{float64(3.14), float32(2.71)},
			seq:  6,
			validate: func(t *testing.T, packet []byte) {
				if len(packet) < 10 {
					t.Errorf("Packet too short: got %d bytes", len(packet))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packet := EncodeRowDataPacket(tt.row, tt.seq)

			t.Logf("Packet length: %d bytes", len(packet))
			t.Logf("Packet hex: %x", packet)

			if tt.validate != nil {
				tt.validate(t, packet)
			}
		})
	}
}

// TestEncodeEOFPacket 测试 EOF 包编码
func TestEncodeEOFPacket(t *testing.T) {
	packet := EncodeEOFPacketWithSeq(0, SERVER_STATUS_AUTOCOMMIT, 7)

	// EOF 包固定 9 字节: 4 (header) + 5 (payload)
	if len(packet) != 9 {
		t.Errorf("Wrong EOF packet length: got %d, want 9", len(packet))
	}

	// 检查包长度
	length := int(packet[0]) | int(packet[1])<<8 | int(packet[2])<<16
	if length != 5 {
		t.Errorf("Wrong length in header: got %d, want 5", length)
	}

	// 检查序列号
	if packet[3] != 7 {
		t.Errorf("Wrong sequence ID: got %d, want 7", packet[3])
	}

	// 检查 EOF 标记
	if packet[4] != 0xFE {
		t.Errorf("Wrong EOF marker: got 0x%02X, want 0xFE", packet[4])
	}

	// 检查状态标志
	statusFlags := uint16(packet[7]) | uint16(packet[8])<<8
	if statusFlags != SERVER_STATUS_AUTOCOMMIT {
		t.Errorf("Wrong status flags: got 0x%04X, want 0x%04X", statusFlags, SERVER_STATUS_AUTOCOMMIT)
	}

	t.Logf("EOF packet: %x", packet)
}

// TestEncodeOKPacket 测试 OK 包编码
func TestEncodeOKPacket(t *testing.T) {
	packet := EncodeOKPacketWithSeq(1, 0, SERVER_STATUS_AUTOCOMMIT, 0, 1)

	// OK 包至少 11 字节: 4 (header) + 1 (0x00) + 1 (affected) + 1 (insert_id) + 2 (status) + 2 (warnings)
	if len(packet) < 11 {
		t.Errorf("OK packet too short: got %d bytes", len(packet))
	}

	// 检查序列号
	if packet[3] != 1 {
		t.Errorf("Wrong sequence ID: got %d, want 1", packet[3])
	}

	// 检查 OK 标记
	if packet[4] != 0x00 {
		t.Errorf("Wrong OK marker: got 0x%02X, want 0x00", packet[4])
	}

	t.Logf("OK packet: %x", packet)
}

// TestEncodeErrorPacket 测试 Error 包编码
func TestEncodeErrorPacket(t *testing.T) {
	packet := EncodeErrorPacketWithSeq(1064, "42000", "Syntax error", 1)

	// Error 包至少 17 字节: 4 (header) + 1 (0xFF) + 2 (code) + 1 ('#') + 5 (state) + message
	if len(packet) < 17 {
		t.Errorf("Error packet too short: got %d bytes", len(packet))
	}

	// 检查 Error 标记
	if packet[4] != 0xFF {
		t.Errorf("Wrong Error marker: got 0x%02X, want 0xFF", packet[4])
	}

	// 检查错误代码
	errorCode := uint16(packet[5]) | uint16(packet[6])<<8
	if errorCode != 1064 {
		t.Errorf("Wrong error code: got %d, want 1064", errorCode)
	}

	// 检查 SQL state marker
	if packet[7] != '#' {
		t.Errorf("Wrong SQL state marker: got %c, want '#'", packet[7])
	}

	// 检查 SQL state
	sqlState := string(packet[8:13])
	if sqlState != "42000" {
		t.Errorf("Wrong SQL state: got %s, want '42000'", sqlState)
	}

	t.Logf("Error packet: %x", packet)
}

// TestLenEncInt 测试长度编码整数
func TestLenEncInt(t *testing.T) {
	tests := []struct {
		value    uint64
		expected []byte
	}{
		{0, []byte{0x00}},
		{250, []byte{0xFA}},
		{251, []byte{0xFC, 0xFB, 0x00}},
		{65535, []byte{0xFC, 0xFF, 0xFF}},
		{65536, []byte{0xFD, 0x00, 0x00, 0x01}},
		{16777215, []byte{0xFD, 0xFF, 0xFF, 0xFF}},
		{16777216, []byte{0xFE, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := appendLenEncInt(nil, tt.value)
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("appendLenEncInt(%d) = %x, want %x", tt.value, result, tt.expected)
			}
		})
	}
}

// TestLenEncString 测试长度编码字符串
func TestLenEncString(t *testing.T) {
	tests := []struct {
		str      string
		expected []byte
	}{
		{"", []byte{0x00}},
		{"a", []byte{0x01, 'a'}},
		{"test", []byte{0x04, 't', 'e', 's', 't'}},
		{"@@version", []byte{0x09, '@', '@', 'v', 'e', 'r', 's', 'i', 'o', 'n'}},
	}

	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			result := appendLenEncString(nil, tt.str)
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("appendLenEncString(%q) = %x, want %x", tt.str, result, tt.expected)
			}
		})
	}
}

// BenchmarkEncodeColumnDefinitionPacket 性能测试
func BenchmarkEncodeColumnDefinitionPacket(b *testing.B) {
	def := &ColumnDefinition{
		Catalog:      "def",
		Schema:       "test",
		Table:        "users",
		OrgTable:     "users",
		Name:         "id",
		OrgName:      "id",
		CharacterSet: 0x21,
		ColumnLength: 11,
		ColumnType:   MYSQL_TYPE_LONG,
		Flags:        FLAG_NOT_NULL | FLAG_PRI_KEY,
		Decimals:     0,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeColumnDefinitionPacket(def, 1)
	}
}

// BenchmarkEncodeRowDataPacket 性能测试
func BenchmarkEncodeRowDataPacket(b *testing.B) {
	row := []interface{}{"test", 123, "value", true, nil}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeRowDataPacket(row, 1)
	}
}
