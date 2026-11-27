package net

import (
	"bytes"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

func newCompressionTestSession() Session {
	return newSession(nil, newMySQLTCPConn(nil))
}

// makeRawMySQLPacket 构造一个原始 MySQL 协议包: 3 字节长度 + 1 字节序号 + body
func makeRawMySQLPacket(seq byte, body []byte) []byte {
	length := len(body)
	header := []byte{
		byte(length),
		byte(length >> 8),
		byte(length >> 16),
		seq,
	}
	return append(header, body...)
}

func TestMySQLPkgHandler_ReadSinglePacket(t *testing.T) {
	handler := &MySQLPkgHandler{}
	body := []byte{0x01, 0x02, 0x03}
	raw := makeRawMySQLPacket(0, body)

	var sess Session // 不在 handler 中使用，保持为 nil 即可

	pkgInterface, n, err := handler.Read(sess, raw)
	if err != nil {
		t.Fatalf("Read 返回错误: %v", err)
	}
	if pkgInterface == nil {
		t.Fatalf("期望解析到一个包，结果为 nil")
	}
	if n != len(raw) {
		t.Fatalf("期望消费 %d 字节，实际消费 %d 字节", len(raw), n)
	}

	pkg, ok := pkgInterface.(*MySQLPackage)
	if !ok {
		t.Fatalf("期望类型 *MySQLPackage，实际为 %T", pkgInterface)
	}

	if len(pkg.Header.PacketLength) != 3 {
		t.Fatalf("期望 PacketLength 长度为 3，实际为 %d", len(pkg.Header.PacketLength))
	}
	length := int(pkg.Header.PacketLength[0]) |
		int(pkg.Header.PacketLength[1])<<8 |
		int(pkg.Header.PacketLength[2])<<16
	if length != len(body) {
		t.Fatalf("期望 payload 长度=%d，实际为 %d", len(body), length)
	}
	if pkg.Header.PacketId != 0 {
		t.Fatalf("期望 PacketId=0，实际为 %d", pkg.Header.PacketId)
	}
	if !bytes.Equal(pkg.Body, body) {
		t.Fatalf("期望 Body=%v，实际为 %v", body, pkg.Body)
	}
}

func TestMySQLPkgHandler_ReadStickyPackets(t *testing.T) {
	handler := &MySQLPkgHandler{}
	body1 := []byte{0x01}
	body2 := []byte{0x02, 0x03}
	p1 := makeRawMySQLPacket(0, body1)
	p2 := makeRawMySQLPacket(1, body2)
	sticky := append(p1, p2...)

	var sess Session

	// 第一次读取，应只返回第一个包
	pkg1Int, n1, err := handler.Read(sess, sticky)
	if err != nil {
		t.Fatalf("第一次 Read 返回错误: %v", err)
	}
	if pkg1Int == nil {
		t.Fatalf("期望解析到第一个包，结果为 nil")
	}
	if n1 != len(p1) {
		t.Fatalf("期望第一次消费 %d 字节，实际消费 %d 字节", len(p1), n1)
	}

	pkg1 := pkg1Int.(*MySQLPackage)
	if !bytes.Equal(pkg1.Body, body1) {
		t.Fatalf("期望第一个包的 Body=%v，实际为 %v", body1, pkg1.Body)
	}

	// 第二次读取，传入剩余数据，应返回第二个包
	remaining := sticky[n1:]
	pkg2Int, n2, err := handler.Read(sess, remaining)
	if err != nil {
		t.Fatalf("第二次 Read 返回错误: %v", err)
	}
	if pkg2Int == nil {
		t.Fatalf("期望解析到第二个包，结果为 nil")
	}
	if n2 != len(p2) {
		t.Fatalf("期望第二次消费 %d 字节，实际消费 %d 字节", len(p2), n2)
	}
	pkg2 := pkg2Int.(*MySQLPackage)
	if !bytes.Equal(pkg2.Body, body2) {
		t.Fatalf("期望第二个包的 Body=%v，实际为 %v", body2, pkg2.Body)
	}
}

func TestMySQLPkgHandler_ReadHalfPacket(t *testing.T) {
	handler := &MySQLPkgHandler{}
	body := []byte{0x01, 0x02, 0x03}
	full := makeRawMySQLPacket(0, body)

	var sess Session

	// 情况 1：不足 4 字节头部
	data1 := full[:3]
	pkg, n, err := handler.Read(sess, data1)
	if err != nil {
		t.Fatalf("不足头部长度时不应返回错误，实际为: %v", err)
	}
	if pkg != nil || n != 0 {
		t.Fatalf("不足头部长度时应返回 (nil,0,nil)，实际为 (%v,%d,%v)", pkg, n, err)
	}

	// 情况 2：头部完整但 payload 不完整
	data2 := full[:len(full)-1]
	pkg, n, err = handler.Read(sess, data2)
	if err != nil {
		t.Fatalf("半包时不应返回错误，实际为: %v", err)
	}
	if pkg != nil || n != 0 {
		t.Fatalf("半包时应返回 (nil,0,nil)，实际为 (%v,%d,%v)", pkg, n, err)
	}
}

func TestMySQLPkgHandler_WriteMySQLPackage(t *testing.T) {
	handler := &MySQLPkgHandler{}
	body := []byte{0x01, 0x02, 0x03}
	seq := byte(7)

	length := len(body)
	headerLen := []byte{byte(length), byte(length >> 8), byte(length >> 16)}
	pkg := &MySQLPackage{
		Header: MySQLPkgHeader{
			PacketLength: headerLen,
			PacketId:     seq,
		},
		Body: body,
	}

	var sess Session
	got, err := handler.Write(sess, pkg)
	if err != nil {
		t.Fatalf("Write 返回错误: %v", err)
	}

	want := makeRawMySQLPacket(seq, body)
	if !bytes.Equal(got, want) {
		t.Fatalf("编码结果不一致\n期望: %v\n实际:  %v", want, got)
	}
}

func TestMySQLPkgHandler_WriteRawBytes(t *testing.T) {
	handler := &MySQLPkgHandler{}
	raw := []byte{0x01, 0x02, 0x03}

	var sess Session
	got, err := handler.Write(sess, raw)
	if err != nil {
		t.Fatalf("Write 返回错误: %v", err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("期望透传原始字节，结果为 %v", got)
	}
}

func TestMySQLPkgHandler_CompressesNegotiatedWriteAndReads(t *testing.T) {
	handler := &MySQLPkgHandler{}
	session := newCompressionTestSession()
	session.SetAttribute(mysqlCompressionEnabledKey, true)
	session.SetAttribute(mysqlCompressionWriteSeqKey, uint8(0))
	raw := makeRawMySQLPacket(1, bytes.Repeat([]byte("select compressed"), 8))

	encoded, err := compressMySQLTransportPayload(session, raw)
	if err != nil {
		t.Fatalf("compressed write failed: %v", err)
	}
	decoded, sequence, err := protocol.NewCompressionHandler(true).DecompressPacket(encoded)
	if err != nil {
		t.Fatalf("compressed response could not be decoded: %v", err)
	}
	if sequence != 0 || !bytes.Equal(decoded, raw) {
		t.Fatalf("unexpected compressed response: sequence=%d decoded=%v", sequence, decoded)
	}

	packet, consumed, err := handler.Read(session, encoded)
	if err != nil {
		t.Fatalf("compressed read failed: %v", err)
	}
	if consumed != len(encoded) {
		t.Fatalf("expected to consume compressed frame %d, got %d", len(encoded), consumed)
	}
	decodedPacket, ok := packet.(*MySQLPackage)
	if !ok || !bytes.Equal(decodedPacket.Body, raw[4:]) || decodedPacket.Header.PacketId != 1 {
		t.Fatalf("unexpected decoded MySQL packet: %#v", packet)
	}
}

func TestMySQLPkgHandler_DrainsMultiplePacketsFromCompressedFrame(t *testing.T) {
	handler := &MySQLPkgHandler{}
	session := newCompressionTestSession()
	session.SetAttribute(mysqlCompressionEnabledKey, true)
	first := makeRawMySQLPacket(0, []byte{0x03, 'a'})
	second := makeRawMySQLPacket(1, []byte{0x03, 'b'})
	frame, err := protocol.NewCompressionHandler(true).CompressPacket(append(first, second...), 0)
	if err != nil {
		t.Fatalf("could not build compressed frame: %v", err)
	}

	packet, _, err := handler.Read(session, frame)
	if err != nil {
		t.Fatalf("compressed multi-packet read failed: %v", err)
	}
	if !bytes.Equal(packet.(*MySQLPackage).Body, first[4:]) {
		t.Fatalf("unexpected first packet: %#v", packet)
	}
	pending, hasPending, err := handler.ReadPending(session)
	if err != nil || !hasPending {
		t.Fatalf("expected second pending packet, hasPending=%v err=%v", hasPending, err)
	}
	if !bytes.Equal(pending.(*MySQLPackage).Body, second[4:]) {
		t.Fatalf("unexpected second packet: %#v", pending)
	}
	if _, hasPending, _ := handler.ReadPending(session); hasPending {
		t.Fatal("pending compressed packet queue was not drained")
	}
}
