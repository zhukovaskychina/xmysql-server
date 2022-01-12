package net

import (
	"bytes"
	"encoding/binary"
	"github.com/zhukovaskychina/xmysql-server/util"
)

type MySQLPkgHeader struct {
	PacketLength []byte //3
	PacketId     byte
}

type MySQLPackage struct {
	Header MySQLPkgHeader
	Body   []byte
}

func (p MySQLPackage) Marshal() (*bytes.Buffer, error) {
	var (
		err error
		buf *bytes.Buffer
	)

	buf = &bytes.Buffer{}
	err = binary.Write(buf, binary.LittleEndian, p.Header)
	if err != nil {
		return nil, err
	}
	buf.Write(p.Body)

	return buf, nil
}

func (p *MySQLPackage) Unmarshal(buf *bytes.Buffer) (int, error) {

	if buf.Len() < 4 {
		return 0, ErrNotEnoughStream
	}
	var cursor = 0
	cursor, packetLength := util.ReadUB3(buf.Bytes(), cursor)
	p.Header.PacketLength = buf.Bytes()[0:3]
	p.Header.PacketId = buf.Bytes()[3]
	p.Body = buf.Bytes()[4 : packetLength+4]
	return int(packetLength + 4), nil
}
