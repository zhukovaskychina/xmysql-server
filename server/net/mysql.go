package net

import (
	"bytes"
	"errors"
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
	if len(p.Header.PacketLength) != 3 {
		return nil, errors.New("invalid packet header length")
	}

	_, err = buf.Write(p.Header.PacketLength)
	if err != nil {
		return nil, err
	}
	if err = buf.WriteByte(p.Header.PacketId); err != nil {
		return nil, err
	}
	if _, err = buf.Write(p.Body); err != nil {
		return nil, err
	}

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
