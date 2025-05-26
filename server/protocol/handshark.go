package protocol

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/util"
)

const (
	ServerVersion   = "5.7.32"
	ServerStatus    = 2
	CharSet         = 1
	ProtocolVersion = 10
)

type HandsharkProtocol struct {
	MySQLPacket
	ProtocolVersion          byte
	ServerVersion            string
	ServerThreadID           uint32
	Seed                     []byte
	ServerCapabilitiesLow    uint16
	CharSet                  byte
	ServerStatus             uint16
	ServerCapabilitiesHeight uint16
	RestOfScrambleBuff       []byte
	Auth_plugin_name         string
}

func CalHandShakePacketSize() int {
	size := 1
	size += len(ServerVersion)
	size += 5
	size += 20
	size += 19
	size += 12
	size += 1
	return size
}

func DecodeHandshake(buff []byte) HandsharkProtocol {
	var cursor int
	var tmp []byte
	hs := new(HandsharkProtocol)

	cursor, hs.ProtocolVersion = util.ReadByte(buff, cursor)
	cursor, tmp = util.ReadWithNull(buff, cursor)
	hs.ServerVersion = string(tmp)
	cursor, hs.ServerThreadID = util.ReadUB4(buff, cursor)
	cursor, hs.Seed = util.ReadWithNull(buff, cursor)
	cursor, hs.ServerCapabilitiesLow = util.ReadUB2(buff, cursor)
	cursor, hs.CharSet = util.ReadByte(buff, cursor)
	cursor, hs.ServerStatus = util.ReadUB2(buff, cursor)
	cursor, hs.ServerCapabilitiesHeight = util.ReadUB2(buff, cursor)
	cursor, _ = util.ReadBytes(buff, cursor, 11)
	cursor, hs.RestOfScrambleBuff = util.ReadWithNull(buff, cursor)
	cursor, tmp = util.ReadWithNull(buff, cursor)
	hs.Auth_plugin_name = string(tmp)

	fmt.Printf("DecodeHanshark: %+v\n", hs)

	return *hs
}

func EncodeHandshake(buff []byte) []byte {
	ServerCapablities := GetCapabilitiesWithoutParams()
	Filler13 := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	//rand1
	rand1 := util.RandomBytes(8)
	rand2 := util.RandomBytes(12)

	size := CalHandShakePacketSize()
	buff = util.WriteUB3(buff, uint32(size))
	buff = util.WriteByte(buff, 0)
	buff = util.WriteByte(buff, ProtocolVersion)
	buff = util.WriteWithNull(buff, ([]byte)(ServerVersion))
	buff = util.WriteUB4(buff, uint32(util.Goid()))
	buff = util.WriteWithNull(buff, append(rand1, rand2...))
	buff = util.WriteUB2(buff, uint16(ServerCapablities))
	buff = util.WriteByte(buff, CharSet)
	buff = util.WriteUB2(buff, ServerStatus)
	buff = util.WriteBytes(buff, Filler13)
	buff = util.WriteWithNull(buff, rand2)

	return buff
}
