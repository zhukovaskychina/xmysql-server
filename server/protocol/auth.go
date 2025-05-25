package protocol

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/util"
)

func GetCapabilities(hs HandsharkProtocol) uint32 {
	var capabilities uint32 = 0
	capabilities |= common.CLIENT_LONG_PASSWORD
	capabilities |= common.CLIENT_FOUND_ROWS
	capabilities |= common.CLIENT_LONG_FLAG
	capabilities |= common.CLIENT_CONNECT_WITH_DB
	capabilities |= common.CLIENT_ODBC
	capabilities |= common.CLIENT_IGNORE_SPACE
	capabilities |= common.CLIENT_PROTOCOL_41
	capabilities |= common.CLIENT_INTERACTIVE
	capabilities |= common.CLIENT_IGNORE_SIGPIPE
	capabilities |= common.CLIENT_TRANSACTIONS
	capabilities |= common.CLIENT_SECURE_CONNECTION

	return capabilities
}
func GetCapabilitiesWithoutParams() uint32 {
	var capabilities uint32 = 0
	capabilities |= common.CLIENT_LONG_PASSWORD
	capabilities |= common.CLIENT_FOUND_ROWS
	capabilities |= common.CLIENT_LONG_FLAG
	capabilities |= common.CLIENT_CONNECT_WITH_DB
	capabilities |= common.CLIENT_ODBC
	capabilities |= common.CLIENT_IGNORE_SPACE
	capabilities |= common.CLIENT_PROTOCOL_41
	capabilities |= common.CLIENT_INTERACTIVE
	capabilities |= common.CLIENT_IGNORE_SIGPIPE
	capabilities |= common.CLIENT_TRANSACTIONS
	capabilities |= common.CLIENT_SECURE_CONNECTION
	//capabilities |=common.CLIENT_SSL
	return capabilities
}

/**
 * 生成登录验证报文
 */
func EncodeLogin(hs HandsharkProtocol, uname string, password string, dbname string) []byte {
	buf := []byte{}

	capabilities := GetCapabilities(hs)
	capabilities |= common.CLIENT_CONNECT_WITH_DB

	buf = util.WriteUB4(buf, capabilities)
	buf = util.WriteUB4(buf, 1024*1024*16)
	buf = util.WriteByte(buf, hs.CharSet)
	for i := 0; i < 23; i++ {
		buf = append(buf, 0)
	}
	if len(uname) == 0 {
		buf = append(buf, 0)
	} else {
		buf = util.WriteWithNull(buf, []byte(uname))
	}

	encryPass := util.GetPassword([]byte(password), hs.Seed, hs.RestOfScrambleBuff)
	if (capabilities & common.CLIENT_SECURE_CONNECTION) > 0 {
		buf = util.WriteWithLength(buf, encryPass)
	} else {
		buf = util.WriteBytes(buf, encryPass)
		buf = util.WriteByte(buf, 0)
	}

	buf = util.WriteWithNull(buf, []byte(dbname))
	buf = util.WriteWithNull(buf, []byte(hs.Auth_plugin_name))

	return buf
}

type AuthPacket struct {
	clientFlag    int
	maxPacketSize int
	CharsetIndex  int
	extra         []byte
	FILTER        []byte
	User          string
	Password      []byte
	Database      string
}

func (ap *AuthPacket) DecodeAuth(buff []byte) *AuthPacket {

	//解析packetLength

	var cursor = 0
	var database = ""
	cursor, packetLength := util.ReadUB3(buff, cursor)

	if packetLength <= 32 {
		cursor, packetId := util.ReadByte(buff, cursor)
		fmt.Println(packetId)
		cursor, maxPacketSize := util.ReadUB4(buff, cursor)

		cursor, charsetIndex := util.ReadByte(buff, cursor)
		fmt.Println(maxPacketSize, charsetIndex)
		cursor, user := util.ReadStringWithNull(buff, cursor)

		cursor, password := util.ReadBytesWithNull(buff, cursor-1)
		cursor, database = util.ReadStringWithNull(buff, cursor-1)
		fmt.Println(user)
		fmt.Println(password)
		fmt.Println(database)
		return ap
	}
	cursor = 4

	cursor, clientFlag := util.ReadUB4(buff, cursor)

	cursor, maxPacketSize := util.ReadUB4(buff, cursor)

	cursor, charsetIndex := util.ReadByte(buff, cursor)
	var currentCursor = cursor
	cursor, length := util.ReadLength(buff, cursor)
	if currentCursor > 0 && length < 23 {
		//获取extra
	}
	cursor = cursor + 22
	cursor, user := util.ReadStringWithNull(buff, cursor)
	cursor, password := util.ReadBytesWithNull(buff, cursor)

	if (len(buff) > cursor) && (int32(clientFlag)&int32(common.CLIENT_CONNECT_WITH_DB)) != 0 {
		_, database = util.ReadStringWithNull(buff, cursor)
	}

	ap.clientFlag = int(clientFlag)
	ap.CharsetIndex = int(charsetIndex)
	ap.maxPacketSize = int(int32(maxPacketSize))
	ap.User = user
	ap.Password = password
	ap.Database = database

	return ap
}
