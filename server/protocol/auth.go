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
	// 解析packetLength
	var cursor = 0
	var database = ""

	// 检查最小长度
	if len(buff) < 4 {
		fmt.Println("认证包太短，无法解析包头")
		return nil
	}

	cursor, packetLength := util.ReadUB3(buff, cursor)
	cursor, _ = util.ReadByte(buff, cursor) // 跳过packetId

	// 检查是否有足够的数据
	if len(buff) < int(packetLength)+4 {
		fmt.Println("认证包数据不完整")
		return nil
	}

	// 认证包的payload从第4字节开始
	payload := buff[4:]
	cursor = 0

	// 检查payload最小长度（至少需要：4字节clientFlag + 4字节maxPacketSize + 1字节charset + 23字节保留）
	if len(payload) < 32 {
		fmt.Println("认证包payload太短")
		return nil
	}

	// 解析客户端能力标志（4字节）
	cursor, clientFlag := util.ReadUB4(payload, cursor)

	// 解析最大包大小（4字节）
	cursor, maxPacketSize := util.ReadUB4(payload, cursor)

	// 解析字符集（1字节）
	cursor, charsetIndex := util.ReadByte(payload, cursor)

	// 跳过保留字段（23字节）
	cursor += 23

	// 检查是否还有数据可读
	if cursor >= len(payload) {
		fmt.Println("认证包缺少用户名字段")
		return nil
	}

	// 解析用户名（null结尾字符串）
	cursor, user := util.ReadStringWithNull(payload, cursor)

	// 解析密码（可能为空）
	var password []byte
	if cursor < len(payload) {
		cursor, password = util.ReadBytesWithNull(payload, cursor)
	}

	// 解析数据库名（如果客户端指定了）
	if cursor < len(payload) && (int32(clientFlag)&int32(common.CLIENT_CONNECT_WITH_DB)) != 0 {
		_, database = util.ReadStringWithNull(payload, cursor)
	}

	ap.clientFlag = int(clientFlag)
	ap.CharsetIndex = int(charsetIndex)
	ap.maxPacketSize = int(int32(maxPacketSize))
	ap.User = user
	ap.Password = password
	ap.Database = database

	return ap
}
