package protocol

import (
	"encoding/binary"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func TestAuthPacketDecodeAuthKeepsPluginNameOutOfDatabase(t *testing.T) {
	flags := common.CLIENT_CONNECT_WITH_DB |
		common.CLIENT_SECURE_CONNECTION |
		common.CLIENT_PLUGIN_AUTH |
		common.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA

	payload := make([]byte, 0, 96)
	var fixed [4]byte
	binary.LittleEndian.PutUint32(fixed[:], flags)
	payload = append(payload, fixed[:]...)
	binary.LittleEndian.PutUint32(fixed[:], 16*1024*1024)
	payload = append(payload, fixed[:]...)
	payload = append(payload, 45)
	payload = append(payload, make([]byte, 23)...)
	payload = append(payload, []byte("root\x00")...)
	// CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA encodes the one-byte auth response
	// with a length-encoded integer, followed by the response bytes.
	payload = append(payload, 1, 'x')
	// The client requested a database but selected no default database.
	payload = append(payload, 0)
	payload = append(payload, []byte("mysql_native_password\x00")...)

	packet := make([]byte, 4+len(payload))
	packet[0] = byte(len(payload))
	packet[1] = byte(len(payload) >> 8)
	packet[2] = byte(len(payload) >> 16)
	packet[3] = 1
	copy(packet[4:], payload)

	auth := new(AuthPacket).DecodeAuth(packet)
	if auth == nil {
		t.Fatal("DecodeAuth returned nil")
	}
	if auth.User != "root" {
		t.Fatalf("user = %q, want root", auth.User)
	}
	if auth.Database != "" {
		t.Fatalf("database = %q, want empty database", auth.Database)
	}
	if string(auth.Password) != "x" {
		t.Fatalf("password = %q, want x", auth.Password)
	}
}
