package net

import (
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/auth"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

type authSwitchState struct {
	Username         string
	Database         string
	Host             string
	Challenge        []byte
	ChangeUser       bool
	Charset          string
	Collation        string
	ResponseSequence byte
}

func encodeAuthSwitchRequest(plugin string, challenge []byte, sequence byte) []byte {
	payload := make([]byte, 0, 1+len(plugin)+1+len(challenge)+1)
	payload = append(payload, 0xfe)
	payload = append(payload, plugin...)
	payload = append(payload, 0)
	payload = append(payload, challenge...)
	payload = append(payload, 0)
	packets := protocol.EncodePacketWithSplit(payload, sequence)
	if len(packets) == 0 {
		return nil
	}
	return packets[0]
}

// authSwitchPlugin returns the command-phase plugin required by an account.
// The empty-plugin/64-hex fallback preserves compatibility with legacy
// imported caching_sha2_password rows that predate persisted plugin metadata.
func authSwitchPlugin(userInfo *auth.UserInfo) string {
	if userInfo == nil {
		return ""
	}
	plugin := strings.ToLower(strings.TrimSpace(userInfo.AuthPlugin))
	switch plugin {
	case "caching_sha2_password", "sha256_password":
		return plugin
	case "":
		if len(strings.TrimSpace(userInfo.Password)) == 64 {
			return "caching_sha2_password"
		}
	}
	return ""
}
