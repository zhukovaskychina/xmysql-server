package auth

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func TestMySQLUserToUserInfoMapsAuthFields(t *testing.T) {
	mysqlUser := &manager.MySQLUser{
		User:                 "root",
		Host:                 "localhost",
		AuthenticationString: "*C8706ADB2BB0A94DB0F119AA30D57A2DA723AD82",
		AccountLocked:        "N",
		PasswordExpired:      "N",
		SelectPriv:           "Y",
		InsertPriv:           "Y",
		UpdatePriv:           "Y",
		DeletePriv:           "Y",
		CreatePriv:           "Y",
		DropPriv:             "Y",
		SuperPriv:            "Y",
	}

	userInfo := mysqlUserToUserInfo(mysqlUser)
	if userInfo == nil {
		t.Fatal("expected user info")
	}
	if userInfo.User != "root" || userInfo.Host != "localhost" {
		t.Fatalf("unexpected user identity: %s@%s", userInfo.User, userInfo.Host)
	}
	if userInfo.Password != mysqlUser.AuthenticationString {
		t.Fatalf("password hash mismatch: %q", userInfo.Password)
	}
	if userInfo.AccountLocked {
		t.Fatal("expected account to be unlocked")
	}
	if userInfo.PasswordExpired {
		t.Fatal("expected password to be active")
	}

	expectedPrivileges := []common.PrivilegeType{
		common.SelectPriv,
		common.InsertPriv,
		common.UpdatePriv,
		common.DeletePriv,
		common.CreatePriv,
		common.DropPriv,
		common.SuperPriv,
	}
	for _, privilege := range expectedPrivileges {
		if !containsPrivilege(userInfo.GlobalPrivileges, privilege) {
			t.Fatalf("expected privilege %s in %#v", privilege.String(), userInfo.GlobalPrivileges)
		}
	}
}

func containsPrivilege(privileges []common.PrivilegeType, expected common.PrivilegeType) bool {
	for _, privilege := range privileges {
		if privilege == expected {
			return true
		}
	}
	return false
}
