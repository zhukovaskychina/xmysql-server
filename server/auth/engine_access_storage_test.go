package auth

import (
	"encoding/json"
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

func TestPersistedAccountToUserInfoIncludesStandaloneGlobalGrants(t *testing.T) {
	var account persistedAccountForAuth
	err := json.Unmarshal([]byte(`{"user":"backup_operator","host":"localhost","password":"secret","global_grants":["BACKUP_ADMIN"]}`), &account)
	if err != nil {
		t.Fatalf("unmarshal persisted account: %v", err)
	}

	info := persistedAccountToUserInfo(&account)
	if info == nil {
		t.Fatal("expected user info")
	}
	if len(info.DynamicPrivileges) != 1 || info.DynamicPrivileges[0] != "BACKUP_ADMIN" {
		t.Fatalf("expected persisted global grant in dynamic privileges, got %#v", info.DynamicPrivileges)
	}
}

func TestPersistedAccountToUserInfoIncludesPartialRevokeRestrictions(t *testing.T) {
	var account persistedAccountForAuth
	err := json.Unmarshal([]byte(`{"user":"restricted","host":"localhost","grants":{"*.*":["SELECT","INSERT"]},"restrictions":{"app":["INSERT"]}}`), &account)
	if err != nil {
		t.Fatalf("unmarshal persisted account: %v", err)
	}

	info := persistedAccountToUserInfo(&account)
	if info == nil {
		t.Fatal("expected user info")
	}
	if !containsPrivilege(info.GlobalPrivileges, common.InsertPriv) {
		t.Fatalf("expected global INSERT before schema restriction filtering, got %#v", info.GlobalPrivileges)
	}
	if !containsPrivilege(info.Restrictions["app"], common.InsertPriv) {
		t.Fatalf("expected app INSERT restriction, got %#v", info.Restrictions)
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
