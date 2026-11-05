package sqlparser

import (
	"strings"
	"testing"
)

func TestAccountUpgradeStructuredTargets(t *testing.T) {
	parsed, err := ParseForExecution("SET PASSWORD FOR 'alice'@'localhost' = 'secret'")
	if err != nil {
		t.Fatalf("ParseForExecution() error = %v", err)
	}
	statement, ok := parsed.AST.(*AccountStatement)
	if !ok || statement.Kind != "set password" || len(statement.Accounts) != 1 {
		t.Fatalf("account statement = %#v", parsed.AST)
	}
	if !IsSelfServicePasswordChange(statement, AccountName{User: "alice", Host: "localhost"}) {
		t.Fatal("matching SET PASSWORD target was not recognized as self-service")
	}
	if IsSelfServicePasswordChange(statement, AccountName{User: "bob", Host: "localhost"}) {
		t.Fatal("different SET PASSWORD target was recognized as self-service")
	}
}

func TestAccountUpgradeRetainsSecurityOptions(t *testing.T) {
	parsed, err := ParseForExecution("CREATE USER 'app''owner'@'10.%' IDENTIFIED WITH caching_sha2_password BY 'secret' REQUIRE SSL ACCOUNT LOCK")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*AccountStatement)
	if len(stmt.Accounts) != 1 || stmt.Accounts[0].User == "" || stmt.Accounts[0].Host == "" {
		t.Fatalf("accounts=%#v", stmt.Accounts)
	}
	if stmt.Options["identified"] == "" || stmt.Options["require"] == "" || stmt.Options["account"] == "" {
		t.Fatalf("options=%v", stmt.Options)
	}
}

func TestAccountASTExpiredPasswordBoundary(t *testing.T) {
	self, err := ParseForExecution("SET PASSWORD")
	if err != nil {
		t.Fatal(err)
	}
	if !IsSelfServicePasswordChange(self.AST, AccountName{User: "alice", Host: "localhost"}) {
		t.Fatal("self-service SET PASSWORD was not accepted")
	}
	other, err := ParseForExecution("SET PASSWORD FOR 'bob'@'localhost' = 'secret'")
	if err != nil {
		t.Fatal(err)
	}
	if IsSelfServicePasswordChange(other.AST, AccountName{User: "alice", Host: "localhost"}) {
		t.Fatal("password change for another account was treated as self-service")
	}
}

func TestAccountUpgradeGrantAndRoleTargets(t *testing.T) {
	for _, query := range []string{"CREATE ROLE 'reporter'@'localhost'", "GRANT SELECT, INSERT ON app.* TO 'alice'@'localhost', 'bob'@'localhost'", "REVOKE INSERT ON app.* FROM 'alice'@'localhost'"} {
		parsed, err := ParseForExecution(query)
		if err != nil {
			t.Fatal(err)
		}
		stmt := parsed.AST.(*AccountStatement)
		if len(stmt.Accounts) == 0 {
			t.Fatalf("%s accounts=%#v", query, stmt)
		}
		if strings.HasPrefix(strings.ToLower(query), "grant") {
			if !strings.EqualFold(stmt.Scope, "app.*") || !strings.EqualFold(stmt.PrivilegeSpec, "SELECT, INSERT") || len(stmt.Accounts) != 2 {
				t.Fatalf("grant AST=%#v", stmt)
			}
		}
	}
}

func TestAccountUpgradeDefaultRoleTargets(t *testing.T) {
	parsed, err := ParseForExecution("SET DEFAULT ROLE 'reader'@'localhost' TO 'alice'@'localhost', 'bob'@'localhost'")
	if err != nil {
		t.Fatal(err)
	}
	statement := parsed.AST.(*AccountStatement)
	if statement.Kind != "set default role" || len(statement.Accounts) != 2 || statement.Accounts[1].User != "bob" {
		t.Fatalf("statement=%#v", statement)
	}
}

func TestAccountUpgradeShowCreateUserTarget(t *testing.T) {
	parsed, err := ParseForExecution("SHOW CREATE USER 'alice'@'localhost'")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*AccountStatement)
	if stmt.Kind != "show create user" || len(stmt.Accounts) != 1 || stmt.Accounts[0].User != "alice" {
		t.Fatalf("stmt=%#v", stmt)
	}
}

func TestAccountUpgradeProxyGrantPreservesProxiedAccountScope(t *testing.T) {
	parsed, err := ParseForExecution("GRANT PROXY ON 'proxied'@'localhost' TO 'proxy_a'@'localhost', 'proxy_b'@'localhost'")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*AccountStatement)
	if stmt.Scope != "'proxied'@'localhost'" || len(stmt.Accounts) != 2 || !strings.EqualFold(stmt.PrivilegeSpec, "PROXY") {
		t.Fatalf("proxy grant AST=%#v", stmt)
	}
}
