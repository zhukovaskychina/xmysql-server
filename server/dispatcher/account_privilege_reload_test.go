package dispatcher

import "testing"

func TestPrivilegeReloadTriggerRecognizesAccountAndTransactionStatements(t *testing.T) {
	for _, query := range []string{
		"CREATE USER 'alice'@'localhost' IDENTIFIED BY 'secret'",
		"GRANT SELECT ON app.* TO 'alice'@'localhost'",
		"REVOKE SELECT ON app.* FROM 'alice'@'localhost'",
		"FLUSH PRIVILEGES",
		"COMMIT",
		"ROLLBACK",
	} {
		if !shouldReloadPrivilegesAfterQuery(query) {
			t.Fatalf("query %q did not trigger privilege reload", query)
		}
	}
	if shouldReloadPrivilegesAfterQuery("SELECT * FROM app.users") {
		t.Fatal("ordinary SELECT unexpectedly triggered privilege reload")
	}
}
