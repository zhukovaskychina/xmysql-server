package dispatcher

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func TestFeatureSpecificPrivilegeRouting(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		privilege common.PrivilegeType
	}{
		{name: "create trigger", sql: "CREATE TRIGGER trg BEFORE INSERT ON orders FOR EACH ROW SET @seen = 1", privilege: common.TriggerPriv},
		{name: "create event", sql: "CREATE EVENT ev ON SCHEDULE EVERY 1 DAY DO SELECT 1", privilege: common.EventPriv},
		{name: "create procedure", sql: "CREATE PROCEDURE p() SELECT 1", privilege: common.CreateRoutinePriv},
		{name: "call", sql: "CALL p()", privilege: common.ExecutePriv},
		{name: "alter event", sql: "ALTER EVENT ev DISABLE", privilege: common.EventPriv},
		{name: "alter procedure", sql: "ALTER PROCEDURE p COMMENT 'changed'", privilege: common.AlterRoutinePriv},
		{name: "show create view", sql: "SHOW CREATE VIEW v", privilege: common.ShowViewPriv},
		{name: "create user", sql: "CREATE USER app IDENTIFIED BY 'secret'", privilege: common.CreateUserPriv},
		{name: "alter user", sql: "ALTER USER app IDENTIFIED BY 'secret'", privilege: common.CreateUserPriv},
		{name: "rename user", sql: "RENAME USER app TO renamed", privilege: common.CreateUserPriv},
		{name: "set another user password", sql: "SET PASSWORD FOR 'other'@'localhost' = 'secret'", privilege: common.CreateUserPriv},
		{name: "create role", sql: "CREATE ROLE app_reader", privilege: common.CreateRolePriv},
		{name: "drop role", sql: "DROP ROLE app_reader", privilege: common.DropRolePriv},
		{name: "grant", sql: "GRANT SELECT ON test.orders TO app", privilege: common.GrantPriv},
		{name: "revoke", sql: "REVOKE SELECT ON test.orders FROM app", privilege: common.GrantPriv},
		{name: "create temporary table", sql: "CREATE TEMPORARY TABLE t (id INT)", privilege: common.CreateTMPTablePriv},
		{name: "create tablespace", sql: "CREATE TABLESPACE ts ADD DATAFILE 'ts.ibd'", privilege: common.CreateTablespacePriv},
		{name: "alter tablespace", sql: "ALTER TABLESPACE ts RENAME TO archive", privilege: common.CreateTablespacePriv},
		{name: "drop tablespace", sql: "DROP TABLESPACE ts ENGINE=InnoDB", privilege: common.CreateTablespacePriv},
		{name: "create index", sql: "CREATE INDEX idx_orders_id ON orders (id)", privilege: common.IndexPriv},
		{name: "drop index", sql: "DROP INDEX idx_orders_id ON orders", privilege: common.IndexPriv},
		{name: "flush", sql: "FLUSH PRIVILEGES", privilege: common.ReloadPriv},
		{name: "lock tables", sql: "LOCK TABLES orders READ", privilege: common.LockTablesPriv},
		{name: "truncate table", sql: "TRUNCATE TABLE orders", privilege: common.DropPriv},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authService := &recordingAuthService{}
			handler := &EnhancedBusinessMessageHandler{authService: authService}

			if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", tt.sql); err != nil {
				t.Fatalf("checkQueryPrivilege() error = %v", err)
			}
			if len(authService.seen) != 1 || authService.seen[0] != tt.privilege {
				t.Fatalf("privileges = %v, want [%s]", authService.seen, tt.privilege.String())
			}
		})
	}
}

func TestSelfServiceSetPasswordDoesNotRequireGlobalPrivilege(t *testing.T) {
	for _, sql := range []string{
		"SET PASSWORD = 'secret'",
		"SET PASSWORD FOR 'app'@'localhost' = 'secret'",
		"SET PASSWORD FOR CURRENT_USER() = 'secret'",
	} {
		t.Run(sql, func(t *testing.T) {
			authService := &recordingAuthService{}
			handler := &EnhancedBusinessMessageHandler{authService: authService}
			if err := handler.checkQueryPrivilege(context.Background(), "app", "localhost", "test", sql); err != nil {
				t.Fatalf("checkQueryPrivilege() error = %v", err)
			}
			if len(authService.seen) != 0 {
				t.Fatalf("privileges = %v, want no global privilege check", authService.seen)
			}
		})
	}
}

func TestFeatureSpecificPrivilegeRoutingKeepsObjectTableContext(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		tables []string
	}{
		{name: "trigger target", sql: "CREATE TRIGGER trg BEFORE INSERT ON orders FOR EACH ROW SET @seen = 1", tables: []string{"orders"}},
		{name: "view definition source", sql: "CREATE VIEW v AS SELECT * FROM orders", tables: []string{"", "orders"}},
		{name: "show view object", sql: "SHOW CREATE VIEW reporting.v", tables: []string{"v"}},
		{name: "call routine object", sql: "CALL reporting.refresh_orders()", tables: []string{"refresh_orders"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authService := &recordingAuthService{}
			handler := &EnhancedBusinessMessageHandler{authService: authService}

			if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", tt.sql); err != nil {
				t.Fatalf("checkQueryPrivilege() error = %v", err)
			}
			if len(authService.tables) != len(tt.tables) {
				t.Fatalf("tables = %v, want %v", authService.tables, tt.tables)
			}
			for index := range tt.tables {
				if authService.tables[index] != tt.tables[index] {
					t.Fatalf("table %d = %q, want %q (all tables: %v)", index, authService.tables[index], tt.tables[index], authService.tables)
				}
			}
		})
	}
}

func TestFeatureSpecificPrivilegeRoutingChecksAllJoinTables(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	if len(authService.tables) != 2 || authService.tables[0] != "orders" || authService.tables[1] != "customers" {
		t.Fatalf("tables = %v, want [orders customers]", authService.tables)
	}
}

func TestFeatureSpecificPrivilegeRoutingRejectsUnauthorizedJoinTable(t *testing.T) {
	authService := &recordingAuthService{denyTable: "customers"}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id")
	if err == nil {
		t.Fatal("checkQueryPrivilege() unexpectedly allowed a join with an unauthorized table")
	}
	if len(authService.tables) != 2 || authService.tables[1] != "customers" {
		t.Fatalf("tables checked = %v, want [orders customers]", authService.tables)
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesInsertSelectPrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "INSERT INTO archive (id) SELECT id FROM source"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "archive", privilege: common.InsertPriv},
		{table: "source", privilege: common.SelectPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesCreateViewPrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "CREATE VIEW reporting_view AS SELECT * FROM orders"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "", privilege: common.CreateViewPriv},
		{table: "orders", privilege: common.SelectPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesReplaceViewPrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "CREATE OR REPLACE VIEW reporting_view AS SELECT * FROM orders"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "", privilege: common.CreateViewPriv},
		{table: "reporting_view", privilege: common.DropPriv},
		{table: "orders", privilege: common.SelectPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingUsesViewObjectForReplaceWithoutSourceTable(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "CREATE OR REPLACE VIEW reporting_view AS SELECT 1"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "", privilege: common.CreateViewPriv},
		{table: "reporting_view", privilege: common.DropPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingUsesViewObjectForDrop(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "DROP VIEW IF EXISTS reporting_view"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{{table: "reporting_view", privilege: common.DropPriv}}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingUsesEachViewObjectForMultiDrop(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "DROP VIEW v1, `reporting`.v2;"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "v1", privilege: common.DropPriv},
		{table: "v2", privilege: common.DropPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesAlterViewPrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "ALTER VIEW reporting_view AS SELECT * FROM orders"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "", privilege: common.CreateViewPriv},
		{table: "reporting_view", privilege: common.DropPriv},
		{table: "orders", privilege: common.SelectPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingUsesAlterViewObjectWithoutSourceTable(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "ALTER VIEW reporting_view AS SELECT 1"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "", privilege: common.CreateViewPriv},
		{table: "reporting_view", privilege: common.DropPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesCreateForeignKeyPrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "CREATE TABLE child (parent_id INT, FOREIGN KEY (parent_id) REFERENCES parent(id))"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "child", privilege: common.CreatePriv},
		{table: "parent", privilege: common.ReferencesPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesAlterForeignKeyPrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "ALTER TABLE child ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "child", privilege: common.AlterPriv},
		{table: "child", privilege: common.CreatePriv},
		{table: "child", privilege: common.InsertPriv},
		{table: "parent", privilege: common.ReferencesPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesAlterTablePrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "ALTER TABLE orders ADD COLUMN status INT"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "orders", privilege: common.AlterPriv},
		{table: "orders", privilege: common.CreatePriv},
		{table: "orders", privilege: common.InsertPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingUsesEachTableObjectForMultiDrop(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "DROP TABLE IF EXISTS orders, `reporting`.archive;"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "orders", privilege: common.DropPriv},
		{table: "archive", privilege: common.DropPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesRenameTablePrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "RENAME TABLE old_orders TO orders, `reporting`.old_archive TO archive"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "old_orders", privilege: common.AlterPriv},
		{table: "old_orders", privilege: common.DropPriv},
		{table: "orders", privilege: common.CreatePriv},
		{table: "orders", privilege: common.InsertPriv},
		{table: "old_archive", privilege: common.AlterPriv},
		{table: "old_archive", privilege: common.DropPriv},
		{table: "archive", privilege: common.CreatePriv},
		{table: "archive", privilege: common.InsertPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesCreateTableSelectPrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "CREATE TABLE archive AS SELECT * FROM source"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "archive", privilege: common.CreatePriv},
		{table: "source", privilege: common.SelectPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesCreateTableLikePrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "CREATE TABLE archive LIKE source"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "archive", privilege: common.CreatePriv},
		{table: "source", privilege: common.SelectPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesUpdateJoinPrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "UPDATE archive JOIN source ON archive.id = source.id SET archive.ready = 1"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "archive", privilege: common.UpdatePriv},
		{table: "source", privilege: common.SelectPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesDeleteJoinPrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "DELETE FROM archive USING archive JOIN source ON archive.id = source.id"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "archive", privilege: common.DeletePriv},
		{table: "source", privilege: common.SelectPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingChecksReplacePrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "REPLACE INTO archive (id) VALUES (1)"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "archive", privilege: common.InsertPriv},
		{table: "archive", privilege: common.DeletePriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingSeparatesReplaceSelectPrivileges(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "REPLACE INTO archive (id) SELECT id FROM source"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "archive", privilege: common.InsertPriv},
		{table: "archive", privilege: common.DeletePriv},
		{table: "source", privilege: common.SelectPriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}

func TestFeatureSpecificPrivilegeRoutingUsesTriggerPrivilegeForReplace(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "CREATE OR REPLACE TRIGGER trg BEFORE INSERT ON orders FOR EACH ROW SET @seen = 1"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{{table: "orders", privilege: common.TriggerPriv}}
	if len(authService.checks) != len(want) || authService.checks[0] != want[0] {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
}

func TestFeatureSpecificPrivilegeRoutingAddsUpdateForInsertDuplicateKey(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "INSERT INTO archive (id) VALUES (1) ON DUPLICATE KEY UPDATE id = VALUES(id)"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	want := []privilegeCheck{
		{table: "archive", privilege: common.InsertPriv},
		{table: "archive", privilege: common.UpdatePriv},
	}
	if len(authService.checks) != len(want) {
		t.Fatalf("privilege checks = %+v, want %+v", authService.checks, want)
	}
	for index := range want {
		if authService.checks[index] != want[index] {
			t.Fatalf("privilege check %d = %+v, want %+v", index, authService.checks[index], want[index])
		}
	}
}
