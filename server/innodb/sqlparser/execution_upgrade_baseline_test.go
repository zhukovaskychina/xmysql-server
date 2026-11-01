package sqlparser

import "testing"

// TestSQLParserUpgradeBaselineFixtures records the parser behavior used by the
// upgrade plan before any new grammar is enabled. It is a change detector, not
// a compatibility score: a successful parse may still be a partial AST and a
// parse error may describe a feature that the engine supports through a raw
// compatibility path.
func TestSQLParserUpgradeBaselineFixtures(t *testing.T) {
	cases := []struct {
		name  string
		query string
		err   bool
	}{
		{"alter_add_column", "ALTER TABLE t ADD COLUMN c INT", false},
		{"alter_multiple_operations", "ALTER TABLE t ADD COLUMN c INT, DROP COLUMN d", true},
		{"alter_modify_column", "ALTER TABLE t MODIFY COLUMN c VARCHAR(20) AFTER d", false},
		{"alter_add_index", "ALTER TABLE t ADD INDEX idx_c(c)", false},
		{"alter_options", "ALTER TABLE t ALGORITHM=INPLACE, LOCK=NONE, ADD COLUMN c INT", false},
		{"alter_rename", "ALTER TABLE t RENAME TO t2", false},
		{"show_tables", "SHOW TABLES", false},
		{"show_full_tables", "SHOW FULL TABLES FROM app LIKE 'user%'", true},
		{"show_variables", "SHOW VARIABLES LIKE 'max_%'", false},
		{"show_status", "SHOW STATUS WHERE Variable_name = 'Threads_connected'", false},
		{"show_create_table", "SHOW CREATE TABLE app.t", false},
		{"show_columns", "SHOW COLUMNS FROM app.t LIKE 'id'", false},
		{"show_index", "SHOW INDEX FROM app.t", false},
		{"show_procedure_status", "SHOW PROCEDURE STATUS WHERE Db = 'app'", false},
		{"show_warnings", "SHOW WARNINGS LIMIT 1, 2", false},
		{"show_binlog", "SHOW BINLOG EVENTS IN 'bin.000001' FROM 4 LIMIT 10", false},
		{"show_replica", "SHOW REPLICA STATUS FOR CHANNEL 'ch'", false},
		{"analyze", "ANALYZE TABLE app.t", false},
		{"repair", "REPAIR TABLE app.t QUICK", false},
		{"optimize", "OPTIMIZE TABLE app.t", false},
		{"flush", "FLUSH TABLES WITH READ LOCK", true},
		{"kill", "KILL QUERY 123", true},
		{"lock", "LOCK TABLES app.t READ", true},
		{"change_replication_source", "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1'", true},
		{"install_component", "INSTALL COMPONENT 'file://component_validate_password'", true},
		{"create_procedure", "CREATE PROCEDURE app.p(IN x INT) BEGIN SELECT x; END", true},
		{"create_function", "CREATE FUNCTION app.f(x INT) RETURNS INT DETERMINISTIC RETURN x + 1", true},
		{"create_trigger", "CREATE TRIGGER app.tr BEFORE INSERT ON app.t FOR EACH ROW SET NEW.c = 1", true},
		{"create_event", "CREATE EVENT app.ev ON SCHEDULE EVERY 1 DAY DO DELETE FROM app.t", true},
		{"call", "CALL app.p(1)", true},
		{"drop_procedure", "DROP PROCEDURE app.p", true},
		{"create_view", "CREATE VIEW app.v AS SELECT id FROM app.t", false},
		{"create_user", "CREATE USER 'u'@'localhost' IDENTIFIED BY 'x'", true},
		{"grant", "GRANT SELECT ON app.t TO 'u'@'localhost'", true},
		{"system_variable", "SELECT @@session.autocommit", false},
		{"set_names", "SET NAMES utf8mb4 COLLATE utf8mb4_bin", false},
		{"prepare", "PREPARE s FROM 'SELECT 1'", true},
		{"handler", "HANDLER app.t OPEN", true},
		{"create_like_partial", "CREATE TABLE app.t2 LIKE app.t", false},
		{"temporary_table", "CREATE TEMPORARY TABLE app.tmp(id INT)", true},
		{"between", "SELECT * FROM app.t WHERE id BETWEEN 1 AND 5", false},
		{"exists", "SELECT * FROM app.t WHERE EXISTS (SELECT 1 FROM app.t2 WHERE t2.id=t.id)", false},
		{"cte", "WITH q AS (SELECT 1 AS id) SELECT * FROM q", false},
		{"window", "SELECT row_number() OVER (ORDER BY id) AS rn FROM app.t", false},
		{"alter_invalid_tail_partial", "ALTER TABLE t MODIFY COLUMN c INT totally_invalid_tail", false},
		{"show_invalid_tail_partial", "SHOW CREATE TABLE app.t totally_invalid_tail", false},
		{"repair_invalid_tail_partial", "REPAIR TABLE app.t totally_invalid_tail", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.query)
			if (err != nil) != tc.err {
				t.Fatalf("Parse(%q) error = %v, want error=%v (AST=%T)", tc.query, err, tc.err, stmt)
			}
		})
	}
}
