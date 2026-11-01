package sqlparser

import "testing"

func BenchmarkParseForExecution(b *testing.B) {
	queries := []string{
		"SELECT id FROM app.t WHERE id BETWEEN 1 AND 10",
		"SHOW COLUMNS FROM app.t LIKE 'id'",
		"ALTER TABLE app.t ADD COLUMN c INT, ADD INDEX idx_c(c)",
		"WITH q AS (SELECT * FROM app.t) SELECT * FROM q WHERE id IN (1,2)",
		"CREATE PROCEDURE app.p(IN x INT) BEGIN SET x = x + 1; SELECT x; END",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ParseForExecution(queries[i%len(queries)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseLegacyComparable keeps only statements that the pre-upgrade
// yacc entry accepted, so the comparison measures parser work rather than
// treating expected legacy syntax failures as successful parses.
func BenchmarkParseLegacyComparable(b *testing.B) {
	queries := []string{
		"SELECT id FROM app.t WHERE id BETWEEN 1 AND 10",
		"SHOW COLUMNS FROM app.t LIKE 'id'",
		"SELECT * FROM app.t WHERE EXISTS (SELECT 1 FROM app.t2 WHERE t2.id=t.id)",
		"SELECT @@session.autocommit",
		"SET NAMES utf8mb4 COLLATE utf8mb4_bin",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Parse(queries[i%len(queries)]); err != nil {
			b.Fatal(err)
		}
	}
}
