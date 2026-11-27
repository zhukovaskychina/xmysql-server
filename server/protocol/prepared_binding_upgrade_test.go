package protocol

import (
	"strings"
	"testing"
)

func TestPreparedBindingUpgradeCompatibility(t *testing.T) {
	template := "SELECT '?' AS literal, ? AS value /* ? */"
	if got := BindPreparedSQL(template, []interface{}{int64(7)}); got != "SELECT '?' AS literal, 7 AS value /* ? */" {
		t.Fatalf("bound=%q", got)
	}
	if got := BindPreparedSQL("SELECT ?, ?", []interface{}{nil, []byte{0x01, 0x02}}); !strings.HasPrefix(got, "SELECT NULL, '") {
		t.Fatalf("NULL/binary bound=%q", got)
	}
}

func TestPreparedBindingUpgradeExecutionIsolation(t *testing.T) {
	template := "SELECT ?"
	first := BindPreparedSQL(template, []interface{}{int64(1)})
	second := BindPreparedSQL(template, []interface{}{int64(2)})
	if first == second || first != "SELECT 1" || second != "SELECT 2" {
		t.Fatalf("bound executions crossed: %q %q", first, second)
	}
}

func TestPreparedBindingUpgradeMetadataAndExecutionRemainIndependent(t *testing.T) {
	template := "SELECT ? AS value"
	metadataSQL := BindPreparedSQL(template, []interface{}{nil})
	firstSQL := BindPreparedSQL(template, []interface{}{int64(11)})
	secondSQL := BindPreparedSQL(template, []interface{}{int64(22)})
	if metadataSQL != "SELECT NULL AS value" || firstSQL != "SELECT 11 AS value" || secondSQL != "SELECT 22 AS value" {
		t.Fatalf("metadata/execution binding crossed: %q %q %q", metadataSQL, firstSQL, secondSQL)
	}
}
