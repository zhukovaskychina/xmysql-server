package manager

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"testing"
)

func TestSchemaCharsetPolicyLegacyContract(t *testing.T) {
	m := &SchemaManager{}
	allowed := map[string][]string{
		"utf8mb4": {"utf8mb4_general_ci", "utf8mb4_unicode_ci", "utf8mb4_bin", "utf8mb4_0900_ai_ci"},
		"utf8":    {"utf8_general_ci", "utf8_unicode_ci", "utf8_bin"},
		"latin1":  {"latin1_swedish_ci", "latin1_general_ci", "latin1_bin"},
		"ascii":   {"ascii_general_ci", "ascii_bin"},
		"binary":  {"binary"},
	}
	for charset, collations := range allowed {
		for _, known := range common.Collations {
			want := false
			for _, accepted := range collations {
				if known == accepted {
					want = true
				}
			}
			if got := m.validateCharsetAndCollation(charset, known) == nil; got != want {
				t.Fatalf("schema support changed for %s / %s", charset, known)
			}
		}
		for _, collation := range collations {
			if err := m.validateCharsetAndCollation(charset, collation); err != nil {
				t.Fatal(err)
			}
			for other := range allowed {
				if other != charset {
					if err := m.validateCharsetAndCollation(other, collation); err == nil {
						t.Fatalf("accepted mismatched %s %s", other, collation)
					}
				}
			}
		}
		if err := m.validateCharsetAndCollation(charset, "missing"); err == nil || err.Error() != "collation 'missing' is not valid for charset '"+charset+"'" {
			t.Fatalf("invalid collation error: %v", err)
		}
	}
	for _, charset := range []string{"UTF8", "utf8mb3", "gbk", "utf16", "missing", ""} {
		if err := m.validateCharsetAndCollation(charset, "utf8_general_ci"); err == nil || err.Error() != "unsupported charset '"+charset+"'" {
			t.Fatalf("unsupported charset error: %v", err)
		}
	}
}
