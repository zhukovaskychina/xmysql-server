package common_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"testing"
)

// This digest pins all existing code-to-state mappings, including rare XA errors.
func TestErrorSQLStateCatalogCompatibility(t *testing.T) {
	encoded, err := json.Marshal(common.MySQLState)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(encoded)); got != "427c34ecf6e7cf932939f13f8dd70fdb4bf97a32cdbe3093f402999ca5e6b787" {
		t.Fatalf("SQLSTATE catalog changed: %s", got)
	}
}

func TestSQLErrorStateAndFormattingCompatibility(t *testing.T) {
	tests := []struct {
		code  uint16
		state string
	}{
		{1045, "28000"}, {1146, "42S02"}, {1642, "01000"},
		{1643, "02000"}, {1644, "HY000"}, {65535, "HY000"},
	}
	for _, tt := range tests {
		plain := common.NewErr(tt.code)
		formatted := common.NewErrf(tt.code, "row %d", nil, 7)
		if plain.State != tt.state || formatted.State != tt.state || formatted.Code != tt.code || formatted.Message != "row 7" {
			t.Fatalf("code %d: plain=%+v formatted=%+v", tt.code, plain, formatted)
		}
	}
}
