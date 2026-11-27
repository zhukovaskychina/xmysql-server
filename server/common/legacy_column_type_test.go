package common_test

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"testing"
)

func TestLegacyColumnConversionCompatibility(t *testing.T) {
	for _, tt := range []struct {
		name string
		code byte
	}{
		{"tiny", 1}, {"varchar", 15}, {"float", 4},
		{"decimal", 0}, {"blob", 252}, {"int", 9},
	} {
		if got := common.ConvertColTypeToEnums(tt.name); got != tt.code {
			t.Errorf("%s: got %d, want %d", tt.name, got, tt.code)
		}
		if got := common.ConvertColTypeToStr(tt.code); got != tt.name {
			t.Errorf("%d: got %q, want %q", tt.code, got, tt.name)
		}
	}
	if common.ConvertColTypeToEnums("INT") != 9 || common.ConvertColTypeToEnums("unknown") != 0 || common.ConvertColTypeToEnums(" int ") != 0 {
		t.Fatal("legacy case normalization or unknown-input behavior changed")
	}
	if got := common.ConvertColTypeToStr(255); got != "" {
		t.Fatalf("unknown code = %q, want empty string", got)
	}
}
