package common

import (
	"encoding/json"
	"os"
	"testing"
)

// This fixture was captured before the catalog refactor. It deliberately
// includes historical asymmetries in reverse maps and scope ordering.
func TestMetadataLegacyContract(t *testing.T) {
	defaults := map[string][]int{}
	for tp := 0; tp < 256; tp++ {
		l, d := GetDefaultFieldLengthAndDecimal(byte(tp))
		cl, cd := GetDefaultFieldLengthAndDecimalForCast(byte(tp))
		integer := 0
		if IsIntegerType(byte(tp)) {
			integer = 1
		}
		defaults[string(rune(tp))] = []int{l, d, cl, cd, integer}
	}
	got, err := json.MarshalIndent(map[string]interface{}{
		"charsetIDs": CharsetIDs, "protocolCharsets": CharacterSetMap,
		"collations": Collations, "collationNames": CollationNames,
		"priv2Str": Priv2Str, "priv2SetStr": Priv2SetStr, "setStr2Priv": SetStr2Priv,
		"priv2UserCol": Priv2UserCol, "col2PrivType": Col2PrivType,
		"global": AllGlobalPrivs, "db": AllDBPrivs, "table": AllTablePrivs,
		"column": AllColumnPrivs, "globalOnly": StaticGlobalOnlyPrivs,
		"defaults": defaults,
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/metadata_legacy_contract.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != string(got) {
		t.Fatal("legacy metadata contract changed")
	}
}
