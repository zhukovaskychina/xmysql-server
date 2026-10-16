package dispatcher

import (
	"reflect"
	"testing"
)

func TestDispatcherCharsetCatalogLegacyContract(t *testing.T) {
	e := &SystemVariableEngine{}
	wantCharsets := [][]interface{}{
		{"utf8", "UTF-8 Unicode", "utf8_general_ci", 3},
		{"utf8mb4", "UTF-8 Unicode", "utf8mb4_0900_ai_ci", 4},
		{"latin1", "cp1252 West European", "latin1_swedish_ci", 1},
		{"ascii", "US ASCII", "ascii_general_ci", 1},
		{"binary", "Binary pseudo charset", "binary", 1},
	}
	wantCollations := [][]interface{}{
		{"utf8mb4_0900_ai_ci", "utf8mb4", 255, "Yes", "Yes", 0, "NO PAD"},
		{"utf8mb4_general_ci", "utf8mb4", 45, "", "Yes", 1, "PAD SPACE"},
		{"utf8_general_ci", "utf8", 33, "Yes", "Yes", 1, "PAD SPACE"},
		{"latin1_swedish_ci", "latin1", 8, "Yes", "Yes", 1, "PAD SPACE"},
		{"ascii_general_ci", "ascii", 11, "Yes", "Yes", 1, "PAD SPACE"},
		{"binary", "binary", 63, "Yes", "Yes", 1, "NO PAD"},
	}
	if got := e.executeShowCharset(); !reflect.DeepEqual(got.Rows, wantCharsets) || !reflect.DeepEqual(got.Columns, []string{"Charset", "Description", "Default collation", "Maxlen"}) {
		t.Fatalf("charset contract: %+v", got)
	}
	if got := e.executeShowCollation(); !reflect.DeepEqual(got.Rows, wantCollations) || !reflect.DeepEqual(got.Columns, []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"}) {
		t.Fatalf("collation contract: %+v", got)
	}
}
