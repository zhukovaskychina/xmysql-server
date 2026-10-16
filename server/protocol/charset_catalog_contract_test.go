package protocol

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func TestCharsetManagerCatalogLegacyContract(t *testing.T) {
	cm := NewCharsetManager()
	charsets := map[uint8]*CharsetInfo{}
	collations := map[uint16]*CollationInfo{}
	names := map[string]uint8{}
	defaults := map[string]string{}
	conversion := map[uint8][]string{}
	for id := 0; id < 256; id++ {
		if cs, err := cm.GetCharsetByID(uint8(id)); err == nil {
			charsets[uint8(id)] = cs
			sample := "abc"
			switch cs.Name {
			case "utf8", "utf8mb4", "gbk", "gb2312", "big5":
				sample = "中文"
			case "sjis", "cp932":
				sample = "日本"
			case "euckr":
				sample = "한국"
			case "cp1251":
				sample = "Привет"
			case "latin1":
				sample = "café"
			}
			converter := NewCharsetConverter(cm)
			encoded, encodeErr := converter.Convert([]byte(sample), 45, uint8(id))
			decoded, decodeErr := converter.Convert(encoded, uint8(id), 45)
			validErr := converter.ValidateString(encoded, uint8(id))
			conversion[uint8(id)] = []string{sample, fmt.Sprintf("%x", encoded), fmt.Sprint(encodeErr), string(decoded), fmt.Sprint(decodeErr), fmt.Sprint(validErr)}
			names[cs.Name], _ = cm.GetCharsetIDByName(cs.Name)
			def, err := cm.GetDefaultCollation(cs.Name)
			if err != nil {
				defaults[cs.Name] = err.Error()
			} else {
				defaults[cs.Name] = fmt.Sprintf("%T:%v", def.ID, def.ID)
			}
		}
	}
	for id := 0; id < 65536; id++ {
		if col, err := cm.GetCollationByID(uint16(id)); err == nil {
			collations[uint16(id)] = col
		}
	}
	got, err := json.MarshalIndent(map[string]interface{}{
		"charsets": charsets, "collations": collations, "names": names, "defaults": defaults, "conversion": conversion,
		"getterIDs":  []uint8{cm.GetUTF8CharsetID(), cm.GetUTF8MB4CharsetID(), cm.GetBinaryCharsetID(), cm.GetLatin1CharsetID()},
		"fieldTypes": fmt.Sprintf("%T %T %T", CharsetInfo{}.ID, CharsetInfo{}.MaxLen, CollationInfo{}.ID),
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/charset_manager_legacy_contract.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatal("protocol charset manager contract changed")
	}
}

func TestCharsetManagerCatalogObjectsAndAliases(t *testing.T) {
	left, right := NewCharsetManager(), NewCharsetManager()
	cs, _ := left.GetCharsetByID(33)
	cs.Description = "changed"
	again, _ := left.GetCharsetByID(33)
	other, _ := right.GetCharsetByID(33)
	if again != cs || other.Description == "changed" {
		t.Fatal("manager pointer ownership changed")
	}
	col, _ := left.GetCollationByID(33)
	col.Name = "changed"
	otherCol, _ := right.GetCollationByID(33)
	if otherCol.Name == "changed" {
		t.Fatal("managers share mutable collation objects")
	}
	for input, want := range map[string]string{"UTF-8": "utf8", "utf8mb3": "utf8", "UTF-8MB4": "utf8mb4", " LATIN1 ": " latin1 "} {
		if got := right.ConvertCharsetName(input); got != want {
			t.Fatalf("alias %q = %q, want %q", input, got, want)
		}
	}
	if _, err := right.GetCharsetByName("utf8mb3"); err == nil {
		t.Fatal("name lookup unexpectedly applied aliases")
	}
}
