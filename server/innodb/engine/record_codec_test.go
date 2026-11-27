package engine

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestRecordCodecRoundTripUsesTableColumnOrder(t *testing.T) {
	tableMeta := recordCodecTestTableMeta()
	row := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"note":   []byte("raw"),
			"active": true,
			"name":   "alice",
			"id":     int64(42),
			"score":  float64(99.5),
		},
		ColumnTypes: map[string]metadata.DataType{},
	}

	encoded, err := EncodeClusteredRecord(row, tableMeta)
	if err != nil {
		t.Fatalf("EncodeClusteredRecord() error = %v", err)
	}
	decoded, err := DecodeClusteredRecord(encoded, tableMeta)
	if err != nil {
		t.Fatalf("DecodeClusteredRecord() error = %v", err)
	}

	wantValues := map[string]interface{}{
		"id":     int64(42),
		"name":   "alice",
		"active": true,
		"score":  float64(99.5),
		"note":   []byte("raw"),
	}
	if !reflect.DeepEqual(decoded.ColumnValues, wantValues) {
		t.Fatalf("decoded values = %#v, want %#v", decoded.ColumnValues, wantValues)
	}
	for _, col := range tableMeta.Columns {
		if got := decoded.ColumnTypes[col.Name]; got != col.Type {
			t.Fatalf("decoded type for %s = %s, want %s", col.Name, got, col.Type)
		}
	}
}

func TestRecordCodecRejectsCorruptPayload(t *testing.T) {
	tableMeta := recordCodecTestTableMeta()
	row := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"id":     int64(42),
			"name":   "alice",
			"active": true,
			"score":  float64(99.5),
			"note":   []byte("raw"),
		},
	}

	encoded, err := EncodeClusteredRecord(row, tableMeta)
	if err != nil {
		t.Fatalf("EncodeClusteredRecord() error = %v", err)
	}
	if _, err := DecodeClusteredRecord(encoded[:len(encoded)-1], tableMeta); err == nil {
		t.Fatal("DecodeClusteredRecord() expected error for truncated payload")
	}

	corruptMagic := append([]byte(nil), encoded...)
	copy(corruptMagic[:4], []byte("BAD!"))
	if _, err := DecodeClusteredRecord(corruptMagic, tableMeta); err == nil {
		t.Fatal("DecodeClusteredRecord() expected error for invalid magic")
	}
}

func TestRecordCodecMissingColumnsUseDefaultOrNil(t *testing.T) {
	tableMeta := recordCodecTestTableMeta()
	row := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"id": int(7),
		},
	}

	encoded, err := EncodeClusteredRecord(row, tableMeta)
	if err != nil {
		t.Fatalf("EncodeClusteredRecord() error = %v", err)
	}
	decoded, err := DecodeClusteredRecord(encoded, tableMeta)
	if err != nil {
		t.Fatalf("DecodeClusteredRecord() error = %v", err)
	}

	if got := decoded.ColumnValues["id"]; got != int64(7) {
		t.Fatalf("decoded id = %#v, want int64(7)", got)
	}
	if got := decoded.ColumnValues["name"]; got != "anonymous" {
		t.Fatalf("decoded default name = %#v, want anonymous", got)
	}
	if got := decoded.ColumnValues["active"]; got != false {
		t.Fatalf("decoded default active = %#v, want false", got)
	}
	if got := decoded.ColumnValues["score"]; got != nil {
		t.Fatalf("decoded missing score = %#v, want nil", got)
	}
	if got := decoded.ColumnValues["note"]; got != nil {
		t.Fatalf("decoded missing note = %#v, want nil", got)
	}
}

func TestRecordCodecProjectedDecodeSkipsUnrequestedColumns(t *testing.T) {
	tableMeta := recordCodecTestTableMeta()
	encoded, err := EncodeClusteredRecord(&InsertRowData{ColumnValues: map[string]interface{}{
		"id": 42, "name": "alice", "active": true, "score": 99.5, "note": []byte("raw"),
	}}, tableMeta)
	if err != nil {
		t.Fatalf("EncodeClusteredRecord() error = %v", err)
	}

	decoded, err := DecodeClusteredRecordProjected(encoded, tableMeta, []string{"name", "active"})
	if err != nil {
		t.Fatalf("DecodeClusteredRecordProjected() error = %v", err)
	}
	if !reflect.DeepEqual(decoded.ColumnValues, map[string]interface{}{"name": "alice", "active": true}) {
		t.Fatalf("projected values = %#v", decoded.ColumnValues)
	}
	if len(decoded.ColumnTypes) != 2 || decoded.ColumnTypes["name"] != metadata.TypeVarchar || decoded.ColumnTypes["active"] != metadata.TypeBool {
		t.Fatalf("projected types = %#v", decoded.ColumnTypes)
	}
}

func TestRecordCodecPreservesCharAndBinaryFixedLengthSemantics(t *testing.T) {
	tableMeta := &metadata.TableMeta{Columns: []*metadata.ColumnMeta{
		{Name: "label", Type: metadata.TypeChar, Length: 5},
		{Name: "token", Type: metadata.TypeBinary, Length: 4},
		{Name: "name", Type: metadata.TypeVarchar, Length: 5},
		{Name: "raw", Type: metadata.TypeVarBinary, Length: 4},
	}}
	encoded, err := EncodeClusteredRecord(&InsertRowData{ColumnValues: map[string]interface{}{
		"label": "ab  ", "token": "xy", "name": "ab", "raw": []byte("xy"),
	}}, tableMeta)
	if err != nil {
		t.Fatalf("EncodeClusteredRecord() error = %v", err)
	}
	decoded, err := DecodeClusteredRecord(encoded, tableMeta)
	if err != nil {
		t.Fatalf("DecodeClusteredRecord() error = %v", err)
	}
	if got := decoded.ColumnValues["label"]; got != "ab" {
		t.Fatalf("CHAR value = %#v, want trimmed text", got)
	}
	if got := decoded.ColumnValues["token"]; !bytes.Equal(got.([]byte), []byte{'x', 'y', 0, 0}) {
		t.Fatalf("BINARY value = %#v, want zero-padded bytes", got)
	}
	if got := decoded.ColumnValues["name"]; got != "ab" {
		t.Fatalf("VARCHAR value = %#v, want unchanged text", got)
	}
	if got := decoded.ColumnValues["raw"]; !bytes.Equal(got.([]byte), []byte("xy")) {
		t.Fatalf("VARBINARY value = %#v, want unchanged bytes", got)
	}
	if _, err := EncodeClusteredRecord(&InsertRowData{ColumnValues: map[string]interface{}{"token": "12345"}}, tableMeta); err == nil {
		t.Fatal("BINARY overflow should be rejected")
	}
}

func TestRecordCodecEncodingIsDeterministic(t *testing.T) {
	tableMeta := recordCodecTestTableMeta()
	first := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"id":     int64(11),
			"name":   "bob",
			"active": true,
			"score":  float64(12.25),
			"note":   []byte("same"),
		},
	}
	second := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"note":   []byte("same"),
			"score":  float64(12.25),
			"active": true,
			"name":   "bob",
			"id":     int64(11),
		},
	}

	firstEncoded, err := EncodeClusteredRecord(first, tableMeta)
	if err != nil {
		t.Fatalf("EncodeClusteredRecord(first) error = %v", err)
	}
	secondEncoded, err := EncodeClusteredRecord(second, tableMeta)
	if err != nil {
		t.Fatalf("EncodeClusteredRecord(second) error = %v", err)
	}
	if !bytes.Equal(firstEncoded, secondEncoded) {
		t.Fatalf("encoded bytes differ for same row values inserted in different map order")
	}
}

func recordCodecTestTableMeta() *metadata.TableMeta {
	return &metadata.TableMeta{
		Name: "codec_test",
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: metadata.TypeInt, IsPrimary: true},
			{Name: "name", Type: metadata.TypeVarchar, DefaultValue: "anonymous"},
			{Name: "active", Type: metadata.TypeBool, DefaultValue: false},
			{Name: "score", Type: metadata.TypeDouble},
			{Name: "note", Type: metadata.TypeVarBinary},
		},
		PrimaryKey: []string{"id"},
	}
}
