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
