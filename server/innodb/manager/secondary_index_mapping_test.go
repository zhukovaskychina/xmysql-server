package manager

import (
	"bytes"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestEncodeSecondaryIndexKeyUniqueUsesOnlyIndexedColumns(t *testing.T) {
	index := metadata.IndexMeta{Name: "uk_email", Columns: []string{"email"}, Unique: true}
	first, err := EncodeSecondaryIndexKey(7, index, map[string]interface{}{"email": "a@example.com"}, []byte("pk-1"))
	if err != nil {
		t.Fatalf("EncodeSecondaryIndexKey(first) error = %v", err)
	}
	second, err := EncodeSecondaryIndexKey(7, index, map[string]interface{}{"email": "a@example.com"}, []byte("pk-2"))
	if err != nil {
		t.Fatalf("EncodeSecondaryIndexKey(second) error = %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("unique secondary keys differ for same indexed value")
	}
}

func TestEncodeSecondaryIndexKeyNonUniqueIncludesPrimaryKey(t *testing.T) {
	index := metadata.IndexMeta{Name: "idx_city", Columns: []string{"city"}, Unique: false}
	first, err := EncodeSecondaryIndexKey(7, index, map[string]interface{}{"city": "hz"}, []byte("pk-1"))
	if err != nil {
		t.Fatalf("EncodeSecondaryIndexKey(first) error = %v", err)
	}
	second, err := EncodeSecondaryIndexKey(7, index, map[string]interface{}{"city": "hz"}, []byte("pk-2"))
	if err != nil {
		t.Fatalf("EncodeSecondaryIndexKey(second) error = %v", err)
	}
	if bytes.Equal(first, second) {
		t.Fatalf("non-unique secondary keys should include primary key suffix")
	}
}

func TestEncodeSecondaryIndexKeyRejectsMissingColumn(t *testing.T) {
	index := metadata.IndexMeta{Name: "uk_email", Columns: []string{"email"}, Unique: true}
	if _, err := EncodeSecondaryIndexKey(7, index, map[string]interface{}{"name": "alice"}, []byte("pk-1")); err == nil {
		t.Fatalf("EncodeSecondaryIndexKey() expected missing column error")
	}
}

func TestSecondaryIndexFullRangeContainsCompositeIndexKey(t *testing.T) {
	index := metadata.IndexMeta{Name: "idx_city_name", Columns: []string{"city", "name"}}
	key, err := EncodeSecondaryIndexKey(7, index, map[string]interface{}{"city": "hz", "name": "alice"}, []byte("pk-1"))
	if err != nil {
		t.Fatalf("EncodeSecondaryIndexKey() error = %v", err)
	}
	start, end, err := SecondaryIndexFullRange(7, index)
	if err != nil {
		t.Fatalf("SecondaryIndexFullRange() error = %v", err)
	}
	if bytes.Compare(key, start) < 0 || bytes.Compare(key, end) > 0 {
		t.Fatalf("composite key is outside full index range")
	}
}
