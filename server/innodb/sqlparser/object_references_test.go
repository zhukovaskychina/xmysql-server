package sqlparser

import (
	"reflect"
	"testing"
)

func TestCollectObjectReferencesScopesAndDefaultSchema(t *testing.T) {
	parsed, err := ParseForExecution("WITH q AS (SELECT * FROM app.source) SELECT * FROM q JOIN target ON q.id = target.id")
	if err != nil {
		t.Fatalf("ParseForExecution() error = %v", err)
	}
	references, err := CollectObjectReferences(parsed.AST, "app")
	if err != nil {
		t.Fatalf("CollectObjectReferences() error = %v", err)
	}
	want := []ObjectReference{
		{Schema: "app", Name: "source", Kind: ObjectKindTable, Access: AccessKindRead},
		{Schema: "app", Name: "target", Kind: ObjectKindTable, Access: AccessKindRead},
	}
	if !reflect.DeepEqual(references, want) {
		t.Fatalf("references = %#v, want %#v", references, want)
	}
}

func TestCollectObjectReferencesDMLAccess(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  []ObjectReference
	}{
		{
			name:  "insert select",
			query: "INSERT INTO app.target SELECT * FROM source",
			want: []ObjectReference{
				{Schema: "app", Name: "source", Kind: ObjectKindTable, Access: AccessKindRead},
				{Schema: "app", Name: "target", Kind: ObjectKindTable, Access: AccessKindWrite},
			},
		},
		{
			name:  "delete",
			query: "DELETE FROM target WHERE id IN (SELECT id FROM app.source)",
			want: []ObjectReference{
				{Schema: "app", Name: "source", Kind: ObjectKindTable, Access: AccessKindRead},
				{Schema: "app", Name: "target", Kind: ObjectKindTable, Access: AccessKindWrite},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseForExecution(tc.query)
			if err != nil {
				t.Fatalf("ParseForExecution() error = %v", err)
			}
			references, err := CollectObjectReferences(parsed.AST, "app")
			if err != nil {
				t.Fatalf("CollectObjectReferences() error = %v", err)
			}
			if !reflect.DeepEqual(references, tc.want) {
				t.Fatalf("references = %#v, want %#v", references, tc.want)
			}
		})
	}
}
