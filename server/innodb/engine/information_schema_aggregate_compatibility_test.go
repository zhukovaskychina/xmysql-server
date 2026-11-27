package engine

import "testing"

func TestNormalizeInformationSchemaAggregateCount(t *testing.T) {
	base := newInformationSchemaSelectResult("information_schema_columns", []string{"TABLE_NAME"}, [][]interface{}{{"one"}, {"two"}})
	result := normalizeInformationSchemaAggregate("SELECT COUNT(*) AS count FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix'", base)
	if result == nil || len(result.Columns) != 1 || result.Columns[0] != "count" {
		t.Fatalf("aggregate columns = %#v, want [count]", result)
	}
	if len(result.Records) != 1 || len(result.Records[0].GetValues()) != 1 || result.Records[0].GetValues()[0].Int() != int64(2) {
		t.Fatalf("aggregate rows = %#v, want [[2]]", result.Records)
	}
}

func TestNormalizeInformationSchemaAggregateIfCount(t *testing.T) {
	query := "SELECT IF(COUNT(*) > 0, 'PASS', 'FAIL') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix'"
	result := normalizeInformationSchemaAggregate(query, newInformationSchemaSelectResult("information_schema_columns", []string{"TABLE_NAME"}, [][]interface{}{{"one"}}))
	if result == nil || len(result.Records) != 1 || result.Records[0].GetValues()[0].String() != "PASS" {
		t.Fatalf("aggregate IF rows = %#v, want [[PASS]]", result.Records)
	}

	empty := normalizeInformationSchemaAggregate(query, newInformationSchemaSelectResult("information_schema_columns", []string{"TABLE_NAME"}, nil))
	if empty == nil || len(empty.Records) != 1 || empty.Records[0].GetValues()[0].String() != "FAIL" {
		t.Fatalf("empty aggregate IF rows = %#v, want [[FAIL]]", empty.Records)
	}
}
