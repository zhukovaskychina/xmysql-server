package plan

import "testing"

func TestGTIDSubsetAndSubtractKeepIntervalsCompact(t *testing.T) {
	if got, err := evalGTIDFunction("GTID_SUBSET", []interface{}{
		"uuid-b:4-5:8,uuid-a:1-3", "uuid-a:1-10,uuid-b:1-9",
	}); err != nil || got != int64(1) {
		t.Fatalf("GTID_SUBSET = %#v, %v; want 1", got, err)
	}
	got, err := evalGTIDFunction("GTID_SUBTRACT", []interface{}{
		"uuid-a:1-10,uuid-b:4-8", "uuid-a:3-5,uuid-b:6",
	})
	if err != nil || got != "uuid-a:1-2:6-10,uuid-b:4-5:7-8" {
		t.Fatalf("GTID_SUBTRACT = %#v, %v", got, err)
	}
}

func TestGTIDFunctionsRejectInvalidIntervals(t *testing.T) {
	if _, err := evalGTIDFunction("GTID_SUBSET", []interface{}{"uuid:0", "uuid:1"}); err == nil {
		t.Fatal("GTID_SUBSET should reject zero sequence")
	}
	if _, err := evalGTIDFunction("GTID_SUBTRACT", []interface{}{"uuid:4-2", "uuid:1"}); err == nil {
		t.Fatal("GTID_SUBTRACT should reject reversed interval")
	}
}
