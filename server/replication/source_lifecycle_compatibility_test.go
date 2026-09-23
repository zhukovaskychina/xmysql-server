package replication

import (
	"strings"
	"testing"
)

func TestUninitializedSourceExportAndAppendReturnErrors(t *testing.T) {
	var source *Source
	if _, err := source.Dump(4); err == nil || !strings.Contains(err.Error(), "replication source is nil") {
		t.Fatalf("nil Source.Dump() error = %v", err)
	}
	if _, err := source.Append(1, nil); err == nil || !strings.Contains(err.Error(), "replication source is nil") {
		t.Fatalf("nil Source.Append() error = %v", err)
	}
	if _, err := source.DumpFile("binlog.000001", 4); err == nil || !strings.Contains(err.Error(), "replication source is nil") {
		t.Fatalf("nil Source.DumpFile() error = %v", err)
	}

	partial := &Source{}
	if _, err := partial.Dump(4); err == nil || !strings.Contains(err.Error(), "replication source writer is nil") {
		t.Fatalf("partial Source.Dump() error = %v", err)
	}
	if _, err := partial.Append(1, nil); err == nil || !strings.Contains(err.Error(), "replication source writer is nil") {
		t.Fatalf("partial Source.Append() error = %v", err)
	}
}
