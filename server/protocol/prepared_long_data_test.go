package protocol

import (
	"bytes"
	"testing"
)

func TestPreparedStatementLongDataAccumulatesAndResets(t *testing.T) {
	mgr := NewPreparedStatementManager()
	stmt, err := mgr.Prepare("insert into blobs (payload) values (?)")
	if err != nil {
		t.Fatal(err)
	}

	if err := mgr.AppendLongData(stmt.ID, 0, []byte("hello ")); err != nil {
		t.Fatal(err)
	}
	if err := mgr.AppendLongData(stmt.ID, 0, []byte("world")); err != nil {
		t.Fatal(err)
	}
	data, err := mgr.ConsumeLongData(stmt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data[0], []byte("hello world")) {
		t.Fatalf("long data = %q, want %q", data[0], "hello world")
	}
	data, err = mgr.ConsumeLongData(stmt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("long data after consume = %#v, want empty", data)
	}
}
