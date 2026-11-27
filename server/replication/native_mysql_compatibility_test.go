package replication

import (
	"path/filepath"
	"testing"

	mysqlreplication "github.com/go-mysql-org/go-mysql/replication"
)

func TestNativeBinlogGTIDMetadataParsesWithGoMySQL(t *testing.T) {
	source, err := NewSource(t.TempDir(), "debug-source", 3401)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.AppendCommittedTransaction([]RowChange{{Table: "app.rows", Action: "insert", After: map[string]interface{}{"id": int64(3)}}}, nil); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filepath.Dir(source.Writer.path), "binlog.000001")
	p := mysqlreplication.NewBinlogParser()
	err = p.ParseFile(path, 4, func(e *mysqlreplication.BinlogEvent) error {
		if e.Header.EventType == mysqlreplication.GTID_EVENT {
			if _, ok := e.Event.(*mysqlreplication.GTIDEvent); !ok {
				t.Fatalf("GTID event decoded as %T", e.Event)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
}
