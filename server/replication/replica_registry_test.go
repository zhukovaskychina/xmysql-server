package replication

import (
	"testing"
	"time"
)

func TestReplicaRegistryReplacesAndProtectsNewerRegistration(t *testing.T) {
	registry := NewReplicaRegistry()
	registeredAt := time.Unix(123, 0).UTC()
	registry.Register("old-connection", ReplicaRegistration{ServerID: 7, ReportHost: "old", RegisteredAt: registeredAt})
	registry.Register("new-connection", ReplicaRegistration{ServerID: 7, ReportHost: "new", RegisteredAt: registeredAt})

	registry.Unregister("old-connection", 7)
	snapshot := registry.Snapshot()
	if len(snapshot) != 1 || snapshot[0].ReportHost != "new" {
		t.Fatalf("old connection removed newer registration: %#v", snapshot)
	}

	registry.Unregister("new-connection", 7)
	if got := registry.Snapshot(); len(got) != 0 {
		t.Fatalf("registration remains after owner closed: %#v", got)
	}
}

func TestReplicaRegistrySnapshotIsSortedAndDetached(t *testing.T) {
	registry := NewReplicaRegistry()
	registry.Register("seven", ReplicaRegistration{ServerID: 7, ReportHost: "seven"})
	registry.Register("three", ReplicaRegistration{ServerID: 3, ReportHost: "three"})

	snapshot := registry.Snapshot()
	if len(snapshot) != 2 || snapshot[0].ServerID != 3 || snapshot[1].ServerID != 7 {
		t.Fatalf("unexpected snapshot order: %#v", snapshot)
	}
	snapshot[0].ReportHost = "mutated"
	if got := registry.Snapshot()[0].ReportHost; got != "three" {
		t.Fatalf("snapshot mutation leaked into registry: %q", got)
	}
}
