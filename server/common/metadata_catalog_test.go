package common

import "testing"

func TestMetadataCatalogViewsAreIndependent(t *testing.T) {
	protocol := ProtocolCharsetIDs()
	protocol["utf8"] = 0
	if ProtocolCharsetIDs()["utf8"] != 33 {
		t.Fatal("protocol view exposed catalog state")
	}
	show := ShowCollations()
	show[0].Name = "changed"
	show[0].SortLen = 999
	if ShowCollations()[0].Name != "utf8mb4_0900_ai_ci" {
		t.Fatal("SHOW view exposed catalog state")
	}
	is := InformationSchemaCollations()[0]
	if is.SortLen != 1 || is.PadAttribute != "PAD SPACE" {
		t.Fatal("information_schema ID 255 compatibility projection changed")
	}
	if show := ShowCollations()[0]; show.SortLen != 0 || show.PadAttribute != "NO PAD" {
		t.Fatal("SHOW ID 255 compatibility projection changed")
	}
	if CharsetNameToID("utf8") != 83 || ProtocolCharsetIDs()["utf8"] != 33 {
		t.Fatal("distinct legacy and protocol default policies merged")
	}
}

func TestShowPrivilegeRowsFollowRuntimeRegistry(t *testing.T) {
	const custom = "XMYSQL_METADATA_CATALOG_TEST"
	if err := RegisterDynamicPrivilege(custom); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = UnregisterDynamicPrivilege(custom) })
	found := false
	for _, row := range ShowPrivilegeRows() {
		if row[0] == custom {
			found = true
			if row[1] != "Global" || row[2] != "Dynamic privilege" {
				t.Fatal(row)
			}
		}
	}
	if !found {
		t.Fatal("runtime registration absent from SHOW")
	}
	if err := UnregisterDynamicPrivilege(custom); err != nil {
		t.Fatal(err)
	}
	for _, row := range ShowPrivilegeRows() {
		if row[0] == custom {
			t.Fatal("runtime unregistration absent from SHOW")
		}
	}
	rows := ShowPrivilegeRows()
	rows[0][0] = "changed"
	if ShowPrivilegeRows()[0][0] == "changed" {
		t.Fatal("SHOW privilege row exposed catalog state")
	}
}

func TestPrivilegeSentinelBitValues(t *testing.T) {
	if UsagePriv != 1 || AllPriv != 1<<33 || ExtendedPriv != 1<<34 || AllPrivMask != (1<<33)-1 {
		t.Fatal("public privilege bit contract changed")
	}
}

func TestStaticGrantPrivilegeScope(t *testing.T) {
	for _, name := range []string{"ALL", "ALL PRIVILEGES", "PROXY", "USAGE", "GRANT OPTION", "CREATE ROLE", "SHUTDOWN"} {
		if !IsStaticGrantPrivilege(name) {
			t.Fatalf("static GRANT name missing: %s", name)
		}
	}
	for _, name := range []string{"CONFIG", "", "select", "UNKNOWN"} {
		if IsStaticGrantPrivilege(name) {
			t.Fatalf("unexpected static GRANT name: %s", name)
		}
	}
}
