package metrics

import "testing"

func TestTableIOReferencesTrustSuccessfulASTParse(t *testing.T) {
	if refs := tableIOReferences("SELECT 'from app.orders' AS text_value", "app"); len(refs) != 0 {
		t.Fatalf("literal produced table references: %#v", refs)
	}
	refs := tableIOReferences("SELECT * FROM app.orders", "app")
	if len(refs) != 1 || refs[0][0] != "app" || refs[0][1] != "orders" {
		t.Fatalf("refs=%#v", refs)
	}
}
