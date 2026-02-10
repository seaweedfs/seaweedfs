package iceberg

import "testing"

func TestNormalizeNamespacePropertiesNil(t *testing.T) {
	properties := normalizeNamespaceProperties(nil)
	if properties == nil {
		t.Fatalf("normalizeNamespaceProperties(nil) returned nil map")
	}
	if len(properties) != 0 {
		t.Fatalf("normalizeNamespaceProperties(nil) length = %d, want 0", len(properties))
	}
}

func TestNormalizeNamespacePropertiesClonesInput(t *testing.T) {
	input := map[string]string{
		"owner": "analytics",
	}

	properties := normalizeNamespaceProperties(input)
	if properties["owner"] != "analytics" {
		t.Fatalf("normalized properties value = %q, want %q", properties["owner"], "analytics")
	}

	input["owner"] = "mutated"
	if properties["owner"] != "analytics" {
		t.Fatalf("normalized properties was mutated via input map")
	}
}
