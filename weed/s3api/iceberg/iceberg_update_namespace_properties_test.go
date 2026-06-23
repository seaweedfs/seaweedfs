package iceberg

import (
	"reflect"
	"testing"
)

func TestApplyNamespacePropertyUpdates(t *testing.T) {
	current := map[string]string{"foo": "bar", "prop": "yes"}

	props, summary := applyNamespacePropertyUpdates(current, []string{"abc"}, map[string]string{"prop": "no"})

	// Removal of a missing key reports missing, not removed.
	if !reflect.DeepEqual(summary.Removed, []string{}) {
		t.Errorf("Removed = %v, want []", summary.Removed)
	}
	if !reflect.DeepEqual(summary.Updated, []string{"prop"}) {
		t.Errorf("Updated = %v, want [prop]", summary.Updated)
	}
	if !reflect.DeepEqual(summary.Missing, []string{"abc"}) {
		t.Errorf("Missing = %v, want [abc]", summary.Missing)
	}
	if props["prop"] != "no" || props["foo"] != "bar" {
		t.Errorf("merged properties = %v, want foo=bar prop=no", props)
	}

	// Input map must not be mutated.
	if current["prop"] != "yes" {
		t.Errorf("source map mutated: prop = %q, want yes", current["prop"])
	}
}

func TestApplyNamespacePropertyUpdatesRemoveExisting(t *testing.T) {
	props, summary := applyNamespacePropertyUpdates(
		map[string]string{"a": "1", "b": "2"}, []string{"a"}, nil)

	if !reflect.DeepEqual(summary.Removed, []string{"a"}) {
		t.Errorf("Removed = %v, want [a]", summary.Removed)
	}
	if _, ok := props["a"]; ok {
		t.Errorf("key a should have been removed: %v", props)
	}
	if len(summary.Missing) != 0 || len(summary.Updated) != 0 {
		t.Errorf("unexpected summary: %+v", summary)
	}
}

func TestApplyNamespacePropertyUpdatesDuplicateRemovals(t *testing.T) {
	// A repeated removal key must not be reported as both removed and missing.
	_, summary := applyNamespacePropertyUpdates(
		map[string]string{"a": "1"}, []string{"a", "a"}, nil)

	if !reflect.DeepEqual(summary.Removed, []string{"a"}) {
		t.Errorf("Removed = %v, want [a]", summary.Removed)
	}
	if len(summary.Missing) != 0 {
		t.Errorf("Missing = %v, want []", summary.Missing)
	}
}
