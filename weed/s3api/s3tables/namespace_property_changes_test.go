package s3tables

import (
	"reflect"
	"testing"
)

// These cases moved here from the Iceberg handler's own merge, which this
// function replaces; the summary semantics are unchanged.
func TestApplyNamespacePropertyChanges(t *testing.T) {
	current := map[string]string{"foo": "bar", "prop": "yes"}

	props, removed, missing := ApplyNamespacePropertyChanges(current, []string{"abc"}, map[string]string{"prop": "no"})

	// Removal of a missing key reports missing, not removed.
	if !reflect.DeepEqual(removed, []string{}) {
		t.Errorf("removed = %v, want []", removed)
	}
	if !reflect.DeepEqual(missing, []string{"abc"}) {
		t.Errorf("missing = %v, want [abc]", missing)
	}
	if props["prop"] != "no" || props["foo"] != "bar" {
		t.Errorf("merged properties = %v, want foo=bar prop=no", props)
	}
	// Input map must not be mutated.
	if current["prop"] != "yes" {
		t.Errorf("source map mutated: prop = %q, want yes", current["prop"])
	}
}

func TestApplyNamespacePropertyChangesRemoveExisting(t *testing.T) {
	props, removed, missing := ApplyNamespacePropertyChanges(map[string]string{"a": "1", "b": "2"}, []string{"a"}, nil)

	if !reflect.DeepEqual(removed, []string{"a"}) {
		t.Errorf("removed = %v, want [a]", removed)
	}
	if _, ok := props["a"]; ok {
		t.Errorf("key a should have been removed: %v", props)
	}
	if len(missing) != 0 || props["b"] != "2" {
		t.Errorf("missing = %v, properties = %v", missing, props)
	}
}

func TestApplyNamespacePropertyChangesDuplicateRemovals(t *testing.T) {
	// A repeated removal key must not be reported as both removed and missing.
	_, removed, missing := ApplyNamespacePropertyChanges(map[string]string{"a": "1"}, []string{"a", "a"}, nil)

	if !reflect.DeepEqual(removed, []string{"a"}) {
		t.Errorf("removed = %v, want [a]", removed)
	}
	if len(missing) != 0 {
		t.Errorf("missing = %v, want []", missing)
	}
}
