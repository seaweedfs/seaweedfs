package iceberg

import (
	"encoding/json"
	"testing"
)

func TestEnsureMetadataSpecCompliance_BackfillsMissingFields(t *testing.T) {
	// Mirrors the real iceberg-go v0.5.0 output for a freshly created table:
	// current-snapshot-id, snapshots, snapshot-log, metadata-log, refs all absent.
	input := []byte(`{
		"format-version": 2,
		"table-uuid": "82e3eec4-3aee-414f-a444-94c03c641d20",
		"location": "s3://s3table/default/t1",
		"last-sequence-number": 0,
		"last-updated-ms": 1779866785466,
		"last-column-id": 2,
		"current-schema-id": 0,
		"default-spec-id": 0,
		"last-partition-id": 999,
		"default-sort-order-id": 0
	}`)

	out := ensureMetadataSpecCompliance(input)

	var got map[string]json.RawMessage
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}

	if v, ok := got["current-snapshot-id"]; !ok || string(v) != "-1" {
		t.Errorf("current-snapshot-id missing or wrong: present=%v value=%s", ok, string(v))
	}
	for _, key := range []string{"snapshots", "snapshot-log", "metadata-log"} {
		v, ok := got[key]
		if !ok || string(v) != "[]" {
			t.Errorf("%s missing or wrong: present=%v value=%s", key, ok, string(v))
		}
	}
	if v, ok := got["refs"]; !ok || string(v) != "{}" {
		t.Errorf("refs missing or wrong: present=%v value=%s", ok, string(v))
	}
}

func TestEnsureMetadataSpecCompliance_PreservesExistingFields(t *testing.T) {
	// Real snapshot id and refs must not be overwritten by sentinels.
	input := []byte(`{
		"format-version": 2,
		"current-snapshot-id": 9876543210,
		"snapshots": [{"snapshot-id": 9876543210}],
		"snapshot-log": [{"snapshot-id": 9876543210, "timestamp-ms": 1}],
		"metadata-log": [{"metadata-file": "v1.metadata.json", "timestamp-ms": 1}],
		"refs": {"main": {"snapshot-id": 9876543210, "type": "branch"}}
	}`)

	out := ensureMetadataSpecCompliance(input)

	var got map[string]json.RawMessage
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if string(got["current-snapshot-id"]) != "9876543210" {
		t.Errorf("real snapshot id was overwritten: %s", string(got["current-snapshot-id"]))
	}
	if string(got["snapshots"]) == "[]" {
		t.Errorf("non-empty snapshots was overwritten with empty array")
	}
}

func TestEnsureMetadataSpecCompliance_ReplacesExplicitNullsWithSentinels(t *testing.T) {
	// Some writers emit explicit JSON null for unset values instead of omitting
	// the key. Strict Iceberg clients reject these the same way as missing keys.
	input := []byte(`{
		"format-version": 2,
		"current-snapshot-id": null,
		"snapshots": null,
		"snapshot-log": null,
		"metadata-log": null,
		"refs": null
	}`)

	out := ensureMetadataSpecCompliance(input)

	var got map[string]json.RawMessage
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if string(got["current-snapshot-id"]) != "-1" {
		t.Errorf("current-snapshot-id should be replaced with -1, got %s", string(got["current-snapshot-id"]))
	}
	for _, key := range []string{"snapshots", "snapshot-log", "metadata-log"} {
		if string(got[key]) != "[]" {
			t.Errorf("%s should be replaced with [], got %s", key, string(got[key]))
		}
	}
	if string(got["refs"]) != "{}" {
		t.Errorf("refs should be replaced with {}, got %s", string(got["refs"]))
	}
}

func TestEnsureMetadataSpecCompliance_InvalidJSONReturnedUnchanged(t *testing.T) {
	input := []byte(`{not valid json`)
	out := ensureMetadataSpecCompliance(input)
	if string(out) != string(input) {
		t.Errorf("invalid JSON should be returned unchanged; got %s", string(out))
	}
}

func TestEnsureMetadataSpecCompliance_EmptyInputReturnedUnchanged(t *testing.T) {
	if out := ensureMetadataSpecCompliance(nil); out != nil {
		t.Errorf("nil input should be returned unchanged, got %v", out)
	}
	if out := ensureMetadataSpecCompliance([]byte{}); len(out) != 0 {
		t.Errorf("empty input should be returned unchanged, got %v", out)
	}
	// A top-level JSON null literal must not panic on the slow path.
	if out := ensureMetadataSpecCompliance([]byte("null")); string(out) != "null" {
		t.Errorf("top-level null should be returned unchanged, got %s", string(out))
	}
}

// Original iceberg-go key ordering must survive the backfill: appended
// sentinels go at the end without disturbing prior fields. A map-based
// remarshal would have sorted everything alphabetically.
func TestEnsureMetadataSpecCompliance_PreservesOriginalKeyOrder(t *testing.T) {
	// Compact JSON, keys in struct-declared order from iceberg-go.
	input := []byte(`{"format-version":2,"table-uuid":"82e3eec4-3aee-414f-a444-94c03c641d20","location":"s3://x/t","last-sequence-number":0,"last-updated-ms":1,"last-column-id":2,"current-schema-id":0,"default-spec-id":0,"last-partition-id":999,"default-sort-order-id":0}`)

	out := ensureMetadataSpecCompliance(input)

	// Prior keys keep their order, sentinels appended at the end.
	want := `{"format-version":2,"table-uuid":"82e3eec4-3aee-414f-a444-94c03c641d20","location":"s3://x/t","last-sequence-number":0,"last-updated-ms":1,"last-column-id":2,"current-schema-id":0,"default-spec-id":0,"last-partition-id":999,"default-sort-order-id":0,"current-snapshot-id":-1,"snapshots":[],"snapshot-log":[],"metadata-log":[],"refs":{}}`
	if string(out) != want {
		t.Errorf("unexpected output\n got: %s\nwant: %s", string(out), want)
	}

	// Sanity: still valid JSON.
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
}

// An empty object {} must round-trip to a valid JSON object containing
// only the spec sentinels (no leading comma).
func TestEnsureMetadataSpecCompliance_EmptyObjectBackfilled(t *testing.T) {
	out := ensureMetadataSpecCompliance([]byte(`{}`))
	want := `{"current-snapshot-id":-1,"snapshots":[],"snapshot-log":[],"metadata-log":[],"refs":{}}`
	if string(out) != want {
		t.Errorf("unexpected output\n got: %s\nwant: %s", string(out), want)
	}
}

// When all fields are already present, the original bytes must be returned
// untouched (no whitespace normalization, no key reordering).
func TestEnsureMetadataSpecCompliance_AllPresentReturnsSameBytes(t *testing.T) {
	input := []byte("{\n  \"current-snapshot-id\": 1,\n  \"snapshots\": [],\n  \"snapshot-log\": [],\n  \"metadata-log\": [],\n  \"refs\": {}\n}")
	out := ensureMetadataSpecCompliance(input)
	if string(out) != string(input) {
		t.Errorf("expected original bytes returned unchanged\n got: %q\nwant: %q", string(out), string(input))
	}
}
