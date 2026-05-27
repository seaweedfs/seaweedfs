package iceberg

import (
	"bytes"
	"encoding/json"
)

// specRequiredEmptyDefaults holds the sentinel values Iceberg requires when
// the corresponding state is empty. Maintained at package scope so we don't
// re-allocate per call.
var specRequiredEmptyDefaults = map[string]json.RawMessage{
	"current-snapshot-id": json.RawMessage("-1"),
	"snapshots":           json.RawMessage("[]"),
	"snapshot-log":        json.RawMessage("[]"),
	"metadata-log":        json.RawMessage("[]"),
	"refs":                json.RawMessage("{}"),
}

// isJSONNull reports whether raw encodes the JSON null literal, ignoring
// insignificant whitespace allowed by RFC 8259.
func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// ensureMetadataSpecCompliance backfills Iceberg spec-required fields that
// iceberg-go v0.5.0 drops via `omitempty` on optional pointer/slice fields.
//
// For tables with no snapshots (e.g. freshly created tables), iceberg-go
// represents an empty state with nil/empty Go values, then strips those keys
// entirely from the JSON output. The Iceberg REST spec, however, requires
// these keys to be present even when empty — Java/Spark/Trino clients
// fail to parse responses missing them (most visibly with
// "Cannot parse missing long current-snapshot-id"). This helper rehydrates
// the missing keys without overwriting any present real values.
//
// Returns the original bytes unchanged when parsing fails or no key needs
// to be added, to avoid corrupting an otherwise-valid payload.
func ensureMetadataSpecCompliance(raw []byte) []byte {
	if len(raw) == 0 {
		return raw
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}

	// A field is "missing" for spec purposes if it's absent OR encoded as
	// JSON null. Some writers emit `"current-snapshot-id": null` for empty
	// state instead of omitting; strict clients reject both the same way.
	changed := false
	for key, fallback := range specRequiredEmptyDefaults {
		v, present := obj[key]
		if !present || isJSONNull(v) {
			obj[key] = fallback
			changed = true
		}
	}
	if !changed {
		return raw
	}
	fixed, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return fixed
}
