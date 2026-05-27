package iceberg

import (
	"bytes"
	"encoding/json"
)

// specRequiredEmptyOrder is iterated in this fixed order so the byte-level
// append in ensureMetadataSpecCompliance produces deterministic output.
var specRequiredEmptyOrder = []string{
	"current-snapshot-id",
	"snapshots",
	"snapshot-log",
	"metadata-log",
	"refs",
}

// specRequiredEmptyDefaults holds the sentinel values Iceberg requires when
// the corresponding state is empty.
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
// Missing keys are spliced in at the byte level just before the closing
// brace so iceberg-go's struct-declared key order is preserved (a naive
// map remarshal would alphabetize every key in the document). The slower
// remarshal path is only used when an explicit JSON null needs replacing,
// which iceberg-go itself never emits.
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
	// A top-level JSON null leaves obj == nil with no error; writes would
	// panic on the slow path. It is not a valid TableMetadata document, so
	// pass it through unchanged.
	if obj == nil {
		return raw
	}

	// A field is "missing" for spec purposes if it's absent OR encoded as
	// JSON null. Some writers emit `"current-snapshot-id": null` for empty
	// state instead of omitting; strict clients reject both the same way.
	var toAppend []string
	hasExplicitNull := false
	for _, key := range specRequiredEmptyOrder {
		v, present := obj[key]
		switch {
		case !present:
			toAppend = append(toAppend, key)
		case isJSONNull(v):
			hasExplicitNull = true
		}
	}
	if len(toAppend) == 0 && !hasExplicitNull {
		return raw
	}

	if !hasExplicitNull {
		return appendMissingObjectKeys(raw, toAppend, len(obj) > 0)
	}

	// Slow path: replace explicit nulls. Rare in practice, so the
	// alphabetical key reordering inherent to map remarshal is acceptable.
	for key, v := range obj {
		if _, required := specRequiredEmptyDefaults[key]; required && isJSONNull(v) {
			obj[key] = specRequiredEmptyDefaults[key]
		}
	}
	for _, key := range toAppend {
		obj[key] = specRequiredEmptyDefaults[key]
	}
	fixed, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return fixed
}

// appendMissingObjectKeys splices the given keys into raw just before the
// closing brace of the top-level JSON object. raw is assumed to be a valid
// JSON object (the caller has already unmarshalled it). hasMembers tells us
// whether the object had any existing members, so we know whether to emit a
// leading comma for the first appended key.
func appendMissingObjectKeys(raw []byte, keys []string, hasMembers bool) []byte {
	closeIdx := bytes.LastIndexByte(raw, '}')
	if closeIdx < 0 {
		return raw
	}
	var buf bytes.Buffer
	buf.Grow(len(raw) + len(keys)*48)
	buf.Write(raw[:closeIdx])
	for _, key := range keys {
		if hasMembers {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		buf.WriteString(key)
		buf.WriteString(`":`)
		buf.Write(specRequiredEmptyDefaults[key])
		hasMembers = true
	}
	buf.Write(raw[closeIdx:])
	return buf.Bytes()
}
