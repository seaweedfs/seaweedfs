package iceberg

import (
	"encoding/json"
)

// normalizeRequirements repairs requirement JSON that iceberg-go's parser
// rejects but the Iceberg REST spec permits.
//
// iceberg-go's assert-ref-snapshot-id parser uses a nullableInt64 that tracks
// whether the "snapshot-id" field was present in the JSON. An absent field
// (set == false) is rejected as "missing required field", even though the spec
// makes snapshot-id optional — null means the ref must not already exist.
// iceberg-go v0.6.0 used a plain *int64, so absent was nil and accepted.
//
// Clients like ClickHouse send assert-ref-snapshot-id without snapshot-id when
// asserting a branch does not yet exist. Splicing in an explicit null before
// unmarshaling restores the v0.6.0 behavior across both iceberg-go versions.
func normalizeRequirements(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return raw, nil
	}
	var reqs []json.RawMessage
	if err := json.Unmarshal(raw, &reqs); err != nil {
		return raw, err
	}
	changed := false
	for i, req := range reqs {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(req, &obj); err != nil {
			continue // let the downstream parser report the error
		}
		var typ string
		if rawType, ok := obj["type"]; ok {
			_ = json.Unmarshal(rawType, &typ)
		}
		if typ != "assert-ref-snapshot-id" {
			continue
		}
		if _, hasSnapshotID := obj["snapshot-id"]; hasSnapshotID {
			continue
		}
		obj["snapshot-id"] = json.RawMessage("null")
		fixed, err := json.Marshal(obj)
		if err != nil {
			continue
		}
		reqs[i] = fixed
		changed = true
	}
	if !changed {
		return raw, nil
	}
	return json.Marshal(reqs)
}
