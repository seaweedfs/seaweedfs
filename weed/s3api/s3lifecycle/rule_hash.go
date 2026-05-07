package s3lifecycle

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"time"
)

// RuleHash returns the first 8 bytes of sha256 over a canonicalized rule
// representation. The hash is stable across:
//
//   - tag-key reorder (FilterTags is sorted before hashing)
//   - rule.ID changes (ID is excluded — it's display-only)
//   - rule.Status flips (Enabled <-> Disabled — state continuity preserved
//     across operator toggles)
//
// Prefix is hashed verbatim: "logs" and "logs/" produce different hashes
// because they match different objects under literal prefix semantics
// (strings.HasPrefix). Collapsing them would let an edit silently reuse
// per-rule durable state for a rule that no longer matches the same set.
//
// Different action shapes (different days, different filter, different
// action types) hash to different values.
//
// Encoding is length-prefixed to avoid delimiter ambiguity: a tag value
// containing "=" or "\n" or a prefix containing the field-tag separator
// must not be able to forge a different tuple that hashes the same. Each
// scalar is written as `<field-tag-byte> <uvarint-length> <bytes>`.
func RuleHash(rule *Rule) [8]byte {
	if rule == nil {
		var zero [8]byte
		return zero
	}
	h := sha256.New()
	// Filter.
	writeBytes(h, fieldPrefix, []byte(rule.Prefix))
	tagKeys := make([]string, 0, len(rule.FilterTags))
	for k := range rule.FilterTags {
		tagKeys = append(tagKeys, k)
	}
	sort.Strings(tagKeys)
	writeUvarint(h, fieldTagCount, uint64(len(tagKeys)))
	for _, k := range tagKeys {
		writeBytes(h, fieldTagKey, []byte(k))
		writeBytes(h, fieldTagValue, []byte(rule.FilterTags[k]))
	}
	writeInt64(h, fieldSizeGT, rule.FilterSizeGreaterThan)
	writeInt64(h, fieldSizeLT, rule.FilterSizeLessThan)
	// Actions.
	writeInt64(h, fieldExpDays, int64(rule.ExpirationDays))
	writeBytes(h, fieldExpDate, []byte(canonicalTime(rule.ExpirationDate)))
	writeBool(h, fieldExpDeleteMarker, rule.ExpiredObjectDeleteMarker)
	writeInt64(h, fieldNoncurDays, int64(rule.NoncurrentVersionExpirationDays))
	writeInt64(h, fieldNoncurKeep, int64(rule.NewerNoncurrentVersions))
	writeInt64(h, fieldMPUDays, int64(rule.AbortMPUDaysAfterInitiation))

	sum := h.Sum(nil)
	var out [8]byte
	copy(out[:], sum[:8])
	return out
}

// Field tags namespace each scalar so an attacker can't substitute one
// string for another and re-collide. Values are arbitrary but stable.
const (
	fieldPrefix          byte = 0x01
	fieldTagCount        byte = 0x02
	fieldTagKey          byte = 0x03
	fieldTagValue        byte = 0x04
	fieldSizeGT          byte = 0x05
	fieldSizeLT          byte = 0x06
	fieldExpDays         byte = 0x10
	fieldExpDate         byte = 0x11
	fieldExpDeleteMarker byte = 0x12
	fieldNoncurDays      byte = 0x13
	fieldNoncurKeep      byte = 0x14
	fieldMPUDays         byte = 0x15
)

type byteSink interface {
	Write(p []byte) (n int, err error)
}

func writeBytes(w byteSink, tag byte, b []byte) {
	var lenbuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenbuf[:], uint64(len(b)))
	w.Write([]byte{tag})
	w.Write(lenbuf[:n])
	w.Write(b)
}

func writeUvarint(w byteSink, tag byte, v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	w.Write([]byte{tag})
	w.Write(buf[:n])
}

func writeInt64(w byteSink, tag byte, v int64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], v)
	w.Write([]byte{tag})
	w.Write(buf[:n])
}

func writeBool(w byteSink, tag byte, b bool) {
	v := byte(0)
	if b {
		v = 1
	}
	w.Write([]byte{tag, v})
}

func canonicalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
