package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// ReplayContentHash hashes the content (action kind, predicate, TTL value)
// of every replay-eligible compiled action in the base snapshot, returning
// the empty hash when no replay-eligible action exists. The hash is:
//   - Partition-independent. A retention-driven scan_only promotion does
//     NOT change this hash; only the dispatch path changes, not the rule
//     content. (PromotedHash exists to catch partition flips separately.)
//   - Stable across snapshot reorderings. Actions are pre-sorted by
//     RuleHash + ActionKind + Bucket so two snapshots with the same rules
//     compiled in any order hash identically.
//   - Disabled-rule-aware. ModeDisabled actions are excluded so disabling
//     a rule changes the hash (it changed the rule set the worker is
//     scanning under).
//
// Used as cursor.RuleSetHash. A mismatch between persisted and current
// triggers the recovery branch on next daily_run.
func ReplayContentHash(s *Snapshot) [32]byte {
	var empty [32]byte
	if s == nil {
		return empty
	}
	type item struct {
		key    s3lifecycle.ActionKey
		action *CompiledAction
	}
	var items []item
	for k, a := range s.actions {
		if a == nil || a.Mode == ModeDisabled {
			continue
		}
		if !isReplayKind(k.ActionKind) {
			continue
		}
		items = append(items, item{key: k, action: a})
	}
	if len(items) == 0 {
		return empty
	}
	// Sort by RuleHash, then ActionKind, then Bucket. RuleHash is already a
	// content-derived identifier, but we don't rely on it alone — including
	// the bucket scope means two buckets carrying the same XML still hash
	// distinctly (matches the ActionKey identity model). ActionKind
	// disambiguates siblings that share a RuleHash.
	sort.Slice(items, func(i, j int) bool {
		if c := bytes.Compare(items[i].key.RuleHash[:], items[j].key.RuleHash[:]); c != 0 {
			return c < 0
		}
		if items[i].key.ActionKind != items[j].key.ActionKind {
			return items[i].key.ActionKind < items[j].key.ActionKind
		}
		return items[i].key.Bucket < items[j].key.Bucket
	})

	h := sha256.New()
	var lenbuf [binary.MaxVarintLen64]byte
	writeField := func(tag byte, b []byte) {
		h.Write([]byte{tag})
		n := binary.PutUvarint(lenbuf[:], uint64(len(b)))
		h.Write(lenbuf[:n])
		h.Write(b)
	}
	writeInt := func(tag byte, v int64) {
		h.Write([]byte{tag})
		n := binary.PutVarint(lenbuf[:], v)
		h.Write(lenbuf[:n])
	}
	for _, it := range items {
		writeField(0x01, []byte(it.key.Bucket))
		writeField(0x02, it.key.RuleHash[:])
		writeInt(0x03, int64(it.key.ActionKind))
		// RuleHash already covers the predicate (Prefix + FilterTags + size
		// filters) and per-kind TTLs, so we don't need to re-canonicalise
		// the *Rule. But we also include the action's effective TTL
		// directly so that an "effective TTL of 0" (a malformed rule where
		// the kind doesn't match the populated field) is distinguishable
		// from a valid one.
		writeInt(0x04, int64(effectiveTTL(it.action)))
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

// PromotedHash hashes the set of replay-eligible actions that *would* land
// in walk (rather than replay) for the given retentionWindow, due to TTL >
// retentionWindow. Empty hash when no rules are promoted. Takes the SAME
// retentionWindow value as RulesForShard so the two helpers cannot disagree
// about partition membership.
//
// Detects partition flips in either direction:
//   - replay → walk (retention dropped): rule appears in this hash but
//     didn't before.
//   - walk → replay (retention recovered): rule used to appear here but no
//     longer does.
// In both cases the persisted hash differs from the freshly computed one,
// firing the recovery branch.
//
// A mismatch with the persisted PromotedHash triggers recovery even when
// rule content is unchanged.
func PromotedHash(s *Snapshot, retentionWindow time.Duration) [32]byte {
	var empty [32]byte
	if s == nil {
		return empty
	}
	type item struct {
		key    s3lifecycle.ActionKey
		action *CompiledAction
	}
	var items []item
	for k, a := range s.actions {
		if a == nil || a.Mode == ModeDisabled {
			continue
		}
		if !isReplayKind(k.ActionKind) {
			continue
		}
		ttl := effectiveTTL(a)
		// Mirror RulesForShard's partition predicate exactly: a replay
		// kind lands in walk when ttl is 0 (malformed) or ttl >
		// retentionWindow. PromotedHash hashes that walk-bound subset.
		if ttl > 0 && ttl <= retentionWindow {
			continue
		}
		items = append(items, item{key: k, action: a})
	}
	if len(items) == 0 {
		return empty
	}
	sort.Slice(items, func(i, j int) bool {
		if c := bytes.Compare(items[i].key.RuleHash[:], items[j].key.RuleHash[:]); c != 0 {
			return c < 0
		}
		if items[i].key.ActionKind != items[j].key.ActionKind {
			return items[i].key.ActionKind < items[j].key.ActionKind
		}
		return items[i].key.Bucket < items[j].key.Bucket
	})

	h := sha256.New()
	var lenbuf [binary.MaxVarintLen64]byte
	writeField := func(tag byte, b []byte) {
		h.Write([]byte{tag})
		n := binary.PutUvarint(lenbuf[:], uint64(len(b)))
		h.Write(lenbuf[:n])
		h.Write(b)
	}
	writeInt := func(tag byte, v int64) {
		h.Write([]byte{tag})
		n := binary.PutVarint(lenbuf[:], v)
		h.Write(lenbuf[:n])
	}
	for _, it := range items {
		writeField(0x01, []byte(it.key.Bucket))
		writeField(0x02, it.key.RuleHash[:])
		writeInt(0x03, int64(it.key.ActionKind))
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

// MaxEffectiveTTL returns the maximum effective TTL across the *active*
// replay-eligible actions in s. Returns 0 for a nil snapshot or one with no
// active replay actions; the caller is expected to be in the empty-replay
// branch already (per the design's sentinel-cursor logic).
//
// "Effective TTL" mirrors the partition predicate in views.go: derived from
// the rule field that matches the action kind. Walker-only action kinds
// (ExpirationDate / ExpiredDeleteMarker / NewerNoncurrent) contribute
// nothing — they're either not replay-eligible or, in a walk view, not
// active in the replay sense.
func MaxEffectiveTTL(s *Snapshot) time.Duration {
	if s == nil {
		return 0
	}
	var max time.Duration
	for k, a := range s.actions {
		if a == nil || !a.IsActive() {
			continue
		}
		if !isReplayKind(k.ActionKind) {
			continue
		}
		if ttl := effectiveTTL(a); ttl > max {
			max = ttl
		}
	}
	return max
}
