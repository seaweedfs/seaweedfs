package engine

import (
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// EventShape classifies a meta-log event for routing. The reader fills this
// in from the raw meta-log entry (OldEntry/NewEntry diff) before calling
// MatchOriginalWrite or MatchPredicateChange.
type EventShape int

const (
	EventShapeUnknown EventShape = iota
	// EventShapeOriginalWrite covers any event that establishes a new
	// "object age" for AWS-semantic purposes: fresh PUT, overwrite (mtime
	// or content changed), MPU init, version flip.
	EventShapeOriginalWrite
	// EventShapePredicateChange is metadata-only: tags / Extended changed
	// without resetting mtime. Drives the predicate-change sweep.
	EventShapePredicateChange
)

// Event is the routing-relevant slice of a meta-log event. The reader
// extracts these fields from the persisted *filer_pb.LogEntry payload (see
// Phase 3 reader pseudocode); the engine only needs this minimal shape so
// it stays free of filer_pb dependencies.
type Event struct {
	Shape    EventShape
	Bucket   string
	Path     string // bucket-relative key, no leading slash
	Tags     map[string]string
	Size     int64
	IsLatest bool
	// IsDeleteMarker indicates this entry is an S3 delete marker (per the
	// ExtDeleteMarkerKey on the live entry's Extended). Used to gate
	// EXPIRED_DELETE_MARKER routing.
	IsDeleteMarker bool
	// IsMPUInit indicates this event is for an in-flight multipart upload
	// init under <bucket>/.uploads/<id>/. Used to gate ABORT_MPU routing.
	IsMPUInit bool
	// EventTime is when the event was logged. Currently unused by Match*
	// (the cutoff is supplied by the reader's sweep cadence) but available
	// for future fan-out logic.
	EventTime time.Time
}

// MatchOriginalWrite returns the active ActionKeys in the given delay group
// whose filter matches this event. The reader sweeps each delay group once
// per pass and dispatches each event to the matched actions; the worker
// then re-fetches the live entry once and calls EvaluateAction per matched
// key.
//
// Match returns nil when nothing applies (the reader advances the cursor
// without dispatching).
func (s *Snapshot) MatchOriginalWrite(ev *Event, delay time.Duration) []s3lifecycle.ActionKey {
	if ev == nil || ev.Shape != EventShapeOriginalWrite {
		return nil
	}
	keys := s.originalDelayGroups[delay]
	if len(keys) == 0 {
		return nil
	}
	return s.filterMatching(keys, ev)
}

// MatchPredicateChange returns the active predicate-sensitive ActionKeys
// whose filter matches this event. Used by the single near-now predicate
// sweep; the reader pulls live entry tags/size and feeds an Event with
// Shape=EventShapePredicateChange.
func (s *Snapshot) MatchPredicateChange(ev *Event) []s3lifecycle.ActionKey {
	if ev == nil || ev.Shape != EventShapePredicateChange {
		return nil
	}
	if len(s.predicateActions) == 0 {
		return nil
	}
	return s.filterMatching(s.predicateActions, ev)
}

// MatchPath returns the active ActionKeys for a specific bucket+path,
// regardless of event shape. Used by the bootstrap walker, which iterates
// every entry once and evaluates all applicable actions per object.
//
// When ev != nil, the result is filtered to actions whose filter matches
// the live entry's tags/size. When ev == nil, only prefix matching applies
// (caller will fetch live state and filter further).
func (s *Snapshot) MatchPath(bucket, path string, ev *Event) []s3lifecycle.ActionKey {
	bi := s.buckets[bucket]
	if bi == nil {
		return nil
	}
	out := make([]s3lifecycle.ActionKey, 0, len(bi.actionKeys))
	for _, k := range bi.actionKeys {
		a := s.actions[k]
		if a == nil || !a.IsActive() {
			continue
		}
		if !prefixMatches(a.Rule.Prefix, path) {
			continue
		}
		if ev != nil && !filterAllows(a.Rule, ev) {
			continue
		}
		out = append(out, k)
	}
	return out
}

func (s *Snapshot) filterMatching(keys []s3lifecycle.ActionKey, ev *Event) []s3lifecycle.ActionKey {
	out := make([]s3lifecycle.ActionKey, 0, len(keys))
	for _, k := range keys {
		a := s.actions[k]
		if a == nil || !a.IsActive() || a.Bucket != ev.Bucket {
			continue
		}
		if !prefixMatches(a.Rule.Prefix, ev.Path) {
			continue
		}
		if !filterAllows(a.Rule, ev) {
			continue
		}
		// Shape-specific gating: ABORT_MPU only on MPU-init events;
		// EXPIRED_DELETE_MARKER only on delete-marker events. The
		// caller's EvaluateAction re-checks this against the live
		// entry, but pruning here avoids dispatching uninteresting
		// events into the worker.
		switch k.ActionKind {
		case s3lifecycle.ActionKindAbortMPU:
			if !ev.IsMPUInit {
				continue
			}
		case s3lifecycle.ActionKindExpiredDeleteMarker:
			if !ev.IsDeleteMarker || !ev.IsLatest {
				continue
			}
		}
		out = append(out, k)
	}
	return out
}

func prefixMatches(rulePrefix, path string) bool {
	if rulePrefix == "" {
		return true
	}
	return strings.HasPrefix(path, rulePrefix)
}

func filterAllows(rule *s3lifecycle.Rule, ev *Event) bool {
	if rule.FilterSizeGreaterThan > 0 && ev.Size <= rule.FilterSizeGreaterThan {
		return false
	}
	if rule.FilterSizeLessThan > 0 && ev.Size >= rule.FilterSizeLessThan {
		return false
	}
	for k, v := range rule.FilterTags {
		if got, ok := ev.Tags[k]; !ok || got != v {
			return false
		}
	}
	return true
}
