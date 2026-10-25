package engine

import (
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

type EventShape int

const (
	EventShapeUnknown EventShape = iota
	// EventShapeOriginalWrite: any event resetting AWS LastModified
	// (fresh PUT, overwrite, MPU init, version flip).
	EventShapeOriginalWrite
	// EventShapePredicateChange: tags / Extended changed without an mtime
	// reset.
	EventShapePredicateChange
)

// Event is the routing-relevant slice of a meta-log event. Kept minimal so
// the engine doesn't depend on filer_pb.
type Event struct {
	Shape          EventShape
	Bucket         string
	Path           string
	Tags           map[string]string
	Size           int64
	IsLatest       bool
	IsDeleteMarker bool
	IsMPUInit      bool
	EventTime      time.Time
}

// MatchOriginalWrite returns active ActionKeys in the given delay group
// whose filter matches the event.
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

// MatchPredicateChange returns active predicate-sensitive ActionKeys whose
// filter matches the event.
func (s *Snapshot) MatchPredicateChange(ev *Event) []s3lifecycle.ActionKey {
	if ev == nil || ev.Shape != EventShapePredicateChange {
		return nil
	}
	if len(s.predicateActions) == 0 {
		return nil
	}
	return s.filterMatching(s.predicateActions, ev)
}

// MatchPath returns active ActionKeys for a specific bucket+path. ev=nil
// applies prefix matching only; pass a non-nil Event to also gate on tags
// and size.
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
