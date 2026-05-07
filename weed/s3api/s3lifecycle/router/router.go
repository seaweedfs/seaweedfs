package router

import (
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
)

// Match is one (event, action) pair where EvaluateAction fired. The
// dispatcher runs `LifecycleDelete` at DueTime; identity-CAS in the RPC
// guards against drift between schedule time and dispatch time.
type Match struct {
	Key      s3lifecycle.ActionKey
	Action   *engine.CompiledAction
	Result   s3lifecycle.EvalResult
	EventTs  time.Time
	DueTime  time.Time
	Bucket   string
	ObjectKey string
	VersionID string
	Identity *EntryIdentity
}

// EntryIdentity is the schedule-time CAS witness; the dispatcher serializes
// it into the LifecycleDelete request. The fields mirror
// s3_lifecycle_pb.EntryIdentity but stay in-package so the router doesn't
// pull a proto dependency.
type EntryIdentity struct {
	MtimeNs int64
	Size    int64
	HeadFid string
}

// Route returns the matches that fire for ev against snap. Only EVENT_DRIVEN
// actions on this bucket are considered; actions in SCAN_AT_DATE or DISABLED
// modes are out-of-band of the event stream. Inactive actions
// (BootstrapComplete=false) are also skipped.
func Route(snap *engine.Snapshot, ev *reader.Event, now time.Time) []Match {
	if snap == nil || ev == nil {
		return nil
	}
	keys := snap.BucketActionKeys(ev.Bucket)
	if len(keys) == 0 {
		return nil
	}
	versioned := snap.BucketVersioned(ev.Bucket)
	info := buildObjectInfo(ev, versioned)
	if info == nil {
		return nil
	}
	eventTime := time.Unix(0, ev.TsNs)

	var matches []Match
	for _, key := range keys {
		action := snap.Action(key)
		if action == nil || !action.IsActive() {
			continue
		}
		if action.Mode != engine.ModeEventDriven {
			continue
		}
		// Evaluate at the scheduled dispatch time, not the event time:
		// ExpirationDays gates on now >= modtime + N, which is exactly
		// what holds when we actually dispatch. The dispatcher's
		// identity-CAS catches drift if the object changes meanwhile.
		dueTime := eventTime.Add(action.Delay)
		res := s3lifecycle.EvaluateAction(action.Rule, key.ActionKind, info, dueTime)
		if res.Action == s3lifecycle.ActionNone {
			continue
		}
		matches = append(matches, Match{
			Key:       key,
			Action:    action,
			Result:    res,
			EventTs:   eventTime,
			DueTime:   dueTime,
			Bucket:    ev.Bucket,
			ObjectKey: ev.Key,
			Identity:  buildIdentity(ev),
		})
	}
	return matches
}

// buildObjectInfo derives a non-versioned ObjectInfo from a meta-log event.
// Versioned-bucket semantics (IsLatest, NumVersions, NoncurrentIndex,
// IsDeleteMarker for noncurrent versions) require listing siblings and land
// in Phase 5; for now an event on a versioned bucket is treated as
// IsLatest=true with the same caveat that the LifecycleDelete RPC's
// identity-CAS catches stale schedules.
func buildObjectInfo(ev *reader.Event, versioned bool) *s3lifecycle.ObjectInfo {
	entry := ev.NewEntry
	if entry == nil {
		entry = ev.OldEntry
	}
	if entry == nil {
		return nil
	}
	info := &s3lifecycle.ObjectInfo{
		Key:         ev.Key,
		IsLatest:    true,
		NumVersions: 1,
	}
	if entry.Attributes != nil {
		info.ModTime = time.Unix(entry.Attributes.Mtime, 0)
		info.Size = int64(entry.Attributes.FileSize)
	}
	if tags := extractTags(entry.Extended); len(tags) > 0 {
		info.Tags = tags
	}
	if isDeleteMarkerEntry(entry) {
		info.IsDeleteMarker = true
	}
	_ = versioned
	return info
}

// buildIdentity captures the entry's schedule-time fingerprint for the CAS
// witness. Returns nil if the event has no entry to fingerprint (deletes).
func buildIdentity(ev *reader.Event) *EntryIdentity {
	entry := ev.NewEntry
	if entry == nil {
		return nil
	}
	id := &EntryIdentity{}
	if entry.Attributes != nil {
		id.MtimeNs = entry.Attributes.Mtime
		id.Size = int64(entry.Attributes.FileSize)
	}
	if len(entry.GetChunks()) > 0 {
		id.HeadFid = entry.GetChunks()[0].FileId
	}
	return id
}

func extractTags(ext map[string][]byte) map[string]string {
	if len(ext) == 0 {
		return nil
	}
	prefix := s3_constants.AmzObjectTagging + "-"
	var out map[string]string
	for k, v := range ext {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[k[len(prefix):]] = string(v)
	}
	return out
}

func isDeleteMarkerEntry(entry *filer_pb.Entry) bool {
	if entry == nil || len(entry.Extended) == 0 {
		return false
	}
	v, ok := entry.Extended[s3_constants.ExtDeleteMarkerKey]
	return ok && len(v) == 1 && v[0] == 1
}
