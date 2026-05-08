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
	MtimeNs      int64
	Size         int64
	HeadFid      string
	ExtendedHash []byte
}

// Route returns the matches that fire for ev against snap. Only EVENT_DRIVEN
// actions on this bucket are considered; actions in SCAN_AT_DATE or DISABLED
// modes are out-of-band of the event stream. Inactive actions
// (BootstrapComplete=false) are also skipped.
func Route(snap *engine.Snapshot, ev *reader.Event, now time.Time) []Match {
	if snap == nil || ev == nil {
		return nil
	}
	// Hard deletes carry no schedule-relevant state: an Expiration would
	// hit NOOP_RESOLVED at dispatch time anyway, ExpiredObjectDeleteMarker
	// only fires on the latest-version delete-marker which is a Create
	// from the server's perspective. Skip rather than burn a schedule slot.
	if ev.NewEntry == nil {
		return nil
	}
	keys := snap.BucketActionKeys(ev.Bucket)
	if len(keys) == 0 {
		return nil
	}
	info := buildObjectInfo(ev)
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
		// (kind, info) shape gate: ABORT_MPU only fires on MPU init events,
		// and other kinds never do. Without this an MPU init would be
		// matched against NONCURRENT_DAYS (IsLatest=false reads as a
		// non-current version) and the dispatcher would BLOCK on empty
		// version_id.
		if info.IsMPUInit && key.ActionKind != s3lifecycle.ActionKindAbortMPU {
			continue
		}
		if !info.IsMPUInit && key.ActionKind == s3lifecycle.ActionKindAbortMPU {
			continue
		}
		// Schedule from ModTime, not the meta-log event time: a backdated
		// or out-of-band entry update has eventTime ≈ now but ModTime far
		// in the past, so eventTime+Delay would push the dispatch into the
		// future even though the rule fires immediately. ModTime+Delay is
		// the correct fire moment; the dispatcher's identity-CAS catches
		// drift if the object changes meanwhile.
		dueTime := info.ModTime.Add(action.Delay)
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
// in Phase 5; for now any non-MPU event is treated as IsLatest=true with the
// LifecycleDelete RPC's identity-CAS catching stale schedules.
//
// MPU init directories at .uploads/<upload_id> populate IsMPUInit and use the
// destination object key from the entry's Extended map for filter matching,
// so a rule with Filter.Prefix=foo/ matches an MPU uploading to foo/bar.txt.
//
// Returns nil when Attributes are missing — without ModTime, EvaluateAction
// would compute due against year-0001 and fire immediately.
func buildObjectInfo(ev *reader.Event) *s3lifecycle.ObjectInfo {
	entry := ev.NewEntry
	if entry == nil || entry.Attributes == nil {
		return nil
	}
	if destKey, ok := mpuInitInfo(ev, entry); ok {
		// MPU intermediate state has no tags, no versions, no delete-marker
		// semantics. info.Key is the user's destination key so a rule's
		// Filter.Prefix matches the eventual object; the dispatcher already
		// carries .uploads/<upload_id> in m.ObjectKey for ABORT_MPU.
		return &s3lifecycle.ObjectInfo{
			Key:       destKey,
			ModTime:   time.Unix(entry.Attributes.Mtime, int64(entry.Attributes.MtimeNs)),
			IsMPUInit: true,
		}
	}
	info := &s3lifecycle.ObjectInfo{
		Key:         ev.Key,
		ModTime:     time.Unix(entry.Attributes.Mtime, int64(entry.Attributes.MtimeNs)),
		Size:        int64(entry.Attributes.FileSize),
		IsLatest:    true,
		NumVersions: 1,
	}
	if tags := extractTags(entry.Extended); len(tags) > 0 {
		info.Tags = tags
	}
	if isDeleteMarkerEntry(entry) {
		info.IsDeleteMarker = true
	}
	return info
}

// mpuInitInfo recognizes a multipart-upload init: a directory entry at
// `.uploads/<upload_id>` carrying the destination key in Extended. Sub-events
// for part uploads (deeper paths under the upload directory) are deliberately
// rejected — they ride a different mtime and would over-fire ABORT_MPU.
func mpuInitInfo(ev *reader.Event, entry *filer_pb.Entry) (destKey string, ok bool) {
	uploadsPrefix := s3_constants.MultipartUploadsFolder + "/"
	if !entry.IsDirectory || !strings.HasPrefix(ev.Key, uploadsPrefix) {
		return "", false
	}
	rest := ev.Key[len(uploadsPrefix):]
	if rest == "" || strings.ContainsRune(rest, '/') {
		// `.uploads/` itself or `.uploads/<id>/<part>...`; not the init.
		return "", false
	}
	keyBytes, hasKey := entry.Extended[s3_constants.ExtMultipartObjectKey]
	if !hasKey || len(keyBytes) == 0 {
		return "", false
	}
	return string(keyBytes), true
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
		// Mirror the server-side computeEntryIdentity encoding: Mtime
		// (seconds) and MtimeNs (nanosecond component) combine into
		// EntryIdentity.MtimeNs as nanoseconds-since-epoch.
		id.MtimeNs = entry.Attributes.Mtime*int64(1e9) + int64(entry.Attributes.MtimeNs)
		id.Size = int64(entry.Attributes.FileSize)
	}
	if len(entry.GetChunks()) > 0 {
		// Meta-log events arrive with chunk.FileId cleared by
		// BeforeEntrySerialization; GetFileIdString reconstructs it from
		// Fid so the worker matches the server-side fingerprint.
		id.HeadFid = entry.GetChunks()[0].GetFileIdString()
	}
	id.ExtendedHash = s3lifecycle.HashExtended(entry.Extended)
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
