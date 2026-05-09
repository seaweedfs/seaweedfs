package router

import (
	"context"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
)

// SiblingLister inspects the surviving versions of a versioned key.
// nil receiver or an error means "unknown" — callers suppress.
type SiblingLister interface {
	Survivors(ctx context.Context, bucket, objectKey string) (Survivors, error)
}

// Survivors describes the state under .versions/<key>/ plus the bare
// null-version that exists when versioning was turned on after the
// object was first PUT (s3api_object_versioning.go treats <bucket>/<key>
// as a regular file, the null version, in that case).
type Survivors struct {
	Count          int             // entries under .versions/<key>/, capped at 2
	LoneEntry      *filer_pb.Entry // populated when Count == 1
	HasNullVersion bool            // bare <bucket>/<key> exists as a regular file
}

// Match is one (event, action) pair where EvaluateAction fired. The
// dispatcher runs `LifecycleDelete` at DueTime; identity-CAS in the RPC
// guards against drift between schedule time and dispatch time.
//
// For NoncurrentDays / NewerNoncurrent on a versioned bucket, ObjectKey
// is the seaweedfs storage path (logical-key + ".versions/" + version_id)
// so the dispatcher can locate the specific version, and VersionID
// carries the AWS-visible version ID separately.
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

// Route returns the matches that fire for ev against snap. Only active
// event-driven actions are considered; SCAN_AT_DATE and DISABLED bypass
// this path.
func Route(ctx context.Context, snap *engine.Snapshot, ev *reader.Event, now time.Time, lister SiblingLister) []Match {
	if snap == nil || ev == nil {
		return nil
	}
	keys := snap.BucketActionKeys(ev.Bucket)
	if len(keys) == 0 {
		return nil
	}
	// Bootstrap-expanded version event: sibling state is pre-computed,
	// info.Key is the LOGICAL key so rule prefixes match. Skip the
	// meta-log path's version-folder skip.
	if ev.BootstrapVersion != nil {
		return routeBootstrapVersion(snap, ev, keys)
	}

	versioned := snap.BucketVersioned(ev.Bucket)

	// EXP_DM can fire on two version-folder events: the marker create
	// (sole survivor immediately) and a noncurrent hard-delete that
	// leaves only the marker behind. Both reach routeSoleSurvivorMarker.
	if versioned && isVersionFolderPath(ev.Key) {
		if !hasActiveEventDrivenAction(snap, keys, s3lifecycle.ActionKindExpiredDeleteMarker) {
			return nil
		}
		return routeSoleSurvivorMarker(ctx, snap, ev, keys, lister)
	}

	if ev.NewEntry == nil {
		return nil
	}
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

// routeSoleSurvivorMarker emits an EXP_DM Match against the LOGICAL key
// (so the dispatcher can call deleteSpecificObjectVersion) with the
// marker's version_id. Handles two events: a marker create (the new
// entry IS the marker) and a noncurrent hard-delete that leaves the
// marker behind (the listing's lone entry IS the marker). The server
// re-checks before deleting.
func routeSoleSurvivorMarker(ctx context.Context, snap *engine.Snapshot, ev *reader.Event, keys []s3lifecycle.ActionKey, lister SiblingLister) []Match {
	if lister == nil {
		return nil
	}
	// Skip the listing RPC for events that can't possibly produce a
	// sole-survivor marker: a regular non-marker version create/update
	// always lands at Count >= 2.
	if ev.NewEntry != nil && !isDeleteMarkerEntry(ev.NewEntry) {
		return nil
	}
	logicalKey, ok := logicalKeyFromVersionPath(ev.Key)
	if !ok {
		return nil
	}
	s, err := lister.Survivors(ctx, ev.Bucket, logicalKey)
	if err != nil {
		glog.V(2).Infof("lifecycle router: survivors %s/%s: %v", ev.Bucket, logicalKey, err)
		return nil
	}
	// Pre-versioning bare-key objects (the "null" version) live outside
	// .versions/. Treating count==1 as sole-survivor while a null
	// version exists would let lifecycle delete the marker and re-expose
	// the old object.
	if s.Count != 1 || s.HasNullVersion || s.LoneEntry == nil {
		return nil
	}
	if !isDeleteMarkerEntry(s.LoneEntry) {
		return nil
	}
	versionID := string(s.LoneEntry.Extended[s3_constants.ExtVersionIdKey])
	if versionID == "" {
		// Empty version_id would BLOCK at dispatch and freeze the cursor.
		return nil
	}
	entry := s.LoneEntry
	info := &s3lifecycle.ObjectInfo{
		Key:            logicalKey,
		ModTime:        time.Unix(entry.Attributes.Mtime, int64(entry.Attributes.MtimeNs)),
		Size:           int64(entry.Attributes.FileSize),
		IsLatest:       true,
		IsDeleteMarker: true,
		NumVersions:    1,
	}
	if tags := extractTags(entry.Extended); len(tags) > 0 {
		info.Tags = tags
	}
	eventTime := time.Unix(0, ev.TsNs)
	identity := buildIdentityFromEntry(entry)
	var matches []Match
	for _, key := range keys {
		if key.ActionKind != s3lifecycle.ActionKindExpiredDeleteMarker {
			continue
		}
		action := snap.Action(key)
		if action == nil || !action.IsActive() || action.Mode != engine.ModeEventDriven {
			continue
		}
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
			ObjectKey: logicalKey,
			VersionID: versionID,
			Identity:  identity,
		})
	}
	return matches
}

// routeBootstrapVersion handles a synthesized event from BucketBootstrapper.
// The bootstrap walker has already listed .versions/<key>/, sorted siblings
// newest-first, and stamped each one's IsLatest / NoncurrentIndex /
// SuccessorModTime. The router only needs to assemble ObjectInfo and run
// the match loop with the standard kind gates. ev.NewEntry is the version
// file itself; ev.Key is the version-folder path; the LOGICAL key from
// BootstrapVersion drives prefix matching and the dispatcher.
func routeBootstrapVersion(snap *engine.Snapshot, ev *reader.Event, keys []s3lifecycle.ActionKey) []Match {
	bv := ev.BootstrapVersion
	entry := ev.NewEntry
	if entry == nil || entry.Attributes == nil || bv.LogicalKey == "" {
		return nil
	}
	idx := bv.NoncurrentIndex
	info := &s3lifecycle.ObjectInfo{
		Key:              bv.LogicalKey,
		ModTime:          time.Unix(entry.Attributes.Mtime, int64(entry.Attributes.MtimeNs)),
		Size:             int64(entry.Attributes.FileSize),
		IsLatest:         bv.IsLatest,
		IsDeleteMarker:   bv.IsDeleteMarker,
		NumVersions:      bv.NumVersions,
		SuccessorModTime: bv.SuccessorModTime,
	}
	if !bv.IsLatest {
		info.NoncurrentIndex = &idx
	}
	if tags := extractTags(entry.Extended); len(tags) > 0 {
		info.Tags = tags
	}
	eventTime := time.Unix(0, ev.TsNs)
	identity := buildIdentityFromEntry(entry)
	var matches []Match
	for _, key := range keys {
		action := snap.Action(key)
		if action == nil || !action.IsActive() || action.Mode != engine.ModeEventDriven {
			continue
		}
		// ABORT_MPU never applies to a versioned object.
		if key.ActionKind == s3lifecycle.ActionKindAbortMPU {
			continue
		}
		// Noncurrent rules clock from when this version was replaced
		// (SuccessorModTime), not from when it was originally written.
		// Bootstrap populates SuccessorModTime; fall back to ModTime
		// for the latest version (no successor exists).
		clock := info.ModTime
		if !info.IsLatest && !info.SuccessorModTime.IsZero() {
			clock = info.SuccessorModTime
		}
		dueTime := clock.Add(action.Delay)
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
			ObjectKey: bv.LogicalKey,
			VersionID: bv.VersionID,
			Identity:  identity,
		})
	}
	return matches
}

// logicalKeyFromVersionPath extracts <logical> from <logical>.versions/<file>.
// Returns false for a bucket-root marker (path = ".versions/<file>"), since
// AWS S3 has no concept of an object at the bucket key itself.
func logicalKeyFromVersionPath(versionPath string) (string, bool) {
	lastSlash := strings.LastIndex(versionPath, "/")
	if lastSlash <= 0 {
		return "", false
	}
	parent := versionPath[:lastSlash]
	if !strings.HasSuffix(parent, s3_constants.VersionsFolder) {
		return "", false
	}
	logical := strings.TrimSuffix(parent, s3_constants.VersionsFolder)
	if logical == "" {
		return "", false
	}
	return logical, true
}

// buildObjectInfo derives an ObjectInfo from a meta-log event. Returns
// nil for shapes the router can't classify safely: missing attributes,
// non-MPU directories, version-folder files (those route through
// routeSoleSurvivorMarker upstream when EXP_DM applies). On a versioned
// bucket the latest pointer lives in the .versions/ directory's
// Extended map; without it we leave NumVersions=0 so the bootstrap walk
// drives noncurrent retention.
func buildObjectInfo(ev *reader.Event, versioned bool) *s3lifecycle.ObjectInfo {
	entry := ev.NewEntry
	if entry == nil || entry.Attributes == nil {
		return nil
	}
	if destKey, ok := mpuInitInfo(ev, entry); ok {
		return &s3lifecycle.ObjectInfo{
			Key:       destKey,
			ModTime:   time.Unix(entry.Attributes.Mtime, int64(entry.Attributes.MtimeNs)),
			IsMPUInit: true,
		}
	}
	if entry.IsDirectory {
		return nil
	}
	info := &s3lifecycle.ObjectInfo{
		Key:         ev.Key,
		ModTime:     time.Unix(entry.Attributes.Mtime, int64(entry.Attributes.MtimeNs)),
		Size:        int64(entry.Attributes.FileSize),
		IsLatest:    true,
		NumVersions: 1,
	}
	if versioned {
		if isVersionFolderPath(ev.Key) {
			return nil
		}
		info.NumVersions = 0
	}
	if tags := extractTags(entry.Extended); len(tags) > 0 {
		info.Tags = tags
	}
	if isDeleteMarkerEntry(entry) {
		info.IsDeleteMarker = true
	}
	return info
}

// isVersionFolderPath reports whether the bucket-relative key sits inside a
// .versions/ folder — i.e. the path's parent segment ends with the
// VersionsFolder suffix. Used by the versioned-bucket gate so the router
// skips version-file events that need sibling state to be classified
// safely.
func isVersionFolderPath(key string) bool {
	idx := strings.LastIndex(key, "/")
	if idx <= 0 {
		return false
	}
	parent := key[:idx]
	parentIdx := strings.LastIndex(parent, "/")
	leaf := parent[parentIdx+1:]
	return strings.HasSuffix(leaf, s3_constants.VersionsFolder)
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
	return buildIdentityFromEntry(ev.NewEntry)
}

func buildIdentityFromEntry(entry *filer_pb.Entry) *EntryIdentity {
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

// hasActiveEventDrivenAction gates I/O (e.g. sibling listing) on whether
// a match could actually fire. Mirrors the per-key filter in Route so
// disabled or scan-only actions don't pay the RPC.
func hasActiveEventDrivenAction(snap *engine.Snapshot, keys []s3lifecycle.ActionKey, kind s3lifecycle.ActionKind) bool {
	for _, k := range keys {
		if k.ActionKind != kind {
			continue
		}
		a := snap.Action(k)
		if a == nil {
			continue
		}
		if a.IsActive() && a.Mode == engine.ModeEventDriven {
			return true
		}
	}
	return false
}

// isDeleteMarkerEntry mirrors every read site for ExtDeleteMarkerKey:
// production writes []byte("true").
func isDeleteMarkerEntry(entry *filer_pb.Entry) bool {
	if entry == nil || len(entry.Extended) == 0 {
		return false
	}
	v, ok := entry.Extended[s3_constants.ExtDeleteMarkerKey]
	return ok && string(v) == "true"
}
