package router

import (
	"context"
	"sort"
	"strconv"
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
// nil receiver or an error means "unknown" — callers suppress. Four
// queries: Survivors paginates the .versions/ container plus the bare
// null version (used by sole-survivor and bootstrap); LookupVersion
// fetches a single version file by id (used by pointer-transition
// routing to read the displaced version's identity and mtime);
// ListVersions paginates every version file in the .versions/
// container (used to compute NoncurrentIndex when a NewerNoncurrent
// rule is active); LookupNullVersion returns the bare-key entry that
// represents the null version (used by pointer-transition routing
// when oldID is empty and to include the null in expansion ranks).
type SiblingLister interface {
	Survivors(ctx context.Context, bucket, objectKey string) (Survivors, error)
	LookupVersion(ctx context.Context, bucket, objectKey, versionID string) (*filer_pb.Entry, error)
	ListVersions(ctx context.Context, bucket, objectKey string) ([]*filer_pb.Entry, error)
	LookupNullVersion(ctx context.Context, bucket, objectKey string) (entry *filer_pb.Entry, explicit bool, err error)
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

	// .versions/ directory metadata update: when ExtLatestVersionIdKey
	// changes, the OLD pointer value names a version that's now
	// noncurrent. Route NoncurrentDays / NewerNoncurrent for it without
	// waiting for the next bootstrap.
	if versioned && ev.NewEntry != nil && ev.OldEntry != nil && ev.NewEntry.IsDirectory && isVersionsContainerKey(ev.Key) {
		if !hasActiveEventDrivenAction(snap, keys, s3lifecycle.ActionKindNoncurrentDays) &&
			!hasActiveEventDrivenAction(snap, keys, s3lifecycle.ActionKindNewerNoncurrent) {
			return nil
		}
		return routePointerTransition(ctx, snap, ev, keys, lister)
	}

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
		// Schedule from the per-kind due moment. ExpirationDate is
		// rule-relative (the date IS the moment); other kinds are
		// ModTime-relative. Using ModTime+Delay for ExpirationDate
		// (Delay=0) puts dueTime at the entry's mtime — a backdated
		// object's mtime is BEFORE the rule's date, so the eligibility
		// check below would skip it. ComputeDueAt encapsulates both
		// shapes; the dispatcher's identity-CAS catches drift if the
		// object changes meanwhile.
		dueTime := s3lifecycle.ComputeDueAt(action.Rule, key.ActionKind, info)
		if dueTime.IsZero() {
			continue
		}
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

// routePointerTransition handles a .versions/ container update where
// ExtLatestVersionIdKey changed: the OLD pointer value names a version
// that just became noncurrent. Two lookup shapes:
//
//   - Pure NoncurrentVersionExpirationDays without NewerNoncurrentVersions:
//     a single LookupVersion of oldID is enough — the displaced version
//     is the only one that newly entered eligibility for this rule.
//
//   - Any active NewerNoncurrentVersions rule: a pointer flip shifts
//     every prior noncurrent's rank by one, so the version that *just
//     crossed* the keep-count threshold needs evaluation too. List the
//     full .versions/ container, rank newest-first, and route every
//     eligible noncurrent. Identity-CAS handles dedup with earlier
//     schedules.
//
// Without this branch the worker has to wait for the next bootstrap to
// schedule retention on a freshly-noncurrent version.
func routePointerTransition(ctx context.Context, snap *engine.Snapshot, ev *reader.Event, keys []s3lifecycle.ActionKey, lister SiblingLister) []Match {
	if lister == nil {
		return nil
	}
	logical := strings.TrimSuffix(ev.Key, s3_constants.VersionsFolder)
	if logical == "" {
		return nil
	}
	oldID := string(ev.OldEntry.Extended[s3_constants.ExtLatestVersionIdKey])
	newID := string(ev.NewEntry.Extended[s3_constants.ExtLatestVersionIdKey])
	if oldID == newID {
		// Same id means the update didn't transition the pointer.
		return nil
	}
	// oldID == "" doesn't mean "nothing displaced": a bare null may
	// have been the implicit/explicit latest before the pointer
	// flipped to a real id.
	// newID == "" means a suspended-versioning write cleared the
	// pointer and made the bare null current. The cached
	// ExtLatestVersionMtimeKey may still hold the prior latest's
	// mtime (stale), so we must NOT use successorModTimeFromContainer
	// in that case — derive the successor clock from the null entry's
	// mtime instead. latestIDForExpand carries the same substitution
	// so the expansion path's latestPos lookup matches the null sibling.
	var successor time.Time
	latestIDForExpand := newID
	if newID == "" {
		nullEntry, _, err := lister.LookupNullVersion(ctx, ev.Bucket, logical)
		if err != nil {
			glog.V(2).Infof("lifecycle router: lookup null %s/%s: %v", ev.Bucket, logical, err)
			return nil
		}
		if nullEntry == nil || nullEntry.Attributes == nil {
			return nil
		}
		successor = time.Unix(nullEntry.Attributes.Mtime, int64(nullEntry.Attributes.MtimeNs))
		latestIDForExpand = "null"
	} else {
		successor = successorModTimeFromContainer(ev.NewEntry)
	}
	if successor.IsZero() {
		return nil
	}
	if needsFullExpansion(snap, keys) {
		return routePointerTransitionExpand(ctx, snap, ev, keys, lister, logical, latestIDForExpand, successor)
	}
	return routePointerTransitionDisplaced(ctx, snap, ev, keys, lister, logical, oldID, successor)
}

// needsFullExpansion reports whether any active event-driven rule on
// this bucket cares about NoncurrentIndex (NewerNoncurrentVersions > 0
// in either NoncurrentDays or pure-count NewerNoncurrent).
func needsFullExpansion(snap *engine.Snapshot, keys []s3lifecycle.ActionKey) bool {
	for _, k := range keys {
		if k.ActionKind != s3lifecycle.ActionKindNoncurrentDays && k.ActionKind != s3lifecycle.ActionKindNewerNoncurrent {
			continue
		}
		a := snap.Action(k)
		if a == nil || !a.IsActive() || a.Mode != engine.ModeEventDriven {
			continue
		}
		if a.Rule != nil && a.Rule.NewerNoncurrentVersions > 0 {
			return true
		}
	}
	return false
}

// successorModTimeFromContainer reads the cached latest-version mtime
// from the .versions/ container's Extended map.
// updateLatestVersionInDirectory writes it via setCachedListMetadata
// alongside ExtLatestVersionIdKey, but the directory's own
// Attributes.Mtime is preserved across pointer updates — using it
// directly would let a stale dir mtime trigger expiration immediately.
// Returns zero time if the cached mtime is missing or unparseable; the
// caller suppresses in that case.
func successorModTimeFromContainer(entry *filer_pb.Entry) time.Time {
	raw, ok := entry.Extended[s3_constants.ExtLatestVersionMtimeKey]
	if !ok || len(raw) == 0 {
		return time.Time{}
	}
	secs, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || secs <= 0 {
		return time.Time{}
	}
	return time.Unix(secs, 0)
}

// successorFromEntryStamp returns the explicit noncurrent-since timestamp
// written by the S3 PUT handler at demotion time
// (s3_constants.ExtNoncurrentSinceNsKey). Returns zero time if the stamp
// is missing or unparseable — the caller falls back to derived values
// (sibling mtime or entry mtime) for legacy entries written before the
// stamp existed.
func successorFromEntryStamp(entry *filer_pb.Entry) time.Time {
	if entry == nil {
		return time.Time{}
	}
	raw, ok := entry.Extended[s3_constants.ExtNoncurrentSinceNsKey]
	if !ok || len(raw) == 0 {
		return time.Time{}
	}
	ns, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || ns <= 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// routePointerTransitionDisplaced is the single-lookup path: only the
// displaced version's noncurrent eligibility could have changed, so
// fetching just its file is enough. oldID == "" routes the bare null
// version instead — it was the implicit latest before the pointer
// flipped to a real id.
func routePointerTransitionDisplaced(ctx context.Context, snap *engine.Snapshot, ev *reader.Event, keys []s3lifecycle.ActionKey, lister SiblingLister, logical, oldID string, successor time.Time) []Match {
	var displaced *filer_pb.Entry
	displacedID := oldID
	if oldID == "" {
		nullEntry, _, err := lister.LookupNullVersion(ctx, ev.Bucket, logical)
		if err != nil {
			glog.V(2).Infof("lifecycle router: lookup null version %s/%s: %v", ev.Bucket, logical, err)
			return nil
		}
		if nullEntry == nil {
			return nil
		}
		displaced = nullEntry
		displacedID = "null"
	} else {
		entry, err := lister.LookupVersion(ctx, ev.Bucket, logical, oldID)
		if err != nil {
			glog.V(2).Infof("lifecycle router: lookup displaced version %s/%s/%s: %v", ev.Bucket, logical, oldID, err)
			return nil
		}
		displaced = entry
	}
	if displaced == nil || displaced.Attributes == nil {
		return nil
	}
	// Prefer the explicit demotion stamp on the displaced entry over the
	// container-derived successor. Stamp is written by the S3 PUT handler
	// at the moment the pointer flipped; container value is derived from
	// the new latest's mtime and may drift across retries.
	effectiveSuccessor := successor
	if stamp := successorFromEntryStamp(displaced); !stamp.IsZero() {
		effectiveSuccessor = stamp
	}
	idx := 0
	info := &s3lifecycle.ObjectInfo{
		Key:              logical,
		ModTime:          time.Unix(displaced.Attributes.Mtime, int64(displaced.Attributes.MtimeNs)),
		Size:             int64(displaced.Attributes.FileSize),
		IsLatest:         false,
		IsDeleteMarker:   string(displaced.Extended[s3_constants.ExtDeleteMarkerKey]) == "true",
		NoncurrentIndex:  &idx,
		SuccessorModTime: effectiveSuccessor,
	}
	if tags := extractTags(displaced.Extended); len(tags) > 0 {
		info.Tags = tags
	}
	return emitNoncurrentMatches(snap, ev, keys, info, displaced, displacedID, effectiveSuccessor)
}

// routePointerTransitionExpand routes only the versions that newly
// became eligible by the pointer flip:
//
//   - rank 0: the displaced version (newly noncurrent), needed for the
//     pure-NoncurrentDays clock,
//   - rank == rule.NewerNoncurrentVersions for each active rule that
//     gates on count: the version at exactly that rank just crossed
//     from kept to expired.
//
// Emitting every eligible noncurrent on every PUT would push
// O(versions) heap entries per flip — Schedule.Add doesn't dedup, so
// identity-CAS at dispatch only stops the wasted RPC, not the heap
// growth. Bootstrap still owns full backfill.
func routePointerTransitionExpand(ctx context.Context, snap *engine.Snapshot, ev *reader.Event, keys []s3lifecycle.ActionKey, lister SiblingLister, logical, newID string, successor time.Time) []Match {
	rawVersions, err := lister.ListVersions(ctx, ev.Bucket, logical)
	if err != nil {
		glog.V(2).Infof("lifecycle router: list versions %s/%s: %v", ev.Bucket, logical, err)
		return nil
	}
	// Include the bare null version in the sibling set: count-based
	// ranks are wrong if a pre-versioning or suspended-null entry
	// exists outside .versions/. ID is "null" for sort + emit.
	nullEntry, _, nullErr := lister.LookupNullVersion(ctx, ev.Bucket, logical)
	if nullErr != nil {
		glog.V(2).Infof("lifecycle router: lookup null %s/%s: %v", ev.Bucket, logical, nullErr)
		return nil
	}
	type sibling struct {
		entry *filer_pb.Entry
		id    string
	}
	siblings := make([]sibling, 0, len(rawVersions)+1)
	for _, v := range rawVersions {
		if v == nil || v.Attributes == nil {
			continue
		}
		id := string(v.Extended[s3_constants.ExtVersionIdKey])
		if id == "" {
			continue
		}
		siblings = append(siblings, sibling{entry: v, id: id})
	}
	if nullEntry != nil && nullEntry.Attributes != nil {
		siblings = append(siblings, sibling{entry: nullEntry, id: "null"})
	}
	if len(siblings) == 0 {
		return nil
	}
	sort.SliceStable(siblings, func(i, j int) bool {
		mi := siblings[i].entry.Attributes.Mtime*int64(1e9) + int64(siblings[i].entry.Attributes.MtimeNs)
		mj := siblings[j].entry.Attributes.Mtime*int64(1e9) + int64(siblings[j].entry.Attributes.MtimeNs)
		if mi != mj {
			return mi > mj
		}
		return s3lifecycle.CompareVersionIds(siblings[i].id, siblings[j].id) < 0
	})
	// Resolve latestPos by finding newID. Default to -1 so a missing
	// newID (race with the listing, or torn write) suppresses the
	// expansion: we'd otherwise call the actual newest sibling "latest"
	// against the pointer's intent and misrank every noncurrent.
	// Bootstrap repairs state on the next walk.
	latestPos := -1
	if newID != "" {
		for i, s := range siblings {
			if s.id == newID {
				latestPos = i
				break
			}
		}
	}
	if latestPos < 0 {
		glog.V(2).Infof("lifecycle router: pointer transition %s/%s: new id %s not found in listing", ev.Bucket, logical, newID)
		return nil
	}
	noncurrentCount := len(siblings) - 1

	// Collect the target noncurrent ranks: 0 (the freshly displaced)
	// plus N for each active count-gated rule.
	rankSet := map[int]struct{}{0: {}}
	for _, k := range keys {
		if k.ActionKind != s3lifecycle.ActionKindNoncurrentDays && k.ActionKind != s3lifecycle.ActionKindNewerNoncurrent {
			continue
		}
		a := snap.Action(k)
		if a == nil || !a.IsActive() || a.Mode != engine.ModeEventDriven {
			continue
		}
		if a.Rule != nil && a.Rule.NewerNoncurrentVersions > 0 {
			rankSet[a.Rule.NewerNoncurrentVersions] = struct{}{}
		}
	}
	ranks := make([]int, 0, len(rankSet))
	for r := range rankSet {
		ranks = append(ranks, r)
	}
	sort.Ints(ranks)

	var matches []Match
	for _, rank := range ranks {
		if rank >= noncurrentCount {
			continue
		}
		// Convert noncurrent rank to position in the sorted slice,
		// skipping the latest's slot.
		i := rank
		if rank >= latestPos {
			i = rank + 1
		}
		s := siblings[i]
		// Successor mtime: the entry directly newer than this one in
		// the sorted list. When the next-newer slot is the latest,
		// use the cached successor (the new latest's mtime); otherwise
		// the immediate predecessor's mtime.
		var thisSuccessor time.Time
		if i > 0 && i-1 != latestPos {
			thisSuccessor = time.Unix(siblings[i-1].entry.Attributes.Mtime, int64(siblings[i-1].entry.Attributes.MtimeNs))
		} else {
			thisSuccessor = successor
		}
		// Override with the explicit demotion stamp when present —
		// PUT-time wall clock beats derived sibling mtime for accuracy
		// and is immune to mtime edits on the sibling itself.
		if stamp := successorFromEntryStamp(s.entry); !stamp.IsZero() {
			thisSuccessor = stamp
		}
		idx := rank
		info := &s3lifecycle.ObjectInfo{
			Key:              logical,
			ModTime:          time.Unix(s.entry.Attributes.Mtime, int64(s.entry.Attributes.MtimeNs)),
			Size:             int64(s.entry.Attributes.FileSize),
			IsLatest:         false,
			IsDeleteMarker:   string(s.entry.Extended[s3_constants.ExtDeleteMarkerKey]) == "true",
			NoncurrentIndex:  &idx,
			SuccessorModTime: thisSuccessor,
			NumVersions:      len(siblings),
		}
		if tags := extractTags(s.entry.Extended); len(tags) > 0 {
			info.Tags = tags
		}
		matches = append(matches, emitNoncurrentMatches(snap, ev, keys, info, s.entry, s.id, thisSuccessor)...)
	}
	return matches
}

// emitNoncurrentMatches walks NoncurrentDays / NewerNoncurrent action
// keys and emits Matches for each one that fires. Shared between the
// single-lookup and full-expansion paths.
func emitNoncurrentMatches(snap *engine.Snapshot, ev *reader.Event, keys []s3lifecycle.ActionKey, info *s3lifecycle.ObjectInfo, entry *filer_pb.Entry, versionID string, successor time.Time) []Match {
	eventTime := time.Unix(0, ev.TsNs)
	identity := buildIdentityFromEntry(entry)
	var matches []Match
	for _, key := range keys {
		if key.ActionKind != s3lifecycle.ActionKindNoncurrentDays && key.ActionKind != s3lifecycle.ActionKindNewerNoncurrent {
			continue
		}
		action := snap.Action(key)
		if action == nil || !action.IsActive() || action.Mode != engine.ModeEventDriven {
			continue
		}
		clock := successor
		if clock.IsZero() {
			clock = info.ModTime
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
			ObjectKey: info.Key,
			VersionID: versionID,
			Identity:  identity,
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
		// Pin the version_id only for kinds the dispatcher needs to
		// target by version: noncurrent retention and the marker
		// itself. EXPIRATION_DAYS / EXPIRATION_DATE on the latest
		// must NOT carry it — between schedule and dispatch a fresh
		// PUT can land, and identity-CAS against the original
		// version's bytes would still pass even though the latest has
		// moved on. Empty VersionID makes the dispatcher fetch the
		// current latest, where identity-CAS resolves to STALE_IDENTITY
		// and bootstrap re-schedules with the new latest's identity.
		var matchVersionID string
		switch key.ActionKind {
		case s3lifecycle.ActionKindNoncurrentDays,
			s3lifecycle.ActionKindNewerNoncurrent,
			s3lifecycle.ActionKindExpiredDeleteMarker:
			matchVersionID = bv.VersionID
		}
		matches = append(matches, Match{
			Key:       key,
			Action:    action,
			Result:    res,
			EventTs:   eventTime,
			DueTime:   dueTime,
			Bucket:    ev.Bucket,
			ObjectKey: bv.LogicalKey,
			VersionID: matchVersionID,
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

// isVersionsContainerKey reports whether the bucket-relative key IS a
// .versions/ container itself (e.g. "logs/foo.versions"), as opposed to
// a file inside one. Used to recognize directory-level events whose
// Extended map carries the latest pointer for an object.
func isVersionsContainerKey(key string) bool {
	if key == s3_constants.VersionsFolder {
		// Bucket-root .versions: no logical object key.
		return false
	}
	return strings.HasSuffix(key, s3_constants.VersionsFolder)
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
