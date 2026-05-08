package router

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
)

func compileWith(rule *s3lifecycle.Rule, prior map[s3lifecycle.ActionKey]engine.PriorState) *engine.Snapshot {
	e := engine.New()
	return e.Compile([]engine.CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		engine.CompileOptions{PriorStates: prior})
}

func activatedPrior(rule *s3lifecycle.Rule) map[s3lifecycle.ActionKey]engine.PriorState {
	prior := map[s3lifecycle.ActionKey]engine.PriorState{}
	hash := s3lifecycle.RuleHash(rule)
	for _, k := range s3lifecycle.RuleActionKinds(rule) {
		prior[s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: k}] = engine.PriorState{
			BootstrapComplete: true,
			Mode:              engine.ModeEventDriven,
		}
	}
	return prior
}

func eventCreate(bucket, key string, modTimeS, size int64, ts int64) *reader.Event {
	return &reader.Event{
		TsNs:   ts,
		Bucket: bucket,
		Key:    key,
		NewEntry: &filer_pb.Entry{
			Name: key,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    modTimeS,
				FileSize: uint64(size),
			},
		},
	}
}

func TestRouteNoSnapshotNoMatches(t *testing.T) {
	if got := Route(nil, eventCreate("bk", "k", 0, 1, 1), time.Now()); got != nil {
		t.Fatalf("nil snap should yield nil, got %v", got)
	}
}

func TestRouteMissingBucketNoMatches(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWith(rule, activatedPrior(rule))
	ev := eventCreate("other-bucket", "k", 0, 1, 1)
	if got := Route(snap, ev, time.Now()); got != nil {
		t.Fatalf("foreign bucket should yield nil, got %v", got)
	}
}

func TestRouteInactiveSkipped(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	// PriorStates omitted => BootstrapComplete=false => action stays inactive.
	snap := compileWith(rule, nil)

	now := time.Now()
	old := now.Add(-48 * time.Hour) // past 1-day expiration
	ev := eventCreate("bk", "k", old.Unix(), 1, old.UnixNano())

	if got := Route(snap, ev, now); got != nil {
		t.Fatalf("inactive action should not match, got %v", got)
	}
}

func TestRouteExpirationDaysFires(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	old := now.Add(-48 * time.Hour) // past 1-day expiration
	ev := eventCreate("bk", "k", old.Unix(), 1, old.UnixNano())

	matches := Route(snap, ev, now)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %+v", matches)
	}
	m := matches[0]
	if m.Bucket != "bk" || m.ObjectKey != "k" {
		t.Fatalf("unexpected match shape: %+v", m)
	}
	if m.Result.Action != s3lifecycle.ActionDeleteObject {
		t.Fatalf("action=%v, want DeleteObject", m.Result.Action)
	}
	if m.DueTime.Before(m.EventTs) {
		t.Fatalf("DueTime < EventTs: %v < %v", m.DueTime, m.EventTs)
	}
}

func TestRouteFreshObjectSchedulesInFuture(t *testing.T) {
	// A fresh object DOES route — the action is scheduled for ModTime+N days.
	// EvaluateAction is called at the scheduled dispatch time so the age
	// gate passes; the dispatcher waits until DueTime to actually run.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 7}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	ev := eventCreate("bk", "k", now.Unix(), 1, now.UnixNano())

	matches := Route(snap, ev, now)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match (scheduled), got %v", matches)
	}
	if !matches[0].DueTime.After(now.Add(6 * 24 * time.Hour)) {
		t.Fatalf("DueTime=%v, want ~7 days from now", matches[0].DueTime)
	}
}

func TestRouteRespectsPrefixFilter(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID: "r", Status: s3lifecycle.StatusEnabled,
		Prefix: "logs/", ExpirationDays: 1,
	}
	snap := compileWith(rule, activatedPrior(rule))
	now := time.Now()
	old := now.Add(-48 * time.Hour)

	// Out of prefix: no match.
	ev := eventCreate("bk", "data/file", old.Unix(), 1, old.UnixNano())
	if got := Route(snap, ev, now); got != nil {
		t.Fatalf("out-of-prefix should not match, got %v", got)
	}

	// In prefix: matches.
	ev = eventCreate("bk", "logs/file", old.Unix(), 1, old.UnixNano())
	if got := Route(snap, ev, now); len(got) != 1 {
		t.Fatalf("in-prefix should match, got %v", got)
	}
}

func TestRouteSkipsHardDelete(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWith(rule, activatedPrior(rule))
	now := time.Now()
	old := now.Add(-48 * time.Hour)
	// Hard delete: NewEntry is nil; OldEntry holds the gone object.
	ev := &reader.Event{
		TsNs:   old.UnixNano(),
		Bucket: "bk",
		Key:    "gone.txt",
		OldEntry: &filer_pb.Entry{
			Name: "gone.txt",
			Attributes: &filer_pb.FuseAttributes{Mtime: old.Unix(), FileSize: 1},
		},
	}
	if got := Route(snap, ev, now); got != nil {
		t.Fatalf("hard delete should not route, got %v", got)
	}
}

func TestRouteSkipsMissingAttributes(t *testing.T) {
	// Without Attributes there's no ModTime, and EvaluateAction would
	// compute due against year-0001 and fire immediately. Skip the event.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWith(rule, activatedPrior(rule))
	ev := &reader.Event{
		TsNs:     time.Now().UnixNano(),
		Bucket:   "bk",
		Key:      "k",
		NewEntry: &filer_pb.Entry{Name: "k"}, // no Attributes
	}
	if got := Route(snap, ev, time.Now()); got != nil {
		t.Fatalf("missing-Attributes event should not route, got %v", got)
	}
}

func TestRouteIdentityCapturedForNewEntry(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	old := now.Add(-48 * time.Hour)
	ev := eventCreate("bk", "k", old.Unix(), 42, old.UnixNano())
	ev.NewEntry.Chunks = []*filer_pb.FileChunk{{FileId: "1,abc"}}

	matches := Route(snap, ev, now)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %v", matches)
	}
	id := matches[0].Identity
	if id == nil || id.Size != 42 || id.HeadFid != "1,abc" {
		t.Fatalf("identity capture: %+v", id)
	}
	wantNs := old.Unix()*int64(1e9) + 0
	if id.MtimeNs != wantNs {
		t.Fatalf("MtimeNs=%d, want %d (Mtime*1e9)", id.MtimeNs, wantNs)
	}
}

func TestRouteIdentityHashesExtended(t *testing.T) {
	// A normal S3 PUT stores ETag/content-type etc. in Extended; the worker
	// must hash these into ExtendedHash so the server's identity-CAS sees
	// the same fingerprint. Without this the live entry's non-nil
	// ExtendedHash diverges from the worker's nil value and every
	// dispatch returns NOOP_RESOLVED:STALE_IDENTITY.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	old := now.Add(-48 * time.Hour)
	ev := eventCreate("bk", "k", old.Unix(), 1, old.UnixNano())
	ev.NewEntry.Extended = map[string][]byte{
		"X-Amz-Meta-Etag": []byte("abc123"),
		"Content-Type":    []byte("text/plain"),
	}

	matches := Route(snap, ev, now)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %v", matches)
	}
	id := matches[0].Identity
	if id == nil || len(id.ExtendedHash) == 0 {
		t.Fatalf("ExtendedHash not captured for non-empty Extended: %+v", id)
	}
	want := s3lifecycle.HashExtended(ev.NewEntry.Extended)
	if string(id.ExtendedHash) != string(want) {
		t.Fatalf("ExtendedHash mismatch:\n got %x\nwant %x", id.ExtendedHash, want)
	}
}

func mpuInitEvent(bucket, uploadID, destKey string, initS, ts int64) *reader.Event {
	return &reader.Event{
		TsNs:   ts,
		Bucket: bucket,
		Key:    ".uploads/" + uploadID,
		NewEntry: &filer_pb.Entry{
			Name:        uploadID,
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{Mtime: initS},
			Extended: map[string][]byte{
				s3_constants.ExtMultipartObjectKey: []byte(destKey),
			},
		},
	}
}

func TestRouteMPUInitFiresAbortAfterDelay(t *testing.T) {
	// 7-day MPU abort. Init 8 days ago must fire; rule's prefix matches the
	// destination key, not the .uploads/ path.
	rule := &s3lifecycle.Rule{
		ID:                          "r-mpu",
		Status:                      s3lifecycle.StatusEnabled,
		Prefix:                      "logs/",
		AbortMPUDaysAfterInitiation: 7,
	}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	init := now.AddDate(0, 0, -8)
	ev := mpuInitEvent("bk", "u1", "logs/foo.txt", init.Unix(), init.UnixNano())

	matches := Route(snap, ev, now)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %v", matches)
	}
	if got, want := matches[0].Key.ActionKind, s3lifecycle.ActionKindAbortMPU; got != want {
		t.Fatalf("ActionKind=%v, want %v", got, want)
	}
	if got, want := matches[0].ObjectKey, ".uploads/u1"; got != want {
		t.Fatalf("ObjectKey=%q, want %q (server needs the upload directory path)", got, want)
	}
}

func TestRouteMPUInitFilteredOutByPrefix(t *testing.T) {
	// Same rule, MPU uploads to a different prefix → no match.
	rule := &s3lifecycle.Rule{
		ID:                          "r-mpu",
		Status:                      s3lifecycle.StatusEnabled,
		Prefix:                      "logs/",
		AbortMPUDaysAfterInitiation: 7,
	}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	init := now.AddDate(0, 0, -8)
	ev := mpuInitEvent("bk", "u2", "data/foo.txt", init.Unix(), init.UnixNano())

	if got := Route(snap, ev, now); len(got) != 0 {
		t.Fatalf("expected 0 matches for foreign prefix, got %v", got)
	}
}

func TestRouteMPUInitMissingDestKeySkipped(t *testing.T) {
	// .uploads/<id> directory without ExtMultipartObjectKey is malformed
	// (mkdir wrote it before the metadata, or it's a stray dir). Skip.
	rule := &s3lifecycle.Rule{
		ID:                          "r-mpu",
		Status:                      s3lifecycle.StatusEnabled,
		AbortMPUDaysAfterInitiation: 7,
	}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	init := now.AddDate(0, 0, -8)
	ev := &reader.Event{
		TsNs:   init.UnixNano(),
		Bucket: "bk",
		Key:    ".uploads/u3",
		NewEntry: &filer_pb.Entry{
			Name:        "u3",
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{Mtime: init.Unix()},
		},
	}

	if got := Route(snap, ev, now); len(got) != 0 {
		t.Fatalf("expected 0 matches for missing destKey, got %v", got)
	}
}

func TestRouteMPUPartEventSkipped(t *testing.T) {
	// Part-upload events at .uploads/<id>/<part> ride a different mtime and
	// must not over-fire ABORT_MPU.
	rule := &s3lifecycle.Rule{
		ID:                          "r-mpu",
		Status:                      s3lifecycle.StatusEnabled,
		AbortMPUDaysAfterInitiation: 7,
	}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	init := now.AddDate(0, 0, -8)
	ev := &reader.Event{
		TsNs:   init.UnixNano(),
		Bucket: "bk",
		Key:    ".uploads/u4/0001",
		NewEntry: &filer_pb.Entry{
			Name:       "0001",
			Attributes: &filer_pb.FuseAttributes{Mtime: init.Unix()},
			Extended:   map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte("logs/foo.txt")},
		},
	}

	if got := Route(snap, ev, now); len(got) != 0 {
		t.Fatalf("expected 0 matches for part event, got %v", got)
	}
}

func TestRouteMPUInitDoesNotFireNoncurrent(t *testing.T) {
	// One rule with both ABORT_MPU and NONCURRENT_DAYS; an MPU init must
	// only emit the ABORT_MPU match. Otherwise the dispatcher receives a
	// NONCURRENT_DAYS action with version_id="" and freezes the cursor.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		AbortMPUDaysAfterInitiation:     7,
		NoncurrentVersionExpirationDays: 7,
	}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	init := now.AddDate(0, 0, -30)
	ev := mpuInitEvent("bk", "u1", "logs/foo.txt", init.Unix(), init.UnixNano())

	matches := Route(snap, ev, now)
	if len(matches) != 1 {
		t.Fatalf("expected exactly 1 match (ABORT_MPU only), got %v", matches)
	}
	if got := matches[0].Key.ActionKind; got != s3lifecycle.ActionKindAbortMPU {
		t.Fatalf("ActionKind=%v, want AbortMPU", got)
	}
}

func TestRouteRegularObjectUnderDualRuleSkipsAbortMPU(t *testing.T) {
	// Converse of TestRouteMPUInitDoesNotFireNoncurrent: a regular
	// current-version object under a rule that has both ExpirationDays
	// and AbortIncompleteMultipartUpload must fire EXPIRATION_DAYS only.
	// Without the gate the dispatcher would also receive an ABORT_MPU
	// action targeting the object path, which would rm a regular object
	// via the MPU code path.
	rule := &s3lifecycle.Rule{
		ID:                          "r",
		Status:                      s3lifecycle.StatusEnabled,
		ExpirationDays:              1,
		AbortMPUDaysAfterInitiation: 7,
	}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -2) // past the 1d expiration
	ev := eventCreate("bk", "obj", old.Unix(), 1, old.UnixNano())

	matches := Route(snap, ev, now)
	if len(matches) != 1 {
		t.Fatalf("expected exactly 1 match (EXPIRATION_DAYS only), got %v", matches)
	}
	if got := matches[0].Key.ActionKind; got != s3lifecycle.ActionKindExpirationDays {
		t.Fatalf("ActionKind=%v, want ExpirationDays", got)
	}
}

func compileWithVersioned(rule *s3lifecycle.Rule, prior map[s3lifecycle.ActionKey]engine.PriorState) *engine.Snapshot {
	e := engine.New()
	return e.Compile([]engine.CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}, Versioned: true}},
		engine.CompileOptions{PriorStates: prior})
}

func TestRouteVersionedNoncurrentEventDoesNotFireFromRouter(t *testing.T) {
	// Versioned bucket: the storage layout <key>.versions/v_<id> is
	// shared between the current latest and noncurrent versions, and
	// the latest pointer lives in the parent directory's metadata —
	// not on the version file itself. The router cannot distinguish
	// without consulting the .versions/ directory, so it must not
	// emit NONCURRENT_* matches; bootstrap (with sibling listing) is
	// responsible for those.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 7,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -30)
	ev := eventCreate("bk", "logs/foo.versions/v_v1", old.Unix(), 1, old.UnixNano())
	ev.NewEntry.Extended = map[string][]byte{
		s3_constants.ExtVersionIdKey: []byte("v1"),
	}

	if got := Route(snap, ev, now); len(got) != 0 {
		t.Fatalf("router must not emit noncurrent matches yet, got %v", got)
	}
}

func TestRouteVersionedCurrentEventStaysLatest(t *testing.T) {
	// Versioned bucket, ExpirationDays rule. The current-version event
	// arrives at the bare <key>; nothing about its path looks like a
	// .versions/ entry, so IsLatest stays true and the rule fires.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -2)
	ev := eventCreate("bk", "logs/foo", old.Unix(), 1, old.UnixNano())

	matches := Route(snap, ev, now)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match (EXPIRATION_DAYS), got %v", matches)
	}
	if matches[0].Key.ActionKind != s3lifecycle.ActionKindExpirationDays {
		t.Fatalf("ActionKind=%v, want ExpirationDays", matches[0].Key.ActionKind)
	}
	if matches[0].VersionID != "" {
		t.Fatalf("VersionID=%q, want empty for current-version path", matches[0].VersionID)
	}
}

func TestRouteNonVersionedBucketIgnoresVersionsSuffix(t *testing.T) {
	// A non-versioned bucket happens to have an object literally named
	// "logs/foo.versions/v1" — that's just a regular path. The router
	// must NOT classify it as noncurrent or set IsLatest=false; the
	// rule fires as a normal current-object expiration.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -2)
	ev := eventCreate("bk", "logs/foo.versions/v1", old.Unix(), 1, old.UnixNano())

	matches := Route(snap, ev, now)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %v", matches)
	}
	if matches[0].VersionID != "" {
		t.Fatalf("VersionID=%q, want empty for non-versioned bucket", matches[0].VersionID)
	}
	if matches[0].ObjectKey != "logs/foo.versions/v1" {
		t.Fatalf("ObjectKey=%q, want unchanged for non-versioned bucket", matches[0].ObjectKey)
	}
}

func TestRouteVersionedExpiredDeleteMarkerSuppressedWithoutSiblings(t *testing.T) {
	// ExpiredObjectDeleteMarker requires NumVersions==1 — the marker is
	// the sole-survivor. Without sibling listing the router can't
	// confirm that, so the rule must NOT fire just because the latest
	// is a delete marker. A future PR adds sibling listing.
	rule := &s3lifecycle.Rule{
		ID:                        "r",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -1)
	ev := eventCreate("bk", "logs/gone", old.Unix(), 0, old.UnixNano())
	ev.NewEntry.Extended = map[string][]byte{
		s3_constants.ExtDeleteMarkerKey: {1},
	}

	if got := Route(snap, ev, now); len(got) != 0 {
		t.Fatalf("ExpiredDeleteMarker without sibling count must not fire, got %v", got)
	}
}

func TestRouteVersionedAllVersionFolderPathsSkipped(t *testing.T) {
	// On a versioned bucket the router skips every event whose parent
	// directory name ends with ".versions" — both the version files
	// SeaweedFS itself writes (logs/foo.versions/v_v1) and any literal
	// key the user happens to put under such a parent — because the
	// current-vs-noncurrent classification needs the .versions/
	// directory's latest pointer, which isn't carried by these events.
	// Bootstrap covers retention for those entries.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -2)
	cases := []struct {
		name  string
		key   string
		isDir bool
		ext   map[string][]byte
	}{
		{
			name: "tracked version file",
			key:  "logs/foo.versions/v_v1",
			ext:  map[string][]byte{s3_constants.ExtVersionIdKey: []byte("v1")},
		},
		{
			name: "literal-key collision",
			key:  "logs/backup.versions/2023",
		},
		{
			name: "bucket-root .versions",
			key:  ".versions/v_v1",
			ext:  map[string][]byte{s3_constants.ExtVersionIdKey: []byte("v1")},
		},
		{
			// The .versions/ folder itself is a directory entry; the
			// router must not emit ObjectInfo for it. Without the
			// directory short-circuit it would route as a regular
			// object and the dispatcher would target a directory path.
			name:  "versions dir itself",
			key:   "logs/foo.versions",
			isDir: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := eventCreate("bk", tc.key, old.Unix(), 1, old.UnixNano())
			if tc.ext != nil {
				ev.NewEntry.Extended = tc.ext
			}
			if tc.isDir {
				ev.NewEntry.IsDirectory = true
			}
			if got := Route(snap, ev, now); len(got) != 0 {
				t.Fatalf("version-folder event should be skipped, got %v", got)
			}
		})
	}
}

