package router

import (
	"context"
	"errors"
	"fmt"
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
	if got := Route(context.Background(), nil, eventCreate("bk", "k", 0, 1, 1), time.Now(), nil); got != nil {
		t.Fatalf("nil snap should yield nil, got %v", got)
	}
}

func TestRouteMissingBucketNoMatches(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWith(rule, activatedPrior(rule))
	ev := eventCreate("other-bucket", "k", 0, 1, 1)
	if got := Route(context.Background(), snap, ev, time.Now(), nil); got != nil {
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

	if got := Route(context.Background(), snap, ev, now, nil); got != nil {
		t.Fatalf("inactive action should not match, got %v", got)
	}
}

func TestRouteExpirationDaysFires(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	old := now.Add(-48 * time.Hour) // past 1-day expiration
	ev := eventCreate("bk", "k", old.Unix(), 1, old.UnixNano())

	matches := Route(context.Background(), snap, ev, now, nil)
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

	matches := Route(context.Background(), snap, ev, now, nil)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match (scheduled), got %v", matches)
	}
	if !matches[0].DueTime.After(now.Add(s3lifecycle.DaysToDuration(6))) {
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
	if got := Route(context.Background(), snap, ev, now, nil); got != nil {
		t.Fatalf("out-of-prefix should not match, got %v", got)
	}

	// In prefix: matches.
	ev = eventCreate("bk", "logs/file", old.Unix(), 1, old.UnixNano())
	if got := Route(context.Background(), snap, ev, now, nil); len(got) != 1 {
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
	if got := Route(context.Background(), snap, ev, now, nil); got != nil {
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
	if got := Route(context.Background(), snap, ev, time.Now(), nil); got != nil {
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

	matches := Route(context.Background(), snap, ev, now, nil)
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

	matches := Route(context.Background(), snap, ev, now, nil)
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

	matches := Route(context.Background(), snap, ev, now, nil)
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

	if got := Route(context.Background(), snap, ev, now, nil); len(got) != 0 {
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

	if got := Route(context.Background(), snap, ev, now, nil); len(got) != 0 {
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

	if got := Route(context.Background(), snap, ev, now, nil); len(got) != 0 {
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

	matches := Route(context.Background(), snap, ev, now, nil)
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

	matches := Route(context.Background(), snap, ev, now, nil)
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

	if got := Route(context.Background(), snap, ev, now, nil); len(got) != 0 {
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

	matches := Route(context.Background(), snap, ev, now, nil)
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

	matches := Route(context.Background(), snap, ev, now, nil)
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

// markerEventBytes returns the production shape: a file event under
// <key>.versions/v_<version-id>, with ExtDeleteMarkerKey="true" and
// ExtVersionIdKey populated. Mirrors createDeleteMarker in
// s3api_object_versioning.go.
func markerEvent(bucket, logicalKey, versionID string, mtimeUnix, mtimeNs int64) *reader.Event {
	versionPath := logicalKey + s3_constants.VersionsFolder + "/v_" + versionID
	ev := eventCreate(bucket, versionPath, mtimeUnix, 0, mtimeNs)
	ev.NewEntry.Extended = map[string][]byte{
		s3_constants.ExtDeleteMarkerKey: []byte("true"),
		s3_constants.ExtVersionIdKey:    []byte(versionID),
	}
	return ev
}

// recordingLister captures Survivors calls. Configure with the exact
// state to return; calls list is appended on each invocation so tests
// can assert whether the lister was consulted at all.
type recordingLister struct {
	calls           []string
	survivors       Survivors
	err             error
	lookupCalls     []string
	lookupEntry     *filer_pb.Entry
	lookupErr       error
	listCalls       []string
	listVersions    []*filer_pb.Entry
	listErr         error
	nullCalls       []string
	nullEntry       *filer_pb.Entry
	nullExplicit    bool
	nullErr         error
}

func (r *recordingLister) ListVersions(_ context.Context, bucket, key string) ([]*filer_pb.Entry, error) {
	r.listCalls = append(r.listCalls, bucket+"/"+key)
	return r.listVersions, r.listErr
}

func (r *recordingLister) LookupNullVersion(_ context.Context, bucket, key string) (*filer_pb.Entry, bool, error) {
	r.nullCalls = append(r.nullCalls, bucket+"/"+key)
	return r.nullEntry, r.nullExplicit, r.nullErr
}

func (r *recordingLister) LookupVersion(_ context.Context, bucket, key, versionID string) (*filer_pb.Entry, error) {
	r.lookupCalls = append(r.lookupCalls, bucket+"/"+key+"@"+versionID)
	return r.lookupEntry, r.lookupErr
}

func (r *recordingLister) Survivors(_ context.Context, bucket, key string) (Survivors, error) {
	r.calls = append(r.calls, bucket+"/"+key)
	return r.survivors, r.err
}

func markerLoneEntry(versionID string, mtimeUnix, mtimeNs int64) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name: "v_" + versionID,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:   mtimeUnix,
			MtimeNs: int32(mtimeNs - mtimeUnix*int64(1e9)),
		},
		Extended: map[string][]byte{
			s3_constants.ExtDeleteMarkerKey: []byte("true"),
			s3_constants.ExtVersionIdKey:    []byte(versionID),
		},
	}
}

func TestRouteVersionedExpiredDeleteMarkerNilListerSuppresses(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:                        "r",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -1)
	ev := markerEvent("bk", "logs/gone", "2026-05-09-abc", old.Unix(), old.UnixNano())

	if got := Route(context.Background(), snap, ev, now, nil); len(got) != 0 {
		t.Fatalf("nil lister must suppress, got %v", got)
	}
}

func TestRouteVersionedExpiredDeleteMarkerSoleSurvivorFires(t *testing.T) {
	// Exactly one entry under .versions/<key>/ — the marker — and no
	// bare null version: EXP_DM fires with the LOGICAL key + version_id.
	rule := &s3lifecycle.Rule{
		ID:                        "r",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -1)
	ev := markerEvent("bk", "logs/gone", "2026-05-09-abc", old.Unix(), old.UnixNano())

	lister := &recordingLister{survivors: Survivors{
		Count:     1,
		LoneEntry: markerLoneEntry("2026-05-09-abc", old.Unix(), old.UnixNano()),
	}}
	matches := Route(context.Background(), snap, ev, now, lister)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match (ExpiredDeleteMarker), got %v", matches)
	}
	m := matches[0]
	if m.Result.Action != s3lifecycle.ActionExpireDeleteMarker {
		t.Fatalf("Action=%v, want ExpireDeleteMarker", m.Result.Action)
	}
	if m.ObjectKey != "logs/gone" {
		t.Fatalf("ObjectKey=%q, want logical key logs/gone", m.ObjectKey)
	}
	if m.VersionID != "2026-05-09-abc" {
		t.Fatalf("VersionID=%q, want 2026-05-09-abc", m.VersionID)
	}
	if len(lister.calls) != 1 || lister.calls[0] != "bk/logs/gone" {
		t.Fatalf("lister calls=%v, want [bk/logs/gone]", lister.calls)
	}
}

func TestRouteVersionedExpiredDeleteMarkerSiblingsRemainSuppressed(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:                        "r",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -1)
	ev := markerEvent("bk", "logs/gone", "2026-05-09-abc", old.Unix(), old.UnixNano())

	lister := &recordingLister{survivors: Survivors{Count: 2}}
	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("siblings present, must not fire, got %v", got)
	}
}

func TestRouteVersionedExpiredDeleteMarkerNullVersionSuppresses(t *testing.T) {
	// A pre-versioning bare-key object (HasNullVersion=true) still survives
	// outside .versions/. Firing EXP_DM would re-expose it; suppress.
	rule := &s3lifecycle.Rule{
		ID:                        "r",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -1)
	ev := markerEvent("bk", "logs/gone", "2026-05-09-abc", old.Unix(), old.UnixNano())

	lister := &recordingLister{survivors: Survivors{
		Count:          1,
		LoneEntry:      markerLoneEntry("2026-05-09-abc", old.Unix(), old.UnixNano()),
		HasNullVersion: true,
	}}
	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("null-version present, must not fire, got %v", got)
	}
}

func TestRouteVersionedExpiredDeleteMarkerHardDeleteLeavesLoneMarkerFires(t *testing.T) {
	// Sequence: object had v1 + DM, hard-delete of v1 leaves DM as the
	// sole survivor. The hard-delete event has NewEntry=nil; the router
	// must still consult the lister, see the lone DM, and emit a match
	// using the LoneEntry's version_id and identity.
	rule := &s3lifecycle.Rule{
		ID:                        "r",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -1)
	versionPath := "logs/gone" + s3_constants.VersionsFolder + "/v_2026-05-08-old"
	ev := &reader.Event{
		TsNs:     now.UnixNano(),
		Bucket:   "bk",
		Key:      versionPath,
		OldEntry: &filer_pb.Entry{Name: "v_2026-05-08-old"},
	}
	lister := &recordingLister{survivors: Survivors{
		Count:     1,
		LoneEntry: markerLoneEntry("2026-05-09-abc", old.Unix(), old.UnixNano()),
	}}
	matches := Route(context.Background(), snap, ev, now, lister)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match after hard-delete, got %v", matches)
	}
	if matches[0].VersionID != "2026-05-09-abc" {
		t.Fatalf("VersionID=%q, want lone-entry's version 2026-05-09-abc", matches[0].VersionID)
	}
	if matches[0].ObjectKey != "logs/gone" {
		t.Fatalf("ObjectKey=%q, want logs/gone", matches[0].ObjectKey)
	}
}

func TestRouteVersionedExpiredDeleteMarkerHardDeleteLoneNonMarkerNoFire(t *testing.T) {
	// After a hard-delete the surviving entry is a regular version, not
	// a marker. Nothing to expire.
	rule := &s3lifecycle.Rule{
		ID:                        "r",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -1)
	versionPath := "logs/gone" + s3_constants.VersionsFolder + "/v_2026-05-08-old"
	ev := &reader.Event{
		TsNs:     now.UnixNano(),
		Bucket:   "bk",
		Key:      versionPath,
		OldEntry: &filer_pb.Entry{Name: "v_2026-05-08-old"},
	}
	regular := &filer_pb.Entry{
		Name:       "v_v1",
		Attributes: &filer_pb.FuseAttributes{Mtime: old.Unix()},
		Extended:   map[string][]byte{s3_constants.ExtVersionIdKey: []byte("v1")},
	}
	lister := &recordingLister{survivors: Survivors{Count: 1, LoneEntry: regular}}
	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("non-marker survivor must not fire, got %v", got)
	}
}

func TestRouteVersionedExpiredDeleteMarkerListerErrorSuppressed(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:                        "r",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -1)
	ev := markerEvent("bk", "logs/gone", "2026-05-09-abc", old.Unix(), old.UnixNano())

	lister := &recordingLister{err: errors.New("filer down")}
	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("lister error must suppress, got %v", got)
	}
}

func TestRouteVersionedExpiredDeleteMarkerInactiveActionSkipsListing(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:                        "r",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	// PriorStates omitted => BootstrapComplete=false => action stays inactive.
	snap := compileWithVersioned(rule, nil)

	now := time.Now()
	old := now.AddDate(0, 0, -1)
	ev := markerEvent("bk", "logs/gone", "2026-05-09-abc", old.Unix(), old.UnixNano())

	lister := &recordingLister{survivors: Survivors{Count: 1}}
	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("inactive action must not produce a match, got %v", got)
	}
	if len(lister.calls) != 0 {
		t.Fatalf("inactive action must not consult lister, calls=%v", lister.calls)
	}
}

func TestRouteVersionedRegularVersionCreateSkipsListing(t *testing.T) {
	// A non-marker version create under .versions/<key>/ can never be the
	// sole survivor (Count >= 2 by definition), so the lister must NOT
	// be consulted on every regular versioned PUT.
	rule := &s3lifecycle.Rule{
		ID:                        "r",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -1)
	versionPath := "logs/keep" + s3_constants.VersionsFolder + "/v_v1"
	ev := eventCreate("bk", versionPath, old.Unix(), 100, old.UnixNano())
	ev.NewEntry.Extended = map[string][]byte{
		s3_constants.ExtVersionIdKey: []byte("v1"),
	}

	lister := &recordingLister{survivors: Survivors{Count: 1}}
	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("regular version create must not fire EXP_DM, got %v", got)
	}
	if len(lister.calls) != 0 {
		t.Fatalf("lister consulted for regular version create: calls=%v", lister.calls)
	}
}

func TestRouteVersionedDeleteMarkerNoExpDMRuleSkipsListing(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -1)
	ev := markerEvent("bk", "logs/gone", "2026-05-09-abc", old.Unix(), old.UnixNano())

	lister := &recordingLister{survivors: Survivors{Count: 1}}
	Route(context.Background(), snap, ev, now, lister)
	if len(lister.calls) != 0 {
		t.Fatalf("lister consulted without EXP_DM rule: calls=%v", lister.calls)
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
			if got := Route(context.Background(), snap, ev, now, nil); len(got) != 0 {
				t.Fatalf("version-folder event should be skipped, got %v", got)
			}
		})
	}
}


func bootstrapVersionEntry(versionID string, mtime time.Time, isDeleteMarker bool) *filer_pb.Entry {
	ext := map[string][]byte{
		s3_constants.ExtVersionIdKey: []byte(versionID),
	}
	if isDeleteMarker {
		ext[s3_constants.ExtDeleteMarkerKey] = []byte("true")
	}
	mtimeUnix := mtime.Unix()
	return &filer_pb.Entry{
		Name: "v_" + versionID,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:   mtimeUnix,
			MtimeNs: int32(mtime.UnixNano() - mtimeUnix*int64(1e9)),
		},
		Extended: ext,
	}
}

func TestRouteBootstrapVersionLatestExpirationDaysFires(t *testing.T) {
	// Bootstrap-emitted event for the LATEST version of a versioned
	// object. ExpirationDays should fire (creates a delete marker at
	// dispatch). ObjectKey is the LOGICAL key. VersionID must be EMPTY
	// for EXPIRATION_DAYS so the dispatcher fetches the current latest:
	// if a fresh PUT landed between schedule and dispatch, identity-CAS
	// against the original version's bytes would pass even though the
	// latest has moved on.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -2)
	entry := bootstrapVersionEntry("v-current", old, false)
	ev := &reader.Event{
		Bucket:   "bk",
		Key:      "logs/foo" + s3_constants.VersionsFolder + "/v_v-current",
		NewEntry: entry,
		BootstrapVersion: &reader.BootstrapVersion{
			LogicalKey:  "logs/foo",
			VersionID:   "v-current",
			IsLatest:    true,
			NumVersions: 1,
		},
	}
	matches := Route(context.Background(), snap, ev, now, nil)
	if len(matches) != 1 {
		t.Fatalf("want 1 match (ExpirationDays on latest), got %v", matches)
	}
	if matches[0].ObjectKey != "logs/foo" {
		t.Fatalf("ObjectKey=%q, want logs/foo", matches[0].ObjectKey)
	}
	if matches[0].VersionID != "" {
		t.Fatalf("VersionID=%q, want empty for EXPIRATION_DAYS", matches[0].VersionID)
	}
}

func TestRouteBootstrapVersionNoncurrentDaysFires(t *testing.T) {
	// Bootstrap-emitted event for a NONCURRENT version. NoncurrentDays
	// uses SuccessorModTime as the clock — when this version was
	// replaced by the next-newer sibling.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	successor := now.AddDate(0, 0, -3) // replaced 3 days ago
	old := now.AddDate(0, 0, -10)      // mtime older still
	entry := bootstrapVersionEntry("v-old", old, false)
	ev := &reader.Event{
		Bucket:   "bk",
		Key:      "logs/foo" + s3_constants.VersionsFolder + "/v_v-old",
		NewEntry: entry,
		BootstrapVersion: &reader.BootstrapVersion{
			LogicalKey:       "logs/foo",
			VersionID:        "v-old",
			IsLatest:         false,
			NumVersions:      2,
			NoncurrentIndex:  0,
			SuccessorModTime: successor,
		},
	}
	matches := Route(context.Background(), snap, ev, now, nil)
	if len(matches) != 1 {
		t.Fatalf("want 1 match (NoncurrentDays), got %v", matches)
	}
	if matches[0].VersionID != "v-old" {
		t.Fatalf("VersionID=%q, want v-old", matches[0].VersionID)
	}
}

func TestRouteBootstrapVersionNoncurrentRespectsNewerNoncurrentVersions(t *testing.T) {
	// NewerNoncurrentVersions=2 keeps the two newest noncurrents safe.
	// A version at NoncurrentIndex=0 (newest noncurrent) must NOT fire;
	// index=2 (third-newest) MUST fire.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
		NewerNoncurrentVersions:         2,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	successor := now.AddDate(0, 0, -3)
	old := now.AddDate(0, 0, -10)

	mk := func(idx int) *reader.Event {
		return &reader.Event{
			Bucket:   "bk",
			Key:      "logs/foo" + s3_constants.VersionsFolder + "/v_old" + string(rune('0'+idx)),
			NewEntry: bootstrapVersionEntry("old"+string(rune('0'+idx)), old, false),
			BootstrapVersion: &reader.BootstrapVersion{
				LogicalKey:       "logs/foo",
				VersionID:        "old" + string(rune('0'+idx)),
				IsLatest:         false,
				NumVersions:      4,
				NoncurrentIndex:  idx,
				SuccessorModTime: successor,
			},
		}
	}

	if got := Route(context.Background(), snap, mk(0), now, nil); len(got) != 0 {
		t.Fatalf("noncurrent rank 0 must be retained, got %v", got)
	}
	if got := Route(context.Background(), snap, mk(1), now, nil); len(got) != 0 {
		t.Fatalf("noncurrent rank 1 must be retained, got %v", got)
	}
	if got := Route(context.Background(), snap, mk(2), now, nil); len(got) != 1 {
		t.Fatalf("noncurrent rank 2 must fire, got %v", got)
	}
}

func TestRouteBootstrapVersionAbortMPUNeverEmittedForVersion(t *testing.T) {
	// AbortIncompleteMultipartUpload only applies to MPU init dirs, not
	// versioned object versions. Even if the bucket has the rule, a
	// bootstrap version event must not produce an ABORT_MPU match.
	rule := &s3lifecycle.Rule{
		ID:                          "r",
		Status:                      s3lifecycle.StatusEnabled,
		AbortMPUDaysAfterInitiation: 1,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	old := now.AddDate(0, 0, -10)
	ev := &reader.Event{
		Bucket:   "bk",
		Key:      "logs/foo" + s3_constants.VersionsFolder + "/v_x",
		NewEntry: bootstrapVersionEntry("x", old, false),
		BootstrapVersion: &reader.BootstrapVersion{
			LogicalKey:  "logs/foo",
			VersionID:   "x",
			IsLatest:    true,
			NumVersions: 1,
		},
	}
	if got := Route(context.Background(), snap, ev, now, nil); len(got) != 0 {
		t.Fatalf("bootstrap version event must not produce ABORT_MPU match, got %v", got)
	}
}

// versionsContainerEvent builds a .versions/<key>/ directory update.
// The NEW entry carries the cached latest-version mtime (the value
// setCachedListMetadata writes alongside the latest pointer). The
// directory's own Mtime is preserved at containerStaleMtime so the
// router can't accidentally use it as the successor clock.
func versionsContainerEvent(bucket, logical, oldID, newID string, latestMtimeUnix int64) *reader.Event {
	const containerStaleMtime int64 = 1
	mk := func(id string, includeMtime bool) *filer_pb.Entry {
		ext := map[string][]byte{}
		if id != "" {
			ext[s3_constants.ExtLatestVersionIdKey] = []byte(id)
		}
		if includeMtime {
			ext[s3_constants.ExtLatestVersionMtimeKey] = []byte(fmt.Sprintf("%d", latestMtimeUnix))
		}
		return &filer_pb.Entry{
			Name:        logical + s3_constants.VersionsFolder,
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{Mtime: containerStaleMtime},
			Extended:    ext,
		}
	}
	return &reader.Event{
		Bucket:   bucket,
		Key:      logical + s3_constants.VersionsFolder,
		OldEntry: mk(oldID, false),
		NewEntry: mk(newID, true),
	}
}

func displacedVersionEntry(versionID string, mtimeUnix int64) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name: "v_" + versionID,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    mtimeUnix,
			FileSize: 100,
		},
		Extended: map[string][]byte{
			s3_constants.ExtVersionIdKey: []byte(versionID),
		},
	}
}

func TestRoutePointerTransitionFiresNoncurrentDays(t *testing.T) {
	// .versions/<key>/ directory update flips ExtLatestVersionIdKey from
	// v-old to v-new. v-old becomes noncurrent immediately. The router
	// looks up v-old's file (one RPC) and emits NoncurrentDays Match
	// against the LOGICAL key with VersionID=v-old.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	displaced := displacedVersionEntry("v-old", now.AddDate(0, 0, -10).Unix())
	ev := versionsContainerEvent("bk", "logs/foo", "v-old", "v-new", now.Unix())

	lister := &recordingLister{lookupEntry: displaced}
	matches := Route(context.Background(), snap, ev, now, lister)
	if len(matches) != 1 {
		t.Fatalf("want 1 match (NoncurrentDays), got %v", matches)
	}
	m := matches[0]
	if m.ObjectKey != "logs/foo" {
		t.Fatalf("ObjectKey=%q, want logs/foo", m.ObjectKey)
	}
	if m.VersionID != "v-old" {
		t.Fatalf("VersionID=%q, want displaced v-old", m.VersionID)
	}
	if len(lister.lookupCalls) != 1 || lister.lookupCalls[0] != "bk/logs/foo@v-old" {
		t.Fatalf("lookup calls=%v, want [bk/logs/foo@v-old]", lister.lookupCalls)
	}
}

func TestRoutePointerTransitionPointerUnchangedSkipped(t *testing.T) {
	// Directory update with same OLD/NEW pointer (e.g. some other
	// metadata changed): no transition; nothing to schedule and no
	// lookup RPC.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	ev := versionsContainerEvent("bk", "logs/foo", "v-same", "v-same", now.Unix())

	lister := &recordingLister{}
	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("unchanged pointer must not fire, got %v", got)
	}
	if len(lister.lookupCalls) != 0 {
		t.Fatalf("unchanged pointer must not consult lister: %v", lister.lookupCalls)
	}
}

func TestRoutePointerTransitionEmptyOldPointerNoNullSkipped(t *testing.T) {
	// First PUT on a brand-new versioned object: OLD pointer is empty,
	// no bare null version exists. Nothing displaced.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	ev := versionsContainerEvent("bk", "logs/foo", "", "v-new", now.Unix())

	lister := &recordingLister{} // nullEntry is nil
	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("empty old pointer + no null must not fire, got %v", got)
	}
	if len(lister.lookupCalls) != 0 {
		t.Fatalf("LookupVersion must not be called for empty oldID, got %v", lister.lookupCalls)
	}
	if len(lister.nullCalls) != 1 {
		t.Fatalf("expected exactly one LookupNullVersion call, got %v", lister.nullCalls)
	}
}

func TestRoutePointerTransitionEmptyOldPointerWithNullSchedules(t *testing.T) {
	// First versioned PUT after a pre-versioning bare object exists.
	// OLD pointer is empty, but the bare null is the displaced version.
	// NoncurrentDays must schedule it as VersionID="null" so the worker
	// doesn't have to wait for the next bootstrap.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	ev := versionsContainerEvent("bk", "logs/foo", "", "v-new", now.Unix())
	nullMt := now.AddDate(0, 0, -10)
	lister := &recordingLister{nullEntry: &filer_pb.Entry{
		Name: "foo",
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    nullMt.Unix(),
			FileSize: 50,
		},
	}}
	matches := Route(context.Background(), snap, ev, now, lister)
	if len(matches) != 1 {
		t.Fatalf("want 1 match (NoncurrentDays on null), got %v", matches)
	}
	if matches[0].VersionID != "null" {
		t.Fatalf("VersionID=%q, want \"null\"", matches[0].VersionID)
	}
	if matches[0].ObjectKey != "logs/foo" {
		t.Fatalf("ObjectKey=%q, want logs/foo", matches[0].ObjectKey)
	}
}

func TestRoutePointerTransitionExpansionIncludesNullVersion(t *testing.T) {
	// Suspended-bucket history: bare null exists with a recent mtime.
	// Versioning re-enabled and a new version was just written. With
	// NewerNoncurrentVersions=2 the rank-2 entry is the threshold-
	// crosser. Because the null sits between v-mid and v-old by mtime,
	// it occupies a noncurrent rank slot and shifts what the rank-2
	// entry actually IS. Without including null, ranks would be wrong.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
		NewerNoncurrentVersions:         2,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	ev := versionsContainerEvent("bk", "logs/foo", "v-cur", "v-new", now.Unix())
	// Mtimes (newest-first): v-new, v-cur, v-mid, null, v-old, v-old2.
	// Post-flip ranks: latest=v-new, rank0=v-cur, rank1=v-mid,
	// rank2=null, rank3=v-old, rank4=v-old2. The rank-2 crossing is
	// "null" — and without including null in the sibling set, rank2
	// would have been v-old (different version_id, wrong identity).
	listVersions := []*filer_pb.Entry{
		displacedVersionEntry("v-new", now.Unix()),
		displacedVersionEntry("v-cur", now.AddDate(0, 0, -1).Unix()),
		displacedVersionEntry("v-mid", now.AddDate(0, 0, -2).Unix()),
		displacedVersionEntry("v-old", now.AddDate(0, 0, -10).Unix()),
		displacedVersionEntry("v-old2", now.AddDate(0, 0, -20).Unix()),
	}
	nullEntry := &filer_pb.Entry{
		Name:       "foo",
		Attributes: &filer_pb.FuseAttributes{Mtime: now.AddDate(0, 0, -3).Unix()},
	}
	lister := &recordingLister{listVersions: listVersions, nullEntry: nullEntry}

	matches := Route(context.Background(), snap, ev, now, lister)
	versionIDs := []string{}
	for _, m := range matches {
		versionIDs = append(versionIDs, m.VersionID)
	}
	if !contains(versionIDs, "null") {
		t.Fatalf("rank-2 should be null after including bare entry, matches=%v", versionIDs)
	}
	for _, id := range []string{"v-cur", "v-mid", "v-old", "v-old2", "v-new"} {
		if contains(versionIDs, id) {
			t.Fatalf("only the rank-2 (null) entry should fire, got %s in matches=%v", id, versionIDs)
		}
	}
}

func TestRoutePointerTransitionDisplacedVersionMissingSuppressed(t *testing.T) {
	// Race: by the time the router looks up v-old, it's already been
	// hard-deleted. LookupVersion returns (nil, nil); no Match.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	ev := versionsContainerEvent("bk", "logs/foo", "v-old", "v-new", now.Unix())

	lister := &recordingLister{lookupEntry: nil} // not found
	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("missing displaced version must suppress, got %v", got)
	}
	if len(lister.lookupCalls) != 1 {
		t.Fatalf("lookup attempted once, got %v", lister.lookupCalls)
	}
}

func TestRoutePointerTransitionNoNoncurrentRuleSkipsLookup(t *testing.T) {
	// Bucket has only ExpirationDays — no NoncurrentDays / NewerNoncurrent.
	// The router must NOT issue the lookup RPC.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	ev := versionsContainerEvent("bk", "logs/foo", "v-old", "v-new", now.Unix())

	lister := &recordingLister{lookupEntry: displacedVersionEntry("v-old", now.AddDate(0, 0, -10).Unix())}
	Route(context.Background(), snap, ev, now, lister)
	if len(lister.lookupCalls) != 0 {
		t.Fatalf("lister consulted without noncurrent rule: %v", lister.lookupCalls)
	}
}

func TestRoutePointerTransitionNewerNoncurrentNewestNoncurrentRetained(t *testing.T) {
	// NewerNoncurrentVersions=2 routes through the expansion path. The
	// freshly-noncurrent version is at rank 0 (newest noncurrent) and
	// the threshold-crossing rank N=2 doesn't exist (only 2 versions
	// total). No match expected — and ListVersions must be the one
	// consulted, not LookupVersion.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
		NewerNoncurrentVersions:         2,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	ev := versionsContainerEvent("bk", "logs/foo", "v-old", "v-new", now.Unix())
	lister := &recordingLister{listVersions: []*filer_pb.Entry{
		displacedVersionEntry("v-new", now.Unix()),
		displacedVersionEntry("v-old", now.AddDate(0, 0, -10).Unix()),
	}}

	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("rank-0 noncurrent must be retained under NewerNoncurrentVersions=2, got %v", got)
	}
	if len(lister.listCalls) != 1 {
		t.Fatalf("expansion path must consult ListVersions, calls=%v", lister.listCalls)
	}
	if len(lister.lookupCalls) != 0 {
		t.Fatalf("expansion path must not consult LookupVersion, calls=%v", lister.lookupCalls)
	}
}

func TestRoutePointerTransitionExpansionMissingNewIDSuppressed(t *testing.T) {
	// Race window: by the time ListVersions returns, the new pointer's
	// version file isn't visible yet. latestPos can't resolve, so the
	// router suppresses (bootstrap repairs state) instead of treating
	// the actual newest sibling as latest and misranking every other
	// version.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
		NewerNoncurrentVersions:         2,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	ev := versionsContainerEvent("bk", "logs/foo", "v-old", "v-new", now.Unix())
	// Listing returns v-old + v-mid but NOT v-new (the just-named latest).
	lister := &recordingLister{listVersions: []*filer_pb.Entry{
		displacedVersionEntry("v-mid", now.AddDate(0, 0, -1).Unix()),
		displacedVersionEntry("v-old", now.AddDate(0, 0, -10).Unix()),
	}}
	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("missing new id must suppress, got %v", got)
	}
}

func TestRoutePointerTransitionUnversionedBucketSkipped(t *testing.T) {
	// Same event shape on an unversioned bucket: should not even reach
	// the pointer-transition branch.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
	}
	snap := compileWith(rule, activatedPrior(rule))

	now := time.Now()
	ev := versionsContainerEvent("bk", "logs/foo", "v-old", "v-new", now.Unix())
	lister := &recordingLister{}
	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("unversioned bucket must not route pointer transitions, got %v", got)
	}
}

func TestRoutePointerTransitionMissingCachedMtimeSuppressed(t *testing.T) {
	// Older builds (or buggy paths) may write the latest pointer
	// without ExtLatestVersionMtimeKey. Without a reliable successor
	// clock NoncurrentDays would compute a year-0001 base and fire
	// immediately. Suppress until the cache lands.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	// Hand-build the event WITHOUT ExtLatestVersionMtimeKey on NewEntry.
	now := time.Now()
	ev := &reader.Event{
		Bucket: "bk",
		Key:    "logs/foo" + s3_constants.VersionsFolder,
		OldEntry: &filer_pb.Entry{
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{Mtime: 1},
			Extended:    map[string][]byte{s3_constants.ExtLatestVersionIdKey: []byte("v-old")},
		},
		NewEntry: &filer_pb.Entry{
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{Mtime: 1},
			Extended:    map[string][]byte{s3_constants.ExtLatestVersionIdKey: []byte("v-new")},
		},
	}
	displaced := displacedVersionEntry("v-old", now.AddDate(0, 0, -10).Unix())
	lister := &recordingLister{lookupEntry: displaced}
	if got := Route(context.Background(), snap, ev, now, lister); len(got) != 0 {
		t.Fatalf("missing cached mtime must suppress, got %v", got)
	}
	if len(lister.lookupCalls) != 0 {
		t.Fatalf("must not consult lister without successor mtime, got %v", lister.lookupCalls)
	}
}

func TestRoutePointerTransitionUsesCachedMtimeNotStaleDirMtime(t *testing.T) {
	// Regression: the .versions/ directory's own Attributes.Mtime is
	// preserved across pointer updates by updateLatestVersionInDirectory.
	// Using it as SuccessorModTime would let a fresh pointer flip on an
	// old directory fire NoncurrentDays right away. Use the cached
	// latest-mtime instead.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 30,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	// The cached latest-mtime IS now (fresh PUT). Container's Attrs.Mtime
	// is stale (containerStaleMtime=1). With the buggy old code, the
	// match would fire immediately because successor=year-1970 +
	// 30 days < now. With the fix, due = cached latest mtime + 30 days,
	// which is in the future.
	ev := versionsContainerEvent("bk", "logs/foo", "v-old", "v-new", now.Unix())
	displaced := displacedVersionEntry("v-old", now.AddDate(0, 0, -10).Unix())
	lister := &recordingLister{lookupEntry: displaced}

	matches := Route(context.Background(), snap, ev, now, lister)
	if len(matches) != 1 {
		t.Fatalf("want 1 match (scheduled, not fired), got %v", matches)
	}
	if !matches[0].DueTime.After(now.Add(29 * 24 * time.Hour)) {
		t.Fatalf("DueTime=%v, want ~30d from now (cached mtime + 30d)", matches[0].DueTime)
	}
}

func TestRoutePointerTransitionNewerNoncurrentExpansionFiresOnCrossingThreshold(t *testing.T) {
	// NewerNoncurrentVersions=2 keeps the 2 newest noncurrents. Before
	// the pointer flip there were 3 versions: v-cur (latest), v-mid
	// (rank 0 noncurrent), v-old (rank 1 noncurrent). After flipping
	// to v-new the ranks shift to: v-new latest, v-cur rank 0, v-mid
	// rank 1, v-old rank 2 — v-old just crossed the threshold and
	// must fire NoncurrentDays this run instead of waiting for the
	// next bootstrap.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
		NewerNoncurrentVersions:         2,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	// Successor mtime (cached on container) is "now" — the new latest.
	ev := versionsContainerEvent("bk", "logs/foo", "v-cur", "v-new", now.Unix())
	allVersions := []*filer_pb.Entry{
		displacedVersionEntry("v-new", now.Unix()),                  // newest
		displacedVersionEntry("v-cur", now.AddDate(0, 0, -1).Unix()),
		displacedVersionEntry("v-mid", now.AddDate(0, 0, -10).Unix()),
		displacedVersionEntry("v-old", now.AddDate(0, 0, -30).Unix()), // oldest
	}
	lister := &recordingLister{listVersions: allVersions}

	matches := Route(context.Background(), snap, ev, now, lister)
	// v-cur (rank 0) and v-mid (rank 1) retained; v-old (rank 2) fires.
	versionIDs := []string{}
	for _, m := range matches {
		versionIDs = append(versionIDs, m.VersionID)
	}
	if !contains(versionIDs, "v-old") {
		t.Fatalf("v-old at rank 2 (>= NewerNoncurrentVersions=2) must fire, matches=%v", versionIDs)
	}
	for _, id := range []string{"v-cur", "v-mid"} {
		if contains(versionIDs, id) {
			t.Fatalf("rank-%d noncurrent must be retained, matches=%v", indexOf([]string{"v-cur", "v-mid"}, id), versionIDs)
		}
	}
	if len(lister.listCalls) != 1 {
		t.Fatalf("expansion path must call ListVersions once, got %v", lister.listCalls)
	}
	if len(lister.lookupCalls) != 0 {
		t.Fatalf("expansion path must not call LookupVersion, got %v", lister.lookupCalls)
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func indexOf(ss []string, s string) int {
	for i, x := range ss {
		if x == s {
			return i
		}
	}
	return -1
}

func TestRoutePointerTransitionExpansionEmitsOnlyThresholdCrossing(t *testing.T) {
	// Hot key with many already-eligible noncurrents under
	// NewerNoncurrentVersions=2. A pointer flip must NOT enqueue every
	// over-threshold version (Schedule.Add doesn't dedup; identity-CAS
	// only saves the dispatch RPC, not the heap slot). Only the version
	// that JUST crossed from kept to expired needs to enter the heap;
	// everything else was already scheduled by an earlier transition or
	// bootstrap.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 1,
		NewerNoncurrentVersions:         2,
	}
	snap := compileWithVersioned(rule, activatedPrior(rule))

	now := time.Now()
	ev := versionsContainerEvent("bk", "logs/foo", "v-cur", "v-new", now.Unix())
	// 6 versions total: v-new latest after flip, then v-cur (rank 0),
	// v-mid (rank 1), v-2 (rank 2 — newly crossed), v-3, v-4 (already
	// past threshold from previous flips). Only rank 2 should enter
	// the heap on this flip.
	allVersions := []*filer_pb.Entry{
		displacedVersionEntry("v-new", now.Unix()),
		displacedVersionEntry("v-cur", now.AddDate(0, 0, -1).Unix()),
		displacedVersionEntry("v-mid", now.AddDate(0, 0, -2).Unix()),
		displacedVersionEntry("v-2", now.AddDate(0, 0, -10).Unix()),
		displacedVersionEntry("v-3", now.AddDate(0, 0, -20).Unix()),
		displacedVersionEntry("v-4", now.AddDate(0, 0, -30).Unix()),
	}
	lister := &recordingLister{listVersions: allVersions}

	matches := Route(context.Background(), snap, ev, now, lister)
	versionIDs := []string{}
	for _, m := range matches {
		versionIDs = append(versionIDs, m.VersionID)
	}
	// Want exactly v-2 (rank 2 — newly crossed). Not v-3 / v-4 (already
	// over) and not v-cur / v-mid (still kept).
	if !contains(versionIDs, "v-2") {
		t.Fatalf("v-2 at the new crossing rank must fire, matches=%v", versionIDs)
	}
	for _, id := range []string{"v-3", "v-4"} {
		if contains(versionIDs, id) {
			t.Fatalf("over-threshold %s must NOT re-enter the heap on this flip, matches=%v", id, versionIDs)
		}
	}
	for _, id := range []string{"v-cur", "v-mid"} {
		if contains(versionIDs, id) {
			t.Fatalf("retained %s must not fire, matches=%v", id, versionIDs)
		}
	}
}
