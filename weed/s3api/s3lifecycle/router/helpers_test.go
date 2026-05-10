package router

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Direct coverage for the pure helpers in router.go that the larger
// Route* tests exercise indirectly. Pinning each one separately catches
// regressions before they manifest as a broader Route failure that's
// harder to diagnose.

// ---------- successorModTimeFromContainer ----------

func TestSuccessorModTimeFromContainer_MissingExtReturnsZero(t *testing.T) {
	assert.True(t, successorModTimeFromContainer(&filer_pb.Entry{}).IsZero())
	assert.True(t, successorModTimeFromContainer(&filer_pb.Entry{
		Extended: map[string][]byte{},
	}).IsZero())
}

func TestSuccessorModTimeFromContainer_EmptyValueReturnsZero(t *testing.T) {
	got := successorModTimeFromContainer(&filer_pb.Entry{
		Extended: map[string][]byte{s3_constants.ExtLatestVersionMtimeKey: nil},
	})
	assert.True(t, got.IsZero())
}

func TestSuccessorModTimeFromContainer_NonNumericReturnsZero(t *testing.T) {
	// A malformed value is the writer's bug; the helper must not
	// propagate a parse error or surface a nonsense time.
	got := successorModTimeFromContainer(&filer_pb.Entry{
		Extended: map[string][]byte{s3_constants.ExtLatestVersionMtimeKey: []byte("not-a-number")},
	})
	assert.True(t, got.IsZero())
}

func TestSuccessorModTimeFromContainer_NonPositiveReturnsZero(t *testing.T) {
	// The container ext stores Unix seconds; <=0 means "not set" so
	// the helper falls back to zero rather than emitting 1970-01-01.
	for _, raw := range []string{"0", "-1", "-1000"} {
		t.Run(raw, func(t *testing.T) {
			got := successorModTimeFromContainer(&filer_pb.Entry{
				Extended: map[string][]byte{s3_constants.ExtLatestVersionMtimeKey: []byte(raw)},
			})
			assert.True(t, got.IsZero(), "value %q must produce zero time", raw)
		})
	}
}

func TestSuccessorModTimeFromContainer_PositiveSecondsRoundTrip(t *testing.T) {
	got := successorModTimeFromContainer(&filer_pb.Entry{
		Extended: map[string][]byte{s3_constants.ExtLatestVersionMtimeKey: []byte("1700000000")},
	})
	assert.Equal(t, time.Unix(1700000000, 0).UTC(), got.UTC())
}

// ---------- logicalKeyFromVersionPath ----------

func TestLogicalKeyFromVersionPath_ExtractsLogicalKey(t *testing.T) {
	logical, ok := logicalKeyFromVersionPath("a/b/c.versions/v_abc")
	require.True(t, ok)
	assert.Equal(t, "a/b/c", logical)
}

func TestLogicalKeyFromVersionPath_RejectsPathWithoutVersionsParent(t *testing.T) {
	// The parent of the version file must end with .versions; otherwise
	// it's not a version path.
	_, ok := logicalKeyFromVersionPath("a/b/c/v_abc")
	assert.False(t, ok)
}

func TestLogicalKeyFromVersionPath_RejectsRootLevelPath(t *testing.T) {
	// LastIndex returning 0 means the only "/" is at index 0, which
	// would yield an empty parent — not a real version path.
	_, ok := logicalKeyFromVersionPath("/v_abc")
	assert.False(t, ok)
}

func TestLogicalKeyFromVersionPath_RejectsNoSlashes(t *testing.T) {
	_, ok := logicalKeyFromVersionPath("v_abc")
	assert.False(t, ok)
	_, ok = logicalKeyFromVersionPath("")
	assert.False(t, ok)
}

func TestLogicalKeyFromVersionPath_RejectsBareVersionsContainer(t *testing.T) {
	// A path that's only the .versions container has no version-file
	// child to extract a logical key from.
	_, ok := logicalKeyFromVersionPath(s3_constants.VersionsFolder + "/v_x")
	assert.False(t, ok, "logical key cannot be empty after trim")
}

// ---------- isVersionsContainerKey ----------

func TestIsVersionsContainerKey(t *testing.T) {
	cases := []struct {
		key  string
		want bool
	}{
		{"obj" + s3_constants.VersionsFolder, true},
		{"a/b/obj" + s3_constants.VersionsFolder, true},
		// Bucket-root .versions is rejected explicitly: it has no
		// logical object key, so the router can't process it.
		{s3_constants.VersionsFolder, false},
		{"obj.versions/v_x", false}, // the version file inside, not the container
		{"obj", false},
		{"", false},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			assert.Equal(t, c.want, isVersionsContainerKey(c.key))
		})
	}
}

// ---------- isVersionFolderPath ----------

func TestIsVersionFolderPath(t *testing.T) {
	// Reports whether a key sits inside a .versions/ folder, looking at
	// the parent segment specifically. The router uses this to skip
	// version-file events that need sibling state to classify.
	cases := []struct {
		key  string
		want bool
	}{
		{"obj.versions/v_aaa", true},
		{"a/b/obj.versions/v_aaa", true},
		{"obj/v_aaa", false},    // parent isn't .versions
		{"obj.versions", false}, // the container itself, not a child
		// "obj.versions/" reads as the trailing-slash form of the path
		// "obj.versions" → leaf is "obj.versions" which ends with the
		// suffix → true. The router never sees this shape from the
		// reader (events carry concrete leaf names), but pin it here so
		// the contract is documented.
		{"obj.versions/", true},
		{"v_aaa", false}, // no slash at all
		{"", false},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			assert.Equal(t, c.want, isVersionFolderPath(c.key))
		})
	}
}

// ---------- isDeleteMarkerEntry ----------

func TestIsDeleteMarkerEntry_NilOrEmptyExtendedReturnsFalse(t *testing.T) {
	assert.False(t, isDeleteMarkerEntry(nil))
	assert.False(t, isDeleteMarkerEntry(&filer_pb.Entry{}))
	assert.False(t, isDeleteMarkerEntry(&filer_pb.Entry{Extended: map[string][]byte{}}))
}

func TestIsDeleteMarkerEntry_TrueOnlyForLiteralTrue(t *testing.T) {
	// The marker key must be exactly "true"; any other value is not a
	// marker. Pinning catches a regression that does case-insensitive
	// matching or treats "1" as truthy.
	mk := func(val string) *filer_pb.Entry {
		return &filer_pb.Entry{Extended: map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte(val)}}
	}
	assert.True(t, isDeleteMarkerEntry(mk("true")))
	assert.False(t, isDeleteMarkerEntry(mk("True")))
	assert.False(t, isDeleteMarkerEntry(mk("TRUE")))
	assert.False(t, isDeleteMarkerEntry(mk("1")))
	assert.False(t, isDeleteMarkerEntry(mk("")))
	assert.False(t, isDeleteMarkerEntry(mk("false")))
}

// ---------- extractTags ----------

func TestExtractTags_NilOrEmptyReturnsNil(t *testing.T) {
	assert.Nil(t, extractTags(nil))
	assert.Nil(t, extractTags(map[string][]byte{}))
}

func TestExtractTags_OnlyKeysWithObjectTaggingPrefix(t *testing.T) {
	// extractTags returns ExtVersionIdKey, Mime, etc. → no.
	// It only picks up keys with the AmzObjectTagging prefix and strips
	// that prefix to produce the tag map.
	prefix := s3_constants.AmzObjectTagging + "-"
	ext := map[string][]byte{
		prefix + "env":       []byte("prod"),
		prefix + "team":      []byte("data"),
		"X-Amz-Other":        []byte("ignored"),
		"Seaweed-X-Internal": []byte("ignored"),
	}
	got := extractTags(ext)
	require.Len(t, got, 2)
	assert.Equal(t, "prod", got["env"])
	assert.Equal(t, "data", got["team"])
}

func TestExtractTags_ReturnsNilWhenNoTaggingPrefixedKeys(t *testing.T) {
	// Keys with no tagging prefix produce a nil map (not an empty map);
	// the router treats nil as "no tags" without an extra branch.
	ext := map[string][]byte{
		"X-Amz-Other":        []byte("ignored"),
		"Seaweed-X-Internal": []byte("ignored"),
	}
	got := extractTags(ext)
	assert.Nil(t, got)
}

// ---------- hasActiveEventDrivenAction ----------

func TestHasActiveEventDrivenAction(t *testing.T) {
	// Build a snapshot with two action kinds and one of them set to
	// scan-only (inactive in event-driven view). hasActiveEventDrivenAction
	// must answer true only for the active event-driven kind.
	rule := &s3lifecycle.Rule{
		ID:                          "r",
		Status:                      s3lifecycle.StatusEnabled,
		ExpirationDays:              7,
		AbortMPUDaysAfterInitiation: 3,
	}
	hash := s3lifecycle.RuleHash(rule)
	expirationKey := s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: s3lifecycle.ActionKindExpirationDays}
	abortKey := s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: s3lifecycle.ActionKindAbortMPU}
	prior := map[s3lifecycle.ActionKey]engine.PriorState{
		expirationKey: {BootstrapComplete: true, Mode: engine.ModeEventDriven},
		abortKey:      {BootstrapComplete: true, Mode: engine.ModeScanOnly},
	}
	snap := engine.New().Compile(
		[]engine.CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		engine.CompileOptions{PriorStates: prior},
	)
	keys := []s3lifecycle.ActionKey{expirationKey, abortKey}

	// Active event-driven kind matches.
	assert.True(t, hasActiveEventDrivenAction(snap, keys, s3lifecycle.ActionKindExpirationDays))
	// Scan-only kind does NOT match — operator promoted it to scan-only
	// and the router must respect that by skipping the event-driven path.
	assert.False(t, hasActiveEventDrivenAction(snap, keys, s3lifecycle.ActionKindAbortMPU))
	// A kind not in the keys list is false.
	assert.False(t, hasActiveEventDrivenAction(snap, keys, s3lifecycle.ActionKindNoncurrentDays))
}

func TestHasActiveEventDrivenAction_NilActionSkipped(t *testing.T) {
	// A key the snapshot doesn't know about returns nil from Action;
	// the helper must skip rather than panic.
	snap := engine.New().Compile(nil, engine.CompileOptions{})
	keys := []s3lifecycle.ActionKey{
		{Bucket: "ghost", ActionKind: s3lifecycle.ActionKindExpirationDays},
	}
	assert.False(t, hasActiveEventDrivenAction(snap, keys, s3lifecycle.ActionKindExpirationDays))
}

// ---------- engine.Snapshot accessors ----------

func TestSnapshot_BucketVersionedReportsCompiledFlag(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 7}
	snap := engine.New().Compile(
		[]engine.CompileInput{
			{Bucket: "vbk", Rules: []*s3lifecycle.Rule{rule}, Versioned: true},
			{Bucket: "ubk", Rules: []*s3lifecycle.Rule{rule}, Versioned: false},
		},
		engine.CompileOptions{},
	)
	assert.True(t, snap.BucketVersioned("vbk"))
	assert.False(t, snap.BucketVersioned("ubk"))
	// Unknown bucket: not versioned, not a panic.
	assert.False(t, snap.BucketVersioned("missing"))
}

func TestSnapshot_BucketActionKeysReturnsCompiledList(t *testing.T) {
	// Every CompileInput rule emits one action key per RuleActionKind;
	// BucketActionKeys must surface them all (regardless of Mode) so
	// MatchPath can iterate.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 7}
	snap := engine.New().Compile(
		[]engine.CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		engine.CompileOptions{},
	)
	keys := snap.BucketActionKeys("bk")
	require.NotEmpty(t, keys)
	for _, k := range keys {
		assert.Equal(t, "bk", k.Bucket)
	}
}

func TestSnapshot_BucketActionKeysUnknownBucketReturnsNil(t *testing.T) {
	snap := engine.New().Compile(nil, engine.CompileOptions{})
	assert.Nil(t, snap.BucketActionKeys("missing"))
}

func TestSnapshot_ActionUnknownKeyReturnsNil(t *testing.T) {
	snap := engine.New().Compile(nil, engine.CompileOptions{})
	got := snap.Action(s3lifecycle.ActionKey{Bucket: "ghost", ActionKind: s3lifecycle.ActionKindExpirationDays})
	assert.Nil(t, got)
}

func TestSnapshot_AllActionsCoversEveryCompiledKind(t *testing.T) {
	// AllActions must enumerate every compiled action regardless of
	// active state, because the dispatcher uses it for full-snapshot
	// reporting.
	rule := &s3lifecycle.Rule{
		ID:                          "r",
		Status:                      s3lifecycle.StatusEnabled,
		ExpirationDays:              7,
		AbortMPUDaysAfterInitiation: 3,
	}
	snap := engine.New().Compile(
		[]engine.CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		engine.CompileOptions{},
	)
	all := snap.AllActions()
	wantKinds := s3lifecycle.RuleActionKinds(rule)
	require.Len(t, all, len(wantKinds))
	seen := map[s3lifecycle.ActionKind]bool{}
	for _, a := range all {
		seen[a.Key.ActionKind] = true
	}
	for _, k := range wantKinds {
		assert.True(t, seen[k], "missing kind %v in AllActions", k)
	}
}

func TestSnapshot_SnapshotIDIsMonotonicAcrossRecompiles(t *testing.T) {
	// Every Compile call advances snapshotIDSeq; pin that successive
	// snapshots from the same Engine carry strictly increasing IDs so
	// the dispatcher's stale-snapshot check works.
	e := engine.New()
	first := e.Compile(nil, engine.CompileOptions{})
	second := e.Compile(nil, engine.CompileOptions{})
	assert.Greater(t, second.SnapshotID(), first.SnapshotID(),
		"second compile must produce a strictly greater snapshot id")
}
