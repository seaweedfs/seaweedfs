package filer

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestFilerConf(t *testing.T) {

	fc := NewFilerConf()

	conf := &filer_pb.FilerConf{Locations: []*filer_pb.FilerConf_PathConf{
		{
			LocationPrefix: "/buckets/abc",
			Collection:     "abc",
		},
		{
			LocationPrefix: "/buckets/abcd",
			Collection:     "abcd",
		},
		{
			LocationPrefix: "/buckets/",
			Replication:    "001",
		},
		{
			LocationPrefix: "/buckets",
			ReadOnly:       false,
		},
		{
			LocationPrefix: "/buckets/xxx",
			ReadOnly:       true,
		},
		{
			LocationPrefix: "/buckets/xxx/yyy",
			ReadOnly:       false,
		},
	}}
	fc.doLoadConf(conf)

	assert.Equal(t, "abc", fc.MatchStorageRule("/buckets/abc/jasdf").Collection)
	assert.Equal(t, "abcd", fc.MatchStorageRule("/buckets/abcd/jasdf").Collection)
	assert.Equal(t, "001", fc.MatchStorageRule("/buckets/abc/jasdf").Replication)

	assert.Equal(t, true, fc.MatchStorageRule("/buckets/xxx/yyy/zzz").ReadOnly)
	assert.Equal(t, false, fc.MatchStorageRule("/buckets/other").ReadOnly)

}

func TestWormInheritance(t *testing.T) {
	fc := NewFilerConf()
	fc.doLoadConf(&filer_pb.FilerConf{
		Version: FilerConfVersion,
		Locations: []*filer_pb.FilerConf_PathConf{
			{LocationPrefix: "/buckets/b/", Worm: proto.Bool(true), Ttl: "7d"},
			{LocationPrefix: "/buckets/b/quiet/", Collection: "quiet"},
			{LocationPrefix: "/buckets/b/scratch/", Worm: proto.Bool(false)},
			{LocationPrefix: "/buckets/b/scratch/keep/", Worm: proto.Bool(true)},
		},
	})

	// a rule that says nothing about worm keeps inheriting it, along with the ttl
	rule := fc.MatchStorageRule("/buckets/b/quiet/x")
	assert.True(t, rule.GetWorm())
	assert.Equal(t, "7d", rule.Ttl)

	// an explicit false turns it off, and a deeper rule turns it back on
	assert.False(t, fc.MatchStorageRule("/buckets/b/scratch/x").GetWorm())
	assert.True(t, fc.MatchStorageRule("/buckets/b/scratch/keep/x").GetWorm())

	// paths with no rule at all are unaffected
	assert.False(t, fc.MatchStorageRule("/buckets/other/x").GetWorm())
}

// TestWormLegacyFalseIsNotAnOverride pins that the explicit "worm": false every
// version 0 configuration carries does not read as a per-path opt-out.
func TestWormLegacyFalseIsNotAnOverride(t *testing.T) {
	const conf = `{
	  "locations": [
	    {"locationPrefix": "/buckets/b/", "worm": true},
	    {"locationPrefix": "/buckets/b/sub/", "collection": "sub", "worm": false}
	  ]
	}`

	fc := NewFilerConf()
	assert.NoError(t, fc.LoadFromBytes([]byte(conf)))
	assert.True(t, fc.MatchStorageRule("/buckets/b/sub/x").GetWorm())

	// the same file at the current version means what it says
	fc = NewFilerConf()
	assert.NoError(t, fc.LoadFromBytes([]byte(`{"version": 1,`+conf[1:])))
	assert.False(t, fc.MatchStorageRule("/buckets/b/sub/x").GetWorm())
}

// TestWormSurvivesRoundTrip guards the write side: an explicit false has to be
// stamped along with a version that says to honor it.
func TestWormSurvivesRoundTrip(t *testing.T) {
	fc := NewFilerConf()
	fc.SetLocationConf(&filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/b/", Worm: proto.Bool(true)})
	fc.SetLocationConf(&filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/b/sub/", Worm: proto.Bool(false)})
	fc.SetLocationConf(&filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/b/other/", Ttl: "7d"})

	var buf bytes.Buffer
	assert.NoError(t, fc.ToText(&buf))

	reloaded := NewFilerConf()
	assert.NoError(t, reloaded.LoadFromBytes(buf.Bytes()))
	assert.False(t, reloaded.MatchStorageRule("/buckets/b/sub/x").GetWorm())
	assert.True(t, reloaded.MatchStorageRule("/buckets/b/other/x").GetWorm())
}

// TestClonePathConf verifies that ClonePathConf copies all exported fields.
// Uses reflection to automatically detect new fields added to the protobuf,
// ensuring the test fails if ClonePathConf is not updated for new fields.
func TestClonePathConf(t *testing.T) {
	// Create a fully-populated PathConf with non-zero values for all fields
	src := &filer_pb.FilerConf_PathConf{
		LocationPrefix:           "/test/path",
		Collection:               "test_collection",
		Replication:              "001",
		Ttl:                      "7d",
		DiskType:                 "ssd",
		Fsync:                    true,
		VolumeGrowthCount:        5,
		ReadOnly:                 true,
		MaxFileNameLength:        255,
		DataCenter:               "dc1",
		Rack:                     "rack1",
		DataNode:                 "node1",
		DisableChunkDeletion:     true,
		Worm:                     proto.Bool(true),
		WormGracePeriodSeconds:   3600,
		WormRetentionTimeSeconds: 86400,
	}

	clone := ClonePathConf(src)

	// Verify it's a different object
	assert.NotSame(t, src, clone, "ClonePathConf should return a new object, not the same pointer")

	// Use reflection to compare all exported fields
	// This will automatically catch any new fields added to the protobuf
	srcVal := reflect.ValueOf(src).Elem()
	cloneVal := reflect.ValueOf(clone).Elem()
	srcType := srcVal.Type()

	for i := 0; i < srcType.NumField(); i++ {
		field := srcType.Field(i)

		// Skip unexported fields (protobuf internal fields like sizeCache, unknownFields)
		if !field.IsExported() {
			continue
		}

		srcField := srcVal.Field(i)
		cloneField := cloneVal.Field(i)

		// Compare field values
		if !reflect.DeepEqual(srcField.Interface(), cloneField.Interface()) {
			t.Errorf("Field %s not copied correctly: src=%v, clone=%v",
				field.Name, srcField.Interface(), cloneField.Interface())
		}
	}

	// Additionally verify that all exported fields in src are non-zero
	// This ensures we're testing with fully populated data
	for i := 0; i < srcType.NumField(); i++ {
		field := srcType.Field(i)
		if !field.IsExported() {
			continue
		}

		srcField := srcVal.Field(i)
		if srcField.IsZero() {
			t.Errorf("Test setup error: field %s has zero value, update test to set a non-zero value", field.Name)
		}
	}

	// Verify mutation of clone doesn't affect source
	clone.Collection = "modified"
	clone.ReadOnly = false
	*clone.Worm = false
	assert.Equal(t, "test_collection", src.Collection, "Modifying clone should not affect source Collection")
	assert.Equal(t, true, src.ReadOnly, "Modifying clone should not affect source ReadOnly")
	assert.Equal(t, true, src.GetWorm(), "Modifying clone should not affect source Worm")
}

func TestClonePathConfNil(t *testing.T) {
	clone := ClonePathConf(nil)
	assert.NotNil(t, clone, "ClonePathConf(nil) should return a non-nil empty PathConf")
	assert.Equal(t, "", clone.LocationPrefix, "ClonePathConf(nil) should return empty PathConf")
}

func TestApplyBucketQuotaReadOnly(t *testing.T) {
	const prefix = "/buckets/b/"

	// over quota: flips to read-only
	fc := NewFilerConf()
	readOnly, changed := fc.ApplyBucketQuotaReadOnly(prefix, 150, 100)
	assert.True(t, changed)
	assert.True(t, readOnly)
	assert.True(t, fc.MatchStorageRule(prefix).ReadOnly)

	// still over quota: no change
	_, changed = fc.ApplyBucketQuotaReadOnly(prefix, 150, 100)
	assert.False(t, changed)

	// back under quota: flips to writable
	readOnly, changed = fc.ApplyBucketQuotaReadOnly(prefix, 50, 100)
	assert.True(t, changed)
	assert.False(t, readOnly)
	assert.False(t, fc.MatchStorageRule(prefix).ReadOnly)

	// quota disabled leaves the flag untouched, so manual locks survive
	fc = NewFilerConf()
	fc.ApplyBucketQuotaReadOnly(prefix, 150, 100)
	readOnly, changed = fc.ApplyBucketQuotaReadOnly(prefix, 150, -1)
	assert.False(t, changed)
	assert.True(t, readOnly)

	// under quota and not read-only: no rule churn
	fc = NewFilerConf()
	_, changed = fc.ApplyBucketQuotaReadOnly(prefix, 50, 100)
	assert.False(t, changed)
}

func TestClearReadOnly(t *testing.T) {
	const prefix = "/buckets/b/"

	fc := NewFilerConf()
	assert.False(t, fc.ClearReadOnly(prefix), "no rule to clear")

	// locked by quota enforcement, then quota removed: still clearable
	fc.ApplyBucketQuotaReadOnly(prefix, 150, 100)
	assert.True(t, fc.ClearReadOnly(prefix))
	assert.False(t, fc.MatchStorageRule(prefix).ReadOnly)
	assert.False(t, fc.ClearReadOnly(prefix), "already writable")

	// clearing the flag keeps the rule's other settings
	fc = NewFilerConf()
	fc.SetLocationConf(&filer_pb.FilerConf_PathConf{LocationPrefix: prefix, Ttl: "7d", ReadOnly: true})
	assert.True(t, fc.ClearReadOnly(prefix))
	rule := fc.MatchStorageRule(prefix)
	assert.False(t, rule.ReadOnly)
	assert.Equal(t, "7d", rule.Ttl)
}

// fakeFilerConfClient is a minimal in-memory filer_pb.SeaweedFilerClient that
// only supports the single-file round trip ReadInsideFiler/SaveInsideFiler
// need: lookup, create-if-absent, update. Embedding the interface satisfies
// the rest of it; calling any other method panics on the nil embedded value.
type fakeFilerConfClient struct {
	filer_pb.SeaweedFilerClient

	mu      sync.Mutex
	entries map[string]*filer_pb.Entry // key: dir+"/"+name
}

func newFakeFilerConfClient() *fakeFilerConfClient {
	return &fakeFilerConfClient{entries: make(map[string]*filer_pb.Entry)}
}

func (c *fakeFilerConfClient) key(dir, name string) string { return dir + "/" + name }

func (c *fakeFilerConfClient) LookupDirectoryEntry(_ context.Context, in *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[c.key(in.Directory, in.Name)]
	if !ok {
		return nil, filer_pb.ErrNotFound
	}
	// A real gRPC round trip always hands back an independent copy (proto
	// marshal/unmarshal), so mutating what the caller gets back (as
	// saveFilerConfConditionally does before re-sending it) must not affect
	// what conditionHoldsLocked below compares against.
	return &filer_pb.LookupDirectoryEntryResponse{Entry: cloneFakeEntry(e)}, nil
}

func cloneFakeEntry(e *filer_pb.Entry) *filer_pb.Entry {
	if e == nil {
		return nil
	}
	// proto.Clone rather than a struct copy: filer_pb.Entry embeds
	// protoimpl.MessageState (a sync.Mutex), which a plain `*e` copy would
	// duplicate by value — exactly what go vet's copylocks check flags.
	return proto.Clone(e).(*filer_pb.Entry)
}

func (c *fakeFilerConfClient) CreateEntry(_ context.Context, in *filer_pb.CreateEntryRequest, _ ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.key(in.Directory, in.Entry.Name)
	if !conditionHoldsLocked(in.Condition, c.entries[key]) {
		return nil, errors.New("precondition failed")
	}
	c.entries[key] = in.Entry
	return &filer_pb.CreateEntryResponse{}, nil
}

func (c *fakeFilerConfClient) UpdateEntry(_ context.Context, in *filer_pb.UpdateEntryRequest, _ ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.key(in.Directory, in.Entry.Name)
	if !conditionHoldsLocked(in.Condition, c.entries[key]) {
		return nil, errors.New("precondition failed")
	}
	c.entries[key] = in.Entry
	return &filer_pb.UpdateEntryResponse{}, nil
}

// conditionHoldsLocked is a minimal stand-in for the filer server's
// writeConditionSatisfied (weed/server/filer_grpc_server_condition.go),
// covering just the clause kinds saveFilerConfConditionally uses, so tests
// can exercise the CAS path without a real filer server.
func conditionHoldsLocked(cond *filer_pb.WriteCondition, current *filer_pb.Entry) bool {
	if cond == nil {
		return true
	}
	for _, clause := range cond.Clauses {
		switch clause.Kind {
		case filer_pb.WriteCondition_IF_NOT_EXISTS:
			if current != nil {
				return false
			}
		case filer_pb.WriteCondition_IF_UNMODIFIED_SINCE:
			if current != nil && current.Attributes != nil && current.Attributes.Mtime > clause.UnixTime {
				return false
			}
		case filer_pb.WriteCondition_IF_ETAG_MATCH:
			if current == nil {
				return false
			}
			stored := ""
			if current.Attributes != nil {
				stored = fmt.Sprintf("%x", current.Attributes.Md5)
			}
			matched := false
			for _, want := range clause.Etags {
				if want == stored {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
	}
	return true
}

// putFilerConf seeds the fake client's filer.conf with the given rules.
func putFilerConf(t *testing.T, client *fakeFilerConfClient, rules ...*filer_pb.FilerConf_PathConf) {
	t.Helper()
	fc := NewFilerConf()
	for _, r := range rules {
		require.NoError(t, fc.SetLocationConf(r))
	}
	var buf bytes.Buffer
	require.NoError(t, fc.ToText(&buf))
	client.entries[client.key(DirectoryEtcSeaweedFS, FilerConfName)] = &filer_pb.Entry{
		Name:       FilerConfName,
		Content:    buf.Bytes(),
		Attributes: &filer_pb.FuseAttributes{},
	}
}

// readFilerConfText returns the current filer.conf content stored in the fake
// client, or "" if none exists yet.
func readFilerConfText(client *fakeFilerConfClient) string {
	e, ok := client.entries[client.key(DirectoryEtcSeaweedFS, FilerConfName)]
	if !ok {
		return ""
	}
	return string(e.Content)
}

func TestClearBucketLifecycleDayTTLs_NoFilerConf(t *testing.T) {
	client := newFakeFilerConfClient()

	removed, err := ClearBucketLifecycleDayTTLs(context.Background(), client, "/buckets", "mybucket", "mybucket")
	require.NoError(t, err)
	assert.Empty(t, removed)
	assert.Empty(t, readFilerConfText(client))
}

func TestClearBucketLifecycleDayTTLs_NoMatchingRule(t *testing.T) {
	client := newFakeFilerConfClient()
	putFilerConf(t, client, &filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/other/",
		Collection:     "other",
		Ttl:            "7d",
	})

	removed, err := ClearBucketLifecycleDayTTLs(context.Background(), client, "/buckets", "mybucket", "mybucket")
	require.NoError(t, err)
	assert.Empty(t, removed)
	assert.Contains(t, readFilerConfText(client), "other")
}

func TestClearBucketLifecycleDayTTLs_RemovesDayTTLUnderBucket(t *testing.T) {
	client := newFakeFilerConfClient()
	putFilerConf(t, client, &filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/mybucket/",
		Collection:     "mybucket",
		Ttl:            "7d",
	})

	removed, err := ClearBucketLifecycleDayTTLs(context.Background(), client, "/buckets", "mybucket", "mybucket")
	require.NoError(t, err)
	require.Len(t, removed, 1)
	assert.Equal(t, "/buckets/mybucket/", removed[0].LocationPrefix)
	assert.Equal(t, "7d", removed[0].Ttl)

	reloaded := NewFilerConf()
	require.NoError(t, reloaded.LoadFromBytes([]byte(readFilerConfText(client))))
	_, found := reloaded.GetLocationConf("/buckets/mybucket/")
	assert.False(t, found, "day-TTL rule should have been removed")
}

func TestClearBucketLifecycleDayTTLs_KeepsNonDayTTL(t *testing.T) {
	client := newFakeFilerConfClient()
	putFilerConf(t, client, &filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/mybucket/",
		Collection:     "mybucket",
		Ttl:            "7m", // minutes, not days: not a legacy lifecycle TTL rule
	})

	removed, err := ClearBucketLifecycleDayTTLs(context.Background(), client, "/buckets", "mybucket", "mybucket")
	require.NoError(t, err)
	assert.Empty(t, removed)

	reloaded := NewFilerConf()
	require.NoError(t, reloaded.LoadFromBytes([]byte(readFilerConfText(client))))
	_, found := reloaded.GetLocationConf("/buckets/mybucket/")
	assert.True(t, found, "non-day TTL rule should be left alone")
}

func TestClearBucketLifecycleDayTTLs_KeepsOtherBucketsAndCollections(t *testing.T) {
	client := newFakeFilerConfClient()
	putFilerConf(t, client,
		&filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/mybucket/", Collection: "mybucket", Ttl: "7d"},
		&filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/other/", Collection: "other", Ttl: "7d"},
		// nested under the target bucket's path but tagged to a different
		// collection (e.g. a filer-group-prefixed name): must not be swept up
		// by this bucket's cleanup, since GetCollectionTtls filters by collection.
		&filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/mybucket/nested/", Collection: "group_mybucket", Ttl: "7d"},
	)

	removed, err := ClearBucketLifecycleDayTTLs(context.Background(), client, "/buckets", "mybucket", "mybucket")
	require.NoError(t, err)
	require.Len(t, removed, 1)
	assert.Equal(t, "/buckets/mybucket/", removed[0].LocationPrefix)

	text := readFilerConfText(client)
	assert.Contains(t, text, "other")
	assert.Contains(t, text, "group_mybucket")
}

func TestRestoreFilerConfLocationRules_NoOpWhenEmpty(t *testing.T) {
	client := newFakeFilerConfClient()

	require.NoError(t, RestoreFilerConfLocationRules(context.Background(), client, nil))
	assert.Empty(t, readFilerConfText(client))
}

func TestRestoreFilerConfLocationRules_ReAddsRemovedRule(t *testing.T) {
	client := newFakeFilerConfClient()
	putFilerConf(t, client, &filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/mybucket/",
		Collection:     "mybucket",
		Ttl:            "7d",
	})

	removed, err := ClearBucketLifecycleDayTTLs(context.Background(), client, "/buckets", "mybucket", "mybucket")
	require.NoError(t, err)
	require.Len(t, removed, 1)

	require.NoError(t, RestoreFilerConfLocationRules(context.Background(), client, removed))

	reloaded := NewFilerConf()
	require.NoError(t, reloaded.LoadFromBytes([]byte(readFilerConfText(client))))
	rule, found := reloaded.GetLocationConf("/buckets/mybucket/")
	require.True(t, found, "restored rule should be present again")
	assert.Equal(t, "7d", rule.Ttl)
	assert.Equal(t, "mybucket", rule.Collection)
}

func TestRestoreFilerConfLocationRules_PreservesUnrelatedConcurrentEdits(t *testing.T) {
	// Simulates another writer adding an unrelated rule between the cleanup
	// and the restore: the restore must not clobber it, since it re-reads
	// filer.conf fresh rather than overwriting from a stale snapshot.
	client := newFakeFilerConfClient()
	putFilerConf(t, client, &filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/mybucket/",
		Collection:     "mybucket",
		Ttl:            "7d",
	})

	removed, err := ClearBucketLifecycleDayTTLs(context.Background(), client, "/buckets", "mybucket", "mybucket")
	require.NoError(t, err)
	require.Len(t, removed, 1)

	// Concurrent, unrelated edit lands after the cleanup.
	concurrentFc := NewFilerConf()
	require.NoError(t, concurrentFc.LoadFromBytes([]byte(readFilerConfText(client))))
	require.NoError(t, concurrentFc.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/unrelated/",
		Collection:     "unrelated",
		Ttl:            "3d",
	}))
	var buf bytes.Buffer
	require.NoError(t, concurrentFc.ToText(&buf))
	client.entries[client.key(DirectoryEtcSeaweedFS, FilerConfName)] = &filer_pb.Entry{
		Name:       FilerConfName,
		Content:    buf.Bytes(),
		Attributes: &filer_pb.FuseAttributes{},
	}

	require.NoError(t, RestoreFilerConfLocationRules(context.Background(), client, removed))

	reloaded := NewFilerConf()
	require.NoError(t, reloaded.LoadFromBytes([]byte(readFilerConfText(client))))
	_, found := reloaded.GetLocationConf("/buckets/mybucket/")
	assert.True(t, found, "restored rule should be present")
	_, found = reloaded.GetLocationConf("/buckets/unrelated/")
	assert.True(t, found, "concurrent unrelated edit must survive the restore")
}

// TestRestoreFilerConfLocationRules_DoesNotClobberSamePrefixConcurrentWrite
// pins the fix for the case the above test doesn't cover: a concurrent
// writer installing a *different* rule at the exact same LocationPrefix the
// restore is about to re-add. The outer CAS in saveFilerConfConditionally
// only guards against changes between this call's own read and its write —
// it can't see that the restore loop itself would otherwise blindly
// overwrite a value it just read a moment earlier. Restoring must skip a
// prefix that's no longer empty, leaving the concurrent writer's rule alone.
func TestRestoreFilerConfLocationRules_DoesNotClobberSamePrefixConcurrentWrite(t *testing.T) {
	client := newFakeFilerConfClient()
	putFilerConf(t, client, &filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/mybucket/",
		Collection:     "mybucket",
		Ttl:            "7d",
	})

	removed, err := ClearBucketLifecycleDayTTLs(context.Background(), client, "/buckets", "mybucket", "mybucket")
	require.NoError(t, err)
	require.Len(t, removed, 1)

	// Another writer installs a different rule at the exact same prefix
	// before the restore call reads filer.conf.
	concurrentFc := NewFilerConf()
	require.NoError(t, concurrentFc.LoadFromBytes([]byte(readFilerConfText(client))))
	require.NoError(t, concurrentFc.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/mybucket/", Collection: "mybucket", Ttl: "30d", ReadOnly: true,
	}))
	var buf bytes.Buffer
	require.NoError(t, concurrentFc.ToText(&buf))
	client.entries[client.key(DirectoryEtcSeaweedFS, FilerConfName)] = &filer_pb.Entry{
		Name:       FilerConfName,
		Content:    buf.Bytes(),
		Attributes: &filer_pb.FuseAttributes{},
	}

	require.NoError(t, RestoreFilerConfLocationRules(context.Background(), client, removed))

	reloaded := NewFilerConf()
	require.NoError(t, reloaded.LoadFromBytes([]byte(readFilerConfText(client))))
	rule, found := reloaded.GetLocationConf("/buckets/mybucket/")
	require.True(t, found)
	assert.Equal(t, "30d", rule.Ttl, "the concurrent writer's rule must survive, not the stale restored one")
	assert.True(t, rule.ReadOnly, "the concurrent writer's rule must survive, not the stale restored one")
}

// bumpMtime simulates a concurrent writer modifying filer.conf after it was
// read: it advances the stored entry's Mtime past unmodifiedSince so a
// pending IF_UNMODIFIED_SINCE condition built from that earlier read no
// longer holds.
func bumpMtime(client *fakeFilerConfClient, unmodifiedSince int64) {
	client.mu.Lock()
	defer client.mu.Unlock()
	e := client.entries[client.key(DirectoryEtcSeaweedFS, FilerConfName)]
	if e == nil {
		return
	}
	if e.Attributes == nil {
		e.Attributes = &filer_pb.FuseAttributes{}
	}
	e.Attributes.Mtime = unmodifiedSince + 1
}

// TestSaveFilerConfConditionally_RejectsUpdateModifiedConcurrently exercises
// the bootstrap fallback: a filer.conf entry with no Md5 yet (as if written
// before this code existed) falls back to the mtime-based precondition.
func TestSaveFilerConfConditionally_RejectsUpdateModifiedConcurrently(t *testing.T) {
	client := newFakeFilerConfClient()
	putFilerConf(t, client, &filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/a/", Collection: "a", Ttl: "7d"})

	snap, err := readFilerConfSnapshot(context.Background(), client)
	require.NoError(t, err)
	require.NotNil(t, snap.entry)
	require.Empty(t, snap.entry.Attributes.Md5, "test setup should model a pre-existing filer.conf with no stamped Md5")

	// Someone else writes filer.conf after our read but before our write.
	bumpMtime(client, snap.entry.Attributes.Mtime)

	err = saveFilerConfConditionally(context.Background(), client, snap)
	assert.Error(t, err, "expected a concurrent modification to fail the conditional write")
}

// TestSaveFilerConfConditionally_ExactContentCheckRejectsConcurrentModification
// exercises the primary path: once an entry has been written through this
// function (so it carries a content Md5), a later write with a stale
// snapshot is rejected by the exact IF_ETAG_MATCH check even when the
// concurrent writer happened to leave mtime unchanged — something a
// mtime-only precondition would have missed.
func TestSaveFilerConfConditionally_ExactContentCheckRejectsConcurrentModification(t *testing.T) {
	client := newFakeFilerConfClient()
	putFilerConf(t, client, &filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/a/", Collection: "a", Ttl: "7d"})

	// First write through saveFilerConfConditionally stamps Md5, moving
	// subsequent reads onto the exact-content check.
	snap, err := readFilerConfSnapshot(context.Background(), client)
	require.NoError(t, err)
	require.NoError(t, saveFilerConfConditionally(context.Background(), client, snap))

	snap2, err := readFilerConfSnapshot(context.Background(), client)
	require.NoError(t, err)
	require.NotEmpty(t, snap2.entry.Attributes.Md5, "expected Md5 to have been stamped by the prior write")

	// A concurrent writer changes the content but happens to land in the
	// same wall-clock second, so mtime alone would not catch this.
	concurrentFc := NewFilerConf()
	require.NoError(t, concurrentFc.LoadFromBytes([]byte(readFilerConfText(client))))
	require.NoError(t, concurrentFc.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/other/", Collection: "other", Ttl: "3d",
	}))
	var buf bytes.Buffer
	require.NoError(t, concurrentFc.ToText(&buf))
	newMd5 := md5.Sum(buf.Bytes())
	client.entries[client.key(DirectoryEtcSeaweedFS, FilerConfName)] = &filer_pb.Entry{
		Name:    FilerConfName,
		Content: buf.Bytes(),
		Attributes: &filer_pb.FuseAttributes{
			Mtime: snap2.entry.Attributes.Mtime, // unchanged on purpose
			Md5:   newMd5[:],
		},
	}

	err = saveFilerConfConditionally(context.Background(), client, snap2)
	assert.Error(t, err, "expected the exact-content check to reject a write whose baseline content hash no longer matches")
}

func TestSaveFilerConfConditionally_RejectsCreateWhenCreatedConcurrently(t *testing.T) {
	client := newFakeFilerConfClient()

	snap, err := readFilerConfSnapshot(context.Background(), client)
	require.NoError(t, err)
	require.Nil(t, snap.entry, "filer.conf should not exist yet")

	// Someone else creates filer.conf after our read but before our write.
	putFilerConf(t, client, &filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/b/", Collection: "b", Ttl: "3d"})

	err = saveFilerConfConditionally(context.Background(), client, snap)
	assert.Error(t, err, "expected a concurrent create to fail the conditional write")
	assert.Contains(t, readFilerConfText(client), "/buckets/b/", "the concurrent writer's content must survive")
}
