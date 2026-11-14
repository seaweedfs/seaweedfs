package dailyrun

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scriptedDispatcher struct {
	calls []string
	fail  map[string]bool
}

func (d *scriptedDispatcher) Delete(_ context.Context, a *engine.CompiledAction, e *bootstrap.Entry) error {
	k := a.Bucket + "/" + e.Path + "@" + e.VersionID
	d.calls = append(d.calls, k)
	if d.fail[k] {
		return errors.New("delete failed")
	}
	return nil
}

type savingStore struct {
	*fakeStore
	saves    [][]byte
	attempts int
	failAll  bool
}

func (s *savingStore) Read(ctx context.Context, dir, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.fakeStore.Read(ctx, dir, name)
}

func (s *savingStore) Save(ctx context.Context, dir, name string, content []byte) error {
	s.attempts++
	if s.failAll {
		return errors.New("filer down")
	}
	s.saves = append(s.saves, content)
	return s.fakeStore.Save(ctx, dir, name, content)
}

var allShards = func() []int {
	out := make([]int, s3lifecycle.ShardCount)
	for i := range out {
		out[i] = i
	}
	return out
}()

func checkpointTestView(t *testing.T, prefix string) *engine.Snapshot {
	t.Helper()
	rules := func() []*s3lifecycle.Rule {
		return []*s3lifecycle.Rule{{ID: "r", Status: s3lifecycle.StatusEnabled, Prefix: prefix, ExpirationDays: 1, NoncurrentVersionExpirationDays: 1}}
	}
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: "a", Rules: rules()},
		{Bucket: "b", Versioned: true, Rules: rules()},
		{Bucket: "c", Rules: rules()},
	}, engine.CompileOptions{})
	snap := e.Snapshot()
	for _, a := range snap.AllActions() {
		snap.MarkActive(a.Key)
	}
	return snap
}

func checkpointTestFiler() *fakeFiler {
	t1 := time.Now().Add(-30 * 24 * time.Hour)
	t2, t3 := t1.Add(time.Hour), t1.Add(2*time.Hour)
	ver := func(id string, mtime time.Time) *filer_pb.Entry {
		return fileWithExt(id, mtime, 1, map[string][]byte{s3_constants.ExtVersionIdKey: []byte(id)})
	}
	vs := s3_constants.VersionsFolder
	return &fakeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/a":          {file("a1", t1, 1), file("a2", t1, 1)},
		"/buckets/b":          {file("b0", t1, 1), versionsDir("k1"+vs, ""), versionsDir("obj"+vs, ""), file("z", t1, 1)},
		"/buckets/b/k1" + vs:  {ver("k1a", t1), ver("k1b", t2)},
		"/buckets/b/obj" + vs: {ver("v1", t1), ver("v2", t2), ver("v3", t3)},
		"/buckets/c":          {file("c1", t1, 1)},
	}}
}

var fullWalk = []string{
	"a/a1@", "a/a2@",
	"b/k1@k1b", "b/k1@k1a", "b/obj@v3", "b/obj@v2", "b/obj@v1", "b/b0@", "b/z@",
	"c/c1@",
}

type checkpointHarness struct {
	t      *testing.T
	client *fakeFiler
	store  *savingStore
	p      *FilerCursorPersister
}

func newCheckpointHarness(t *testing.T) *checkpointHarness {
	store := &savingStore{fakeStore: newFakeStore()}
	return &checkpointHarness{t: t, client: checkpointTestFiler(), store: store, p: &FilerCursorPersister{Store: store}}
}

func (h *checkpointHarness) walk(ctx context.Context, view *engine.Snapshot, shards []int, fail ...string) ([]string, error) {
	d := &scriptedDispatcher{fail: map[string]bool{}}
	for _, f := range fail {
		d.fail[f] = true
	}
	err := WalkBuckets(ctx, view, shards, []string{"a", "b", "c"}, FilerListFunc(h.client, "/buckets", view), d, h.p)
	return d.calls, err
}

func (h *checkpointHarness) saved(shards []int) WalkCheckpoint {
	cp, found, err := h.p.LoadWalk(context.Background(), shards)
	require.NoError(h.t, err)
	require.True(h.t, found)
	return cp
}

func TestWalkBuckets_ResumesAfterFailureInsideVersionGroup(t *testing.T) {
	h := newCheckpointHarness(t)
	view := checkpointTestView(t, "")

	calls, err := h.walk(context.Background(), view, allShards, "b/obj@v1")
	require.Error(t, err)
	assert.Equal(t, append(fullWalk[:7:7], "c/c1@"), calls, "a failed bucket does not stop the others")
	cp := h.saved(allShards)
	assert.Equal(t, []string{"a", "c"}, cp.Done)
	assert.Equal(t, map[string]string{"b": "1:k1.versions"}, cp.Resume, "the position stays before the unfinished group")
	assert.Equal(t, engine.WalkContentHash(view), cp.ViewHash)

	calls, err = h.walk(context.Background(), view, allShards)
	require.NoError(t, err)
	assert.Equal(t, []string{"b/obj@v3", "b/obj@v2", "b/obj@v1", "b/b0@", "b/z@"}, calls)
	cp = h.saved(allShards)
	assert.Empty(t, cp.Done)
	assert.Empty(t, cp.Resume)

	calls, err = h.walk(context.Background(), view, allShards)
	require.NoError(t, err)
	assert.Equal(t, fullWalk, calls, "after a completed walk the next one is full")
}

func TestWalkBuckets_CheckpointBoundToViewAndShards(t *testing.T) {
	h := newCheckpointHarness(t)
	view := checkpointTestView(t, "")
	_, err := h.walk(context.Background(), view, allShards, "b/obj@v1")
	require.Error(t, err)

	calls, err := h.walk(context.Background(), checkpointTestView(t, "o"), allShards)
	require.NoError(t, err)
	assert.Equal(t, []string{"b/obj@v3", "b/obj@v2", "b/obj@v1"}, calls, "another rule set walks from the start")

	_, err = h.walk(context.Background(), view, allShards, "b/obj@v1")
	require.Error(t, err)
	var some []int
	for _, sh := range allShards {
		if sh != 3 {
			some = append(some, sh)
		}
	}
	calls, err = h.walk(context.Background(), view, some)
	require.NoError(t, err)
	assert.Contains(t, calls, "b/k1@k1b", "another shard set walks from the start")
	assert.Equal(t, map[string]string{"b": "1:k1.versions"}, h.saved(allShards).Resume, "and keeps its own record")
	assert.Contains(t, h.store.files, CursorDir+"/walk-00-15.json")

	var f walkCheckpointFile
	require.NoError(t, json.Unmarshal(h.store.files[CursorDir+"/walk-00-15.json"], &f))
	f.SavedNs = time.Now().Add(-walkCheckpointMaxAge - time.Hour).UnixNano()
	h.store.files[CursorDir+"/walk-00-15.json"], _ = json.Marshal(f)
	calls, err = h.walk(context.Background(), view, allShards)
	require.NoError(t, err)
	assert.Equal(t, fullWalk, calls, "a record nobody saved for too long is not resumed")
}

func TestWalkBuckets_StuckResumeStartsOver(t *testing.T) {
	h := newCheckpointHarness(t)
	view := checkpointTestView(t, "")
	_, err := h.walk(context.Background(), view, allShards, "b/obj@v2")
	require.Error(t, err)

	calls, err := h.walk(context.Background(), view, allShards, "b/obj@v2")
	require.Error(t, err)
	assert.Equal(t, []string{"b/obj@v3", "b/obj@v2"}, calls)
	assert.Empty(t, h.saved(allShards).Done, "a resume without progress is dropped")

	calls, err = h.walk(context.Background(), view, allShards, "b/obj@v2")
	require.Error(t, err)
	assert.Equal(t, fullWalk[:6], calls[:6], "so the finished buckets get walked again")
}

func TestWalkBuckets_CanceledResumeKeepsCheckpoint(t *testing.T) {
	h := newCheckpointHarness(t)
	view := checkpointTestView(t, "")
	_, err := h.walk(context.Background(), view, allShards, "b/obj@v1")
	require.Error(t, err)
	before := h.saved(allShards)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = h.walk(ctx, view, allShards)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, before, h.saved(allShards), "a walk that could not read the record leaves it alone")
}

func TestWalkBuckets_ClearFailureFailsTheWalk(t *testing.T) {
	h := newCheckpointHarness(t)
	view := checkpointTestView(t, "")
	_, err := h.walk(context.Background(), view, allShards, "b/obj@v1")
	require.Error(t, err)

	h.store.failAll = true
	_, err = h.walk(context.Background(), view, allShards)
	require.Error(t, err, "a record left behind would skip buckets on the next walk")

	h.store.failAll = false
	calls, err := h.walk(context.Background(), view, allShards)
	require.NoError(t, err)
	assert.Equal(t, []string{"b/obj@v3", "b/obj@v2", "b/obj@v1", "b/b0@", "b/z@"}, calls)
}

func TestWalkBuckets_ResumesFromPeriodicSave(t *testing.T) {
	prev := walkCheckpointInterval.Load()
	walkCheckpointInterval.Store(0)
	defer walkCheckpointInterval.Store(prev)

	h := newCheckpointHarness(t)
	view := checkpointTestView(t, "")
	calls, err := h.walk(context.Background(), view, allShards)
	require.NoError(t, err)
	require.Equal(t, fullWalk, calls)

	// Replay a crash right after each periodic save.
	saves := h.store.saves
	require.Greater(t, len(saves), 5)
	left := len(fullWalk)
	for _, content := range saves[:len(saves)-1] {
		var f walkCheckpointFile
		require.NoError(t, json.Unmarshal(content, &f))
		h.store.files[CursorDir+"/walk-00-15.json"] = content
		calls, err := h.walk(context.Background(), view, allShards)
		require.NoError(t, err)
		require.LessOrEqual(t, len(calls), left, "resume after %s", content)
		left = len(calls)
		assert.Equal(t, append([]string(nil), fullWalk[len(fullWalk)-left:]...), calls, "resume after %s", content)
	}
	assert.Zero(t, left)
}

func TestWalkProgress_FailedPeriodicSaveWaitsForTheInterval(t *testing.T) {
	h := newCheckpointHarness(t)
	h.store.failAll = true
	w := loadWalkProgress(context.Background(), h.p, checkpointTestView(t, ""), allShards, []string{"a"})
	w.savedAt = time.Now().Add(-time.Hour)
	w.bucketDone(context.Background(), "a")
	w.bucketDone(context.Background(), "a")
	assert.Equal(t, 1, h.store.attempts, "a failed save is retried a full interval later, not on the next entry")
}
