package scheduler

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// fakeListStream implements grpc.ServerStreamingClient[filer_pb.ListEntriesResponse].
type fakeListStream struct {
	responses []*filer_pb.ListEntriesResponse
	index     int
	ctx       context.Context
}

func (s *fakeListStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.ctx != nil {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
	}
	if s.index >= len(s.responses) {
		return nil, io.EOF
	}
	resp := s.responses[s.index]
	s.index++
	return resp, nil
}

func (s *fakeListStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *fakeListStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *fakeListStream) CloseSend() error             { return nil }
func (s *fakeListStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}
func (s *fakeListStream) SendMsg(any) error { return nil }
func (s *fakeListStream) RecvMsg(any) error { return nil }

// fakeFilerClient embeds SeaweedFilerClient (nil interface) and overrides
// only ListEntries. Calling any other method panics, which is fine for
// these tests.
type fakeFilerClient struct {
	filer_pb.SeaweedFilerClient

	mu       sync.Mutex
	tree     map[string][]*filer_pb.Entry // dir path -> immediate children
	listed   []string                     // dirs the walker asked about, in order
	listedN  int32                        // atomic counter for cross-goroutine reads
}

func (c *fakeFilerClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	c.mu.Lock()
	c.listed = append(c.listed, in.Directory)
	c.mu.Unlock()
	atomic.AddInt32(&c.listedN, 1)

	children := c.tree[in.Directory]
	responses := make([]*filer_pb.ListEntriesResponse, 0, len(children))
	for _, e := range children {
		responses = append(responses, &filer_pb.ListEntriesResponse{Entry: e})
	}
	return &fakeListStream{responses: responses, ctx: ctx}, nil
}

func (c *fakeFilerClient) listedCopy() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.listed))
	copy(out, c.listed)
	return out
}

// recordingInjector captures injected events; optionally returns a
// pre-set error to test propagation.
type recordingInjector struct {
	mu     sync.Mutex
	events []*reader.Event
	err    error
}

func (r *recordingInjector) InjectEvent(_ context.Context, ev *reader.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
	return r.err
}

func (r *recordingInjector) snapshot() []*reader.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*reader.Event, len(r.events))
	copy(out, r.events)
	return out
}

func dirEntry(name string, extended map[string][]byte) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        name,
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{},
		Extended:    extended,
	}
}

func fileEntry(name string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        name,
		IsDirectory: false,
		Attributes:  &filer_pb.FuseAttributes{},
	}
}

// ---------- isMPUInitDir ----------

func TestIsMPUInitDir_DetectsValidInitDir(t *testing.T) {
	e := dirEntry("upload-id-1", map[string][]byte{
		s3_constants.ExtMultipartObjectKey: []byte("foo/bar.txt"),
	})
	assert.True(t, isMPUInitDir(s3_constants.MultipartUploadsFolder+"/upload-id-1", e))
}

func TestIsMPUInitDir_RejectsUploadsRoot(t *testing.T) {
	e := dirEntry(s3_constants.MultipartUploadsFolder, nil)
	// key = ".uploads/" -> rest = "" -> false
	assert.False(t, isMPUInitDir(s3_constants.MultipartUploadsFolder+"/", e))
}

func TestIsMPUInitDir_RejectsPartFile(t *testing.T) {
	e := fileEntry("part-0001")
	assert.False(t, isMPUInitDir(s3_constants.MultipartUploadsFolder+"/upload-id-1/part-0001", e))
}

func TestIsMPUInitDir_RejectsRegularKey(t *testing.T) {
	e := fileEntry("key.txt")
	assert.False(t, isMPUInitDir("regular/object/key", e))
}

func TestIsMPUInitDir_RejectsEmptyExtended(t *testing.T) {
	e := dirEntry("upload-id-1", map[string][]byte{})
	assert.False(t, isMPUInitDir(s3_constants.MultipartUploadsFolder+"/upload-id-1", e))
}

func TestIsMPUInitDir_RejectsEmptyMultipartObjectKeyValue(t *testing.T) {
	e := dirEntry("upload-id-1", map[string][]byte{
		s3_constants.ExtMultipartObjectKey: []byte(""),
	})
	assert.False(t, isMPUInitDir(s3_constants.MultipartUploadsFolder+"/upload-id-1", e))
}

// ---------- walkBucketDir ----------

const testBucketRoot = "/buckets/b1"

func TestWalkBucketDir_SingleRegularFile(t *testing.T) {
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {fileEntry("a.txt")},
		},
	}
	var keys []string
	err := walkBucketDir(context.Background(), client, testBucketRoot, testBucketRoot, func(_ *filer_pb.Entry, key string) error {
		keys = append(keys, key)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"a.txt"}, keys)
}

func TestWalkBucketDir_NestedDirectoriesUseBucketRelativeKeys(t *testing.T) {
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot:                {dirEntry("sub", nil)},
			testBucketRoot + "/sub":       {dirEntry("deeper", nil), fileEntry("mid.txt")},
			testBucketRoot + "/sub/deeper": {fileEntry("leaf.txt")},
		},
	}
	var keys []string
	err := walkBucketDir(context.Background(), client, testBucketRoot, testBucketRoot, func(_ *filer_pb.Entry, key string) error {
		keys = append(keys, key)
		return nil
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"sub/deeper/leaf.txt", "sub/mid.txt"}, keys)
}

func TestWalkBucketDir_MPUInitDirEmittedOnceAndNotRecursed(t *testing.T) {
	uploadsDir := testBucketRoot + "/" + s3_constants.MultipartUploadsFolder
	initDir := uploadsDir + "/upload-id-1"
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {dirEntry(s3_constants.MultipartUploadsFolder, nil)},
			uploadsDir: {dirEntry("upload-id-1", map[string][]byte{
				s3_constants.ExtMultipartObjectKey: []byte("foo/bar.txt"),
			})},
			// If recursed (it shouldn't), this part-0001 file would be emitted.
			initDir: {fileEntry("part-0001")},
		},
	}
	var seen []string
	err := walkBucketDir(context.Background(), client, testBucketRoot, testBucketRoot, func(_ *filer_pb.Entry, key string) error {
		seen = append(seen, key)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{s3_constants.MultipartUploadsFolder + "/upload-id-1"}, seen)

	// Walker must not have descended into the init dir.
	for _, d := range client.listedCopy() {
		assert.NotEqual(t, initDir, d, "walker must not list init dir contents")
	}
}

func TestWalkBucketDir_NonMPUDirUnderUploadsRecurses(t *testing.T) {
	// Directory under .uploads/ that lacks ExtMultipartObjectKey: walker
	// recurses normally and emits inner files.
	uploadsDir := testBucketRoot + "/" + s3_constants.MultipartUploadsFolder
	notInitDir := uploadsDir + "/missing-key"
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {dirEntry(s3_constants.MultipartUploadsFolder, nil)},
			uploadsDir:     {dirEntry("missing-key", nil)},
			notInitDir:     {fileEntry("part-0001")},
		},
	}
	var seen []string
	err := walkBucketDir(context.Background(), client, testBucketRoot, testBucketRoot, func(_ *filer_pb.Entry, key string) error {
		seen = append(seen, key)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{s3_constants.MultipartUploadsFolder + "/missing-key/part-0001"}, seen)
}

func TestWalkBucketDir_MixedTreeBucketRelativeKeys(t *testing.T) {
	uploadsDir := testBucketRoot + "/" + s3_constants.MultipartUploadsFolder
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {
				fileEntry("top.txt"),
				dirEntry("nested", nil),
				dirEntry(s3_constants.MultipartUploadsFolder, nil),
			},
			testBucketRoot + "/nested": {fileEntry("inner.txt")},
			uploadsDir: {dirEntry("upload-id-1", map[string][]byte{
				s3_constants.ExtMultipartObjectKey: []byte("destkey"),
			})},
		},
	}
	var seen []string
	err := walkBucketDir(context.Background(), client, testBucketRoot, testBucketRoot, func(_ *filer_pb.Entry, key string) error {
		seen = append(seen, key)
		return nil
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"top.txt",
		"nested/inner.txt",
		s3_constants.MultipartUploadsFolder + "/upload-id-1",
	}, seen)
}

func TestWalkBucketDir_CallbackErrorPropagates(t *testing.T) {
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {fileEntry("a.txt"), fileEntry("b.txt")},
		},
	}
	cbErr := errors.New("boom")
	calls := 0
	err := walkBucketDir(context.Background(), client, testBucketRoot, testBucketRoot, func(_ *filer_pb.Entry, _ string) error {
		calls++
		return cbErr
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, cbErr)
	// Walk must stop at the first failing callback.
	assert.Equal(t, 1, calls)
}

func TestWalkBucketDir_ContextCancelledStopsWalk(t *testing.T) {
	// Big tree under root; cancel context up front so list call returns
	// the context error immediately.
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {fileEntry("a.txt"), dirEntry("d", nil)},
			testBucketRoot + "/d": {fileEntry("inner.txt")},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var seen []string
	err := walkBucketDir(ctx, client, testBucketRoot, testBucketRoot, func(_ *filer_pb.Entry, key string) error {
		seen = append(seen, key)
		return nil
	})
	require.Error(t, err)
	assert.Empty(t, seen, "no entries should be emitted after context cancel")
}

// ---------- BucketBootstrapper.KickOffNew ----------

// emptyClient: a filer client that always returns no children. Walks
// finish almost immediately.
func newEmptyFilerClient() *fakeFilerClient {
	return &fakeFilerClient{tree: map[string][]*filer_pb.Entry{}}
}

// waitFor polls until cond returns true or fails the test.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting: %s", msg)
}

func TestBucketBootstrapper_KickOffNew_LaunchesPerBucket(t *testing.T) {
	client := newEmptyFilerClient()
	inj := &recordingInjector{}
	b := &BucketBootstrapper{
		FilerClient: client,
		BucketsPath: "/buckets",
		Injector:    inj,
	}

	b.KickOffNew(context.Background(), []string{"bucketA", "bucketB"})

	// Both walks must hit ListEntries once each (empty trees -> no recursion).
	waitFor(t, func() bool {
		return atomic.LoadInt32(&client.listedN) >= 2
	}, "both bucket walks to start")

	listed := client.listedCopy()
	assert.ElementsMatch(t, []string{"/buckets/bucketA", "/buckets/bucketB"}, listed)

	b.mu.Lock()
	defer b.mu.Unlock()
	assert.True(t, b.known["bucketA"])
	assert.True(t, b.known["bucketB"])
	assert.Len(t, b.known, 2)
}

func TestBucketBootstrapper_KickOffNew_SkipsAlreadyKnown(t *testing.T) {
	client := newEmptyFilerClient()
	inj := &recordingInjector{}
	b := &BucketBootstrapper{
		FilerClient: client,
		BucketsPath: "/buckets",
		Injector:    inj,
	}

	b.KickOffNew(context.Background(), []string{"bucketA", "bucketB"})
	waitFor(t, func() bool {
		return atomic.LoadInt32(&client.listedN) >= 2
	}, "first wave to complete")

	firstWave := atomic.LoadInt32(&client.listedN)

	// Second call: bucketA is already known, bucketC is new. Only one
	// new walk should fire.
	b.KickOffNew(context.Background(), []string{"bucketA", "bucketC"})
	waitFor(t, func() bool {
		return atomic.LoadInt32(&client.listedN) >= firstWave+1
	}, "bucketC walk to start")

	// Give a moment for any spurious bucketA walk to also tick.
	time.Sleep(20 * time.Millisecond)

	listed := client.listedCopy()
	// Count distinct buckets walked.
	bucketCount := map[string]int{}
	for _, d := range listed {
		bucketCount[d]++
	}
	assert.Equal(t, 1, bucketCount["/buckets/bucketA"], "bucketA must be walked exactly once across both calls")
	assert.Equal(t, 1, bucketCount["/buckets/bucketB"])
	assert.Equal(t, 1, bucketCount["/buckets/bucketC"])

	b.mu.Lock()
	defer b.mu.Unlock()
	assert.Len(t, b.known, 3)
	assert.True(t, b.known["bucketC"])
}

func TestBucketBootstrapper_KickOffNew_NilInjectorIsNoop(t *testing.T) {
	client := newEmptyFilerClient()
	b := &BucketBootstrapper{
		FilerClient: client,
		BucketsPath: "/buckets",
		Injector:    nil,
	}
	require.NotPanics(t, func() {
		b.KickOffNew(context.Background(), []string{"bucketA"})
	})
	// No walks must have been kicked off, and known must remain empty.
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&client.listedN))
	b.mu.Lock()
	defer b.mu.Unlock()
	assert.Empty(t, b.known)
}

func TestBucketBootstrapper_KickOffNew_EmptyBucketListIsNoop(t *testing.T) {
	client := newEmptyFilerClient()
	inj := &recordingInjector{}
	b := &BucketBootstrapper{
		FilerClient: client,
		BucketsPath: "/buckets",
		Injector:    inj,
	}
	b.KickOffNew(context.Background(), nil)
	b.KickOffNew(context.Background(), []string{})

	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&client.listedN))
	assert.Empty(t, inj.snapshot())
}

// versionFile builds a version-file entry with the given mtime, version_id,
// and optional delete-marker flag.
func versionFile(versionID string, mtime time.Time, isMarker bool) *filer_pb.Entry {
	ext := map[string][]byte{
		s3_constants.ExtVersionIdKey: []byte(versionID),
	}
	if isMarker {
		ext[s3_constants.ExtDeleteMarkerKey] = []byte("true")
	}
	return &filer_pb.Entry{
		Name: "v_" + versionID,
		Attributes: &filer_pb.FuseAttributes{
			Mtime: mtime.Unix(),
		},
		Extended: ext,
	}
}

func TestWalkBucketDir_VersionsDirEmittedOnceAndNotRecursed(t *testing.T) {
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, map[string][]byte{
		s3_constants.ExtLatestVersionIdKey: []byte("v2"),
	})
	now := time.Now()
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {versionsDir},
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile("v1", now.Add(-2*time.Hour), false),
				versionFile("v2", now, false),
			},
		},
	}
	var seen []string
	err := walkBucketDir(context.Background(), client, testBucketRoot, testBucketRoot, func(_ *filer_pb.Entry, key string) error {
		seen = append(seen, key)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"foo" + s3_constants.VersionsFolder}, seen)
	assert.NotContains(t, client.listedCopy(), testBucketRoot+"/foo"+s3_constants.VersionsFolder)
}

func TestWalkBucketDir_VersionsDirEmittedRegardlessOfLatestPointer(t *testing.T) {
	// walkBucketDir matches <x>.versions/ purely on the name suffix —
	// gating on ExtLatestVersionIdKey would lose the race window where
	// the version file exists before the parent's metadata update lands.
	// expandVersionsDir handles disambiguation by inspecting children.
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, nil)
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {versionsDir},
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {fileEntry("inner.txt")},
		},
	}
	var seen []string
	err := walkBucketDir(context.Background(), client, testBucketRoot, testBucketRoot, func(_ *filer_pb.Entry, key string) error {
		seen = append(seen, key)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"foo" + s3_constants.VersionsFolder}, seen)
}

func TestExpandVersionsDir_CoincidentallyNamedFolderRecursesViaFallback(t *testing.T) {
	// A user-created folder happening to end in .versions/ has children
	// without ExtVersionIdKey. expandVersionsDir must recurse via the
	// fallback callback so inner files still emit normal events.
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, nil)
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				fileEntry("inner.txt"),
				dirEntry("sub", nil),
			},
			testBucketRoot + "/foo" + s3_constants.VersionsFolder + "/sub": {fileEntry("deep.txt")},
		},
	}
	var seen []string
	cb := func(_ *filer_pb.Entry, key string) error {
		seen = append(seen, key)
		return nil
	}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets"}
	count, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, cb)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "fallback path returns 0 — events go through cb")
	assert.ElementsMatch(t, []string{"foo" + s3_constants.VersionsFolder + "/inner.txt", "foo" + s3_constants.VersionsFolder + "/sub/deep.txt"}, seen)
}

func TestExpandVersionsDir_RaceWithMissingPointerStillExpands(t *testing.T) {
	// Real .versions container whose parent metadata update hasn't
	// landed yet (no ExtLatestVersionIdKey on the dir). Children DO
	// carry ExtVersionIdKey. expandVersionsDir must still emit version
	// events; missing-pointer fallback (newest-by-mtime as latest)
	// covers retention safety.
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, nil) // no Extended at all
	now := time.Now()
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile("v1", now.Add(-2*time.Hour), false),
				versionFile("v2", now.Add(-1*time.Hour), false),
			},
		},
	}
	inj := &recordingInjector{}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets", Injector: inj}
	count, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	byID := map[string]*reader.BootstrapVersion{}
	for _, ev := range inj.snapshot() {
		byID[ev.BootstrapVersion.VersionID] = ev.BootstrapVersion
	}
	assert.True(t, byID["v2"].IsLatest, "newest by mtime is latest when pointer missing")
}

func TestExpandVersionsDir_LatestAndNoncurrentsByMtime(t *testing.T) {
	now := time.Now()
	v1mt := now.Add(-3 * time.Hour)
	v2mt := now.Add(-2 * time.Hour)
	v3mt := now.Add(-1 * time.Hour)
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, map[string][]byte{
		s3_constants.ExtLatestVersionIdKey: []byte("v3"),
	})
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile("v1", v1mt, false),
				versionFile("v2", v2mt, false),
				versionFile("v3", v3mt, false),
			},
		},
	}
	inj := &recordingInjector{}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets", Injector: inj}

	count, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	byID := map[string]*reader.BootstrapVersion{}
	for _, ev := range inj.snapshot() {
		require.NotNil(t, ev.BootstrapVersion)
		assert.Equal(t, "foo", ev.BootstrapVersion.LogicalKey)
		assert.Equal(t, 3, ev.BootstrapVersion.NumVersions)
		byID[ev.BootstrapVersion.VersionID] = ev.BootstrapVersion
	}
	require.Contains(t, byID, "v1")
	require.Contains(t, byID, "v2")
	require.Contains(t, byID, "v3")

	assert.True(t, byID["v3"].IsLatest)
	assert.True(t, byID["v3"].SuccessorModTime.IsZero(), "newest sibling has no successor")

	assert.False(t, byID["v2"].IsLatest)
	assert.Equal(t, 0, byID["v2"].NoncurrentIndex, "newest noncurrent")
	assert.Equal(t, v3mt.Unix(), byID["v2"].SuccessorModTime.Unix())

	assert.False(t, byID["v1"].IsLatest)
	assert.Equal(t, 1, byID["v1"].NoncurrentIndex)
	assert.Equal(t, v2mt.Unix(), byID["v1"].SuccessorModTime.Unix())
}

func TestExpandVersionsDir_LatestPointerOutOfOrderByMtime(t *testing.T) {
	// Backdated PUT scenario: latest pointer names v1 even though v2 has
	// newer mtime. NoncurrentIndex must skip the latest's position.
	now := time.Now()
	v1mt := now.Add(-1 * time.Hour)
	v2mt := now.Add(-3 * time.Hour)
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, map[string][]byte{
		s3_constants.ExtLatestVersionIdKey: []byte("v1"),
	})
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile("v1", v1mt, false),
				versionFile("v2", v2mt, false),
			},
		},
	}
	inj := &recordingInjector{}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets", Injector: inj}
	_, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil)
	require.NoError(t, err)

	byID := map[string]*reader.BootstrapVersion{}
	for _, ev := range inj.snapshot() {
		byID[ev.BootstrapVersion.VersionID] = ev.BootstrapVersion
	}
	assert.True(t, byID["v1"].IsLatest)
	assert.False(t, byID["v2"].IsLatest)
	assert.Equal(t, 0, byID["v2"].NoncurrentIndex, "v2 is the only noncurrent → rank 0")
}

func TestExpandVersionsDir_MissingLatestPointerFallsBackToNewest(t *testing.T) {
	// No latest pointer (rare race window): treat the newest sibling
	// by mtime as latest so retention isn't unsafe.
	now := time.Now()
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, map[string][]byte{})
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile("v1", now.Add(-2*time.Hour), false),
				versionFile("v2", now.Add(-1*time.Hour), false),
			},
		},
	}
	inj := &recordingInjector{}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets", Injector: inj}
	_, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil)
	require.NoError(t, err)

	byID := map[string]*reader.BootstrapVersion{}
	for _, ev := range inj.snapshot() {
		byID[ev.BootstrapVersion.VersionID] = ev.BootstrapVersion
	}
	assert.True(t, byID["v2"].IsLatest, "newest by mtime is latest when pointer missing")
	assert.False(t, byID["v1"].IsLatest)
}

func TestExpandVersionsDir_DeleteMarkerFlagPropagated(t *testing.T) {
	now := time.Now()
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, map[string][]byte{
		s3_constants.ExtLatestVersionIdKey: []byte("v-marker"),
	})
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile("v-marker", now, true),
			},
		},
	}
	inj := &recordingInjector{}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets", Injector: inj}
	_, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil)
	require.NoError(t, err)

	events := inj.snapshot()
	require.Len(t, events, 1)
	assert.True(t, events[0].BootstrapVersion.IsDeleteMarker)
	assert.True(t, events[0].BootstrapVersion.IsLatest)
}
