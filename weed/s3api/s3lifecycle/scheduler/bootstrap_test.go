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
// ListEntries + LookupDirectoryEntry. Calling any other method panics,
// which is fine for these tests.
type fakeFilerClient struct {
	filer_pb.SeaweedFilerClient

	mu       sync.Mutex
	tree     map[string][]*filer_pb.Entry // dir path -> immediate children
	listed   []string                     // dirs the walker asked about, in order
	listedN  int32                        // atomic counter for cross-goroutine reads
}

func (c *fakeFilerClient) LookupDirectoryEntry(ctx context.Context, in *filer_pb.LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.tree[in.Directory] {
		if e != nil && e.Name == in.Name {
			return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
		}
	}
	return nil, filer_pb.ErrNotFound
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
	count, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, cb, nil)
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
	count, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil, nil)
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

	count, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil, nil)
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
	// Backdated PUT scenario: latest pointer names v1 but v1's mtime is
	// OLDER than v2's. After newest-first sort the order is [v2, v1] so
	// latestPos == 1, exercising the rank-skip path for the noncurrent.
	now := time.Now()
	v1mt := now.Add(-3 * time.Hour)
	v2mt := now.Add(-1 * time.Hour)
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
	_, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil, nil)
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
	_, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil, nil)
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
	_, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil, nil)
	require.NoError(t, err)

	events := inj.snapshot()
	require.Len(t, events, 1)
	assert.True(t, events[0].BootstrapVersion.IsDeleteMarker)
	assert.True(t, events[0].BootstrapVersion.IsLatest)
}

func bareFile(name string, mtime time.Time) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name: name,
		Attributes: &filer_pb.FuseAttributes{
			Mtime: mtime.Unix(),
		},
	}
}

// suspendedNullFile mirrors the suspended-versioning write path: the
// bare entry carries ExtVersionIdKey="null" so bootstrap can tell it
// apart from a pre-versioning bare object during a pointer-missing
// race window.
func suspendedNullFile(name string, mtime time.Time) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name: name,
		Attributes: &filer_pb.FuseAttributes{
			Mtime: mtime.Unix(),
		},
		Extended: map[string][]byte{
			s3_constants.ExtVersionIdKey: []byte("null"),
		},
	}
}

func TestExpandVersionsDir_PreVersioningNullIsNoncurrent(t *testing.T) {
	// Object existed pre-versioning as the bare key. Versioning was
	// enabled and a newer version v1 was PUT under .versions/. The
	// .versions/ latest pointer names v1, so null is noncurrent.
	now := time.Now()
	v1mt := now.Add(-1 * time.Hour)
	nullMt := now.Add(-3 * time.Hour)
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, map[string][]byte{
		s3_constants.ExtLatestVersionIdKey: []byte("v1"),
	})
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {bareFile("foo", nullMt), versionsDir},
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile("v1", v1mt, false),
			},
		},
	}
	inj := &recordingInjector{}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets", Injector: inj}
	skipBare := map[string]bool{}
	count, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil, skipBare)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	byID := map[string]*reader.BootstrapVersion{}
	for _, ev := range inj.snapshot() {
		byID[ev.BootstrapVersion.VersionID] = ev.BootstrapVersion
	}
	assert.True(t, byID["v1"].IsLatest)
	assert.False(t, byID["null"].IsLatest)
	assert.Equal(t, 0, byID["null"].NoncurrentIndex)
	assert.Equal(t, 2, byID["null"].NumVersions)
	assert.True(t, skipBare["foo"], "bare-key skip recorded")
}

func TestExpandVersionsDir_SuspendedNullIsCurrent(t *testing.T) {
	// Suspended-bucket scenario: a write to the null version cleared the
	// .versions/ latest pointer AND tagged the bare entry with
	// ExtVersionIdKey="null". Older real versions remain in .versions/.
	// Null must be IsLatest=true; .versions/ children become noncurrent.
	now := time.Now()
	v1mt := now.Add(-3 * time.Hour)
	v2mt := now.Add(-2 * time.Hour)
	nullMt := now.Add(-1 * time.Hour)
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, map[string][]byte{}) // pointer cleared
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {suspendedNullFile("foo", nullMt), versionsDir},
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile("v1", v1mt, false),
				versionFile("v2", v2mt, false),
			},
		},
	}
	inj := &recordingInjector{}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets", Injector: inj}
	skipBare := map[string]bool{}
	_, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil, skipBare)
	require.NoError(t, err)

	byID := map[string]*reader.BootstrapVersion{}
	for _, ev := range inj.snapshot() {
		byID[ev.BootstrapVersion.VersionID] = ev.BootstrapVersion
	}
	assert.True(t, byID["null"].IsLatest, "pointer cleared + null exists -> null is latest")
	assert.False(t, byID["v1"].IsLatest)
	assert.False(t, byID["v2"].IsLatest)
	assert.True(t, skipBare["foo"])
}

func TestExpandVersionsDir_NullVersionDirectoryKeyMarker(t *testing.T) {
	// Directory-key marker (object name ends in /): the bare entry is a
	// directory with Mime set. Treat as null version.
	now := time.Now()
	v1mt := now.Add(-1 * time.Hour)
	dirMarker := &filer_pb.Entry{
		Name:        "foo",
		IsDirectory: true,
		Attributes: &filer_pb.FuseAttributes{
			Mtime: now.Add(-3 * time.Hour).Unix(),
			Mime:  "application/x-directory",
		},
	}
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, map[string][]byte{
		s3_constants.ExtLatestVersionIdKey: []byte("v1"),
	})
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {dirMarker, versionsDir},
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile("v1", v1mt, false),
			},
		},
	}
	inj := &recordingInjector{}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets", Injector: inj}
	skipBare := map[string]bool{}
	_, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil, skipBare)
	require.NoError(t, err)
	byID := map[string]*reader.BootstrapVersion{}
	for _, ev := range inj.snapshot() {
		byID[ev.BootstrapVersion.VersionID] = ev.BootstrapVersion
	}
	require.Contains(t, byID, "null", "directory-key marker counts as null version")
	assert.True(t, skipBare["foo"])
}

func TestWalkBucketDir_VersionsDirOrderingClaimsNullSibling(t *testing.T) {
	// End-to-end through walkBucket: bare foo + foo.versions/ are
	// siblings in the same directory. The two-pass walker processes
	// the .versions/ first; expandVersionsDir claims "foo" as null;
	// the second pass sees skipBare["foo"]==true and emits no regular
	// event for the bare entry.
	now := time.Now()
	v1mt := now.Add(-1 * time.Hour)
	nullMt := now.Add(-3 * time.Hour)
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, map[string][]byte{
		s3_constants.ExtLatestVersionIdKey: []byte("v1"),
	})
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {bareFile("foo", nullMt), versionsDir},
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile("v1", v1mt, false),
			},
		},
	}
	inj := &recordingInjector{}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets", Injector: inj}
	b.KickOffNew(context.Background(), []string{"b1"})
	// give the goroutine time to finish
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(inj.snapshot()) < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	events := inj.snapshot()

	// Exactly two events: v1 (latest) and null (noncurrent). NO third
	// regular event for the bare "foo" entry.
	assert.Len(t, events, 2)
	versionIDs := []string{}
	for _, ev := range events {
		require.NotNil(t, ev.BootstrapVersion, "all events must be BootstrapVersion-tagged")
		versionIDs = append(versionIDs, ev.BootstrapVersion.VersionID)
	}
	assert.ElementsMatch(t, []string{"v1", "null"}, versionIDs)
}

func TestExpandVersionsDir_VersionIDTiebreakOnSameSecondMtime(t *testing.T) {
	// Two versions written in the same second: Mtime ties. The
	// CompareVersionIds tiebreak puts the version_id with newer
	// canonical ordering first. Use new-format IDs (inverted timestamps)
	// so smaller string sorts as newer.
	now := time.Now().Truncate(time.Second)
	idNewer := "8000000000000000aaaaaaaaaaaaaaaa"
	idOlder := "9000000000000000bbbbbbbbbbbbbbbb"
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, map[string][]byte{
		s3_constants.ExtLatestVersionIdKey: []byte(idNewer),
	})
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile(idOlder, now, false),
				versionFile(idNewer, now, false),
			},
		},
	}
	inj := &recordingInjector{}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets", Injector: inj}
	_, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil, nil)
	require.NoError(t, err)
	byID := map[string]*reader.BootstrapVersion{}
	for _, ev := range inj.snapshot() {
		byID[ev.BootstrapVersion.VersionID] = ev.BootstrapVersion
	}
	assert.True(t, byID[idNewer].IsLatest, "newer canonical id wins the tiebreak")
	assert.False(t, byID[idOlder].IsLatest)
	assert.Equal(t, 0, byID[idOlder].NoncurrentIndex)
}

func TestExpandVersionsDir_PreVersioningNullDuringPointerRaceFallsBackToNewest(t *testing.T) {
	// Pre-versioning bare object existed when versioning was enabled.
	// A new version v1 was just written under .versions/<file> but the
	// parent's ExtLatestVersionIdKey update has not landed yet. The
	// bare entry has NO ExtVersionIdKey marker — distinguishing it from
	// a suspended-bucket write. Bootstrap must treat v1 as latest (the
	// newest sibling) and the implicit null as noncurrent, so the null
	// expiration is scheduled this run instead of waiting for a future
	// bootstrap.
	now := time.Now()
	v1mt := now.Add(-1 * time.Hour) // newer
	nullMt := now.Add(-3 * time.Hour)
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, map[string][]byte{}) // pointer not yet written
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {bareFile("foo", nullMt), versionsDir},
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile("v1", v1mt, false),
			},
		},
	}
	inj := &recordingInjector{}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets", Injector: inj}
	skipBare := map[string]bool{}
	_, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil, skipBare)
	require.NoError(t, err)

	byID := map[string]*reader.BootstrapVersion{}
	for _, ev := range inj.snapshot() {
		byID[ev.BootstrapVersion.VersionID] = ev.BootstrapVersion
	}
	assert.True(t, byID["v1"].IsLatest, "newest sibling wins when null is implicit")
	assert.False(t, byID["null"].IsLatest, "implicit null is noncurrent during pointer-missing race")
}

func TestExpandVersionsDir_SuspendedThenReEnabledNullIsNoncurrent(t *testing.T) {
	// Bucket was suspended: bare entry was written with
	// ExtVersionIdKey="null" and the .versions/ pointer cleared.
	// Versioning was re-enabled and a fresh PUT created
	// .versions/<v-new> with newer mtime, but the pointer-update for
	// that new version hasn't landed yet. Bootstrap running in this
	// window must keep v-new as latest (it's newest by mtime); the
	// explicit null is noncurrent. Promoting the older null to latest
	// just because it's explicit would skip current-version expiration
	// of v-new and never schedule the null's noncurrent retention.
	now := time.Now()
	nullMt := now.Add(-3 * time.Hour) // OLDER bare-null
	vNewMt := now.Add(-1 * time.Hour) // newer real version
	versionsDir := dirEntry("foo"+s3_constants.VersionsFolder, map[string][]byte{}) // pointer not yet written
	client := &fakeFilerClient{
		tree: map[string][]*filer_pb.Entry{
			testBucketRoot: {suspendedNullFile("foo", nullMt), versionsDir},
			testBucketRoot + "/foo" + s3_constants.VersionsFolder: {
				versionFile("v-new", vNewMt, false),
			},
		},
	}
	inj := &recordingInjector{}
	b := &BucketBootstrapper{FilerClient: client, BucketsPath: "/buckets", Injector: inj}
	skipBare := map[string]bool{}
	_, err := b.expandVersionsDir(context.Background(), "b1", testBucketRoot, "foo"+s3_constants.VersionsFolder, versionsDir, nil, skipBare)
	require.NoError(t, err)

	byID := map[string]*reader.BootstrapVersion{}
	for _, ev := range inj.snapshot() {
		byID[ev.BootstrapVersion.VersionID] = ev.BootstrapVersion
	}
	assert.True(t, byID["v-new"].IsLatest, "newest sibling wins even when an older explicit null exists")
	assert.False(t, byID["null"].IsLatest)
	assert.Equal(t, 0, byID["null"].NoncurrentIndex)
}
