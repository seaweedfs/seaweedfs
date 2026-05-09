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
