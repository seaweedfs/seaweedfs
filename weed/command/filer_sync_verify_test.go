package command

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// --- stream / inner-client / outer-client mocks ---

type verifyTestStream struct {
	entries []*filer_pb.Entry
	idx     int
}

func (s *verifyTestStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.idx >= len(s.entries) {
		return nil, io.EOF
	}
	resp := &filer_pb.ListEntriesResponse{Entry: s.entries[s.idx]}
	s.idx++
	return resp, nil
}

func (s *verifyTestStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *verifyTestStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *verifyTestStream) CloseSend() error             { return nil }
func (s *verifyTestStream) Context() context.Context     { return context.Background() }
func (s *verifyTestStream) SendMsg(_ any) error          { return nil }
func (s *verifyTestStream) RecvMsg(_ any) error          { return nil }

// verifyTestInnerClient is the SeaweedFilerClient passed to fn inside WithFilerClient.
type verifyTestInnerClient struct {
	filer_pb.SeaweedFilerClient // embed for unimplemented RPCs
	entriesByDir map[string][]*filer_pb.Entry
}

func (c *verifyTestInnerClient) ListEntries(_ context.Context, in *filer_pb.ListEntriesRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	return &verifyTestStream{entries: c.entriesByDir[in.Directory]}, nil
}

// verifyTestFilerClient implements filer_pb.FilerClient and tracks concurrent
// WithFilerClient invocations to let tests verify the global concurrency bound.
type verifyTestFilerClient struct {
	entriesByDir map[string][]*filer_pb.Entry
	inFlight     int64 // accessed via atomic
	peakFlight   int64 // accessed via atomic
	delay        time.Duration
}

func (c *verifyTestFilerClient) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	// track peak concurrent in-flight listings
	n := atomic.AddInt64(&c.inFlight, 1)
	defer atomic.AddInt64(&c.inFlight, -1)
	for {
		peak := atomic.LoadInt64(&c.peakFlight)
		if n <= peak || atomic.CompareAndSwapInt64(&c.peakFlight, peak, n) {
			break
		}
	}
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return fn(&verifyTestInnerClient{entriesByDir: c.entriesByDir})
}

func (c *verifyTestFilerClient) AdjustedUrl(_ *filer_pb.Location) string { return "" }
func (c *verifyTestFilerClient) GetDataCenter() string                   { return "" }

// --- entry helpers ---

func verifyFileEntry(name string, size uint64) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:       name,
		Attributes: &filer_pb.FuseAttributes{FileSize: size},
	}
}

func verifyDirEntry(name string) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name, IsDirectory: true}
}

// --- tests ---

// TestVerifySyncMissingFile confirms that a file present in A but absent in B
// is counted as missing.
func TestVerifySyncMissingFile(t *testing.T) {
	clientA := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/root": {verifyFileEntry("file.txt", 100)},
		},
	}
	clientB := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/root": {},
		},
	}

	result := &VerifyResult{}
	sem := make(chan struct{}, verifySyncConcurrency)
	err := compareDirectory(context.Background(), clientA, clientB,
		"/root", "/root", false, time.Time{}, sem, result)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := result.missingCount.Load(); got != 1 {
		t.Errorf("missingCount = %d, want 1", got)
	}
	if got := result.sizeMismatch.Load(); got != 0 {
		t.Errorf("sizeMismatch = %d, want 0", got)
	}
}

// TestVerifySyncOnlyInB confirms that a file present only in B is counted
// (non-active-passive mode) or ignored (active-passive mode).
func TestVerifySyncOnlyInB(t *testing.T) {
	clientA := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{"/root": {}},
	}
	clientB := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/root": {verifyFileEntry("extra.txt", 50)},
		},
	}

	t.Run("bidirectional", func(t *testing.T) {
		result := &VerifyResult{}
		sem := make(chan struct{}, verifySyncConcurrency)
		if err := compareDirectory(context.Background(), clientA, clientB,
			"/root", "/root", false, time.Time{}, sem, result); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := result.onlyInB.Load(); got != 1 {
			t.Errorf("onlyInB = %d, want 1", got)
		}
	})

	t.Run("active-passive ignores onlyInB", func(t *testing.T) {
		result := &VerifyResult{}
		sem := make(chan struct{}, verifySyncConcurrency)
		if err := compareDirectory(context.Background(), clientA, clientB,
			"/root", "/root", true, time.Time{}, sem, result); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := result.onlyInB.Load(); got != 0 {
			t.Errorf("onlyInB = %d, want 0 in active-passive mode", got)
		}
	})
}

// TestVerifySyncSizeMismatch confirms that a file with differing sizes is
// counted as a size mismatch and not as missing.
func TestVerifySyncSizeMismatch(t *testing.T) {
	clientA := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/root": {verifyFileEntry("data.bin", 1024)},
		},
	}
	clientB := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/root": {verifyFileEntry("data.bin", 512)},
		},
	}

	result := &VerifyResult{}
	sem := make(chan struct{}, verifySyncConcurrency)
	err := compareDirectory(context.Background(), clientA, clientB,
		"/root", "/root", false, time.Time{}, sem, result)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := result.sizeMismatch.Load(); got != 1 {
		t.Errorf("sizeMismatch = %d, want 1", got)
	}
	if got := result.missingCount.Load(); got != 0 {
		t.Errorf("missingCount = %d, want 0", got)
	}
}

// TestVerifySyncConcurrencyBound verifies that the shared semaphore keeps peak
// concurrent filer listings at or below verifySyncConcurrency at all times.
// A 5ms delay per WithFilerClient call makes the concurrency overlap observable.
func TestVerifySyncConcurrencyBound(t *testing.T) {
	// Wide, shallow tree: root with 20 identical subdirectories.
	const fanout = 20
	entriesA := make(map[string][]*filer_pb.Entry)
	entriesB := make(map[string][]*filer_pb.Entry)

	rootDirs := make([]*filer_pb.Entry, fanout)
	for i := range fanout {
		name := fmt.Sprintf("sub%02d", i)
		rootDirs[i] = verifyDirEntry(name)
		entriesA["/root/"+name] = []*filer_pb.Entry{verifyFileEntry("f.txt", 10)}
		entriesB["/root/"+name] = []*filer_pb.Entry{verifyFileEntry("f.txt", 10)}
	}
	entriesA["/root"] = rootDirs
	entriesB["/root"] = rootDirs

	clientA := &verifyTestFilerClient{entriesByDir: entriesA, delay: 5 * time.Millisecond}
	clientB := &verifyTestFilerClient{entriesByDir: entriesB, delay: 5 * time.Millisecond}

	result := &VerifyResult{}
	sem := make(chan struct{}, verifySyncConcurrency)
	if err := compareDirectory(context.Background(), clientA, clientB,
		"/root", "/root", false, time.Time{}, sem, result); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.missingCount.Load() != 0 || result.sizeMismatch.Load() != 0 {
		t.Errorf("unexpected diffs in identical tree")
	}

	if peak := atomic.LoadInt64(&clientA.peakFlight); peak > verifySyncConcurrency {
		t.Errorf("clientA peak concurrent listings = %d, want ≤ %d (verifySyncConcurrency)",
			peak, verifySyncConcurrency)
	}
	if peak := atomic.LoadInt64(&clientB.peakFlight); peak > verifySyncConcurrency {
		t.Errorf("clientB peak concurrent listings = %d, want ≤ %d (verifySyncConcurrency)",
			peak, verifySyncConcurrency)
	}
}

// TestVerifySyncNoDeadlockDeepTree ensures that a tree deeper than
// verifySyncConcurrency completes without deadlocking. With a per-call
// semaphore the walk would still complete (just with unbounded goroutines);
// this test mainly guards that the shared-semaphore release-before-recurse
// invariant holds — i.e. the walk finishes within the timeout.
func TestVerifySyncNoDeadlockDeepTree(t *testing.T) {
	// Build a binary tree of depth 10 (well past verifySyncConcurrency=5).
	const depth = 10
	entriesA := make(map[string][]*filer_pb.Entry)
	entriesB := make(map[string][]*filer_pb.Entry)

	var buildTree func(path string, d int)
	buildTree = func(path string, d int) {
		if d == 0 {
			entriesA[path] = []*filer_pb.Entry{verifyFileEntry("leaf.txt", 1)}
			entriesB[path] = []*filer_pb.Entry{verifyFileEntry("leaf.txt", 1)}
			return
		}
		children := []*filer_pb.Entry{verifyDirEntry("left"), verifyDirEntry("right")}
		entriesA[path] = children
		entriesB[path] = children
		buildTree(path+"/left", d-1)
		buildTree(path+"/right", d-1)
	}
	buildTree("/root", depth)

	clientA := &verifyTestFilerClient{entriesByDir: entriesA}
	clientB := &verifyTestFilerClient{entriesByDir: entriesB}

	done := make(chan error, 1)
	go func() {
		result := &VerifyResult{}
		sem := make(chan struct{}, verifySyncConcurrency)
		done <- compareDirectory(context.Background(), clientA, clientB,
			"/root", "/root", false, time.Time{}, sem, result)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("compareDirectory did not complete within 10s — possible deadlock")
	}
}
