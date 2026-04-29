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

// TestVerifySyncETagMismatch confirms that two files with the same size but
// different Md5 checksums are counted as an ETag mismatch, not a size mismatch.
func TestVerifySyncETagMismatch(t *testing.T) {
	newEntry := func(name string, md5 []byte) *filer_pb.Entry {
		return &filer_pb.Entry{
			Name: name,
			Attributes: &filer_pb.FuseAttributes{
				FileSize: 100,
				Md5:      md5,
			},
		}
	}
	clientA := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/root": {newEntry("data.bin", []byte{0x11, 0x22, 0x33})},
		},
	}
	clientB := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/root": {newEntry("data.bin", []byte{0x44, 0x55, 0x66})},
		},
	}

	result := &VerifyResult{}
	sem := make(chan struct{}, verifySyncConcurrency)
	if err := compareDirectory(context.Background(), clientA, clientB,
		"/root", "/root", false, time.Time{}, sem, result); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := result.etagMismatch.Load(); got != 1 {
		t.Errorf("etagMismatch = %d, want 1", got)
	}
	if got := result.sizeMismatch.Load(); got != 0 {
		t.Errorf("sizeMismatch = %d, want 0 (same size should not trigger size mismatch)", got)
	}
}

// TestVerifySyncCutoffTime verifies that entries newer than cutoffTime are
// skipped in both the A-only (MISSING) and B-only (ONLY_IN_B) branches.
func TestVerifySyncCutoffTime(t *testing.T) {
	cutoff := time.Unix(1000, 0)

	recentEntry := func(name string) *filer_pb.Entry {
		return &filer_pb.Entry{
			Name:       name,
			Attributes: &filer_pb.FuseAttributes{FileSize: 10, Mtime: 2000}, // > cutoff
		}
	}
	oldEntry := func(name string) *filer_pb.Entry {
		return &filer_pb.Entry{
			Name:       name,
			Attributes: &filer_pb.FuseAttributes{FileSize: 10, Mtime: 500}, // < cutoff
		}
	}

	t.Run("A-only recent file is skipped, not reported missing", func(t *testing.T) {
		clientA := &verifyTestFilerClient{
			entriesByDir: map[string][]*filer_pb.Entry{"/": {recentEntry("new.txt")}},
		}
		clientB := &verifyTestFilerClient{
			entriesByDir: map[string][]*filer_pb.Entry{"/": {}},
		}
		result := &VerifyResult{}
		sem := make(chan struct{}, verifySyncConcurrency)
		if err := compareDirectory(context.Background(), clientA, clientB,
			"/", "/", false, cutoff, sem, result); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := result.skippedRecent.Load(); got != 1 {
			t.Errorf("skippedRecent = %d, want 1", got)
		}
		if got := result.missingCount.Load(); got != 0 {
			t.Errorf("missingCount = %d, want 0 (recent file should be skipped)", got)
		}
	})

	t.Run("A-only old file is reported missing", func(t *testing.T) {
		clientA := &verifyTestFilerClient{
			entriesByDir: map[string][]*filer_pb.Entry{"/": {oldEntry("old.txt")}},
		}
		clientB := &verifyTestFilerClient{
			entriesByDir: map[string][]*filer_pb.Entry{"/": {}},
		}
		result := &VerifyResult{}
		sem := make(chan struct{}, verifySyncConcurrency)
		if err := compareDirectory(context.Background(), clientA, clientB,
			"/", "/", false, cutoff, sem, result); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := result.missingCount.Load(); got != 1 {
			t.Errorf("missingCount = %d, want 1", got)
		}
	})

	t.Run("B-only recent file is skipped, not reported as ONLY_IN_B", func(t *testing.T) {
		clientA := &verifyTestFilerClient{
			entriesByDir: map[string][]*filer_pb.Entry{"/": {}},
		}
		clientB := &verifyTestFilerClient{
			entriesByDir: map[string][]*filer_pb.Entry{"/": {recentEntry("new.txt")}},
		}
		result := &VerifyResult{}
		sem := make(chan struct{}, verifySyncConcurrency)
		if err := compareDirectory(context.Background(), clientA, clientB,
			"/", "/", false, cutoff, sem, result); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := result.skippedRecent.Load(); got != 1 {
			t.Errorf("skippedRecent = %d, want 1", got)
		}
		if got := result.onlyInB.Load(); got != 0 {
			t.Errorf("onlyInB = %d, want 0 (recent B-only file should be skipped)", got)
		}
	})

	t.Run("B-only old file is reported as ONLY_IN_B", func(t *testing.T) {
		clientA := &verifyTestFilerClient{
			entriesByDir: map[string][]*filer_pb.Entry{"/": {}},
		}
		clientB := &verifyTestFilerClient{
			entriesByDir: map[string][]*filer_pb.Entry{"/": {oldEntry("old.txt")}},
		}
		result := &VerifyResult{}
		sem := make(chan struct{}, verifySyncConcurrency)
		if err := compareDirectory(context.Background(), clientA, clientB,
			"/", "/", false, cutoff, sem, result); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := result.onlyInB.Load(); got != 1 {
			t.Errorf("onlyInB = %d, want 1", got)
		}
	})
}

// TestVerifySyncCutoffMatchedFileBSideRecent verifies that when matched-name
// files differ but only the B side is recently modified, the comparison is
// skipped (sync-lag tolerance) rather than reporting a spurious mismatch.
func TestVerifySyncCutoffMatchedFileBSideRecent(t *testing.T) {
	cutoff := time.Unix(1000, 0)

	entry := func(size uint64, mtime int64) *filer_pb.Entry {
		return &filer_pb.Entry{
			Name:       "data.bin",
			Attributes: &filer_pb.FuseAttributes{FileSize: size, Mtime: mtime},
		}
	}

	// A is old (size 100), B is recently rewritten with a different size.
	// Without the B-side cutoff check this would surface as SIZE_MISMATCH.
	clientA := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{"/": {entry(100, 500)}},
	}
	clientB := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{"/": {entry(200, 2000)}},
	}

	result := &VerifyResult{}
	sem := make(chan struct{}, verifySyncConcurrency)
	if err := compareDirectory(context.Background(), clientA, clientB,
		"/", "/", false, cutoff, sem, result); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := result.skippedRecent.Load(); got != 1 {
		t.Errorf("skippedRecent = %d, want 1 (B-side recent should skip)", got)
	}
	if got := result.sizeMismatch.Load(); got != 0 {
		t.Errorf("sizeMismatch = %d, want 0 (recent B should not surface as mismatch)", got)
	}
}

// TestVerifySyncMissingDirRecursesEvenWithRecentMtime verifies that a
// directory missing in B with a recent mtime still has its subtree walked,
// so older missing files inside are reported. A recent child write can bump
// the parent mtime even though older missing files exist underneath.
func TestVerifySyncMissingDirRecursesEvenWithRecentMtime(t *testing.T) {
	cutoff := time.Unix(1000, 0)

	recentDir := &filer_pb.Entry{
		Name:        "subdir",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{Mtime: 2000}, // > cutoff
	}
	oldChild := &filer_pb.Entry{
		Name:       "old.txt",
		Attributes: &filer_pb.FuseAttributes{FileSize: 10, Mtime: 500}, // < cutoff
	}

	clientA := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/":        {recentDir},
			"/subdir":  {oldChild},
		},
	}
	clientB := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/": {},
		},
	}

	result := &VerifyResult{}
	sem := make(chan struct{}, verifySyncConcurrency)
	if err := compareDirectory(context.Background(), clientA, clientB,
		"/", "/", false, cutoff, sem, result); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Expect: directory MISSING + recursed-old-file MISSING = 2 missing.
	if got := result.missingCount.Load(); got != 2 {
		t.Errorf("missingCount = %d, want 2 (recent dir + old child inside)", got)
	}
	if got := result.skippedRecent.Load(); got != 0 {
		t.Errorf("skippedRecent = %d, want 0 (dir mtime should not gate recursion)", got)
	}
}

// TestVerifySyncRootPath is a regression test for the path.Join fix.
// fmt.Sprintf("%s/%s", "/", name) produced "//name"; path.Join produces "/name".
// This test walks from "/" and verifies the child directory is found and
// compared correctly (not silently skipped due to a malformed path).
func TestVerifySyncRootPath(t *testing.T) {
	clientA := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/":      {verifyDirEntry("data")},
			"/data":  {verifyFileEntry("file.txt", 42)},
		},
	}
	clientB := &verifyTestFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/":      {verifyDirEntry("data")},
			"/data":  {verifyFileEntry("file.txt", 42)},
		},
	}

	result := &VerifyResult{}
	sem := make(chan struct{}, verifySyncConcurrency)
	if err := compareDirectory(context.Background(), clientA, clientB,
		"/", "/", false, time.Time{}, sem, result); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.missingCount.Load() != 0 || result.sizeMismatch.Load() != 0 {
		t.Errorf("identical trees from root should have no diffs: missing=%d size=%d",
			result.missingCount.Load(), result.sizeMismatch.Load())
	}
	// 2 directories traversed: "/" and "/data"
	if got := result.dirCount.Load(); got != 2 {
		t.Errorf("dirCount = %d, want 2 (root + /data)", got)
	}
	// 1 file compared: /data/file.txt
	if got := result.fileCount.Load(); got != 1 {
		t.Errorf("fileCount = %d, want 1", got)
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
