package command

import (
	"container/heap"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func makeResp(dir, name string, isDir bool, tsNs int64, isNew bool) *filer_pb.SubscribeMetadataResponse {
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory:         dir,
		TsNs:              tsNs,
		EventNotification: &filer_pb.EventNotification{},
	}
	entry := &filer_pb.Entry{
		Name:        name,
		IsDirectory: isDir,
	}
	if isNew {
		resp.EventNotification.NewEntry = entry
	} else {
		resp.EventNotification.OldEntry = entry
	}
	return resp
}

// makeDirUpdateResp builds an in-place attribute update event for a directory
// (same parent and same name on both sides — matches filer_pb.IsUpdate).
func makeDirUpdateResp(parent, name string, tsNs int64) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory: parent,
		TsNs:      tsNs,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: name, IsDirectory: true},
			NewEntry:      &filer_pb.Entry{Name: name, IsDirectory: true},
			NewParentPath: parent,
		},
	}
}

func makeRenameResp(oldDir, oldName, newDir, newName string, isDir bool, tsNs int64) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory: oldDir,
		TsNs:      tsNs,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{
				Name:        oldName,
				IsDirectory: isDir,
			},
			NewEntry: &filer_pb.Entry{
				Name:        newName,
				IsDirectory: isDir,
			},
			NewParentPath: newDir,
		},
	}
}

func TestPathAncestors(t *testing.T) {
	tests := []struct {
		path     util.FullPath
		expected []util.FullPath
	}{
		{"/a/b/c/file.txt", []util.FullPath{"/a/b/c", "/a/b", "/a", "/"}},
		{"/a/b", []util.FullPath{"/a", "/"}},
		{"/a", []util.FullPath{"/"}},
		{"/", nil},
	}
	for _, tt := range tests {
		got := pathAncestors(tt.path)
		if len(got) != len(tt.expected) {
			t.Errorf("pathAncestors(%q) = %v, want %v", tt.path, got, tt.expected)
			continue
		}
		for i := range got {
			if got[i] != tt.expected[i] {
				t.Errorf("pathAncestors(%q)[%d] = %q, want %q", tt.path, i, got[i], tt.expected[i])
			}
		}
	}
}

// TestFileVsFileConflict verifies that two file operations on the same path conflict,
// and on different paths do not.
func TestFileVsFileConflict(t *testing.T) {
	noop := func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }
	p := NewMetadataProcessor(noop, 100, 0)

	// Add a file job
	active := makeResp("/dir1", "file.txt", false, 1, true)
	path, _, kind := extractJobInfo(active)
	p.activeJobTs[active.TsNs] = 1
	p.addPathToIndex(path, kind)

	// Same file should conflict
	same := makeResp("/dir1", "file.txt", false, 2, true)
	if !p.conflictsWith(same) {
		t.Error("expected conflict for same file path")
	}

	// Different file should not conflict
	diff := makeResp("/dir1", "other.txt", false, 3, true)
	if p.conflictsWith(diff) {
		t.Error("unexpected conflict for different file path")
	}

	// File in different directory should not conflict
	diffDir := makeResp("/dir2", "file.txt", false, 4, true)
	if p.conflictsWith(diffDir) {
		t.Error("unexpected conflict for file in different directory")
	}
}

// TestFileUnderActiveDirConflict verifies that a file under an active directory operation
// conflicts, but a file outside does not.
func TestFileUnderActiveDirConflict(t *testing.T) {
	noop := func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }
	p := NewMetadataProcessor(noop, 100, 0)

	// Add a directory job at /dir1
	active := makeResp("/", "dir1", true, 1, true)
	path, _, kind := extractJobInfo(active)
	p.activeJobTs[active.TsNs] = 1
	p.addPathToIndex(path, kind)

	// File under /dir1 should conflict
	under := makeResp("/dir1", "file.txt", false, 2, true)
	if !p.conflictsWith(under) {
		t.Error("expected conflict for file under active directory")
	}

	// File deeply nested under /dir1 should conflict
	deep := makeResp("/dir1/sub/deep", "file.txt", false, 3, true)
	if !p.conflictsWith(deep) {
		t.Error("expected conflict for deeply nested file under active directory")
	}

	// File in /dir2 should not conflict
	outside := makeResp("/dir2", "file.txt", false, 4, true)
	if p.conflictsWith(outside) {
		t.Error("unexpected conflict for file outside active directory")
	}

	// File at /dir1 itself (not under, at) SHOULD conflict with an active
	// barrier dir at /dir1 — same-path promotions must serialize.
	atSame := makeResp("/", "dir1", false, 5, true)
	if !p.conflictsWith(atSame) {
		t.Error("expected conflict for file at same path as active barrier dir")
	}
}

// TestDirWithActiveFileUnder verifies that a directory operation conflicts when
// there are active file jobs under it.
func TestDirWithActiveFileUnder(t *testing.T) {
	noop := func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }
	p := NewMetadataProcessor(noop, 100, 0)

	// Add file jobs under /dir1
	f1 := makeResp("/dir1/sub", "file.txt", false, 1, true)
	path, _, kind := extractJobInfo(f1)
	p.activeJobTs[f1.TsNs] = 1
	p.addPathToIndex(path, kind)

	// Directory /dir1 should conflict (has active file under it)
	dirOp := makeResp("/", "dir1", true, 2, true)
	if !p.conflictsWith(dirOp) {
		t.Error("expected conflict for directory with active file under it")
	}

	// Directory /dir2 should not conflict
	dirOp2 := makeResp("/", "dir2", true, 3, true)
	if p.conflictsWith(dirOp2) {
		t.Error("unexpected conflict for directory with no active jobs under it")
	}
}

// TestDirVsDirConflict verifies ancestor/descendant directory conflict detection.
func TestDirVsDirConflict(t *testing.T) {
	noop := func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }
	p := NewMetadataProcessor(noop, 100, 0)

	// Add directory job at /a/b
	active := makeResp("/a", "b", true, 1, true)
	path, _, kind := extractJobInfo(active)
	p.activeJobTs[active.TsNs] = 1
	p.addPathToIndex(path, kind)

	// /a/b/c (descendant) should conflict
	desc := makeResp("/a/b", "c", true, 2, true)
	if !p.conflictsWith(desc) {
		t.Error("expected conflict for descendant directory")
	}

	// /a (ancestor) should conflict
	anc := makeResp("/", "a", true, 3, true)
	if !p.conflictsWith(anc) {
		t.Error("expected conflict for ancestor directory")
	}

	// Same-path barrier dir SHOULD conflict: concurrent create/delete/rename
	// on the same directory must serialize.
	same := makeResp("/a", "b", true, 4, true)
	if !p.conflictsWith(same) {
		t.Error("expected conflict for same-path barrier directory")
	}

	// Sibling directory should not conflict
	sibling := makeResp("/a", "c", true, 5, true)
	if p.conflictsWith(sibling) {
		t.Error("unexpected conflict for sibling directory")
	}
}

// TestRenameConflict verifies that rename events with two paths check both paths.
func TestRenameConflict(t *testing.T) {
	noop := func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }
	p := NewMetadataProcessor(noop, 100, 0)

	// Add file job at /dir1/file.txt
	f1 := makeResp("/dir1", "file.txt", false, 1, true)
	path, _, kind := extractJobInfo(f1)
	p.activeJobTs[f1.TsNs] = 1
	p.addPathToIndex(path, kind)

	// Rename from /dir2/a.txt to /dir1/file.txt should conflict (newPath matches)
	rename := makeRenameResp("/dir2", "a.txt", "/dir1", "file.txt", false, 2)
	if !p.conflictsWith(rename) {
		t.Error("expected conflict for rename whose destination matches active file")
	}

	// Rename from /dir1/file.txt to /dir2/b.txt should conflict (oldPath matches)
	rename2 := makeRenameResp("/dir1", "file.txt", "/dir2", "b.txt", false, 3)
	if !p.conflictsWith(rename2) {
		t.Error("expected conflict for rename whose source matches active file")
	}

	// Rename between unrelated paths should not conflict
	rename3 := makeRenameResp("/dir3", "x.txt", "/dir4", "y.txt", false, 4)
	if p.conflictsWith(rename3) {
		t.Error("unexpected conflict for rename between unrelated paths")
	}
}

// TestActiveRenameConflict verifies that an active rename job registers both paths.
func TestActiveRenameConflict(t *testing.T) {
	noop := func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }
	p := NewMetadataProcessor(noop, 100, 0)

	// Add active rename job: /dir1/old.txt -> /dir2/new.txt
	rename := makeRenameResp("/dir1", "old.txt", "/dir2", "new.txt", false, 1)
	path, newPath, kind := extractJobInfo(rename)
	p.activeJobTs[rename.TsNs] = 1
	p.addPathToIndex(path, kind)
	if newPath != "" {
		p.addPathToIndex(newPath, kind)
	}

	// File at /dir1/old.txt should conflict
	f1 := makeResp("/dir1", "old.txt", false, 2, true)
	if !p.conflictsWith(f1) {
		t.Error("expected conflict at rename source path")
	}

	// File at /dir2/new.txt should conflict
	f2 := makeResp("/dir2", "new.txt", false, 3, true)
	if !p.conflictsWith(f2) {
		t.Error("expected conflict at rename destination path")
	}

	// File at unrelated path should not conflict
	f3 := makeResp("/dir3", "other.txt", false, 4, true)
	if p.conflictsWith(f3) {
		t.Error("unexpected conflict at unrelated path")
	}
}

// TestRootDirConflict verifies that an active job at / conflicts with everything.
func TestRootDirConflict(t *testing.T) {
	noop := func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }
	p := NewMetadataProcessor(noop, 100, 0)

	// Add directory job at /
	// Note: a dir entry at "/" would be created as FullPath("/").Child("somedir")
	// But let's test what happens with an active dir at /some/path and check root
	active := makeResp("/some", "dir", true, 1, true)
	path, _, kind := extractJobInfo(active)
	p.activeJobTs[active.TsNs] = 1
	p.addPathToIndex(path, kind)

	// Root dir should conflict because active dir /some/dir is under /
	// A new directory at "/" should see descendantCount["/"] > 0
	if p.descendantCount["/"] <= 0 {
		t.Error("expected descendantCount['/'] > 0 for active job under root")
	}
}

// TestIndexCleanup verifies that removing a job properly cleans up all indexes.
func TestIndexCleanup(t *testing.T) {
	noop := func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }
	p := NewMetadataProcessor(noop, 100, 0)

	// Add then remove a file job
	path := util.FullPath("/a/b/c/file.txt")
	p.addPathToIndex(path, kindFile)

	if p.activeFilePaths[path] != 1 {
		t.Errorf("expected activeFilePaths count 1, got %d", p.activeFilePaths[path])
	}
	if p.descendantCount["/a/b/c"] != 1 {
		t.Errorf("expected descendantCount['/a/b/c'] = 1, got %d", p.descendantCount["/a/b/c"])
	}

	p.removePathFromIndex(path, kindFile)

	if len(p.activeFilePaths) != 0 {
		t.Errorf("expected empty activeFilePaths after removal, got %v", p.activeFilePaths)
	}
	if len(p.descendantCount) != 0 {
		t.Errorf("expected empty descendantCount after removal, got %v", p.descendantCount)
	}
}

// TestWatermarkWithHeap verifies watermark advancement using the min-heap.
func TestWatermarkWithHeap(t *testing.T) {
	noop := func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }
	p := NewMetadataProcessor(noop, 100, 0)

	// Simulate adding jobs in order
	for _, ts := range []int64{10, 20, 30} {
		jobPath := util.FullPath("/file" + string(rune('0'+ts/10)))
		p.activeJobTs[ts] = 1
		p.addPathToIndex(jobPath, kindFile)
		heap.Push(&p.tsHeap, ts)
	}

	if p.tsHeap[0] != 10 {
		t.Errorf("expected heap min=10, got %d", p.tsHeap[0])
	}

	// Remove non-oldest (ts=20) — heap top should stay 10
	delete(p.activeJobTs, 20)
	p.removePathFromIndex("/file2", kindFile)
	// Lazy clean: top is 10 which is still active, so no pop
	for p.tsHeap.Len() > 0 {
		if p.activeJobTs[p.tsHeap[0]] > 0 {
			break
		}
		heap.Pop(&p.tsHeap)
	}
	if p.tsHeap[0] != 10 {
		t.Errorf("expected heap min=10 after removing 20, got %d", p.tsHeap[0])
	}

	// Remove oldest (ts=10) — lazy clean should find 30
	delete(p.activeJobTs, 10)
	p.removePathFromIndex("/file1", kindFile)
	for p.tsHeap.Len() > 0 {
		if p.activeJobTs[p.tsHeap[0]] > 0 {
			break
		}
		heap.Pop(&p.tsHeap)
	}
	if p.tsHeap.Len() != 1 || p.tsHeap[0] != 30 {
		t.Errorf("expected heap min=30 after removing 10 and 20, got len=%d", p.tsHeap.Len())
	}
}

// TestNonBarrierDirUpdateDoesNotBlockDescendants verifies the loosened
// dir-conflict rule: an attribute-only directory update (same parent + same
// name) must NOT block file events under that directory. A barrier dir event
// (create/delete/rename) on the same path still must.
func TestNonBarrierDirUpdateDoesNotBlockDescendants(t *testing.T) {
	noop := func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }

	t.Run("attribute update on /dir1 does not block file under it", func(t *testing.T) {
		p := NewMetadataProcessor(noop, 100, 0)

		// Active non-barrier: attribute update on /dir1.
		active := makeDirUpdateResp("/", "dir1", 1)
		path, _, kind := extractJobInfo(active)
		if kind != kindNonBarrierDir {
			t.Fatalf("expected kindNonBarrierDir for dir attribute update, got %v", kind)
		}
		p.activeJobTs[active.TsNs] = 1
		p.addPathToIndex(path, kind)

		// File under /dir1 should NOT conflict with the attribute update.
		under := makeResp("/dir1", "file.txt", false, 2, true)
		if p.conflictsWith(under) {
			t.Error("file under a non-barrier dir update should not conflict")
		}

		// Nested file should also not conflict.
		deep := makeResp("/dir1/sub/deep", "file.txt", false, 3, true)
		if p.conflictsWith(deep) {
			t.Error("deeply nested file under a non-barrier dir update should not conflict")
		}
	})

	t.Run("barrier dir create at the same path still blocks descendants", func(t *testing.T) {
		p := NewMetadataProcessor(noop, 100, 0)

		active := makeResp("/", "dir1", true, 1, true) // create
		path, _, kind := extractJobInfo(active)
		if kind != kindBarrierDir {
			t.Fatalf("expected kindBarrierDir for dir create, got %v", kind)
		}
		p.activeJobTs[active.TsNs] = 1
		p.addPathToIndex(path, kind)

		under := makeResp("/dir1", "file.txt", false, 2, true)
		if !p.conflictsWith(under) {
			t.Error("file under an active barrier dir create should still conflict")
		}
	})

	t.Run("barrier dir delete still waits for in-flight descendants", func(t *testing.T) {
		p := NewMetadataProcessor(noop, 100, 0)

		// Active file under /dir1.
		f := makeResp("/dir1", "file.txt", false, 1, true)
		path, _, kind := extractJobInfo(f)
		p.activeJobTs[f.TsNs] = 1
		p.addPathToIndex(path, kind)

		// Incoming barrier delete on /dir1 should still wait for the
		// in-flight file descendant.
		del := makeResp("/", "dir1", true, 2, false)
		if !p.conflictsWith(del) {
			t.Error("barrier dir delete should wait for descendant file job")
		}
	})

	t.Run("non-barrier dir update still keeps ancestor barrier waiting", func(t *testing.T) {
		p := NewMetadataProcessor(noop, 100, 0)

		// Active non-barrier dir update at /a/b.
		upd := makeDirUpdateResp("/a", "b", 1)
		path, _, kind := extractJobInfo(upd)
		p.activeJobTs[upd.TsNs] = 1
		p.addPathToIndex(path, kind)

		// A barrier delete on /a (the ancestor) should wait for it.
		del := makeResp("/", "a", true, 2, false)
		if !p.conflictsWith(del) {
			t.Error("barrier ancestor dir delete should wait for non-barrier descendant update")
		}
	})
}

// TestSamePathBarrierSerialization verifies the tightened same-path rules:
// a barrier dir in flight at p serializes every other job at p (file, barrier
// dir, or non-barrier update), and a file in flight at p serializes incoming
// files and barrier dirs at p.
func TestSamePathBarrierSerialization(t *testing.T) {
	noop := func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }

	t.Run("barrier dir at p blocks same-path file", func(t *testing.T) {
		p := NewMetadataProcessor(noop, 100, 0)
		active := makeResp("/", "dir1", true, 1, true) // dir create
		path, _, kind := extractJobInfo(active)
		p.activeJobTs[active.TsNs] = 1
		p.addPathToIndex(path, kind)

		file := makeResp("/", "dir1", false, 2, true)
		if !p.conflictsWith(file) {
			t.Error("file at path of active barrier dir should conflict")
		}
	})

	t.Run("barrier dir at p blocks another same-path barrier dir", func(t *testing.T) {
		p := NewMetadataProcessor(noop, 100, 0)
		active := makeResp("/", "dir1", true, 1, true) // dir create
		path, _, kind := extractJobInfo(active)
		p.activeJobTs[active.TsNs] = 1
		p.addPathToIndex(path, kind)

		del := makeResp("/", "dir1", true, 2, false) // dir delete, same path
		if !p.conflictsWith(del) {
			t.Error("concurrent create/delete on same dir path should conflict")
		}
	})

	t.Run("barrier dir at p blocks non-barrier update at same path", func(t *testing.T) {
		p := NewMetadataProcessor(noop, 100, 0)
		active := makeResp("/", "dir1", true, 1, true) // dir create
		path, _, kind := extractJobInfo(active)
		p.activeJobTs[active.TsNs] = 1
		p.addPathToIndex(path, kind)

		upd := makeDirUpdateResp("/", "dir1", 2)
		if !p.conflictsWith(upd) {
			t.Error("attribute update on dir being created should wait for the create")
		}
	})

	t.Run("file at p blocks same-path barrier dir", func(t *testing.T) {
		p := NewMetadataProcessor(noop, 100, 0)
		active := makeResp("/", "thing", false, 1, true) // file create at /thing
		path, _, kind := extractJobInfo(active)
		p.activeJobTs[active.TsNs] = 1
		p.addPathToIndex(path, kind)

		// Barrier dir at /thing (e.g. a file→dir promotion) must wait.
		promoteDir := makeResp("/", "thing", true, 2, true)
		if !p.conflictsWith(promoteDir) {
			t.Error("barrier dir at path of active file should conflict")
		}
	})

	t.Run("non-barrier update at p blocks incoming barrier dir at same path", func(t *testing.T) {
		// Regression test for a bug spotted in review: an in-flight
		// attribute update on /dir1 must serialize against a later
		// delete/rename/create on /dir1.
		p := NewMetadataProcessor(noop, 100, 0)
		active := makeDirUpdateResp("/", "dir1", 1)
		path, _, kind := extractJobInfo(active)
		if kind != kindNonBarrierDir {
			t.Fatalf("expected kindNonBarrierDir, got %v", kind)
		}
		p.activeJobTs[active.TsNs] = 1
		p.addPathToIndex(path, kind)

		del := makeResp("/", "dir1", true, 2, false) // dir delete
		if !p.conflictsWith(del) {
			t.Error("barrier dir at path of active non-barrier update should conflict")
		}
		// Ensure the removal path also cleans up the non-barrier index.
		p.removePathFromIndex(path, kind)
		if len(p.activeNonBarrierDirPaths) != 0 {
			t.Errorf("activeNonBarrierDirPaths not cleaned up, got %v", p.activeNonBarrierDirPaths)
		}
	})

	t.Run("non-barrier update at p does NOT block same-path non-barrier update", func(t *testing.T) {
		p := NewMetadataProcessor(noop, 100, 0)
		active := makeDirUpdateResp("/", "dir1", 1)
		path, _, kind := extractJobInfo(active)
		p.activeJobTs[active.TsNs] = 1
		p.addPathToIndex(path, kind)

		// Concurrent attribute bumps are allowed: last writer wins.
		upd2 := makeDirUpdateResp("/", "dir1", 2)
		if p.conflictsWith(upd2) {
			t.Error("concurrent non-barrier dir updates should not conflict")
		}
	})
}

// benchResult prevents the compiler from optimizing away the conflict check.
var benchResult bool

// BenchmarkConflictCheck measures conflict check cost with varying active job counts.
// With the index-based approach, cost should be O(depth) regardless of job count.
func BenchmarkConflictCheck(b *testing.B) {
	for _, numJobs := range []int{32, 256, 1024} {
		b.Run(fmt.Sprintf("jobs=%d", numJobs), func(b *testing.B) {
			noop := func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }
			p := NewMetadataProcessor(noop, numJobs+1, 0)

			// Fill with active jobs in different directories
			for i := range numJobs {
				dir := fmt.Sprintf("/dir%d/sub%d", i/100, i%100)
				name := fmt.Sprintf("file%d.txt", i)
				resp := makeResp(dir, name, false, int64(i+1), true)
				path, _, kind := extractJobInfo(resp)
				p.activeJobTs[resp.TsNs] = 1
				p.addPathToIndex(path, kind)
			}

			// Benchmark conflict check for a non-conflicting event
			probe := makeResp("/other/path", "test.txt", false, int64(numJobs+1), true)
			var r bool
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r = p.conflictsWith(probe)
			}
			benchResult = r
		})
	}
}

// waitForJobsToDrain blocks until every job goroutine has finished bookkeeping.
func waitForJobsToDrain(t *testing.T, p *MetadataProcessor) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		p.activeJobsLock.Lock()
		remaining := p.activeJobCount
		p.activeJobsLock.Unlock()
		if remaining == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for sync jobs to drain")
}

// TestFailedJobHoldsWatermark verifies that a job that returns an error keeps
// the watermark — and therefore the persisted sync offset — behind the failed
// event, so a restart replays it. Advancing past it drops the event for good:
// the file stays local-only and nothing ever retries the upload.
func TestFailedJobHoldsWatermark(t *testing.T) {
	const failedTsNs = int64(200)
	// a permanent error, so util.Retry gives up on the first attempt
	fn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		if resp.TsNs == failedTsNs {
			return errors.New("AccessDenied: Access Denied")
		}
		return nil
	}
	// concurrency 1 runs the jobs serially in timestamp order
	p := NewMetadataProcessor(fn, 1, 0)

	p.AddSyncJob(makeResp("/dir", "a.txt", false, 100, true))
	waitForJobsToDrain(t, p)
	if got := p.processedTsWatermark.Load(); got != 100 {
		t.Fatalf("watermark = %d after a successful job, want 100", got)
	}

	// a later event still in flight when the failure lands finishes fine, but
	// the offset stays behind the failure
	release := make(chan struct{})
	slowFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		if resp.TsNs == 300 {
			<-release
		}
		return fn(resp)
	}
	p2 := NewMetadataProcessor(slowFn, 10, 0)
	// admit the slow job first so it is in flight when the failure lands
	p2.AddSyncJob(makeResp("/dir", "c.txt", false, 300, true))
	p2.AddSyncJob(makeResp("/dir", "a.txt", false, 100, true))
	p2.AddSyncJob(makeResp("/dir", "b.txt", false, failedTsNs, true))
	deadline := time.Now().Add(10 * time.Second)
	for p2.OldestFailedTsNs() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p2.OldestFailedTsNs() != failedTsNs {
		t.Fatalf("oldest failed = %d, want the pin at %d", p2.OldestFailedTsNs(), failedTsNs)
	}
	close(release)
	waitForJobsToDrain(t, p2)
	if got := p2.processedTsWatermark.Load(); got != 100 {
		t.Fatalf("watermark = %d after a later success, want it held at 100", got)
	}
}

// TestFailedJobHoldsWatermarkAtOldestFailure verifies that the watermark is
// pinned by the oldest failure, not the most recent one. Once the processor
// drains it stops accepting events for the resubscribe, so both failures have
// to be in the same drained batch.
func TestFailedJobHoldsWatermarkAtOldestFailure(t *testing.T) {
	release := make(chan struct{})
	fn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		<-release
		if resp.TsNs == 200 || resp.TsNs == 400 {
			return errors.New("AccessDenied: Access Denied")
		}
		return nil
	}
	p := NewMetadataProcessor(fn, 10, 0)

	for _, ts := range []int64{100, 200, 300, 400, 500} {
		p.AddSyncJob(makeResp("/dir", fmt.Sprintf("f%d.txt", ts), false, ts, true))
	}
	close(release)
	waitForJobsToDrain(t, p)

	if got := p.processedTsWatermark.Load(); got != 100 {
		t.Fatalf("watermark = %d, want it held at 100 by the failure at 200", got)
	}
}

// TestSyncStreamMetrics verifies the per-event and byte counters and the
// in-flight gauges across success and failure outcomes. Bytes count only the
// chunk delta: the failing create's 40, the successful create's 100, the
// update's new 60-byte chunk but not its shared one, and nothing for the
// delete despite its chunk.
func TestSyncStreamMetrics(t *testing.T) {
	release := make(chan struct{})
	fn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		<-release
		if resp.TsNs == 2 {
			return errors.New("AccessDenied: Access Denied")
		}
		return nil
	}
	p := NewMetadataProcessor(fn, 100, 0)
	// the counters are process-global: a unique client name keeps repeated
	// runs (-count>1) from accumulating into each other
	p.SetMetrics("srcFiler", "dstFiler", fmt.Sprintf("TestSyncStreamMetrics-%d", time.Now().UnixNano()), "/")

	create := makeResp("/dir1", "a.txt", false, 1, true)
	create.EventNotification.NewEntry.Chunks = []*filer_pb.FileChunk{{FileId: "1,a0", Size: 100}}
	failing := makeResp("/dir1", "b.txt", false, 2, true)
	failing.EventNotification.NewEntry.Chunks = []*filer_pb.FileChunk{{FileId: "2,b0", Size: 40}}
	shared := &filer_pb.FileChunk{FileId: "3,c0", Size: 30}
	update := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir1",
		TsNs:      3,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "c.txt", Chunks: []*filer_pb.FileChunk{shared}},
			NewEntry:      &filer_pb.Entry{Name: "c.txt", Chunks: []*filer_pb.FileChunk{shared, {FileId: "3,c1", Size: 60}}},
			NewParentPath: "/dir1",
		},
	}
	del := makeResp("/dir1", "d.txt", false, 4, false)
	del.EventNotification.OldEntry.Chunks = []*filer_pb.FileChunk{{FileId: "4,d0", Size: 999}}

	for _, resp := range []*filer_pb.SubscribeMetadataResponse{create, failing, update, del} {
		p.AddSyncJob(resp)
	}
	close(release)
	waitForJobsToDrain(t, p)

	for _, tc := range []struct {
		name string
		got  float64
		want float64
	}{
		{"received", testutil.ToFloat64(p.metrics.received), 4},
		{"processed", testutil.ToFloat64(p.metrics.processed), 3},
		{"failed", testutil.ToFloat64(p.metrics.failed), 1},
		{"in_flight", testutil.ToFloat64(p.metrics.inFlight), 0},
		{"received_bytes", testutil.ToFloat64(p.metrics.receivedBytes), 200},
		{"processed_bytes", testutil.ToFloat64(p.metrics.processedBytes), 160},
		{"failed_bytes", testutil.ToFloat64(p.metrics.failedBytes), 40},
		{"in_flight_bytes", testutil.ToFloat64(p.metrics.inFlightBytes), 0},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}
}

// TestFailedJobReplaySuccessClearsPin verifies that when the failed event is
// redelivered while the processor is still alive and succeeds this time, the
// failure pin clears and the watermark can move again. It is the one event a
// stopped processor still runs. The resubscribe still signals once the jobs
// drain: anything dropped after the stop has to replay too.
func TestFailedJobReplaySuccessClearsPin(t *testing.T) {
	failed := true
	release := make(chan struct{})
	fn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		if resp.TsNs == 300 {
			<-release
		}
		if resp.TsNs == 200 && failed {
			failed = false
			return errors.New("AccessDenied: Access Denied")
		}
		return nil
	}
	p := NewMetadataProcessor(fn, 10, 0)

	// the slow job is admitted first so the processor has in-flight work when
	// the failure lands, keeping the drain — and the resubscribe — open
	p.AddSyncJob(makeResp("/dir", "c.txt", false, 300, true))
	p.AddSyncJob(makeResp("/dir", "b.txt", false, 200, true))

	deadline := time.Now().Add(10 * time.Second)
	for p.OldestFailedTsNs() != 200 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := p.OldestFailedTsNs(); got != 200 {
		t.Fatalf("oldest failed = %d, want 200", got)
	}

	// once stopped, a new event drops instead of queueing into a processor
	// that is about to be abandoned — it replays after the resubscribe
	p.AddSyncJob(makeResp("/dir", "d.txt", false, 400, true))
	p.activeJobsLock.Lock()
	dropped := p.activeJobTs[400] == 0
	p.activeJobsLock.Unlock()
	if !dropped {
		t.Fatal("new event admitted after the failure stopped the processor")
	}

	// the redelivery is the exception: it runs and its success clears the pin
	p.AddSyncJob(makeResp("/dir", "b.txt", false, 200, true))
	deadline = time.Now().Add(10 * time.Second)
	for p.OldestFailedTsNs() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := p.OldestFailedTsNs(); got != 0 {
		t.Fatalf("oldest failed = %d after a successful replay, want 0", got)
	}

	close(release)
	waitForJobsToDrain(t, p)
	select {
	case <-p.ResubscribeCh():
	case <-time.After(time.Second):
		t.Fatal("resubscribe never signaled after the stopped processor drained")
	}
	if got := p.processedTsWatermark.Load(); got != 300 {
		t.Fatalf("watermark = %d after recovery, want 300", got)
	}
}

// TestFilteredMarkerAdvancesWatermark verifies that a filtered-progress marker
// (empty event with a timestamp) moves the watermark once all earlier work has
// finished, but never past an in-flight job or an unresolved failure.
func TestFilteredMarkerAdvancesWatermark(t *testing.T) {
	marker := func(ts int64) *filer_pb.SubscribeMetadataResponse {
		return &filer_pb.SubscribeMetadataResponse{TsNs: ts, EventNotification: &filer_pb.EventNotification{}}
	}

	t.Run("idle", func(t *testing.T) {
		p := NewMetadataProcessor(func(resp *filer_pb.SubscribeMetadataResponse) error { return nil }, 100, 50)
		p.AddSyncJob(marker(90))
		if got := p.processedTsWatermark.Load(); got != 90 {
			t.Fatalf("watermark = %d after marker, want 90", got)
		}
	})

	t.Run("behind in-flight job", func(t *testing.T) {
		release := make(chan struct{})
		p := NewMetadataProcessor(func(resp *filer_pb.SubscribeMetadataResponse) error {
			<-release
			return nil
		}, 100, 50)
		p.AddSyncJob(makeResp("/dir", "f.txt", false, 60, true))
		p.AddSyncJob(marker(80))
		if got := p.processedTsWatermark.Load(); got != 50 {
			t.Fatalf("watermark = %d with a job in flight, want 50", got)
		}
		close(release)
		waitForJobsToDrain(t, p)
		if got := p.processedTsWatermark.Load(); got != 80 {
			t.Fatalf("watermark = %d after drain, want the retained marker at 80", got)
		}
		p.AddSyncJob(marker(90))
		if got := p.processedTsWatermark.Load(); got != 90 {
			t.Fatalf("watermark = %d after drain and marker, want 90", got)
		}
	})

	t.Run("behind a failure", func(t *testing.T) {
		p := NewMetadataProcessor(func(resp *filer_pb.SubscribeMetadataResponse) error {
			return errors.New("AccessDenied: Access Denied")
		}, 100, 50)
		p.AddSyncJob(makeResp("/dir", "f.txt", false, 100, true))
		waitForJobsToDrain(t, p)
		p.AddSyncJob(marker(200))
		if got := p.processedTsWatermark.Load(); got != 50 {
			t.Fatalf("watermark = %d past a failure pin, want 50", got)
		}
		p.AddSyncJob(marker(70))
		if got := p.processedTsWatermark.Load(); got != 70 {
			t.Fatalf("watermark = %d behind the pin, want 70", got)
		}
	})
}

// TestFailedLedgerCapsAndStaysPinned verifies that a sustained run of distinct
// failures cannot grow failedTs without bound: past maxFailedSyncEvents the
// ledger collapses to a sticky pin at the oldest failure, so the watermark
// still replays from it while memory stays bounded.
func TestFailedLedgerCapsAndStaysPinned(t *testing.T) {
	defer func(old int) { maxFailedSyncEvents = old }(maxFailedSyncEvents)
	maxFailedSyncEvents = 4

	fail := true
	release := make(chan struct{})
	p := NewMetadataProcessor(func(resp *filer_pb.SubscribeMetadataResponse) error {
		<-release
		if fail {
			return errors.New("AccessDenied: Access Denied")
		}
		return nil
	}, 100, 0)
	for i := int64(1); i <= 10; i++ {
		p.AddSyncJob(makeResp("/dir", fmt.Sprintf("f%d.txt", i), false, i*100, true))
	}
	close(release)
	waitForJobsToDrain(t, p)

	if !p.failedSticky {
		t.Fatal("ledger did not collapse past the cap")
	}
	if got := p.OldestFailedTsNs(); got != 100 {
		t.Fatalf("oldest failed = %d, want the pin at the oldest failure 100", got)
	}
	if got := p.processedTsWatermark.Load(); got != 0 {
		t.Fatalf("watermark = %d, want it pinned at 0", got)
	}

	fail = false
	p.AddSyncJob(makeResp("/dir", "f1.txt", false, 100, true))
	waitForJobsToDrain(t, p)
	if got := p.OldestFailedTsNs(); got != 100 {
		t.Fatalf("oldest failed = %d after a collapsed replay, want the pin held at 100", got)
	}
	if got := p.processedTsWatermark.Load(); got != 0 {
		t.Fatalf("watermark = %d after a collapsed replay, want it still pinned at 0", got)
	}
}

// TestFailedLedgerDistinguishesEventsAtSameTs verifies that a success for one
// event does not clear the pin recorded for a different event that happened to
// share its timestamp — the ledger keys on event identity, not just TsNs. A
// slow job holds the drain open so the redelivery still lands on this
// processor generation.
func TestFailedLedgerDistinguishesEventsAtSameTs(t *testing.T) {
	fail := true
	release := make(chan struct{})
	hold := make(chan struct{})
	p := NewMetadataProcessor(func(resp *filer_pb.SubscribeMetadataResponse) error {
		if resp.EventNotification.NewEntry.GetName() == "slow.txt" {
			<-hold
		} else {
			<-release
		}
		if resp.EventNotification.NewEntry.GetName() == "bad.txt" && fail {
			return errors.New("AccessDenied: Access Denied")
		}
		return nil
	}, 100, 0)

	// both same-ts events admit before either resolves, so the success lands
	// while the failure is already pinned
	p.AddSyncJob(makeResp("/dir", "slow.txt", false, 900, true))
	p.AddSyncJob(makeResp("/dir", "bad.txt", false, 200, true))
	p.AddSyncJob(makeResp("/dir", "good.txt", false, 200, true))
	close(release)

	deadline := time.Now().Add(10 * time.Second)
	for p.OldestFailedTsNs() != 200 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := p.OldestFailedTsNs(); got != 200 {
		t.Fatalf("oldest failed = %d, want the other event's pin held at 200", got)
	}
	if got := p.processedTsWatermark.Load(); got != 0 {
		t.Fatalf("watermark = %d, want it still pinned at 0", got)
	}

	// redelivering the failed event itself is what clears the pin — and it is
	// the one event a stopped processor still admits
	fail = false
	p.AddSyncJob(makeResp("/dir", "bad.txt", false, 200, true))
	deadline = time.Now().Add(10 * time.Second)
	for p.OldestFailedTsNs() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(hold)
	waitForJobsToDrain(t, p)
	if got := p.OldestFailedTsNs(); got != 0 {
		t.Fatalf("oldest failed = %d after the failed event itself recovered, want 0", got)
	}
	if got := p.processedTsWatermark.Load(); got != 900 {
		t.Fatalf("watermark = %d after the pin cleared and the rest drained, want 900", got)
	}
}

// TestFailedJobSignalsResubscribe verifies that a job exhausting its retries
// closes ResubscribeCh so the follower drops the stream and the reconnect
// replays the pinned event — the path that used to wait for a restart.
func TestFailedJobSignalsResubscribe(t *testing.T) {
	p := NewMetadataProcessor(func(resp *filer_pb.SubscribeMetadataResponse) error {
		return errors.New("AccessDenied: Access Denied")
	}, 100, 0)

	p.AddSyncJob(makeResp("/dir", "a.txt", false, 100, true))
	waitForJobsToDrain(t, p)

	select {
	case <-p.ResubscribeCh():
	case <-time.After(time.Second):
		t.Fatal("resubscribe channel never closed after the failure pinned the watermark")
	}
	if got := p.OldestFailedTsNs(); got != 100 {
		t.Fatalf("oldest failed = %d, want 100", got)
	}
}

// TestResubscribeWaitsForInFlightJobs verifies the signal stays open while
// jobs admitted before the failure are still running — replaying behind them
// could restore older state over their writes — and closes once they drain.
// An event arriving after the stop drops instead of keeping the drain open,
// so a busy stream cannot starve the replay.
func TestResubscribeWaitsForInFlightJobs(t *testing.T) {
	release := make(chan struct{})
	p := NewMetadataProcessor(func(resp *filer_pb.SubscribeMetadataResponse) error {
		if resp.TsNs == 100 {
			return errors.New("AccessDenied: Access Denied")
		}
		<-release
		return nil
	}, 100, 0)

	// the slow job admits first so it is in flight when the failure lands
	p.AddSyncJob(makeResp("/dir", "b.txt", false, 200, true))
	p.AddSyncJob(makeResp("/dir", "a.txt", false, 100, true))

	deadline := time.Now().Add(10 * time.Second)
	for p.OldestFailedTsNs() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.OldestFailedTsNs() != 100 {
		t.Fatalf("oldest failed = %d, want the pin at 100", p.OldestFailedTsNs())
	}
	select {
	case <-p.ResubscribeCh():
		t.Fatal("resubscribe signaled while an in-flight job could still race the replay")
	case <-time.After(50 * time.Millisecond):
	}

	// the processor stopped on the failure, so this event drops — it replays
	// after the resubscribe — instead of starving the drain
	p.AddSyncJob(makeResp("/dir", "c.txt", false, 300, true))
	p.activeJobsLock.Lock()
	dropped := p.activeJobTs[300] == 0
	p.activeJobsLock.Unlock()
	if !dropped {
		t.Fatal("event admitted after the processor stopped")
	}

	close(release)
	waitForJobsToDrain(t, p)
	select {
	case <-p.ResubscribeCh():
	case <-time.After(time.Second):
		t.Fatal("resubscribe channel never closed after the in-flight jobs drained")
	}
}

// TestResubscribeWaitsForSameTsSibling guards the per-timestamp job
// counting: events in one batch can share a TsNs, and the failed job must
// not free the bookkeeping of a sibling still running at that timestamp.
// Otherwise its completion could report the processor drained and the
// resubscribe would replay over the sibling's writes.
func TestResubscribeWaitsForSameTsSibling(t *testing.T) {
	release := make(chan struct{})
	p := NewMetadataProcessor(func(resp *filer_pb.SubscribeMetadataResponse) error {
		if resp.EventNotification.NewEntry.GetName() == "bad.txt" {
			return errors.New("AccessDenied: Access Denied")
		}
		<-release
		return nil
	}, 100, 0)

	// the slow job admits first so it is in flight when the same-ts failure lands
	p.AddSyncJob(makeResp("/dir", "slow.txt", false, 200, true))
	p.AddSyncJob(makeResp("/dir", "bad.txt", false, 200, true))

	deadline := time.Now().Add(10 * time.Second)
	for p.OldestFailedTsNs() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.OldestFailedTsNs() != 200 {
		t.Fatalf("oldest failed = %d, want the pin at 200", p.OldestFailedTsNs())
	}
	select {
	case <-p.ResubscribeCh():
		t.Fatal("resubscribe signaled while a same-ts job was still running")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	waitForJobsToDrain(t, p)
	select {
	case <-p.ResubscribeCh():
	case <-time.After(time.Second):
		t.Fatal("resubscribe channel never closed after the same-ts jobs drained")
	}
}
