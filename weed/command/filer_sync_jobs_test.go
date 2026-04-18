package command

import (
	"container/heap"
	"fmt"
	"testing"

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
	path, newPath, kind := extractJobInfo(active)
	p.activeJobs[active.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
	path, newPath, kind := extractJobInfo(active)
	p.activeJobs[active.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
	path, newPath, kind := extractJobInfo(f1)
	p.activeJobs[f1.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
	path, newPath, kind := extractJobInfo(active)
	p.activeJobs[active.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
	path, newPath, kind := extractJobInfo(f1)
	p.activeJobs[f1.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
	p.activeJobs[rename.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
	path, newPath, kind := extractJobInfo(active)
	p.activeJobs[active.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
		p.activeJobs[ts] = &syncJobPaths{path: jobPath, kind: kindFile}
		p.addPathToIndex(jobPath, kindFile)
		heap.Push(&p.tsHeap, ts)
	}

	if p.tsHeap[0] != 10 {
		t.Errorf("expected heap min=10, got %d", p.tsHeap[0])
	}

	// Remove non-oldest (ts=20) — heap top should stay 10
	delete(p.activeJobs, 20)
	p.removePathFromIndex("/file2", kindFile)
	// Lazy clean: top is 10 which is still active, so no pop
	for p.tsHeap.Len() > 0 {
		if _, active := p.activeJobs[p.tsHeap[0]]; active {
			break
		}
		heap.Pop(&p.tsHeap)
	}
	if p.tsHeap[0] != 10 {
		t.Errorf("expected heap min=10 after removing 20, got %d", p.tsHeap[0])
	}

	// Remove oldest (ts=10) — lazy clean should find 30
	delete(p.activeJobs, 10)
	p.removePathFromIndex("/file1", kindFile)
	for p.tsHeap.Len() > 0 {
		if _, active := p.activeJobs[p.tsHeap[0]]; active {
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
		path, newPath, kind := extractJobInfo(active)
		if kind != kindNonBarrierDir {
			t.Fatalf("expected kindNonBarrierDir for dir attribute update, got %v", kind)
		}
		p.activeJobs[active.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
		path, newPath, kind := extractJobInfo(active)
		if kind != kindBarrierDir {
			t.Fatalf("expected kindBarrierDir for dir create, got %v", kind)
		}
		p.activeJobs[active.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
		path, newPath, kind := extractJobInfo(f)
		p.activeJobs[f.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
		path, newPath, kind := extractJobInfo(upd)
		p.activeJobs[upd.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
		path, newPath, kind := extractJobInfo(active)
		p.activeJobs[active.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
		p.addPathToIndex(path, kind)

		file := makeResp("/", "dir1", false, 2, true)
		if !p.conflictsWith(file) {
			t.Error("file at path of active barrier dir should conflict")
		}
	})

	t.Run("barrier dir at p blocks another same-path barrier dir", func(t *testing.T) {
		p := NewMetadataProcessor(noop, 100, 0)
		active := makeResp("/", "dir1", true, 1, true) // dir create
		path, newPath, kind := extractJobInfo(active)
		p.activeJobs[active.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
		p.addPathToIndex(path, kind)

		del := makeResp("/", "dir1", true, 2, false) // dir delete, same path
		if !p.conflictsWith(del) {
			t.Error("concurrent create/delete on same dir path should conflict")
		}
	})

	t.Run("barrier dir at p blocks non-barrier update at same path", func(t *testing.T) {
		p := NewMetadataProcessor(noop, 100, 0)
		active := makeResp("/", "dir1", true, 1, true) // dir create
		path, newPath, kind := extractJobInfo(active)
		p.activeJobs[active.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
		p.addPathToIndex(path, kind)

		upd := makeDirUpdateResp("/", "dir1", 2)
		if !p.conflictsWith(upd) {
			t.Error("attribute update on dir being created should wait for the create")
		}
	})

	t.Run("file at p blocks same-path barrier dir", func(t *testing.T) {
		p := NewMetadataProcessor(noop, 100, 0)
		active := makeResp("/", "thing", false, 1, true) // file create at /thing
		path, newPath, kind := extractJobInfo(active)
		p.activeJobs[active.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
		path, newPath, kind := extractJobInfo(active)
		if kind != kindNonBarrierDir {
			t.Fatalf("expected kindNonBarrierDir, got %v", kind)
		}
		p.activeJobs[active.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
		path, newPath, kind := extractJobInfo(active)
		p.activeJobs[active.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
				path, newPath, kind := extractJobInfo(resp)
				p.activeJobs[resp.TsNs] = &syncJobPaths{path: path, newPath: newPath, kind: kind}
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
