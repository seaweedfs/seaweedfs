package weed_server

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestMutatePathAncestors mirrors TestPathAncestors in filer_sync_jobs_test.go.
func TestMutatePathAncestors(t *testing.T) {
	tests := []struct {
		path util.FullPath
		want []util.FullPath
	}{
		{"/a/b/c/file.txt", []util.FullPath{"/a/b/c", "/a/b", "/a", "/"}},
		{"/a/b", []util.FullPath{"/a", "/"}},
		{"/a", []util.FullPath{"/"}},
		{"/", nil},
	}
	for _, tt := range tests {
		got := mutatePathAncestors(tt.path)
		if len(got) != len(tt.want) {
			t.Errorf("mutatePathAncestors(%q) = %v, want %v", tt.path, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("mutatePathAncestors(%q)[%d] = %q, want %q", tt.path, i, got[i], tt.want[i])
			}
		}
	}
}

// TestClassifyMutation covers the four request shapes and the delete-recursive
// barrier upgrade.
func TestClassifyMutation(t *testing.T) {
	tests := []struct {
		name          string
		req           *filer_pb.StreamMutateEntryRequest
		wantPrimary   util.FullPath
		wantSecondary util.FullPath
		wantKind      mutateJobKind
	}{
		{
			name: "create file",
			req: &filer_pb.StreamMutateEntryRequest{
				Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{
					CreateRequest: &filer_pb.CreateEntryRequest{
						Directory: "/a",
						Entry:     &filer_pb.Entry{Name: "f", IsDirectory: false},
					},
				},
			},
			wantPrimary: "/a/f",
			wantKind:    kindMutateFile,
		},
		{
			name: "create directory (barrier)",
			req: &filer_pb.StreamMutateEntryRequest{
				Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{
					CreateRequest: &filer_pb.CreateEntryRequest{
						Directory: "/a",
						Entry:     &filer_pb.Entry{Name: "d", IsDirectory: true},
					},
				},
			},
			wantPrimary: "/a/d",
			wantKind:    kindMutateBarrierDir,
		},
		{
			name: "update file",
			req: &filer_pb.StreamMutateEntryRequest{
				Request: &filer_pb.StreamMutateEntryRequest_UpdateRequest{
					UpdateRequest: &filer_pb.UpdateEntryRequest{
						Directory: "/a",
						Entry:     &filer_pb.Entry{Name: "f", IsDirectory: false},
					},
				},
			},
			wantPrimary: "/a/f",
			wantKind:    kindMutateFile,
		},
		{
			name: "update directory (non-barrier attr bump)",
			req: &filer_pb.StreamMutateEntryRequest{
				Request: &filer_pb.StreamMutateEntryRequest_UpdateRequest{
					UpdateRequest: &filer_pb.UpdateEntryRequest{
						Directory: "/",
						Entry:     &filer_pb.Entry{Name: "a", IsDirectory: true},
					},
				},
			},
			wantPrimary: "/a",
			wantKind:    kindMutateNonBarrierDir,
		},
		{
			name: "delete non-recursive (barrier: target type unknown)",
			req: &filer_pb.StreamMutateEntryRequest{
				Request: &filer_pb.StreamMutateEntryRequest_DeleteRequest{
					DeleteRequest: &filer_pb.DeleteEntryRequest{
						Directory: "/a", Name: "f",
					},
				},
			},
			wantPrimary: "/a/f",
			wantKind:    kindMutateBarrierDir,
		},
		{
			name: "delete recursive (barrier)",
			req: &filer_pb.StreamMutateEntryRequest{
				Request: &filer_pb.StreamMutateEntryRequest_DeleteRequest{
					DeleteRequest: &filer_pb.DeleteEntryRequest{
						Directory: "/a", Name: "d", IsRecursive: true,
					},
				},
			},
			wantPrimary: "/a/d",
			wantKind:    kindMutateBarrierDir,
		},
		{
			name: "malformed create (nil Entry)",
			req: &filer_pb.StreamMutateEntryRequest{
				Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{
					CreateRequest: &filer_pb.CreateEntryRequest{Directory: "/a"},
				},
			},
			wantPrimary: "/",
			wantKind:    kindMutateBarrierDir,
		},
		{
			name: "malformed update (nil Entry)",
			req: &filer_pb.StreamMutateEntryRequest{
				Request: &filer_pb.StreamMutateEntryRequest_UpdateRequest{
					UpdateRequest: &filer_pb.UpdateEntryRequest{Directory: "/a"},
				},
			},
			wantPrimary: "/",
			wantKind:    kindMutateBarrierDir,
		},
		{
			name:        "empty oneof",
			req:         &filer_pb.StreamMutateEntryRequest{},
			wantPrimary: "/",
			wantKind:    kindMutateBarrierDir,
		},
		{
			name: "rename",
			req: &filer_pb.StreamMutateEntryRequest{
				Request: &filer_pb.StreamMutateEntryRequest_RenameRequest{
					RenameRequest: &filer_pb.StreamRenameEntryRequest{
						OldDirectory: "/src", OldName: "a",
						NewDirectory: "/dst", NewName: "b",
					},
				},
			},
			wantPrimary:   "/src/a",
			wantSecondary: "/dst/b",
			wantKind:      kindMutateBarrierDir,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, s, k := classifyMutation(tt.req)
			if p != tt.wantPrimary {
				t.Errorf("primary = %q, want %q", p, tt.wantPrimary)
			}
			if s != tt.wantSecondary {
				t.Errorf("secondary = %q, want %q", s, tt.wantSecondary)
			}
			if k != tt.wantKind {
				t.Errorf("kind = %v, want %v", k, tt.wantKind)
			}
		})
	}
}

// TestPathConflictSamePathFile: two file ops on the same path conflict; on
// different paths they do not.
func TestPathConflictSamePathFile(t *testing.T) {
	s := newMutateScheduler(100)
	s.addPathLocked("/a/f", kindMutateFile)

	if !s.pathConflictsLocked("/a/f", kindMutateFile) {
		t.Error("same-path file+file should conflict")
	}
	if s.pathConflictsLocked("/a/g", kindMutateFile) {
		t.Error("different-path file+file should not conflict")
	}
}

// TestPathConflictBarrierBlocksSamePath: a barrier dir in flight at /a blocks
// any new job (file, barrier, non-barrier) at /a.
func TestPathConflictBarrierBlocksSamePath(t *testing.T) {
	s := newMutateScheduler(100)
	s.addPathLocked("/a", kindMutateBarrierDir)

	for _, k := range []mutateJobKind{kindMutateFile, kindMutateBarrierDir, kindMutateNonBarrierDir} {
		if !s.pathConflictsLocked("/a", k) {
			t.Errorf("barrier at /a should block kind %v at /a", k)
		}
	}
}

// TestPathConflictBarrierBlocksDescendants: a barrier at /a blocks any job
// strictly under /a, regardless of kind.
func TestPathConflictBarrierBlocksDescendants(t *testing.T) {
	s := newMutateScheduler(100)
	s.addPathLocked("/a", kindMutateBarrierDir)

	for _, child := range []util.FullPath{"/a/f", "/a/b", "/a/b/c"} {
		for _, k := range []mutateJobKind{kindMutateFile, kindMutateBarrierDir, kindMutateNonBarrierDir} {
			if !s.pathConflictsLocked(child, k) {
				t.Errorf("barrier at /a should block %v at %q", k, child)
			}
		}
	}
}

// TestPathConflictIncomingBarrierWaitsForDescendants: a new barrier at /a must
// wait for any active descendant (even a file) to drain.
func TestPathConflictIncomingBarrierWaitsForDescendants(t *testing.T) {
	s := newMutateScheduler(100)
	s.addPathLocked("/a/f", kindMutateFile)

	if !s.pathConflictsLocked("/a", kindMutateBarrierDir) {
		t.Error("incoming barrier at /a should wait for active file at /a/f")
	}
}

// TestPathConflictNonBarrierDirAllowsDescendants: an in-flight attribute-only
// dir update at /a does NOT block descendants. This is the filer.sync
// optimization that prevents mtime bumps from serializing file writes.
func TestPathConflictNonBarrierDirAllowsDescendants(t *testing.T) {
	s := newMutateScheduler(100)
	s.addPathLocked("/a", kindMutateNonBarrierDir)

	if s.pathConflictsLocked("/a/f", kindMutateFile) {
		t.Error("non-barrier dir update at /a should not block file at /a/f")
	}
	if s.pathConflictsLocked("/a/b", kindMutateNonBarrierDir) {
		t.Error("non-barrier dir update at /a should not block another non-barrier at /a/b")
	}
}

// TestPathConflictIncomingBarrierWaitsForSamePathNonBarrier: a delete/rename on
// a dir must wait for an in-flight chmod/xattr/mtime update at the same dir.
func TestPathConflictIncomingBarrierWaitsForSamePathNonBarrier(t *testing.T) {
	s := newMutateScheduler(100)
	s.addPathLocked("/a", kindMutateNonBarrierDir)

	if !s.pathConflictsLocked("/a", kindMutateBarrierDir) {
		t.Error("incoming barrier at /a should wait for non-barrier update at /a")
	}
	// But non-barrier vs non-barrier at the same path may overlap (attr bumps).
	if s.pathConflictsLocked("/a", kindMutateNonBarrierDir) {
		t.Error("non-barrier vs non-barrier at /a should not conflict")
	}
}

// TestAdmitDoneLifecycle: Admit marks state; Done unmarks; a subsequent
// conflicting Admit that was blocked must unblock after Done.
func TestAdmitDoneLifecycle(t *testing.T) {
	s := newMutateScheduler(100)
	s.Admit("/a/f", "", kindMutateFile)
	// State is registered.
	s.mu.Lock()
	if s.activeFilePaths["/a/f"] != 1 {
		s.mu.Unlock()
		t.Fatalf("activeFilePaths[/a/f] = %d, want 1", s.activeFilePaths["/a/f"])
	}
	s.mu.Unlock()

	// Start a blocked admit on the same path; it must not complete until Done.
	admitted := make(chan struct{})
	go func() {
		s.Admit("/a/f", "", kindMutateFile)
		close(admitted)
	}()
	select {
	case <-admitted:
		t.Fatal("second Admit should block while first is active")
	case <-time.After(50 * time.Millisecond):
	}

	s.Done("/a/f", "", kindMutateFile)

	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("second Admit did not unblock after Done")
	}

	// Clean up.
	s.Done("/a/f", "", kindMutateFile)
}

// TestAdmitParallelDistinctPaths: different paths do not block each other.
func TestAdmitParallelDistinctPaths(t *testing.T) {
	s := newMutateScheduler(100)
	done := make(chan struct{}, 3)
	for i, p := range []util.FullPath{"/a", "/b", "/c"} {
		go func(p util.FullPath, i int) {
			s.Admit(p, "", kindMutateFile)
			done <- struct{}{}
		}(p, i)
	}
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("distinct-path Admits blocked each other")
		}
	}
	for _, p := range []util.FullPath{"/a", "/b", "/c"} {
		s.Done(p, "", kindMutateFile)
	}
}

// TestAdmitConcurrencyCap: with concurrencyLimit=2 and three Admits, the
// third must block until one of the first two calls Done.
func TestAdmitConcurrencyCap(t *testing.T) {
	s := newMutateScheduler(2)
	s.Admit("/a", "", kindMutateFile)
	s.Admit("/b", "", kindMutateFile)

	admittedThird := make(chan struct{})
	go func() {
		s.Admit("/c", "", kindMutateFile)
		close(admittedThird)
	}()
	select {
	case <-admittedThird:
		t.Fatal("third Admit should block when cap is reached")
	case <-time.After(50 * time.Millisecond):
	}

	s.Done("/a", "", kindMutateFile)
	select {
	case <-admittedThird:
	case <-time.After(time.Second):
		t.Fatal("third Admit did not unblock after Done")
	}

	s.Done("/b", "", kindMutateFile)
	s.Done("/c", "", kindMutateFile)
}

// TestAdmitRenameTwoPathConflict: a rename holds both src and dst. A later
// single-path admit on either must block until Done clears both.
func TestAdmitRenameTwoPathConflict(t *testing.T) {
	s := newMutateScheduler(100)
	s.Admit("/src/a", "/dst/b", kindMutateBarrierDir)

	for _, blocked := range []util.FullPath{"/src/a", "/dst/b"} {
		admitted := make(chan struct{})
		go func(p util.FullPath) {
			s.Admit(p, "", kindMutateFile)
			close(admitted)
		}(blocked)
		select {
		case <-admitted:
			t.Fatalf("Admit at %q should block while rename holds both paths", blocked)
		case <-time.After(20 * time.Millisecond):
		}
	}

	s.Done("/src/a", "/dst/b", kindMutateBarrierDir)

	// Two waiters now become admissible; drain them.
	deadline := time.After(time.Second)
	drained := 0
	for drained < 2 {
		select {
		case <-deadline:
			t.Fatalf("only %d waiters unblocked after Done", drained)
		default:
			// Attempt to Done both waiters; they may still be racing to Admit.
			time.Sleep(10 * time.Millisecond)
			s.mu.Lock()
			active := s.totalActive
			s.mu.Unlock()
			if active == 2 {
				s.Done("/src/a", "", kindMutateFile)
				s.Done("/dst/b", "", kindMutateFile)
				drained = 2
			}
		}
	}
}

// TestAdmitSamePathFIFO verifies arrival order is preserved for same-path
// admits. Regression test for the Cond.Broadcast race where later admits
// could be woken and registered before earlier ones.
func TestAdmitSamePathFIFO(t *testing.T) {
	s := newMutateScheduler(100)

	// A barrier on /a holds the whole path while we enqueue N waiters.
	s.Admit("/a", "", kindMutateBarrierDir)

	const N = 20
	order := make(chan int, N)
	started := make(chan struct{}, N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			started <- struct{}{}
			s.Admit("/a", "", kindMutateFile)
			order <- i
			s.Done("/a", "", kindMutateFile)
		}()
		// Wait until this goroutine has observably scheduled its Admit call
		// before spawning the next, so arrival order is deterministic.
		<-started
		// Small yield so the goroutine's Admit actually lands before the next
		// one's; under -race this otherwise sometimes interleaves.
		time.Sleep(time.Millisecond)
	}

	// Still held — nothing admitted yet.
	select {
	case got := <-order:
		t.Fatalf("unexpected early admit %d while /a is held", got)
	case <-time.After(20 * time.Millisecond):
	}

	s.Done("/a", "", kindMutateBarrierDir)

	for i := 0; i < N; i++ {
		select {
		case got := <-order:
			if got != i {
				t.Fatalf("admit order[%d] = %d, want %d (FIFO violated)", i, got, i)
			}
		case <-time.After(time.Second):
			t.Fatalf("only %d/%d admits completed", i, N)
		}
	}
}

// TestAdmitSamePathNonBarrierSerializes verifies that two non-barrier dir
// updates at the same path no longer overlap (filer.sync's last-writer-wins
// optimization is intentionally dropped for streamed mutations, which carry
// client-submitted operations whose order matters).
func TestAdmitSamePathNonBarrierSerializes(t *testing.T) {
	s := newMutateScheduler(100)
	s.Admit("/a", "", kindMutateNonBarrierDir)

	second := make(chan struct{})
	go func() {
		s.Admit("/a", "", kindMutateNonBarrierDir)
		close(second)
	}()
	select {
	case <-second:
		t.Fatal("second non-barrier dir update should not run concurrently with first at same path")
	case <-time.After(30 * time.Millisecond):
	}
	s.Done("/a", "", kindMutateNonBarrierDir)
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("second non-barrier admit did not unblock after first Done")
	}
	s.Done("/a", "", kindMutateNonBarrierDir)
}

// TestAdmitPressureFromManyWaiters: 100 goroutines all want /a; exactly one at
// a time is active; all 100 eventually complete. This is both a smoke test for
// the broadcast/signal wake-up and a guard against lost wake-ups.
func TestAdmitPressureFromManyWaiters(t *testing.T) {
	s := newMutateScheduler(10) // cap higher than peak needed — path conflict should be the bottleneck
	const N = 100

	var activeAtOnce atomic.Int32
	var peak atomic.Int32
	done := make(chan struct{}, N)

	for i := 0; i < N; i++ {
		go func() {
			s.Admit("/a", "", kindMutateFile)
			a := activeAtOnce.Add(1)
			for {
				p := peak.Load()
				if a <= p || peak.CompareAndSwap(p, a) {
					break
				}
			}
			time.Sleep(time.Millisecond) // keep admission held so overlap is observable
			activeAtOnce.Add(-1)
			s.Done("/a", "", kindMutateFile)
			done <- struct{}{}
		}()
	}

	for i := 0; i < N; i++ {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d/%d admits completed — possible lost wake-up", i, N)
		}
	}
	if peak.Load() != 1 {
		t.Errorf("peak concurrent same-path admits = %d, want 1", peak.Load())
	}
}
