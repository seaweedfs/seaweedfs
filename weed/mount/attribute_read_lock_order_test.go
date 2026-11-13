//go:build !freebsd

package mount

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// A subprocess bounds the deliberately deadlocked negative without leaving
// blocked goroutines/cache cleanup behind in the rest of the package suite.
func TestAttributeSetAttrReadLockOrder(t *testing.T) {
	if os.Getenv("MOUNT_ATTR_READ_LOCK_CHILD") == "1" {
		attributeReadLockScenario(t)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAttributeSetAttrReadLockOrder$", "-test.count=1", "-test.timeout=12s")
	cmd.Env = append(os.Environ(), "MOUNT_ATTR_READ_LOCK_CHILD=1")
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("lock-order control failed to finish its bounded child: %v\n%s", ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("concurrent SetAttr/Read did not complete: %v\n%s", err, output)
	}
}

func attributeReadLockScenario(t *testing.T) {
	w := newInvalidateTestWFS(t)
	w.atimeMap = make(map[uint64]time.Time)
	data := []byte("acknowledged payload")
	current := attributeAppendEntry(0)
	current.Content = data
	current.Attributes.FileSize = uint64(len(data))
	fake := &attributeCASFiler{entry: proto.Clone(current).(*filer_pb.Entry)}
	committed, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	t.Cleanup(func() { resumeOnce.Do(func() { close(resume) }) })
	fake.afterCommit = func() { close(committed); <-resume }
	startFakeFiler(t, w, fake)
	path := util.FullPath("/dir/file")
	w.inodeToPath.Lookup(path, 1, false, false, 42, true)
	local := proto.Clone(current).(*filer_pb.Entry)
	w.mapPbIdFromFilerToLocal(local)
	fh := newFileHandle(w, 1, 42, local)
	fh.RememberPath(path)
	w.fhMap.inode2fh[42], w.fhMap.fh2inode[1] = fh, 42
	t.Cleanup(fh.dirtyPages.Destroy)
	attrDone := make(chan fuse.Status, 1)
	go func() {
		in := &fuse.SetAttrIn{}
		in.NodeId = 42
		in.Valid = fuse.FATTR_MODE
		in.Mode = 0600
		attrDone <- w.SetAttr(nil, in, &fuse.AttrOut{})
	}()
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("SetAttr did not reach owner commit hook")
	}
	// Model the background WRITE which makes retention necessary after the owner
	// has committed. No unprotected flag write overlaps a running handler.
	active := w.fhLockTable.AcquireLock("test background write", fh.fh, util.ExclusiveLock)
	fh.dirtyMetadata = true
	w.fhLockTable.ReleaseLock(fh.fh, active)
	// Hold a reader on entryLock until retention is demonstrably waiting for its
	// writer lock. The old path has no active lock; the fix already owns it.
	fh.entryLock.RLock()
	resumeOnce.Do(func() { close(resume) })
	waitAttributeReadFrame(t, "(*WFS).retainAcknowledgedAttributes", "sync.(*RWMutex).Lock")
	readDone := make(chan fuse.Status, 1)
	go func() {
		buf := make([]byte, len(data))
		result, status := w.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: 1, Size: uint32(len(data))}, buf)
		if status == fuse.OK {
			got, code := result.Bytes(buf)
			if code != fuse.OK || string(got) != string(data) {
				status = fuse.EIO
			}
			result.Done()
		}
		readDone <- status
	}()
	// Prove the real Read has arrived before opening the gate: old code waits
	// on entryLock while holding the table entry, fixed code waits on the table.
	waitAttributeReadFrame(t, "(*WFS).Read", "(*FileHandle).readFromChunksWithContext", "AcquireLock")
	fh.entryLock.RUnlock()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for _, done := range []chan fuse.Status{attrDone, readDone} {
		select {
		case status := <-done:
			if status != fuse.OK {
				t.Fatalf("handler returned %v", status)
			}
		case <-timer.C:
			fmt.Fprintln(os.Stderr, "LOCK_ORDER_CYCLE: owner-committed SetAttr retention and same-handle Read cannot finish")
			os.Exit(23)
		}
	}
	fh.entry.RLock()
	mode := fh.entry.Attributes.FileMode
	fh.entry.RUnlock()
	if mode&0777 != 0600 {
		t.Fatalf("acknowledged chmod was not retained: %o", mode)
	}
}

// The attribute snapshot must queue for the handle lock before it takes
// entryLock. Otherwise a Read holding the shared lock waits on entryLock, the
// snapshot waits behind a queued exclusive (an invalidation or Write), and
// that waits on the Read.
func TestAttributeSnapshotReadLockOrder(t *testing.T) {
	w := newInvalidateTestWFS(t)
	entry := attributeAppendEntry(0)
	fh := newFileHandle(w, 1, 42, entry)
	t.Cleanup(fh.dirtyPages.Destroy)

	read := w.fhLockTable.AcquireLock("test read", fh.fh, util.SharedLock)
	exclusiveDone := make(chan struct{})
	go func() {
		lock := w.fhLockTable.AcquireLock("test invalidate", fh.fh, util.ExclusiveLock)
		w.fhLockTable.ReleaseLock(fh.fh, lock)
		close(exclusiveDone)
	}()
	waitAttributeReadFrame(t, "TestAttributeSnapshotReadLockOrder.func1", "sync.(*Cond).Wait")
	snapshotDone := make(chan bool, 1)
	go func() {
		_, ok := w.attributeEntrySnapshot(fh, entry)
		snapshotDone <- ok
	}()
	waitAttributeReadFrame(t, "(*WFS).attributeEntrySnapshot", "sync.(*Cond).Wait")

	readerCanProceed := fh.entryLock.TryRLock()
	if readerCanProceed {
		fh.entryLock.RUnlock()
	}
	w.fhLockTable.ReleaseLock(fh.fh, read)
	<-exclusiveDone
	if ok := <-snapshotDone; !ok {
		t.Fatal("clean handle snapshot was refused")
	}
	if !readerCanProceed {
		t.Fatal("snapshot holds entryLock while queued for the handle lock behind an exclusive waiter")
	}
}

// Stack frames are arrival/blocked witnesses, not an elapsed-time performance
// assertion. Channel/lock gates define the order; timeout only refuses a broken
// test arrangement. No sleep is used to make either handler win a race.
func waitAttributeReadFrame(t *testing.T, caller string, blocked ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			if !strings.Contains(stack, caller) {
				continue
			}
			for _, frame := range blocked {
				if strings.Contains(stack, frame) {
					return
				}
			}
		}
		runtime.Gosched()
	}
	t.Fatalf("did not observe %s blocked at %v", caller, blocked)
}

// FATTR_SIZE bypasses the attribute publication fence but must not reverse
// the handle locks while an acknowledged chmod is retaining its local delta.
func TestAttributeTruncateRetainLockOrder(t *testing.T) {
	if os.Getenv("MOUNT_ATTR_TRUNCATE_LOCK_CHILD") == "1" {
		attributeTruncateLockScenario(t)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAttributeTruncateRetainLockOrder$", "-test.count=1", "-test.timeout=12s")
	cmd.Env = append(os.Environ(), "MOUNT_ATTR_TRUNCATE_LOCK_CHILD=1")
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("truncate lock control exceeded bounded child: %v\n%s", ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("concurrent truncate/acked chmod did not finish: %v\n%s", err, output)
	}
}
func attributeTruncateLockScenario(t *testing.T) {
	w := newInvalidateTestWFS(t)
	w.atimeMap = make(map[uint64]time.Time)
	current := attributeAppendEntry(3)
	fake := &attributeCASFiler{entry: proto.Clone(current).(*filer_pb.Entry)}
	committed, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(resume) }) })
	fake.afterCommit = func() { close(committed); <-resume }
	startFakeFiler(t, w, fake)
	path := util.FullPath("/dir/file")
	w.inodeToPath.Lookup(path, 1, false, false, 42, true)
	local := proto.Clone(current).(*filer_pb.Entry)
	w.mapPbIdFromFilerToLocal(local)
	fh := newFileHandle(w, 1, 42, local)
	fh.RememberPath(path)
	w.fhMap.inode2fh[42], w.fhMap.fh2inode[1] = fh, 42
	t.Cleanup(fh.dirtyPages.Destroy)
	chmodDone := make(chan fuse.Status, 1)
	go func() {
		in := &fuse.SetAttrIn{}
		in.NodeId = 42
		in.Valid = fuse.FATTR_MODE
		in.Mode = 0600
		chmodDone <- w.SetAttr(nil, in, &fuse.AttrOut{})
	}()
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("chmod did not reach owner commit hook")
	}
	active := w.fhLockTable.AcquireLock("test pending content", fh.fh, util.ExclusiveLock)
	fh.dirtyMetadata = true
	w.fhLockTable.ReleaseLock(fh.fh, active)
	fh.entryLock.RLock()
	truncateDone := make(chan fuse.Status, 1)
	go func() {
		in := &fuse.SetAttrIn{}
		in.NodeId = 42
		in.Valid = fuse.FATTR_SIZE
		in.Size = 256
		truncateDone <- w.SetAttr(nil, in, &fuse.AttrOut{})
	}()
	// Truncate queued its entryLock writer before the chmod response is released.
	waitAttributeLockPair(t, [][2]string{{"(*WFS).attributeEntrySnapshot", "sync.(*RWMutex).Lock"}})
	once.Do(func() { close(resume) })
	// Rejected head: retention owns active-exclusive and waits for entryLock.
	// Corrected head: truncate owns active-shared and installation waits for it.
	waitAttributeLockPair(t, [][2]string{{"(*WFS).retainAcknowledgedAttributes", "sync.(*RWMutex).Lock"}, {"(*FileHandle).installAckedEntry", "AcquireLock"}})
	fh.entryLock.RUnlock()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for _, done := range []chan fuse.Status{chmodDone, truncateDone} {
		select {
		case status := <-done:
			if status != fuse.OK {
				t.Fatalf("handler returned %v", status)
			}
		case <-timer.C:
			fmt.Fprintln(os.Stderr, "TRUNCATE_RETENTION_CYCLE: FATTR_SIZE snapshot and acknowledged chmod retention cannot finish")
			os.Exit(24)
		}
	}
	fh.entry.RLock()
	defer fh.entry.RUnlock()
	e := fh.entry.Entry
	if e.Attributes.FileMode&0777 != 0600 || e.Attributes.FileSize != 256 || len(e.Chunks) != 1 || !proto.Equal(e.Chunks[0], current.Chunks[0]) {
		t.Fatalf("lost acknowledged chmod or pending truncate: %v", e)
	}
}
func waitAttributeLockPair(t *testing.T, pairs [][2]string) {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			header, _, _ := strings.Cut(stack, "\n")
			if !strings.Contains(header, "[sync.") && !strings.Contains(header, "[semacquire") {
				continue
			}
			for _, pair := range pairs {
				if strings.Contains(stack, pair[0]) && strings.Contains(stack, pair[1]) {
					return
				}
			}
		}
		runtime.Gosched()
	}
	t.Fatalf("did not observe actual blocked lock pair %v", pairs)
}
