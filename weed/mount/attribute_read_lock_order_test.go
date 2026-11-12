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
