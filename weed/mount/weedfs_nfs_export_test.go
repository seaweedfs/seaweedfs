package mount

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

type fakeKv struct {
	sync.Mutex
	values map[string][]byte
	err    error
}

func (kv *fakeKv) put(key, value []byte) error {
	kv.Lock()
	defer kv.Unlock()
	if len(value) == 0 {
		delete(kv.values, string(key))
	} else {
		kv.values[string(key)] = append([]byte(nil), value...)
	}
	return nil
}

func (kv *fakeKv) get(_ context.Context, key []byte) ([]byte, error) {
	kv.Lock()
	defer kv.Unlock()
	return kv.values[string(key)], kv.err
}

func (kv *fakeKv) waitFor(t *testing.T, inode uint64, want util.FullPath) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		value, _ := kv.get(context.Background(), exportedHandleKey(inode))
		_, got, _ := parseExportedHandle(value)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("record of inode %d = %q, want %q", inode, got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func newExportedHandlesForTest(kv *fakeKv, owner uint32, delay time.Duration) *exportedHandles {
	h := newExportedHandles(owner, kv.put, kv.get)
	h.delay = delay
	return h
}

func TestExportedHandlesRecordFollowsOpenRenameAndClose(t *testing.T) {
	kv := &fakeKv{values: map[string][]byte{}}
	h := newExportedHandlesForTest(kv, 1, 20*time.Millisecond)

	h.RememberPath(7, "/buckets/a.txt")
	h.AcquireHandle(8, "/buckets/c.txt")
	kv.waitFor(t, 8, "/buckets/c.txt")
	if got, _ := kv.get(context.Background(), exportedHandleKey(7)); len(got) != 0 {
		t.Fatalf("a path change recorded inode 7, which was never opened: %q", got)
	}

	h.AcquireHandle(7, "/buckets/a.txt")
	kv.waitFor(t, 7, "/buckets/a.txt")
	h.RememberPath(7, "/buckets/b.txt")
	kv.waitFor(t, 7, "/buckets/b.txt")

	h.ReleaseHandle(7)
	h.AcquireHandle(7, "/buckets/b.txt")
	time.Sleep(60 * time.Millisecond)
	kv.waitFor(t, 7, "/buckets/b.txt")

	h.ReleaseHandle(7)
	h.RememberPath(7, "/buckets/b.txt")
	kv.waitFor(t, 7, "")
	h.RememberPath(7, "/buckets/b.txt")
	time.Sleep(20 * time.Millisecond)
	kv.waitFor(t, 7, "")
}

func TestExportedHandlesReopenBeforeCloseKeepsRecord(t *testing.T) {
	kv := &fakeKv{values: map[string][]byte{}}
	h := newExportedHandlesForTest(kv, 1, 10*time.Millisecond)

	h.AcquireHandle(7, "/buckets/a.txt")
	h.AcquireHandle(7, "/buckets/a.txt")
	h.ReleaseHandle(7)
	time.Sleep(40 * time.Millisecond)
	kv.waitFor(t, 7, "/buckets/a.txt")
	h.ReleaseHandle(7)
	kv.waitFor(t, 7, "")
}

func TestExportedHandlesRemovalKeepsAnotherMountsRecord(t *testing.T) {
	kv := &fakeKv{values: map[string][]byte{}}
	first := newExportedHandlesForTest(kv, 1, 10*time.Millisecond)
	second := newExportedHandlesForTest(kv, 2, time.Minute)

	first.AcquireHandle(7, "/buckets/a.txt")
	kv.waitFor(t, 7, "/buckets/a.txt")
	first.ReleaseHandle(7)
	second.AcquireHandle(7, "/buckets/a.txt")
	time.Sleep(60 * time.Millisecond)
	value, _ := kv.get(context.Background(), exportedHandleKey(7))
	if owner, path, ok := parseExportedHandle(value); !ok || owner != 2 || path != "/buckets/a.txt" {
		t.Fatalf("record = %d %q %v, want the second mount's", owner, path, ok)
	}
}

func TestResolveDotNameFromRecordOfEarlierMount(t *testing.T) {
	kv := &fakeKv{values: map[string][]byte{}}
	file := util.FullPath("/buckets/a/b/c.txt")
	inode := file.AsInode(1)
	earlier := newExportedHandlesForTest(kv, 9, time.Minute)
	earlier.AcquireHandle(inode, file)
	earlier.AcquireHandle(inode+1, "/elsewhere/c.txt")
	kv.waitFor(t, inode, file)
	kv.waitFor(t, inode+1, "/elsewhere/c.txt")

	wfs := newDotLookupTestWFS("/buckets", true)
	wfs.exportedHandles = newExportedHandlesForTest(kv, 1, time.Minute)

	path, gotInode, status := wfs.resolveDotName(context.Background(), inode, ".")
	if status != fuse.OK || path != file || gotInode != inode {
		t.Fatalf("resolveDotName(.) = %q, %d, %v; want %q, %d, OK", path, gotInode, status, file, inode)
	}
	wfs.inodeToPath.RememberForgotten(inode, file)
	if !wfs.inodeToPath.LookupHandle(file, inode, false) {
		t.Fatalf("LookupHandle refused the recorded path")
	}

	if _, _, status := wfs.resolveDotName(context.Background(), inode+1, "."); status != fuse.Status(syscall.ESTALE) {
		t.Fatalf("path outside the mount: status %v, want ESTALE", status)
	}
	if _, _, status := wfs.resolveDotName(context.Background(), inode+2, "."); status != fuse.Status(syscall.ESTALE) {
		t.Fatalf("unrecorded inode: status %v, want ESTALE", status)
	}
	kv.err = errors.New("store unavailable")
	if _, _, status := wfs.resolveDotName(context.Background(), inode+2, "."); status != fuse.EIO {
		t.Fatalf("store error: status %v, want EIO", status)
	}
}
