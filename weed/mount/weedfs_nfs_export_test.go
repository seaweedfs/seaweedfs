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
		got, _ := kv.get(context.Background(), exportedHandleKey(inode))
		if util.FullPath(got) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("record of inode %d = %q, want %q", inode, got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func newExportedHandlesForTest(kv *fakeKv, delay time.Duration) *exportedHandles {
	h := newExportedHandles(kv.put, kv.get)
	h.delay = delay
	return h
}

func TestExportedHandlesRecordFollowsOpenRenameAndClose(t *testing.T) {
	kv := &fakeKv{values: map[string][]byte{}}
	h := newExportedHandlesForTest(kv, 20*time.Millisecond)

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

func TestResolveDotNameFromRecordOfEarlierMount(t *testing.T) {
	kv := &fakeKv{values: map[string][]byte{}}
	file := util.FullPath("/buckets/a/b/c.txt")
	inode := file.AsInode(1)
	kv.values[string(exportedHandleKey(inode))] = []byte(file)
	kv.values[string(exportedHandleKey(inode+1))] = []byte("/elsewhere/c.txt")

	wfs := newDotLookupTestWFS("/buckets", true)
	wfs.exportedHandles = newExportedHandlesForTest(kv, time.Minute)

	path, gotInode, status := wfs.resolveDotName(context.Background(), inode, ".")
	if status != fuse.OK || path != file || gotInode != inode {
		t.Fatalf("resolveDotName(.) = %q, %d, %v; want %q, %d, OK", path, gotInode, status, file, inode)
	}
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
