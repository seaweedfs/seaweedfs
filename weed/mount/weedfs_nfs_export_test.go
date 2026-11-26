package mount

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

// waitFor waits until the inode's record holds only a registration of want,
// or none when want is empty.
func (kv *fakeKv) waitFor(t *testing.T, inode uint64, want util.FullPath) {
	t.Helper()
	kv.waitUntil(t, inode, want, func(got []exportedHandleRegistration) bool {
		return want == "" && len(got) == 0 || len(got) == 1 && got[0].path == want
	})
}

func (kv *fakeKv) waitForRegistrations(t *testing.T, inode uint64, want ...exportedHandleRegistration) {
	t.Helper()
	kv.waitUntil(t, inode, want, func(got []exportedHandleRegistration) bool {
		return slices.Equal(got, want)
	})
}

func (kv *fakeKv) waitUntil(t *testing.T, inode uint64, want any, done func([]exportedHandleRegistration) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		value, _ := kv.get(context.Background(), exportedHandleKey(inode))
		got := parseExportedHandle(value)
		if done(got) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("registrations of inode %d = %v, want %v", inode, got, want)
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

func TestExportedHandlesRemovalKeepsAnotherMountsRegistration(t *testing.T) {
	kv := &fakeKv{values: map[string][]byte{}}
	first := newExportedHandlesForTest(kv, 1, 10*time.Millisecond)
	second := newExportedHandlesForTest(kv, 2, 10*time.Millisecond)

	first.AcquireHandle(7, "/buckets/a.txt")
	kv.waitFor(t, 7, "/buckets/a.txt")
	second.AcquireHandle(7, "/buckets/a.txt")
	kv.waitForRegistrations(t, 7, exportedHandleRegistration{1, "/buckets/a.txt"}, exportedHandleRegistration{2, "/buckets/a.txt"})
	second.ReleaseHandle(7)
	kv.waitFor(t, 7, "/buckets/a.txt")
	first.ReleaseHandle(7)
	kv.waitFor(t, 7, "")

	first.AcquireHandle(8, "/buckets/b.txt")
	kv.waitFor(t, 8, "/buckets/b.txt")
	second.AcquireHandle(8, "/buckets/b.txt")
	kv.waitForRegistrations(t, 8, exportedHandleRegistration{1, "/buckets/b.txt"}, exportedHandleRegistration{2, "/buckets/b.txt"})
	first.ReleaseHandle(8)
	kv.waitForRegistrations(t, 8, exportedHandleRegistration{2, "/buckets/b.txt"})
}

func TestExportedHandlesRecordKeepsNewestRegistrations(t *testing.T) {
	kv := &fakeKv{values: map[string][]byte{}}
	var want []exportedHandleRegistration
	for owner := uint32(1); owner <= maxExportedHandleRegistrations+2; owner++ {
		path := util.FullPath(fmt.Sprintf("/buckets/%d.txt", owner))
		newExportedHandlesForTest(kv, owner, time.Minute).AcquireHandle(7, path)
		want = append(want, exportedHandleRegistration{owner, path})
		if len(want) > maxExportedHandleRegistrations {
			want = want[1:]
		}
		kv.waitForRegistrations(t, 7, want...)
	}

	wfs := newDotLookupTestWFS("/buckets", true)
	wfs.exportedHandles = newExportedHandlesForTest(kv, 99, time.Minute)
	if path, status := wfs.exportedHandles.GetPath(context.Background(), 7); status != fuse.OK || path != want[len(want)-1].path {
		t.Fatalf("GetPath = %q, %v; want the newest registration %q", path, status, want[len(want)-1].path)
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
