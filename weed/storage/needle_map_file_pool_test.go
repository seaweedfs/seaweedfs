package storage

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// openIndexFilesUnder counts the process's descriptors on .idx/.sdx files under
// dir, reading /proc/self/fd where it exists and falling back to lsof. Returns
// false when neither is available, so the caller can skip.
func openIndexFilesUnder(t *testing.T, dir string) (int, bool) {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		resolved = dir
	}
	prefix := resolved + string(os.PathSeparator)

	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		count := 0
		for _, e := range entries {
			target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
			if err != nil {
				continue // raced with a close
			}
			if isIndexFileUnder(target, prefix) {
				count++
			}
		}
		return count, true
	}

	out, err := exec.Command("lsof", "-p", strconv.Itoa(os.Getpid()), "-F", "n").Output()
	if err != nil {
		return 0, false
	}
	count := 0
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "n") && isIndexFileUnder(line[1:], prefix) {
			count++
		}
	}
	return count, true
}

func isIndexFileUnder(target, prefix string) bool {
	return strings.HasPrefix(target, prefix) &&
		(strings.HasSuffix(target, ".idx") || strings.HasSuffix(target, ".sdx"))
}

// TestSortedFileNeedleMap_HoldsNoDescriptors is the regression guard for
// issue #10937: a volume server with hundreds of thousands of read-only or
// cloud-tiered volumes ran out of descriptors because every one of them pinned
// its .idx and .sdx for the life of the process. An idle read-only volume must
// hold neither.
func TestSortedFileNeedleMap_HoldsNoDescriptors(t *testing.T) {
	if _, ok := openIndexFilesUnder(t, t.TempDir()); !ok {
		t.Skip("cannot enumerate open descriptors on this platform")
	}

	t.Run("readonly", func(t *testing.T) {
		dir := t.TempDir()
		v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
		if err != nil {
			t.Fatalf("new volume: %v", err)
		}
		for i := 1; i <= 8; i++ {
			if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false, false); err != nil {
				t.Fatalf("write needle %d: %v", i, err)
			}
		}
		v.PersistReadOnly(true, true)
		v.Close()

		v, err = NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
		if err != nil {
			t.Fatalf("reload volume: %v", err)
		}
		defer v.Close()
		nm, isSorted := v.nm.(*SortedFileNeedleMap)
		if !isSorted {
			t.Fatalf("read-only volume should load a SortedFileNeedleMap, got %T", v.nm)
		}
		assertNoIndexFds(t, dir, "after load")

		// A lookup borrows a handle and hands it straight back; only the pool's
		// bounded cache keeps it, and dropping that leaves nothing behind.
		if _, found := nm.Get(Uint64ToNeedleId(3)); !found {
			t.Fatal("needle 3 not found after reload")
		}
		pooledIndexFiles.discard(nm.dbFileName)
		assertNoIndexFds(t, dir, "after a lookup")
	})

	t.Run("remote", func(t *testing.T) {
		b := newLocalDirBackend(t)
		registerTestBackend(t, b)
		dir := t.TempDir()
		const vid = needle.VolumeId(9)
		tierUpVolume(t, dir, vid, b)

		v := reloadVolume(t, dir, vid)
		defer v.Close()
		if !v.HasRemoteFile() {
			t.Fatal("reloaded volume is not tiered to remote")
		}
		assertNoIndexFds(t, dir, "after load")
	})
}

func assertNoIndexFds(t *testing.T, dir, when string) {
	t.Helper()
	if got, _ := openIndexFilesUnder(t, dir); got != 0 {
		t.Fatalf("%s the volume holds %d .idx/.sdx descriptors under %s, want 0", when, got, dir)
	}
}

// TestIndexFilePool_EvictWhileBorrowed locks in that an eviction does not pull
// a descriptor out from under an in-flight reader.
func TestIndexFilePool_EvictWhileBorrowed(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first")
	second := filepath.Join(dir, "second")
	for _, name := range []string{first, second} {
		if err := os.WriteFile(name, []byte(filepath.Base(name)), 0644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	pool := newIndexFilePool(1)
	borrowed, err := pool.borrow(first, false)
	if err != nil {
		t.Fatalf("borrow first: %v", err)
	}

	// Pushes the single pool slot over, evicting the entry still in use.
	other, err := pool.borrow(second, false)
	if err != nil {
		t.Fatalf("borrow second: %v", err)
	}
	pool.release(other)

	buf := make([]byte, len("first"))
	if _, err := borrowed.file.ReadAt(buf, 0); err != nil {
		t.Fatalf("read through evicted-but-borrowed handle: %v", err)
	}
	if string(buf) != "first" {
		t.Fatalf("read %q, want %q", buf, "first")
	}

	pool.release(borrowed)
	if borrowed.file != nil {
		t.Fatal("evicted handle was not closed once the last borrower released it")
	}
}

// TestIndexFilePool_DiscardClosesIdle covers the path Close/Destroy rely on:
// once a volume is unmounted nothing may keep serving reads from its old inode.
func TestIndexFilePool_DiscardClosesIdle(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "idx")
	if err := os.WriteFile(name, []byte("x"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	pool := newIndexFilePool(4)
	f, err := pool.borrow(name, false)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	pool.release(f)

	pool.discard(name)
	if f.file != nil {
		t.Fatal("discard left the pooled handle open")
	}
	if pool.lru.Len() != 0 {
		t.Fatalf("discard left %d entries in the pool", pool.lru.Len())
	}
}
