// Package winfsp exercises a live SeaweedFS mount on Windows.
//
// It runs against whatever is mounted at -mountpoint, so it needs a real
// WinFsp mount and is skipped otherwise. The CI job drives it; run it by hand
// with: go test ./test/winfsp -mountpoint=S:\
package winfsp

import (
	"bytes"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

var mountPoint = flag.String("mountpoint", "", "a mounted SeaweedFS drive, e.g. S:\\")

// entryCount is the directory width the test builds. The reported case that
// motivated Windows support is 200k files in one folder; CI uses less so the
// job stays inside its budget, but the read path is the same.
var entryCount = flag.Int("entries", 5000, "how many files to put in one directory")

func testRoot(t *testing.T) string {
	t.Helper()
	if *mountPoint == "" {
		t.Skip("no -mountpoint given; this test needs a live WinFsp mount")
	}
	dir := filepath.Join(*mountPoint, "winfsp-test-"+t.Name())
	if err := os.RemoveAll(dir); err != nil && !os.IsNotExist(err) {
		t.Fatalf("clean %s: %v", dir, err)
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestWriteReadRoundTrip(t *testing.T) {
	dir := testRoot(t)
	for _, size := range []int{0, 1, 4095, 4096, 1 << 20, 5 << 20} {
		t.Run(fmt.Sprintf("%dbytes", size), func(t *testing.T) {
			want := make([]byte, size)
			if _, err := rand.Read(want); err != nil {
				t.Fatalf("rand: %v", err)
			}
			path := filepath.Join(dir, fmt.Sprintf("file-%d", size))
			if err := os.WriteFile(path, want, 0644); err != nil {
				t.Fatalf("write: %v", err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("content differs: got %d bytes, want %d", len(got), len(want))
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
			if info.Size() != int64(size) {
				t.Fatalf("stat size = %d, want %d", info.Size(), size)
			}
		})
	}
}

func TestSeekWriteAndOverwrite(t *testing.T) {
	dir := testRoot(t)
	path := filepath.Join(dir, "sparse")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.WriteAt([]byte("tail"), 4096); err != nil {
		t.Fatalf("write at offset: %v", err)
	}
	if _, err := f.WriteAt([]byte("head"), 0); err != nil {
		t.Fatalf("write at start: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 4100 {
		t.Fatalf("size = %d, want 4100", len(got))
	}
	if string(got[:4]) != "head" || string(got[4096:]) != "tail" {
		t.Fatalf("content at the edges is wrong: %q ... %q", got[:4], got[4096:])
	}
}

func TestRenameAndDelete(t *testing.T) {
	dir := testRoot(t)
	src := filepath.Join(dir, "before")
	dst := filepath.Join(dir, "after")

	if err := os.WriteFile(src, []byte("payload"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Rename(src, dst); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("old name still resolves: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read renamed: %v", err)
	}
	if string(got) != "payload" {
		t.Fatalf("content after rename = %q", got)
	}
	if err := os.Remove(dst); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("deleted file still resolves: %v", err)
	}
}

func TestDirectoryTree(t *testing.T) {
	dir := testRoot(t)
	nested := filepath.Join(dir, "a", "b", "c")
	if err := os.MkdirAll(nested, 0755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nested, "leaf"), []byte("x"), 0644); err != nil {
		t.Fatalf("write leaf: %v", err)
	}
	info, err := os.Stat(nested)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("nested path is not reported as a directory")
	}
	// A non-empty directory must not be removable.
	if err := os.Remove(filepath.Join(dir, "a", "b")); err == nil {
		t.Fatal("removed a non-empty directory")
	}
	if err := os.RemoveAll(filepath.Join(dir, "a")); err != nil {
		t.Fatalf("removeall: %v", err)
	}
}

// TestWideDirectory is the case from the report: one folder holding far more
// entries than Explorer or the Windows WebDAV client cope with.
func TestWideDirectory(t *testing.T) {
	dir := testRoot(t)
	want := make([]string, 0, *entryCount)
	for i := 0; i < *entryCount; i++ {
		name := fmt.Sprintf("img-%06d.dat", i)
		if err := os.WriteFile(filepath.Join(dir, name), []byte{byte(i)}, 0644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		want = append(want, name)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	got := make([]string, 0, len(entries))
	for _, e := range entries {
		got = append(got, e.Name())
	}
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("readdir returned %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d = %q, want %q", i, got[i], want[i])
		}
	}

	// Reading the directory twice must be stable; a paging bug shows up here.
	again, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("second readdir: %v", err)
	}
	if len(again) != len(entries) {
		t.Fatalf("second readdir returned %d entries, first returned %d", len(again), len(entries))
	}
}

func TestConcurrentWriters(t *testing.T) {
	dir := testRoot(t)
	const writers = 8
	const perWriter = 32

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				name := filepath.Join(dir, fmt.Sprintf("w%d-%d", w, i))
				body := []byte(strings.Repeat(fmt.Sprintf("%d", w), 128))
				if err := os.WriteFile(name, body, 0644); err != nil {
					errs <- fmt.Errorf("writer %d: %w", w, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("%v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != writers*perWriter {
		t.Fatalf("got %d files, want %d", len(entries), writers*perWriter)
	}
}

func TestStatfsReportsCapacity(t *testing.T) {
	if *mountPoint == "" {
		t.Skip("no -mountpoint given; this test needs a live WinFsp mount")
	}
	// The drive has to report a size, or Explorer refuses to show it.
	var free, total, totalFree uint64
	if err := getDiskFreeSpace(*mountPoint, &free, &total, &totalFree); err != nil {
		t.Fatalf("GetDiskFreeSpaceEx: %v", err)
	}
	if total == 0 {
		t.Fatal("drive reports zero total bytes")
	}
}
