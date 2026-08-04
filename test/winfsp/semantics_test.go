package winfsp

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTruncate(t *testing.T) {
	dir := testRoot(t)
	path := filepath.Join(dir, "resize")
	original := bytes.Repeat([]byte("abcd"), 4096) // 16 KiB

	if err := os.WriteFile(path, original, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Shrink.
	if err := os.Truncate(path, 100); err != nil {
		t.Fatalf("truncate down: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read after shrink: %v", err)
	}
	if len(got) != 100 {
		t.Fatalf("size after shrink = %d, want 100", len(got))
	}
	if !bytes.Equal(got, original[:100]) {
		t.Fatal("the surviving prefix does not match the original")
	}

	// Grow: the new space has to read back as zeros, not stale bytes.
	if err := os.Truncate(path, 8192); err != nil {
		t.Fatalf("truncate up: %v", err)
	}
	got, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read after grow: %v", err)
	}
	if len(got) != 8192 {
		t.Fatalf("size after grow = %d, want 8192", len(got))
	}
	if !bytes.Equal(got[100:], make([]byte, 8192-100)) {
		t.Fatal("extension is not zero-filled")
	}

	// Truncate to empty.
	if err := os.Truncate(path, 0); err != nil {
		t.Fatalf("truncate to zero: %v", err)
	}
	if info, err := os.Stat(path); err != nil || info.Size() != 0 {
		t.Fatalf("stat after zero truncate: size=%v err=%v", info, err)
	}
}

func TestAppend(t *testing.T) {
	dir := testRoot(t)
	path := filepath.Join(dir, "log")

	for i := 0; i < 5; i++ {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			t.Fatalf("open for append: %v", err)
		}
		if _, err := fmt.Fprintf(f, "line %d\n", i); err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := "line 0\nline 1\nline 2\nline 3\nline 4\n"
	if string(got) != want {
		t.Fatalf("appended content = %q, want %q", got, want)
	}
}

func TestReadPastEndOfFile(t *testing.T) {
	dir := testRoot(t)
	path := filepath.Join(dir, "short")
	if err := os.WriteFile(path, []byte("12345"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 16)
	n, err := f.ReadAt(buf, 100)
	if n != 0 {
		t.Fatalf("read %d bytes past EOF, want 0", n)
	}
	if err == nil {
		t.Fatal("reading past EOF returned no error, want io.EOF")
	}
}

func TestErrorPaths(t *testing.T) {
	dir := testRoot(t)

	t.Run("open missing file", func(t *testing.T) {
		_, err := os.Open(filepath.Join(dir, "does-not-exist"))
		if !os.IsNotExist(err) {
			t.Fatalf("got %v, want a not-exist error", err)
		}
	})

	t.Run("exclusive create over existing", func(t *testing.T) {
		path := filepath.Join(dir, "taken")
		if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		_, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if !os.IsExist(err) {
			t.Fatalf("got %v, want an already-exists error", err)
		}
	})

	t.Run("remove missing file", func(t *testing.T) {
		if err := os.Remove(filepath.Join(dir, "never-there")); !os.IsNotExist(err) {
			t.Fatalf("got %v, want a not-exist error", err)
		}
	})

	t.Run("mkdir under a file", func(t *testing.T) {
		path := filepath.Join(dir, "regular")
		if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := os.Mkdir(filepath.Join(path, "child"), 0755); err == nil {
			t.Fatal("created a directory underneath a regular file")
		}
	})

	t.Run("readdir of a missing directory", func(t *testing.T) {
		if _, err := os.ReadDir(filepath.Join(dir, "no-such-dir")); err == nil {
			t.Fatal("listed a directory that does not exist")
		}
	})
}

func TestRenameOverExisting(t *testing.T) {
	dir := testRoot(t)
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	if err := os.WriteFile(src, []byte("new"), 0644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if err := os.WriteFile(dst, []byte("old"), 0644); err != nil {
		t.Fatalf("write dst: %v", err)
	}
	if err := os.Rename(src, dst); err != nil {
		t.Fatalf("rename over existing: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("target holds %q, want the source content", got)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatal("source survived the rename")
	}
}

func TestRenameAcrossDirectories(t *testing.T) {
	dir := testRoot(t)
	from := filepath.Join(dir, "from")
	to := filepath.Join(dir, "to")
	for _, d := range []string{from, to} {
		if err := os.Mkdir(d, 0755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	src := filepath.Join(from, "file")
	dst := filepath.Join(to, "file")
	if err := os.WriteFile(src, []byte("moved"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Rename(src, dst); err != nil {
		t.Fatalf("rename across directories: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "moved" {
		t.Fatalf("content after move = %q", got)
	}

	// A directory should move too, children and all.
	nested := filepath.Join(from, "sub")
	if err := os.Mkdir(nested, 0755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nested, "child"), []byte("c"), 0644); err != nil {
		t.Fatalf("write child: %v", err)
	}
	if err := os.Rename(nested, filepath.Join(to, "sub")); err != nil {
		t.Fatalf("rename directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(to, "sub", "child")); err != nil {
		t.Fatalf("child did not come along: %v", err)
	}
}

// TestAwkwardNames covers what has to survive the UTF-16 boundary and the
// shapes Windows software tends to produce.
func TestAwkwardNames(t *testing.T) {
	dir := testRoot(t)
	names := []string{
		"café.txt",
		"日本語のファイル.dat",
		"emoji-🐟.bin",
		"with space.txt",
		"with.many.dots.txt",
		"UPPER and lower.TXT",
		"dash-and_underscore.txt",
		"'quoted'.txt",
		"(parens).txt",
		"#hash&amp.txt",
		strings.Repeat("x", 200) + ".txt",
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, name)
			want := []byte(name)
			if err := os.WriteFile(path, want, 0644); err != nil {
				t.Fatalf("write: %v", err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("content = %q, want %q", got, want)
			}
		})
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	found := make(map[string]bool, len(entries))
	for _, e := range entries {
		found[e.Name()] = true
	}
	for _, name := range names {
		if !found[name] {
			t.Errorf("%q did not come back from readdir", name)
		}
	}
}

// Windows enumerates a directory without "." and "..", and shows whatever the
// filesystem reports, so they must not reach it. os.ReadDir filters them, so
// this reads the handle the way Windows tooling does.
func TestNoDotEntriesInListing(t *testing.T) {
	dir := testRoot(t)
	for i := 0; i < 3; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d", i)), []byte("x"), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	f, err := os.Open(dir)
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		t.Fatalf("readdirnames: %v", err)
	}
	for _, name := range names {
		if name == "." || name == ".." {
			t.Errorf("listing includes %q", name)
		}
	}
	if len(names) != 3 {
		t.Fatalf("listing has %d entries (%v), want 3", len(names), names)
	}
}

func TestDeepDirectoryNesting(t *testing.T) {
	dir := testRoot(t)
	deep := dir
	for i := 0; i < 24; i++ {
		deep = filepath.Join(deep, fmt.Sprintf("level%02d", i))
	}
	if err := os.MkdirAll(deep, 0755); err != nil {
		t.Fatalf("mkdirall depth 24: %v", err)
	}
	leaf := filepath.Join(deep, "leaf.txt")
	if err := os.WriteFile(leaf, []byte("bottom"), 0644); err != nil {
		t.Fatalf("write leaf: %v", err)
	}
	got, err := os.ReadFile(leaf)
	if err != nil {
		t.Fatalf("read leaf: %v", err)
	}
	if string(got) != "bottom" {
		t.Fatalf("leaf content = %q", got)
	}
}

// TestConcurrentSameFile is the interleaving that matters: several handles on
// one file at once, rather than one writer each on separate files.
func TestConcurrentSameFile(t *testing.T) {
	dir := testRoot(t)
	path := filepath.Join(dir, "shared")

	const writers = 4
	const blockSize = 4096
	if err := os.WriteFile(path, make([]byte, writers*blockSize), 0644); err != nil {
		t.Fatalf("preallocate: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			f, err := os.OpenFile(path, os.O_WRONLY, 0644)
			if err != nil {
				errs <- fmt.Errorf("writer %d open: %w", w, err)
				return
			}
			defer f.Close()
			block := bytes.Repeat([]byte{byte('A' + w)}, blockSize)
			if _, err := f.WriteAt(block, int64(w*blockSize)); err != nil {
				errs <- fmt.Errorf("writer %d write: %w", w, err)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("%v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != writers*blockSize {
		t.Fatalf("size = %d, want %d", len(got), writers*blockSize)
	}
	for w := 0; w < writers; w++ {
		want := bytes.Repeat([]byte{byte('A' + w)}, blockSize)
		if !bytes.Equal(got[w*blockSize:(w+1)*blockSize], want) {
			t.Fatalf("block %d was not written whole", w)
		}
	}
}

func TestConcurrentReaders(t *testing.T) {
	dir := testRoot(t)
	path := filepath.Join(dir, "read-me")
	want := contentFor("concurrent-readers", 1<<20)
	if err := os.WriteFile(path, want, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := os.ReadFile(path)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, want) {
				errs <- fmt.Errorf("reader saw %d bytes, want %d", len(got), len(want))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("%v", err)
	}
}

// Hard links are not something WinFsp offers, so the mount has to refuse them
// rather than appear to succeed.
func TestHardLinkUnsupported(t *testing.T) {
	dir := testRoot(t)
	target := filepath.Join(dir, "original")
	if err := os.WriteFile(target, []byte("x"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Link(target, filepath.Join(dir, "hardlink")); err == nil {
		t.Fatal("hard link creation reported success")
	}
}

// Symlinks are refused rather than half-supported: WinFsp needs a reparse
// point to follow one, and an entry it cannot follow reads back empty.
func TestSymlinkUnsupported(t *testing.T) {
	dir := testRoot(t)
	target := filepath.Join(dir, "target.txt")
	if err := os.WriteFile(target, []byte("pointed at"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "link.txt")); err == nil {
		t.Fatal("symlink creation reported success")
	}
}

func TestModTimeAdvances(t *testing.T) {
	dir := testRoot(t)
	path := filepath.Join(dir, "touched")
	if err := os.WriteFile(path, []byte("one"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	first, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)
	if err := os.WriteFile(path, []byte("two"), 0644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	second, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat again: %v", err)
	}
	if !second.ModTime().After(first.ModTime()) {
		t.Fatalf("mtime did not advance: %v then %v", first.ModTime(), second.ModTime())
	}
}

func TestChtimes(t *testing.T) {
	dir := testRoot(t)
	path := filepath.Join(dir, "dated")
	if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	want := time.Date(2020, 3, 4, 5, 6, 7, 0, time.UTC)
	if err := os.Chtimes(path, want, want); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if diff := info.ModTime().UTC().Sub(want); diff > 2*time.Second || diff < -2*time.Second {
		t.Fatalf("mtime = %v, want about %v", info.ModTime().UTC(), want)
	}
}

func TestStatfsIsSelfConsistent(t *testing.T) {
	if *mountPoint == "" {
		t.Skip("no -mountpoint given; this test needs a live WinFsp mount")
	}
	var free, total, totalFree uint64
	if err := getDiskFreeSpace(*mountPoint, &free, &total, &totalFree); err != nil {
		t.Fatalf("GetDiskFreeSpaceEx: %v", err)
	}
	if total == 0 {
		t.Fatal("drive reports zero total bytes")
	}
	if free > total {
		t.Fatalf("free (%d) exceeds total (%d)", free, total)
	}
	if totalFree > total {
		t.Fatalf("total free (%d) exceeds total (%d)", totalFree, total)
	}
}

// TestDeleteOnClose covers the pattern Windows software uses for temporaries:
// the file goes away when the last handle closes, with no explicit delete. A
// filesystem that ignores the flag leaves the name behind, and the next
// program to create it gets a collision.
func TestDeleteOnClose(t *testing.T) {
	dir := testRoot(t)
	path := filepath.Join(dir, "ephemeral.tmp")

	h, err := createDeleteOnClose(path)
	if err != nil {
		if errors.Is(err, errWindowsOnly) {
			t.Skip("delete-on-close needs the Win32 create call")
		}
		t.Fatalf("open with delete-on-close: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		closeHandle(h)
		t.Fatalf("file is not visible while its handle is open: %v", err)
	}
	if err := closeHandle(h); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("file outlived its last handle")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat after close: %v", err)
	}
	// The name has to be free again, which is what the conformance suite
	// tripped over when an aborted test left its file behind.
	h, err = createDeleteOnClose(path)
	if err != nil {
		t.Fatalf("recreating the same name failed: %v", err)
	}
	if err := closeHandle(h); err != nil {
		t.Fatalf("close after recreate: %v", err)
	}
}
