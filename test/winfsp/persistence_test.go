package winfsp

import (
	"bytes"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"
)

var (
	// phase splits the test across a remount: the write phase is run, the
	// mount is torn down and brought back, then the verify phase runs. Reading
	// back through the same live mount proves nothing about durability, since
	// the answer can come from the mount's own caches.
	phase = flag.String("phase", "", "write or verify; empty skips the persistence test")

	// filerAddr enables a second check, that the bytes reached the filer and
	// are servable without the mount in the path at all.
	filerAddr = flag.String("filer", "", "filer host:port to cross-check through, e.g. localhost:8888")

	// persistSubdir is where the fixtures live, under both the mount and the
	// filer's mount root.
	persistSubdir = flag.String("persistdir", "winfsp-persist", "directory under the mount to persist into")
)

type fixture struct {
	relPath string
	size    int
}

// Sizes straddle the boundaries where the write path changes behavior: inline,
// a single chunk, and several chunks.
var fixtures = []fixture{
	{"small.txt", 11},
	{"medium.bin", 300 << 10},
	{"multichunk.bin", 9 << 20},
	{"nested/deep/leaf.bin", 65536},
	{"unicode-café-日本.txt", 64},
}

// contentFor derives bytes from the name, so the write and verify phases agree
// without carrying a manifest between them.
func contentFor(relPath string, size int) []byte {
	h := fnv.New64a()
	h.Write([]byte(relPath))
	state := h.Sum64() | 1
	out := make([]byte, size)
	for i := range out {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		out[i] = byte(state)
	}
	return out
}

func TestPersistence(t *testing.T) {
	if *mountPoint == "" {
		t.Skip("no -mountpoint given; this test needs a live WinFsp mount")
	}
	switch *phase {
	case "write":
		persistenceWrite(t)
	case "verify":
		persistenceVerify(t)
	default:
		t.Skip("no -phase given; run with -phase=write before a remount and -phase=verify after")
	}
}

func persistenceWrite(t *testing.T) {
	root := filepath.Join(*mountPoint, *persistSubdir)
	if err := os.RemoveAll(root); err != nil && !os.IsNotExist(err) {
		t.Fatalf("clean %s: %v", root, err)
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", root, err)
	}

	for _, f := range fixtures {
		target := filepath.Join(root, filepath.FromSlash(f.relPath))
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			t.Fatalf("mkdir for %s: %v", f.relPath, err)
		}
		if err := os.WriteFile(target, contentFor(f.relPath, f.size), 0644); err != nil {
			t.Fatalf("write %s: %v", f.relPath, err)
		}
		t.Logf("wrote %s (%d bytes)", f.relPath, f.size)
	}

	// Closing the files should have pushed them to the filer. Check that
	// directly, so a failure here separates "never left the mount" from
	// "did not survive the remount".
	if *filerAddr == "" {
		return
	}
	for _, f := range fixtures {
		want := contentFor(f.relPath, f.size)
		got, err := fetchFromFiler(*filerAddr, path.Join(*persistSubdir, f.relPath))
		if err != nil {
			t.Errorf("fetch %s from filer: %v", f.relPath, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("filer served %d bytes for %s, want %d", len(got), f.relPath, len(want))
		}
	}
}

func persistenceVerify(t *testing.T) {
	root := filepath.Join(*mountPoint, *persistSubdir)
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("stat %s after remount: %v", root, err)
	}

	for _, f := range fixtures {
		target := filepath.Join(root, filepath.FromSlash(f.relPath))
		got, err := os.ReadFile(target)
		if err != nil {
			t.Errorf("read %s after remount: %v", f.relPath, err)
			continue
		}
		want := contentFor(f.relPath, f.size)
		if len(got) != len(want) {
			t.Errorf("%s is %d bytes after remount, want %d", f.relPath, len(got), len(want))
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s survived the remount with different content", f.relPath)
		}
	}

	// The directory structure has to come back too, not just the files.
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("readdir %s after remount: %v", root, err)
	}
	if len(entries) == 0 {
		t.Fatal("mount root directory is empty after remount")
	}

	if err := os.RemoveAll(root); err != nil {
		t.Errorf("cleanup: %v", err)
	}
}

func fetchFromFiler(addr, filerPath string) ([]byte, error) {
	endpoint := &url.URL{Scheme: "http", Host: addr, Path: "/" + filerPath}
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(endpoint.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", endpoint, resp.Status)
	}
	return io.ReadAll(resp.Body)
}
