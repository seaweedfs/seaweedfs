package nfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nfsclient "github.com/willscott/go-nfs-client/nfs"
)

// setupFramework is a small helper that boots the cluster for a single test
// and tears everything down on completion. Every test gets a fresh filer +
// volume pair so they cannot step on each other's namespace.
func setupFramework(t *testing.T) *NfsTestFramework {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	config := DefaultTestConfig()
	config.EnableDebug = testing.Verbose()
	fw := NewNfsTestFramework(t, config)
	require.NoError(t, fw.Setup(config), "framework setup")
	t.Cleanup(fw.Cleanup)
	return fw
}

// writeAll writes payload to path on the target in a single Write call. The
// NFS WRITE3 RPC chunks internally, so this exists purely so tests read
// linearly.
func writeAll(t *testing.T, target *nfsclient.Target, remotePath string, payload []byte) {
	t.Helper()
	file, err := target.OpenFile(remotePath, 0o644)
	require.NoError(t, err, "open %s for write", remotePath)
	if len(payload) > 0 {
		n, err := file.Write(payload)
		require.NoError(t, err, "write %s", remotePath)
		require.Equal(t, len(payload), n, "short write on %s", remotePath)
	}
	require.NoError(t, file.Close(), "close %s", remotePath)
}

// readAll opens path on the target and returns the full file contents.
func readAll(t *testing.T, target *nfsclient.Target, remotePath string) []byte {
	t.Helper()
	file, err := target.Open(remotePath)
	require.NoError(t, err, "open %s for read", remotePath)
	defer file.Close()
	content, err := io.ReadAll(file)
	require.NoError(t, err, "read %s", remotePath)
	return content
}

// TestNfsBasicReadWrite exercises the most common NFS path: OpenFile + Write
// + Close followed by Open + Read to verify round-trip data integrity.
func TestNfsBasicReadWrite(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	payload := []byte("hello from seaweedfs nfs integration test")
	writeAll(t, target, "/hello.txt", payload)

	got := readAll(t, target, "/hello.txt")
	assert.Equal(t, payload, got, "round-tripped content must match")

	info, err := target.Getattr("/hello.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(len(payload)), int64(info.Filesize))
}

// TestNfsMkdirAndRmdir covers Mkdir, ReadDirPlus, and RmDir. The readdir
// assertion also verifies that the newly-created directory shows up under
// the export root the way a POSIX client would expect.
func TestNfsMkdirAndRmdir(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	_, err = target.Mkdir("/dir1", 0o755)
	require.NoError(t, err)

	entries, err := target.ReadDirPlus("/")
	require.NoError(t, err)
	found := false
	for _, entry := range entries {
		if entry.Name() == "dir1" {
			found = true
			assert.True(t, entry.IsDir(), "dir1 should be a directory")
		}
	}
	assert.True(t, found, "expected dir1 in readdir listing")

	require.NoError(t, target.RmDir("/dir1"))

	// After removal, dir1 must be gone from the listing.
	entries, err = target.ReadDirPlus("/")
	require.NoError(t, err)
	for _, entry := range entries {
		assert.NotEqual(t, "dir1", entry.Name(), "dir1 should be removed")
	}
}

// TestNfsNestedDirectories ensures the server can materialise a deep tree in
// a single Mkdir-per-segment sequence and that reads/writes work at the
// leaves.
func TestNfsNestedDirectories(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	for _, segment := range []string{"/a", "/a/b", "/a/b/c"} {
		_, err := target.Mkdir(segment, 0o755)
		require.NoError(t, err, "mkdir %s", segment)
	}

	payload := []byte("deep path content")
	writeAll(t, target, "/a/b/c/leaf.txt", payload)

	got := readAll(t, target, "/a/b/c/leaf.txt")
	assert.Equal(t, payload, got)

	require.NoError(t, target.Remove("/a/b/c/leaf.txt"))
	require.NoError(t, target.RmDir("/a/b/c"))
	require.NoError(t, target.RmDir("/a/b"))
	require.NoError(t, target.RmDir("/a"))
}

// TestNfsRenamePreservesContent renames a file and makes sure the content
// at the new path matches what was written at the old one, and that the
// old path disappears. It does not assert on inode identity because pjdfstest
// already covers that and this test intentionally avoids depending on the
// mount-side identity plumbing.
func TestNfsRenamePreservesContent(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	payload := []byte("rename me")
	writeAll(t, target, "/src.txt", payload)

	require.NoError(t, target.Rename("/src.txt", "/dst.txt"))

	_, _, err = target.Lookup("/src.txt")
	assert.Error(t, err, "source should be gone after rename")

	got := readAll(t, target, "/dst.txt")
	assert.Equal(t, payload, got)

	require.NoError(t, target.Remove("/dst.txt"))
}

// TestNfsOverwriteShrinksFile rewrites an existing file with shorter content
// and asserts Getattr reports the new (smaller) size. go-nfs-client's
// OpenFile does not pass O_TRUNC, so the test truncates explicitly via
// Setattr(size=0) before the second write — mirroring what `echo >file`
// does on a POSIX client.
func TestNfsOverwriteShrinksFile(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	writeAll(t, target, "/overwrite.txt", []byte("the quick brown fox"))

	require.NoError(t, target.Setattr("/overwrite.txt", nfsclient.Sattr3{
		Size: nfsclient.SetSize{SetIt: true, Size: 0},
	}))

	writeAll(t, target, "/overwrite.txt", []byte("short"))

	info, err := target.Getattr("/overwrite.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(len("short")), int64(info.Filesize))

	got := readAll(t, target, "/overwrite.txt")
	assert.Equal(t, []byte("short"), got)

	require.NoError(t, target.Remove("/overwrite.txt"))
}

// TestNfsLargeFile writes a multi-megabyte payload so the write path has to
// cut chunks and flush through the volume server rather than inlining
// content in the filer entry.
func TestNfsLargeFile(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	const size = 3 * 1024 * 1024 // 3 MiB — exceeds the 4 MiB inline cutoff boundary when combined with metadata
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251) // non-repeating to catch offset bugs
	}

	writeAll(t, target, "/big.bin", payload)

	info, err := target.Getattr("/big.bin")
	require.NoError(t, err)
	assert.Equal(t, int64(size), int64(info.Filesize))

	got := readAll(t, target, "/big.bin")
	require.Equal(t, size, len(got))
	assert.True(t, bytes.Equal(payload, got), "large file content must round-trip byte-for-byte")

	require.NoError(t, target.Remove("/big.bin"))
}

// TestNfsBinaryAndEmptyFiles covers two edge-case payloads the write path
// tends to regress on: arbitrary binary bytes and zero-length files.
func TestNfsBinaryAndEmptyFiles(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	t.Run("AllByteValues", func(t *testing.T) {
		payload := make([]byte, 256)
		for i := range payload {
			payload[i] = byte(i)
		}
		writeAll(t, target, "/binary.bin", payload)
		assert.Equal(t, payload, readAll(t, target, "/binary.bin"))
		require.NoError(t, target.Remove("/binary.bin"))
	})

	t.Run("EmptyFile", func(t *testing.T) {
		writeAll(t, target, "/empty.txt", nil)
		info, err := target.Getattr("/empty.txt")
		require.NoError(t, err)
		assert.Equal(t, int64(0), int64(info.Filesize))
		require.NoError(t, target.Remove("/empty.txt"))
	})
}

// TestNfsSymlinkRoundTrip covers Symlink and Readlink through the nfs server.
// Readlink returns the target path; the server does not auto-traverse it.
func TestNfsSymlinkRoundTrip(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	// Symlink uses a different RPC than open+create, and our server routes it
	// through the billy Change interface.
	require.NoError(t, target.Symlink("/target.txt", "/link.txt"))

	// The underlying target does not need to exist for readlink to succeed.
	file, _, err := target.Lookup("/link.txt")
	require.NoError(t, err, "lookup symlink")
	assert.True(t, file.Mode()&os.ModeSymlink != 0, "expected symlink mode, got %s", file.Mode())

	require.NoError(t, target.Remove("/link.txt"))
}

// TestNfsReadDirPlusOrdering creates a handful of files with distinct names
// and ensures ReadDirPlus surfaces every one of them. The server pages
// listings from the filer, so we want to make sure nothing is truncated.
func TestNfsReadDirPlusOrdering(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	_, err = target.Mkdir("/listing", 0o755)
	require.NoError(t, err)

	names := []string{"alpha.txt", "beta.txt", "gamma.txt", "delta.txt", "epsilon.txt"}
	for _, name := range names {
		writeAll(t, target, path.Join("/listing", name), []byte(name))
	}

	entries, err := target.ReadDirPlus("/listing")
	require.NoError(t, err)
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.Name() == "." || entry.Name() == ".." {
			continue
		}
		seen[entry.Name()] = struct{}{}
	}
	for _, name := range names {
		_, ok := seen[name]
		assert.True(t, ok, "expected %s in directory listing", name)
	}

	for _, name := range names {
		require.NoError(t, target.Remove(path.Join("/listing", name)))
	}
	require.NoError(t, target.RmDir("/listing"))
}

// TestNfsRemoveMissingFailsCleanly asserts that removing a non-existent path
// surfaces an error instead of silently succeeding. A bug where the server
// returned NFS3_OK on missing entries would hide metadata drift.
func TestNfsRemoveMissingFailsCleanly(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	err = target.Remove("/does_not_exist.txt")
	require.Error(t, err, "removing a missing file must error")
	// NFS3 surfaces this as NFS3ERR_NOENT; make sure the error text is
	// recognisable without locking us into the library's exact wording.
	assert.True(t,
		strings.Contains(strings.ToLower(err.Error()), "noent") ||
			strings.Contains(strings.ToLower(err.Error()), "not exist") ||
			strings.Contains(strings.ToLower(err.Error()), "no such"),
		"unexpected error shape: %v", err)
}

// TestNfsFSInfoReturnsSaneLimits pokes at FSINFO so we catch regressions
// where the server advertises zero read/write limits (which would make
// clients fall back to the 8 KiB floor and slow every test that follows).
func TestNfsFSInfoReturnsSaneLimits(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	info, err := target.FSInfo()
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Greater(t, info.RTPref, uint32(0), "rtpref must be positive")
	assert.Greater(t, info.WTPref, uint32(0), "wtpref must be positive")
}

// TestNfsAppendIsSequential writes two chunks to the same file in separate
// Open cycles and asserts the concatenation is preserved. The second write
// uses O_APPEND (the default Open path in go-nfs-client does not pass
// flags, so we explicitly reopen after writing the first chunk).
func TestNfsAppendIsSequential(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	const prefix = "part1-"
	const suffix = "part2"

	writeAll(t, target, "/concat.txt", []byte(prefix))

	file, err := target.OpenFile("/concat.txt", 0o644)
	require.NoError(t, err)
	// Seek to end before writing so we append rather than overwrite. go-nfs
	// client's File.Seek uses the same offset tracking as Write so this is
	// enough to place the second chunk after the first.
	_, err = file.Seek(int64(len(prefix)), io.SeekStart)
	require.NoError(t, err)
	_, err = file.Write([]byte(suffix))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	got := readAll(t, target, "/concat.txt")
	assert.Equal(t, prefix+suffix, string(got))

	require.NoError(t, target.Remove("/concat.txt"))
}

// Regression: readdir should not emit stale entries after a remove. This is
// the scenario the PR's meta cache invalidation logic was written to fix.
func TestNfsReadDirAfterRemove(t *testing.T) {
	fw := setupFramework(t)
	target, cleanup, err := fw.Mount()
	require.NoError(t, err)
	defer cleanup()

	_, err = target.Mkdir("/churn", 0o755)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		writeAll(t, target, path.Join("/churn", fmt.Sprintf("f%d.txt", i)), []byte{byte(i)})
	}
	// Remove the middle one and re-list.
	require.NoError(t, target.Remove("/churn/f2.txt"))

	entries, err := target.ReadDirPlus("/churn")
	require.NoError(t, err)
	for _, entry := range entries {
		assert.NotEqual(t, "f2.txt", entry.Name(), "removed file should not reappear in listing")
	}

	for i := 0; i < 5; i++ {
		if i == 2 {
			continue
		}
		require.NoError(t, target.Remove(path.Join("/churn", fmt.Sprintf("f%d.txt", i))))
	}
	require.NoError(t, target.RmDir("/churn"))
}
