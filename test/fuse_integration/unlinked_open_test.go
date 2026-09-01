//go:build linux

package fuse_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestMetadataOnUnlinkedOpen covers the POSIX rule that an inode outlives its
// last name while a descriptor holds it open: metadata operations through the
// descriptor keep working between the removal and the final close, for a file
// after unlink and for a directory after rmdir.
func TestMetadataOnUnlinkedOpen(t *testing.T) {
	framework := NewFuseTestFramework(t, DefaultTestConfig())
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(DefaultTestConfig()))

	t.Run("UnlinkedOpenFile", func(t *testing.T) {
		testUnlinkedOpenFile(t, framework)
	})

	t.Run("RemovedOpenDirectory", func(t *testing.T) {
		testRemovedOpenDirectory(t, framework)
	})
}

func testUnlinkedOpenFile(t *testing.T, framework *FuseTestFramework) {
	path := filepath.Join(framework.GetMountPoint(), "unlinked_open_file")
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR, 0644)
	require.NoError(t, err)
	defer unix.Close(fd)
	_, err = unix.Write(fd, []byte("hello"))
	require.NoError(t, err)

	require.NoError(t, os.Remove(path))

	require.NoError(t, unix.Ftruncate(fd, 2), "ftruncate on an unlinked open file")
	require.NoError(t, unix.Fchmod(fd, 0600), "fchmod on an unlinked open file")
	require.NoError(t, unix.Futimes(fd, []unix.Timeval{{Sec: 1234567890}, {Sec: 987654321}}),
		"futimes on an unlinked open file")

	var st unix.Stat_t
	require.NoError(t, unix.Fstat(fd, &st), "fstat on an unlinked open file")
	assert.EqualValues(t, 0, st.Nlink, "nlink after unlink")
	assert.EqualValues(t, 0600, st.Mode&0777, "mode after fchmod")
	assert.EqualValues(t, 2, st.Size, "size after ftruncate")
	assert.EqualValues(t, 1234567890, st.Atim.Sec, "atime after futimes")
	assert.EqualValues(t, 987654321, st.Mtim.Sec, "mtime after futimes")

	testFdXattrRoundTrip(t, fd)
}

func testRemovedOpenDirectory(t *testing.T, framework *FuseTestFramework) {
	path := filepath.Join(framework.GetMountPoint(), "removed_open_dir")
	require.NoError(t, os.Mkdir(path, 0755))
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	require.NoError(t, err)
	defer unix.Close(fd)

	require.NoError(t, os.Remove(path))

	require.NoError(t, unix.Fchmod(fd, 0770), "fchmod on a removed open directory")
	require.NoError(t, unix.Futimes(fd, []unix.Timeval{{Sec: 1234567890}, {Sec: 987654321}}),
		"futimes on a removed open directory")

	var st unix.Stat_t
	require.NoError(t, unix.Fstat(fd, &st), "fstat on a removed open directory")
	assert.EqualValues(t, unix.S_IFDIR, st.Mode&unix.S_IFMT, "type after rmdir")
	assert.EqualValues(t, 0770, st.Mode&0777, "mode after fchmod")
	assert.EqualValues(t, 0, st.Nlink, "nlink after rmdir")
	assert.EqualValues(t, 987654321, st.Mtim.Sec, "mtime after futimes")

	testFdXattrRoundTrip(t, fd)
}

func testFdXattrRoundTrip(t *testing.T, fd int) {
	require.NoError(t, unix.Fsetxattr(fd, "user.k", []byte("v"), 0), "fsetxattr on a removed open inode")

	buf := make([]byte, 8)
	n, err := unix.Fgetxattr(fd, "user.k", buf)
	require.NoError(t, err, "fgetxattr on a removed open inode")
	assert.Equal(t, "v", string(buf[:n]))

	require.NoError(t, unix.Fremovexattr(fd, "user.k"), "fremovexattr on a removed open inode")
	_, err = unix.Fgetxattr(fd, "user.k", buf)
	assert.ErrorIs(t, err, unix.ENODATA, "fgetxattr after fremovexattr")
}
