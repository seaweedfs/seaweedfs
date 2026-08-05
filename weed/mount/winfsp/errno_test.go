package winfsp

import (
	"syscall"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// The numbers have to match what cgofuse decodes on the far side, regardless
// of how the host platform spells its own errno constants. That numbering is
// MSVC's, which parts company with Linux above 35.
func TestToErrnoUsesCgofuseNumbering(t *testing.T) {
	cases := []struct {
		status fuse.Status
		want   int
	}{
		{fuse.OK, 0},
		{fuse.ENOENT, -2},
		{fuse.EIO, -5},
		{fuse.EACCES, -13},
		{fuse.EINVAL, -22},
		{fuse.ENOSYS, -40},
		{fuse.Status(syscall.EEXIST), -17},
		{fuse.Status(syscall.ENOSPC), -28},
		{fuse.Status(syscall.ENOTEMPTY), -41},
	}
	for _, c := range cases {
		if got := toErrno(c.status); got != c.want {
			t.Errorf("toErrno(%v) = %d, want %d", c.status, got, c.want)
		}
	}
}

// An unmapped failure must not pass its raw number through: on Windows those
// are APPLICATION_ERROR offsets that mean something else entirely.
func TestToErrnoUnknownBecomesEIO(t *testing.T) {
	if got := toErrno(fuse.Status(1 << 20)); got != -eIO {
		t.Errorf("toErrno(unknown) = %d, want %d", got, -eIO)
	}
}

func TestToErrnoNeverReturnsPositive(t *testing.T) {
	for _, status := range []fuse.Status{fuse.ENOENT, fuse.EPERM, fuse.EBUSY, fuse.Status(4242)} {
		if got := toErrno(status); got > 0 {
			t.Errorf("toErrno(%v) = %d, want <= 0", status, got)
		}
	}
}
