//go:build linux

package backend

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestCreateVolumeFilePreallocateKeepsSize(t *testing.T) {
	dir := t.TempDir()
	const preallocate = 1 << 20

	probe, err := os.Create(filepath.Join(dir, "probe"))
	if err != nil {
		t.Fatalf("create probe: %v", err)
	}
	err = unix.Fallocate(int(probe.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, preallocate)
	probe.Close()
	if err != nil {
		t.Skipf("fallocate not supported in %s: %v", dir, err)
	}

	fileName := filepath.Join(dir, "1.dat")
	f, err := CreateVolumeFile(fileName, preallocate, 0)
	if err != nil {
		t.Fatalf("CreateVolumeFile: %v", err)
	}
	defer f.Close()

	size, _, err := f.GetStat()
	if err != nil {
		t.Fatalf("GetStat: %v", err)
	}
	if size != 0 {
		t.Errorf("file size = %d, want 0", size)
	}

	var st syscall.Stat_t
	if err := syscall.Stat(fileName, &st); err != nil {
		t.Fatalf("stat: %v", err)
	}
	if st.Blocks*512 < preallocate {
		t.Errorf("allocated %d bytes, want at least %d", st.Blocks*512, preallocate)
	}
}
