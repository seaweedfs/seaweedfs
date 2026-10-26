package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestUnUsedSpaceCountsPreallocatedVolumesAsFull(t *testing.T) {
	const volumeSizeLimit = 1 << 20
	dir := t.TempDir()

	preallocated, err := backend.CreateVolumeFile(filepath.Join(dir, "1.dat"), volumeSizeLimit, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer preallocated.Close()
	fi, err := preallocated.(*backend.DiskFile).File.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if AllocatedSize(fi) < volumeSizeLimit {
		t.Skipf("filesystem at %s does not support fallocate", dir)
	}

	plainFile, err := os.Create(filepath.Join(dir, "2.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plainFile.Write(make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	plain := backend.NewDiskFile(plainFile)
	defer plain.Close()
	fi, err = plainFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	// The filesystem may round a small file up to a larger allocation unit.
	plainSize := max(uint64(fi.Size()), AllocatedSize(fi))

	diskLocation := DiskLocation{Directory: dir, IdxDirectory: dir}
	diskLocation.volumes = make(map[needle.VolumeId]*Volume)
	diskLocation.SetVolume(1, &Volume{Id: 1, dir: dir, dirIdx: dir, DataBackend: preallocated, nm: &mockNeedleMapper{}})
	diskLocation.SetVolume(2, &Volume{Id: 2, dir: dir, dirIdx: dir, DataBackend: plain, nm: &mockNeedleMapper{}})

	if got, want := diskLocation.UnUsedSpace(volumeSizeLimit), volumeSizeLimit-plainSize; got != want {
		t.Errorf("unUsedSpace = %d, want %d", got, want)
	}
}
