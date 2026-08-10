package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A provisional (grow-time) registration carries no disk id; when it lands
// after the server's own report -- the server pushes its report during the
// AllocateVolume RPC, so either order happens -- it must not erase the disk id
// the report recorded.
func TestProvisionalUpdateKeepsReportedDiskId(t *testing.T) {
	disk := NewDisk(types.HardDriveType.String())

	disk.AddOrUpdateVolume(storage.VolumeInfo{Id: 1, DiskId: 2})
	disk.AddProvisionalVolume(storage.VolumeInfo{Id: 1})

	v, err := disk.GetVolumesById(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.DiskId != 2 {
		t.Fatalf("DiskId = %d, want 2 (provisional update clobbered the reported value)", v.DiskId)
	}

	// A later server report naming a different disk still wins.
	disk.AddOrUpdateVolume(storage.VolumeInfo{Id: 1, DiskId: 1})
	if v, _ = disk.GetVolumesById(1); v.DiskId != 1 {
		t.Fatalf("DiskId = %d, want 1 (a real report must override)", v.DiskId)
	}
}
