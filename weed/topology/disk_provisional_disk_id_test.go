package topology

import (
	"sync/atomic"
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

// A read-only mark from the server is not a volume report: it must not end the
// grace period that keeps a just-grown volume safe from a full report collected
// before the grow, while still keeping the digest and active count in step.
func TestSetVolumeReadOnlyKeepsProvisionalProtection(t *testing.T) {
	dn := NewDataNode("dn1")
	vi := storage.VolumeInfo{Id: 1, DiskType: types.HardDriveType.String()}
	dn.AddProvisionalVolume(vi)

	dn.SetVolumeReadOnly(1, true)

	readOnly := vi
	readOnly.ReadOnly = true
	if got, want := dn.VolumeDigest(), readOnly.ReportHash(); got != want {
		t.Errorf("digest = %x, want %x, the hash of the volume as the server will report it", got, want)
	}
	if got := atomic.LoadInt64(&dn.GetDiskUsages().getOrCreateDisk(types.HardDriveType).activeVolumeCount); got != 0 {
		t.Errorf("activeVolumeCount = %d, want 0 after the read-only mark", got)
	}

	// the stale report, collected before the grow, does not name the volume
	if _, deleted, _ := dn.UpdateVolumes(nil); len(deleted) != 0 {
		t.Fatalf("a report that raced the grow removed the volume: %v", deleted)
	}
	if v, err := dn.GetVolumesById(1); err != nil || !v.ReadOnly {
		t.Fatalf("GetVolumesById = %+v, %v; want the volume kept, read-only", v, err)
	}

	// the server's own report confirms it, and from then on absence counts
	dn.UpdateVolumes([]storage.VolumeInfo{readOnly})
	if _, deleted, _ := dn.UpdateVolumes(nil); len(deleted) != 1 {
		t.Fatalf("a report after confirmation should remove the volume, got %v", deleted)
	}
}
