package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const testMiB = uint64(1 << 20)

// fakeDisk stands in for the disk under a location. Preallocating a volume
// takes its size out of free space, as fallocate does on Linux.
type fakeDisk struct {
	all  uint64
	free uint64
}

// newHeadroomStore builds a store with one location whose disk has the given
// free space and whose -volume.minFreeSpace floor is floor. The low-disk flag
// stays false, as it does between two once-a-minute probes.
func newHeadroomStore(t *testing.T, free, floor uint64) (*Store, *DiskLocation, *fakeDisk) {
	t.Helper()
	dir := t.TempDir()
	disk := &fakeDisk{all: 10 * 1024 * testMiB, free: free}

	orig := diskStatusOf
	diskStatusOf = func(string) *volume_server_pb.DiskStatus {
		return &volume_server_pb.DiskStatus{
			All:         disk.all,
			Free:        disk.free,
			Used:        disk.all - disk.free,
			PercentFree: float32(disk.free) * 100 / float32(disk.all),
		}
	}
	t.Cleanup(func() { diskStatusOf = orig })

	loc := &DiskLocation{
		Directory:    dir,
		IdxDirectory: dir,
		DiskType:     types.HardDriveType,
		MinFreeSpace: util.MinFreeSpace{Type: util.AsBytes, Bytes: floor},
	}
	loc.volumes = make(map[needle.VolumeId]*Volume)
	// Close created volumes before TempDir removes them; Windows cannot
	// delete a file that is still open.
	t.Cleanup(func() {
		for _, v := range loc.volumes {
			v.Close()
		}
	})
	s := &Store{
		Locations:      []*DiskLocation{loc},
		NewVolumesChan: make(chan *master_pb.VolumeShortInformationMessage, 100),
	}
	return s, loc, disk
}

// The master sizes a disk's volume slots from its free space. Free space
// inside the -volume.minFreeSpace floor is not available for new volumes, so
// it must not add slots.
func TestMaybeAdjustVolumeMaxIgnoresFreeSpaceInsideFloor(t *testing.T) {
	const floor = 1000 * testMiB
	const volumeSize = 30 * testMiB
	// 90 MiB above the floor is 3 volumes. The other 1000 MiB is headroom.
	s, loc, _ := newHeadroomStore(t, floor+90*testMiB, floor)
	s.SetVolumeSizeLimit(volumeSize)
	s.SetPreallocate(true)

	s.MaybeAdjustVolumeMax()

	if loc.MaxVolumeCount != 3 {
		t.Errorf("MaxVolumeCount=%d; want 3 (only the 90 MiB above the floor is usable)", loc.MaxVolumeCount)
	}
}

// A preallocated volume must not be created when its reservation would take
// the disk under the floor, even if the low-disk flag has not been probed yet.
func TestAddVolumeRefusesPreallocationThatCrossesFloor(t *testing.T) {
	const floor = 1000 * testMiB
	const preallocate = 8 * testMiB
	// 4 MiB above the floor, so an 8 MiB reservation would cross it.
	s, _, disk := newHeadroomStore(t, floor+4*testMiB, floor)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	err := s.addVolume(1, "", NeedleMapInMemory, rp, needle.EMPTY_TTL, int64(preallocate), needle.GetCurrentVersion(), 0, types.HardDriveType, 0)

	if err == nil {
		t.Errorf("addVolume succeeded; want refusal (free %d MiB, floor %d MiB, reserve %d MiB)",
			disk.free/testMiB, floor/testMiB, preallocate/testMiB)
	}
	if s.findVolume(1) != nil {
		t.Errorf("volume 1 exists after refused addVolume")
	}
}

// A volume with no preallocation is still created while free space is above
// the floor, so the guard only holds back growth that would cross it.
func TestAddVolumeAllowsPlainVolumeAboveFloor(t *testing.T) {
	const floor = 1000 * testMiB
	s, _, _ := newHeadroomStore(t, floor+1*testMiB, floor)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	if err := s.addVolume(1, "", NeedleMapInMemory, rp, needle.EMPTY_TTL, 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Errorf("addVolume without preallocation failed above the floor: %v", err)
	}
}

// Copying existing data into a new volume (a merge) reserves nothing and must
// still go through while the disk is below its floor.
func TestAddVolumeAllowsMergeVolumeBelowFloor(t *testing.T) {
	const floor = 1000 * testMiB
	s, _, _ := newHeadroomStore(t, floor-1*testMiB, floor)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	if err := s.addVolume(1, "", NeedleMapInMemory, rp, needle.EMPTY_TTL, 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Errorf("merge volume refused below the floor: %v", err)
	}
}

// Replays the incident in miniature: the master keeps asking for volumes
// while the disk sits just above its floor. Each created volume reserves its
// full size, so the disk must not be driven below the floor.
func TestGrowthLoopStopsAtFloorWhenVolumesPreallocate(t *testing.T) {
	const floor = 1000 * testMiB
	const preallocate = 1 * testMiB
	const attempts = 20
	// Room for 4 preallocated volumes above the floor.
	s, _, disk := newHeadroomStore(t, floor+4*testMiB, floor)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	created := 0
	for i := 1; i <= attempts; i++ {
		err := s.addVolume(needle.VolumeId(i), "", NeedleMapInMemory, rp, needle.EMPTY_TTL, int64(preallocate), needle.GetCurrentVersion(), 0, types.HardDriveType, 0)
		if err != nil {
			continue
		}
		created++
		// fallocate reserves the volume's full size on disk.
		disk.free -= preallocate
	}

	if disk.free < floor {
		t.Errorf("free space fell to %d MiB, below the %d MiB floor, after %d volumes",
			disk.free/testMiB, floor/testMiB, created)
	}
	// Exactly the room above the floor must be used. Checking only an upper
	// bound would also pass when every create fails.
	if created != 4 {
		t.Errorf("created %d preallocated volumes; want exactly 4 (the room above the floor)", created)
	}
}
