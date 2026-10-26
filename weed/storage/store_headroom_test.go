package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
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

	// The fake disk behaves like fallocate on Linux: pretend creates reserve.
	origPreallocates := createVolumeFilePreallocates
	createVolumeFilePreallocates = true
	t.Cleanup(func() { createVolumeFilePreallocates = origPreallocates })

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

// Where CreateVolumeFile cannot preallocate, a create reserves nothing and
// must not be held back by a reservation that will never exist.
func TestAddVolumeAllowsPreallocationWhereUnsupported(t *testing.T) {
	const floor = 1000 * testMiB
	s, _, _ := newHeadroomStore(t, floor+1*testMiB, floor)
	createVolumeFilePreallocates = false
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	if err := s.addVolume(1, "", NeedleMapInMemory, rp, needle.EMPTY_TTL, 8*int64(testMiB), needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Errorf("addVolume refused where preallocation is unsupported: %v", err)
	}
}

// Replays the incident in miniature: the master keeps asking for volumes
// while the disk sits just above its floor. Each created volume reserves its
// full size, so the disk must not be driven below the floor.
func TestGrowthLoopStopsAtFloorWhenVolumesPreallocate(t *testing.T) {
	const floor = 1000 * testMiB
	const preallocate = 1 * testMiB
	const attempts = 20
	// Room for 4 preallocated volumes above the floor once each create's
	// metadata allowance is held back.
	const createCost = preallocate + volumeCreateVifBytes + volumeCreateIdxBytes
	s, _, disk := newHeadroomStore(t, floor+4*createCost, floor)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	created := 0
	for i := 1; i <= attempts; i++ {
		err := s.addVolume(needle.VolumeId(i), "", NeedleMapInMemory, rp, needle.EMPTY_TTL, int64(preallocate), needle.GetCurrentVersion(), 0, types.HardDriveType, 0)
		if err != nil {
			continue
		}
		created++
		// fallocate reserves the volume's full size on disk; the metadata
		// allowance it was charged goes with it.
		disk.free -= createCost
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

// A copy's pick and its .dat reservation are separate steps. The picked
// location's cached free space must carry the charge at once, so a second
// copy cannot spend the same headroom before the first reservation lands.
func TestReserveFreeLocationChargesPickedReservation(t *testing.T) {
	const floor = 1000 * testMiB
	const need = 30 * testMiB
	// 40 MiB above the floor: room for one 30 MiB reservation, not two.
	s, loc, _ := newHeadroomStore(t, floor+40*testMiB, floor)
	loc.MaxVolumeCount = 10
	loc.AvailableSpace.Store(40 * testMiB)

	pick := s.ReserveFreeLocation(need, func(location *DiskLocation) bool {
		return location.DiskType == types.HardDriveType
	})
	if pick != loc {
		t.Fatalf("first pick = %v; want the only location", pick)
	}
	if got := loc.AvailableSpace.Load(); got != 10*testMiB {
		t.Fatalf("AvailableSpace after charge = %d MiB; want 10", got/testMiB)
	}

	if pick := s.ReserveFreeLocation(need, func(location *DiskLocation) bool {
		return location.DiskType == types.HardDriveType
	}); pick != nil {
		t.Fatalf("second pick = %v; want nil (the first reservation still holds the headroom)", pick)
	}
}

// The cached free space is refreshed only once a minute, so a stale-high
// value must not admit a reservation the live disk cannot fit.
func TestReserveFreeLocationRechecksLiveDisk(t *testing.T) {
	const floor = 1000 * testMiB
	const need = 30 * testMiB
	// Only 5 MiB above the floor, but the probe last saw 100 MiB of headroom.
	s, loc, _ := newHeadroomStore(t, floor+5*testMiB, floor)
	loc.MaxVolumeCount = 10
	loc.AvailableSpace.Store(100 * testMiB)

	if pick := s.ReserveFreeLocation(need, func(location *DiskLocation) bool {
		return location.DiskType == types.HardDriveType
	}); pick != nil {
		t.Fatalf("pick = %v; want nil (live disk has no room for the reservation)", pick)
	}
}

// A failed copy must return its reservation, or the headroom stays booked
// until the next disk probe and blocks retries.
func TestReleaseFreeLocationRestoresReservation(t *testing.T) {
	const floor = 1000 * testMiB
	const need = 30 * testMiB
	s, loc, _ := newHeadroomStore(t, floor+40*testMiB, floor)
	loc.MaxVolumeCount = 10
	loc.AvailableSpace.Store(40 * testMiB)

	pick := s.ReserveFreeLocation(need, func(location *DiskLocation) bool {
		return location.DiskType == types.HardDriveType
	})
	if pick != loc {
		t.Fatalf("first pick = %v; want the only location", pick)
	}

	s.ReleaseFreeLocation(pick, need)
	if pick := s.ReserveFreeLocation(need, func(location *DiskLocation) bool {
		return location.DiskType == types.HardDriveType
	}); pick == nil {
		t.Fatal("retry pick = nil; want the refunded reservation to admit a copy")
	}
}

// A probe between reserve and release must not resurrect the charge: the
// probe's live reading already forgot the failed copy's pending bytes, so
// refunding the cached figure would let two copies spend the same headroom.
func TestReleaseFreeLocationAfterProbeRefundsOnlyOwnCharge(t *testing.T) {
	const floor = 1000 * testMiB
	const need = 30 * testMiB
	s, loc, disk := newHeadroomStore(t, floor+40*testMiB, floor)
	loc.MaxVolumeCount = 10
	loc.AvailableSpace.Store(40 * testMiB)

	orig := newDiskStatus
	newDiskStatus = func(string, stats.DiskIOProbeConfig) *volume_server_pb.DiskStatus {
		return &volume_server_pb.DiskStatus{All: disk.all, Free: disk.free}
	}
	t.Cleanup(func() { newDiskStatus = orig })

	hdd := func(location *DiskLocation) bool { return location.DiskType == types.HardDriveType }
	if pick := s.ReserveFreeLocation(need, hdd); pick == nil {
		t.Fatal("first pick = nil; want the only location")
	}

	// Mid-copy probe: the live disk still shows the not-yet-written bytes
	// free, but the pending charge must keep them out of the cache.
	loc.CheckDiskSpace(stats.DiskIOProbeConfig{})
	if got := loc.AvailableSpace.Load(); got != 10*testMiB {
		t.Fatalf("AvailableSpace after probe = %d MiB; want 10 (charge survives the probe)", got/testMiB)
	}
	if pick := s.ReserveFreeLocation(need, hdd); pick != nil {
		t.Fatalf("second pick = %v; want nil (the in-flight reservation still holds the headroom)", pick)
	}

	// The failed copy removes only its own charge; the probe-rebased cache
	// must end up holding exactly the live headroom, not the headroom plus
	// a resurrected second charge.
	s.ReleaseFreeLocation(loc, need)
	if got := loc.AvailableSpace.Load(); got != 40*testMiB {
		t.Fatalf("AvailableSpace after refund = %d MiB; want 40", got/testMiB)
	}
	if pick := s.ReserveFreeLocation(need, hdd); pick == nil {
		t.Fatal("retry pick = nil; want the live headroom to admit a copy")
	}
	if pick := s.ReserveFreeLocation(need, hdd); pick != nil {
		t.Fatalf("third pick = %v; want nil (only 40 MiB of headroom exists)", pick)
	}
}

// A copy that lands must end its pending charge without refunding the cache:
// the cache's charge and the landed bytes cancel out, and the next probe
// reads the disk exactly as it is.
func TestCommitFreeLocationSettlesLandedReservation(t *testing.T) {
	const floor = 1000 * testMiB
	const need = 30 * testMiB
	s, loc, disk := newHeadroomStore(t, floor+40*testMiB, floor)
	loc.MaxVolumeCount = 10
	loc.AvailableSpace.Store(40 * testMiB)

	orig := newDiskStatus
	newDiskStatus = func(string, stats.DiskIOProbeConfig) *volume_server_pb.DiskStatus {
		return &volume_server_pb.DiskStatus{All: disk.all, Free: disk.free}
	}
	t.Cleanup(func() { newDiskStatus = orig })

	pick := s.ReserveFreeLocation(need, func(location *DiskLocation) bool {
		return location.DiskType == types.HardDriveType
	})
	if pick != loc {
		t.Fatalf("pick = %v; want the only location", pick)
	}

	disk.free -= need // the copied bytes are on disk now
	s.CommitFreeLocation(pick, need)
	if got := loc.ReservedSpace.Load(); got != 0 {
		t.Fatalf("ReservedSpace after commit = %d MiB; want 0", got/testMiB)
	}
	if got := loc.AvailableSpace.Load(); got != 10*testMiB {
		t.Fatalf("AvailableSpace after commit = %d MiB; want 10 (no refund)", got/testMiB)
	}

	loc.CheckDiskSpace(stats.DiskIOProbeConfig{})
	if got := loc.AvailableSpace.Load(); got != 10*testMiB {
		t.Fatalf("AvailableSpace after post-landing probe = %d MiB; want 10", got/testMiB)
	}
}

// A running copy settles its charge as its bytes land on disk. The
// live-space check must then see the written part once — in the disk
// reading — not again inside the pending charge, or a slow copy would block
// reservations the disk has room for.
func TestCommitFreeLocationSettlesAsBytesLand(t *testing.T) {
	const floor = 1000 * testMiB
	const need = 30 * testMiB
	s, loc, disk := newHeadroomStore(t, floor+70*testMiB, floor)
	loc.MaxVolumeCount = 10
	loc.AvailableSpace.Store(70 * testMiB)
	hdd := func(location *DiskLocation) bool { return location.DiskType == types.HardDriveType }

	if pick := s.ReserveFreeLocation(need, hdd); pick != loc {
		t.Fatalf("first pick = %v; want the only location", pick)
	}

	// Half the copy's bytes are on disk now; settling that much charge
	// keeps the second 30 MiB reservation inside the real headroom.
	disk.free -= 15 * testMiB
	s.CommitFreeLocation(loc, 15*testMiB)

	if pick := s.ReserveFreeLocation(need, hdd); pick == nil {
		t.Fatal("second pick = nil; want the settled headroom to admit the copy")
	}
	if got := loc.ReservedSpace.Load(); got != 45*testMiB {
		t.Fatalf("ReservedSpace after second pick = %d MiB; want 45", got/testMiB)
	}
	if got := loc.AvailableSpace.Load(); got != 10*testMiB {
		t.Fatalf("AvailableSpace after second pick = %d MiB; want 10", got/testMiB)
	}
}
