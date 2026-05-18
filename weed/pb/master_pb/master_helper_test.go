package master_pb

import (
	"sort"
	"testing"
)

func TestDiskInfoSplitByPhysicalDisk_collapsesOnSingleDisk(t *testing.T) {
	d := &DiskInfo{
		Type:           "hdd",
		MaxVolumeCount: 10,
		VolumeInfos: []*VolumeInformationMessage{
			{Id: 1, DiskId: 0},
			{Id: 2, DiskId: 0},
		},
		DiskId: 0,
	}

	got := d.SplitByPhysicalDisk()
	if len(got) != 1 {
		t.Fatalf("want 1 split disk, got %d", len(got))
	}
	if got[0] != d {
		t.Errorf("single-disk input should be returned unchanged; got a copy")
	}
}

func TestDiskInfoSplitByPhysicalDisk_splitsByVolumeDiskId(t *testing.T) {
	d := &DiskInfo{
		Type:              "hdd",
		MaxVolumeCount:    60,
		FreeVolumeCount:   30,
		ActiveVolumeCount: 12,
		VolumeInfos: []*VolumeInformationMessage{
			{Id: 10, DiskId: 0},
			{Id: 11, DiskId: 0},
			{Id: 20, DiskId: 1},
			{Id: 21, DiskId: 2},
			{Id: 22, DiskId: 2},
			{Id: 23, DiskId: 2},
		},
	}

	got := d.SplitByPhysicalDisk()
	if len(got) != 3 {
		t.Fatalf("want 3 split disks, got %d", len(got))
	}

	byID := map[uint32]*DiskInfo{}
	for _, di := range got {
		byID[di.DiskId] = di
	}
	for _, want := range []uint32{0, 1, 2} {
		if _, ok := byID[want]; !ok {
			t.Errorf("missing DiskId=%d in split result", want)
		}
	}

	if byID[0].VolumeCount != 2 {
		t.Errorf("disk 0: want 2 volumes, got %d", byID[0].VolumeCount)
	}
	if byID[1].VolumeCount != 1 {
		t.Errorf("disk 1: want 1 volume, got %d", byID[1].VolumeCount)
	}
	if byID[2].VolumeCount != 3 {
		t.Errorf("disk 2: want 3 volumes, got %d", byID[2].VolumeCount)
	}

	// Capacity is split evenly across the reconstructed disks. With 3 disks
	// and a max of 60, every reconstructed disk gets 20.
	for id, di := range byID {
		if di.MaxVolumeCount != 20 {
			t.Errorf("disk %d: want MaxVolumeCount=20 (60/3), got %d", id, di.MaxVolumeCount)
		}
	}

	// Disk type is preserved on every reconstructed entry so writers that
	// label by type still see "hdd".
	for id, di := range byID {
		if di.Type != "hdd" {
			t.Errorf("disk %d: want Type=hdd, got %q", id, di.Type)
		}
	}
}

func TestDiskInfoSplitByPhysicalDisk_splitsByEcShardDiskId(t *testing.T) {
	d := &DiskInfo{
		Type: "hdd",
		EcShardInfos: []*VolumeEcShardInformationMessage{
			{Id: 100, DiskId: 4},
			{Id: 101, DiskId: 4},
			{Id: 102, DiskId: 5},
		},
	}

	got := d.SplitByPhysicalDisk()

	ids := make([]int, 0, len(got))
	for _, di := range got {
		ids = append(ids, int(di.DiskId))
	}
	sort.Ints(ids)
	if len(ids) != 2 || ids[0] != 4 || ids[1] != 5 {
		t.Fatalf("want split disk ids [4,5], got %v", ids)
	}
}

func TestDiskInfoSplitByPhysicalDisk_normalizesZeroToOuterDiskId(t *testing.T) {
	// Older payloads / fixtures omit the per-record DiskId. The outer DiskId
	// is the authoritative fallback.
	d := &DiskInfo{
		Type:   "hdd",
		DiskId: 7,
		VolumeInfos: []*VolumeInformationMessage{
			{Id: 1, DiskId: 0}, // normalize to 7
			{Id: 2, DiskId: 0}, // normalize to 7
		},
	}
	got := d.SplitByPhysicalDisk()
	if len(got) != 1 {
		t.Fatalf("want 1 disk, got %d", len(got))
	}
	if got[0].DiskId != 7 {
		t.Errorf("want DiskId=7 after normalization, got %d", got[0].DiskId)
	}
}

func TestDiskInfoSplitByPhysicalDisk_nilSafe(t *testing.T) {
	var d *DiskInfo
	if got := d.SplitByPhysicalDisk(); got != nil {
		t.Errorf("nil receiver should return nil slice, got %v", got)
	}
}

func TestDiskInfoSplitByPhysicalDisk_emptyDiskReturnsSelf(t *testing.T) {
	d := &DiskInfo{Type: "hdd", MaxVolumeCount: 10, DiskId: 3}
	got := d.SplitByPhysicalDisk()
	if len(got) != 1 || got[0] != d {
		t.Errorf("empty disk should be passed through unchanged; got %v", got)
	}
}
