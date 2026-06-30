package master_pb

import (
	"sort"
	"testing"
)

// TestDiskInfoSplitByPhysicalDisk_includesEmptyDiskFromPhysicalDisks pins the
// empty-disk fix: PhysicalDisks lists every physical disk of the type, so a disk
// holding no volumes still surfaces, and each disk reports its exact max instead
// of the aggregate split evenly.
func TestDiskInfoSplitByPhysicalDisk_includesEmptyDiskFromPhysicalDisks(t *testing.T) {
	d := &DiskInfo{
		Type:           "hdd",
		MaxVolumeCount: 1000, // per-type aggregate
		VolumeInfos: []*VolumeInformationMessage{
			{Id: 10, DiskId: 0},
			{Id: 11, DiskId: 1},
		},
		PhysicalDisks: []*PhysicalDiskInfo{
			{DiskId: 0, MaxVolumeCount: 350},
			{DiskId: 1, MaxVolumeCount: 350},
			{DiskId: 2, MaxVolumeCount: 300}, // empty disk, no volumes/shards
		},
	}

	got := d.SplitByPhysicalDisk()
	if len(got) != 3 {
		t.Fatalf("want 3 physical disks including the empty one, got %d", len(got))
	}
	byID := map[uint32]*DiskInfo{}
	for _, di := range got {
		byID[di.DiskId] = di
	}

	empty, ok := byID[2]
	if !ok {
		t.Fatalf("empty disk 2 not surfaced; got %v", byID)
	}
	if empty.VolumeCount != 0 {
		t.Errorf("empty disk: want 0 volumes, got %d", empty.VolumeCount)
	}
	if empty.FreeVolumeCount != 300 {
		t.Errorf("empty disk free: want its full max 300, got %d", empty.FreeVolumeCount)
	}

	// Exact per-disk max, not the even split (which would give ~334/333/333).
	if byID[0].MaxVolumeCount != 350 || byID[1].MaxVolumeCount != 350 || byID[2].MaxVolumeCount != 300 {
		t.Errorf("want exact per-disk max 350/350/300, got %d/%d/%d",
			byID[0].MaxVolumeCount, byID[1].MaxVolumeCount, byID[2].MaxVolumeCount)
	}
	if byID[0].FreeVolumeCount != 349 {
		t.Errorf("disk 0 free: want 349 (350-1 volume), got %d", byID[0].FreeVolumeCount)
	}
}

// TestDiskInfoSplitByPhysicalDisk_clampsNegativeFreeOnOverAllocation pins that
// an over-allocated disk reports zero free, not negative, so it does not drag
// down the node's summed free capacity.
func TestDiskInfoSplitByPhysicalDisk_clampsNegativeFreeOnOverAllocation(t *testing.T) {
	d := &DiskInfo{
		Type: "hdd",
		VolumeInfos: []*VolumeInformationMessage{
			{Id: 1, DiskId: 0},
			{Id: 2, DiskId: 0},
			{Id: 3, DiskId: 0},
		},
		PhysicalDisks: []*PhysicalDiskInfo{
			{DiskId: 0, MaxVolumeCount: 2}, // 3 volumes on a max-2 disk
		},
	}

	got := d.SplitByPhysicalDisk()
	if len(got) != 1 {
		t.Fatalf("want 1 disk, got %d", len(got))
	}
	if got[0].FreeVolumeCount != 0 {
		t.Errorf("over-allocated disk free: want clamped 0, got %d", got[0].FreeVolumeCount)
	}
}

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

// TestDiskInfoSplitByPhysicalDisk_preservesAggregateCapacityWithRemainder
// pins the invariant that the sum of reconstructed counters equals the
// original aggregate, even when the count does not divide evenly.
func TestDiskInfoSplitByPhysicalDisk_preservesAggregateCapacityWithRemainder(t *testing.T) {
	d := &DiskInfo{
		Type:            "hdd",
		MaxVolumeCount:  10,
		FreeVolumeCount: 7,
		VolumeInfos: []*VolumeInformationMessage{
			{Id: 1, DiskId: 0},
			{Id: 2, DiskId: 1},
			{Id: 3, DiskId: 2},
		},
	}

	got := d.SplitByPhysicalDisk()
	if len(got) != 3 {
		t.Fatalf("want 3 disks, got %d", len(got))
	}

	var sumMax, sumFree int64
	for _, di := range got {
		sumMax += di.MaxVolumeCount
		sumFree += di.FreeVolumeCount
	}
	if sumMax != 10 {
		t.Errorf("sum of MaxVolumeCount = %d, want 10 (lossless split)", sumMax)
	}
	if sumFree != 7 {
		t.Errorf("sum of FreeVolumeCount = %d, want 7 (lossless split)", sumFree)
	}
}

// TestDiskInfoSplitByPhysicalDisk_countsActiveAndRemoteExactly verifies
// the per-disk ActiveVolumeCount and RemoteVolumeCount are derived from
// the actual VolumeInfos rather than an even split of the node totals.
func TestDiskInfoSplitByPhysicalDisk_countsActiveAndRemoteExactly(t *testing.T) {
	d := &DiskInfo{
		Type: "hdd",
		VolumeInfos: []*VolumeInformationMessage{
			{Id: 1, DiskId: 0, ReadOnly: false},
			{Id: 2, DiskId: 0, ReadOnly: true},
			{Id: 3, DiskId: 1, ReadOnly: false, RemoteStorageName: "s3"},
			{Id: 4, DiskId: 2, ReadOnly: false},
			{Id: 5, DiskId: 2, ReadOnly: false, RemoteStorageName: "s3"},
		},
	}

	got := d.SplitByPhysicalDisk()
	byID := map[uint32]*DiskInfo{}
	for _, di := range got {
		byID[di.DiskId] = di
	}

	cases := []struct {
		id         uint32
		wantActive int64
		wantRemote int64
	}{
		{0, 1, 0},
		{1, 1, 1},
		{2, 2, 1},
	}
	for _, c := range cases {
		di := byID[c.id]
		if di == nil {
			t.Errorf("missing disk %d", c.id)
			continue
		}
		if di.ActiveVolumeCount != c.wantActive {
			t.Errorf("disk %d: ActiveVolumeCount = %d, want %d", c.id, di.ActiveVolumeCount, c.wantActive)
		}
		if di.RemoteVolumeCount != c.wantRemote {
			t.Errorf("disk %d: RemoteVolumeCount = %d, want %d", c.id, di.RemoteVolumeCount, c.wantRemote)
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

// TestDiskInfoSplitByPhysicalDisk_keepsRealDiskZeroWhenMixed reproduces a
// multi-disk node where physical disk 0 (Locations[0]) holds volumes and the
// aggregate DiskId is seeded from a non-zero sibling (volumes[0] landed on
// disk 6). DiskId 0 must stay its own physical disk, not get folded onto disk
// 6 — folding drops a disk from the count and doubles the sibling's volumes.
func TestDiskInfoSplitByPhysicalDisk_keepsRealDiskZeroWhenMixed(t *testing.T) {
	d := &DiskInfo{
		Type:           "hdd",
		MaxVolumeCount: 30,
		DiskId:         6, // seeded from volumes[0].DiskId in ToDiskInfo
		VolumeInfos: []*VolumeInformationMessage{
			{Id: 60, DiskId: 6},
			{Id: 61, DiskId: 6},
			{Id: 1, DiskId: 0}, // real disk 0, must not be remapped to 6
			{Id: 2, DiskId: 0},
			{Id: 30, DiskId: 3},
		},
	}

	got := d.SplitByPhysicalDisk()
	byID := map[uint32]*DiskInfo{}
	for _, di := range got {
		byID[di.DiskId] = di
	}

	if len(got) != 3 {
		t.Fatalf("want 3 physical disks (0, 3, 6), got %d: %v", len(got), byID)
	}
	if _, ok := byID[0]; !ok {
		t.Fatalf("physical disk 0 was dropped; got disks %v", byID)
	}
	if byID[0].VolumeCount != 2 {
		t.Errorf("disk 0: want 2 volumes, got %d", byID[0].VolumeCount)
	}
	if byID[6].VolumeCount != 2 {
		t.Errorf("disk 6: want 2 volumes (not merged with disk 0), got %d", byID[6].VolumeCount)
	}
	if byID[3].VolumeCount != 1 {
		t.Errorf("disk 3: want 1 volume, got %d", byID[3].VolumeCount)
	}

	var sumMax int64
	for _, di := range got {
		sumMax += di.MaxVolumeCount
	}
	if sumMax != 30 {
		t.Errorf("sum of MaxVolumeCount = %d, want 30 (lossless split)", sumMax)
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
