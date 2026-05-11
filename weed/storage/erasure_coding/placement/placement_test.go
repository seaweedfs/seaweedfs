package placement

import (
	"strconv"
	"testing"
)

// makeDisk builds a DiskCandidate with sensible defaults; tests override
// only the fields they care about.
func makeDisk(node, rack, diskType string, diskID uint32) *DiskCandidate {
	return &DiskCandidate{
		NodeID:         node,
		DiskID:         diskID,
		DataCenter:     "dc1",
		Rack:           rack,
		DiskType:       diskType,
		VolumeCount:    0,
		MaxVolumeCount: 100,
		FreeSlots:      100,
	}
}

func disksByType(disks []*DiskCandidate) map[string]int {
	out := map[string]int{}
	for _, d := range disks {
		out[d.DiskType]++
	}
	return out
}

func newRequest(shards int, preferred string) PlacementRequest {
	return PlacementRequest{
		ShardsNeeded:           shards,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
		PreferredDiskType:      preferred,
	}
}

// Plenty of SSD disks available: placement should fill entirely from SSD
// when PreferredDiskType="ssd", leaving HDDs untouched and not flagging
// spillover.
func TestSelectDestinations_PrefersMatchingDiskType(t *testing.T) {
	var disks []*DiskCandidate
	for i := 0; i < 6; i++ {
		disks = append(disks, makeDisk("ssd-"+strconv.Itoa(i), "r"+strconv.Itoa(i%3), "ssd", uint32(i)))
	}
	for i := 0; i < 6; i++ {
		disks = append(disks, makeDisk("hdd-"+strconv.Itoa(i), "r"+strconv.Itoa(i%3), "", uint32(i)))
	}

	result, err := SelectDestinations(disks, newRequest(4, "ssd"))
	if err != nil {
		t.Fatalf("SelectDestinations: %v", err)
	}
	if got := len(result.SelectedDisks); got != 4 {
		t.Fatalf("selected %d disks, want 4", got)
	}
	if counts := disksByType(result.SelectedDisks); counts["ssd"] != 4 {
		t.Fatalf("disk-type counts = %v, want ssd=4", counts)
	}
	if result.SpilledToOtherDiskType {
		t.Fatalf("SpilledToOtherDiskType should be false when preferred pool was sufficient")
	}
}

// Only one SSD disk available but 4 shards needed: placement must consume
// the SSD first, then spill to HDD for the remainder, and report spillover.
func TestSelectDestinations_SpillsWhenPreferredScarce(t *testing.T) {
	disks := []*DiskCandidate{
		makeDisk("ssd-0", "r0", "ssd", 0),
		makeDisk("hdd-0", "r1", "", 0),
		makeDisk("hdd-1", "r2", "", 0),
		makeDisk("hdd-2", "r3", "", 0),
	}

	result, err := SelectDestinations(disks, newRequest(4, "ssd"))
	if err != nil {
		t.Fatalf("SelectDestinations: %v", err)
	}
	if got := len(result.SelectedDisks); got != 4 {
		t.Fatalf("selected %d disks, want 4", got)
	}
	counts := disksByType(result.SelectedDisks)
	if counts["ssd"] != 1 || counts[""] != 3 {
		t.Fatalf("disk-type counts = %v, want ssd=1 hdd=3", counts)
	}
	if !result.SpilledToOtherDiskType {
		t.Fatalf("SpilledToOtherDiskType should be true after falling back to HDD")
	}
}

// Empty PreferredDiskType: pre-#9423 behavior, single pool, no spillover
// flag regardless of disk-type mix.
func TestSelectDestinations_EmptyPreferredDiskTypeKeepsPriorBehavior(t *testing.T) {
	disks := []*DiskCandidate{
		makeDisk("ssd-0", "r0", "ssd", 0),
		makeDisk("hdd-0", "r1", "", 0),
		makeDisk("hdd-1", "r2", "", 0),
		makeDisk("ssd-1", "r3", "ssd", 0),
	}

	result, err := SelectDestinations(disks, newRequest(3, ""))
	if err != nil {
		t.Fatalf("SelectDestinations: %v", err)
	}
	if got := len(result.SelectedDisks); got != 3 {
		t.Fatalf("selected %d disks, want 3", got)
	}
	if result.SpilledToOtherDiskType {
		t.Fatalf("SpilledToOtherDiskType should never be set when PreferredDiskType is empty")
	}
}
