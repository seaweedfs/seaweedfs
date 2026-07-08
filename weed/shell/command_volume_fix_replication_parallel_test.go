package shell

import (
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func testFixLocation(dc, rack, id string, maxVolumes int64) location {
	return location{
		dc:   dc,
		rack: rack,
		dataNode: &master_pb.DataNodeInfo{
			Id: id,
			DiskInfos: map[string]*master_pb.DiskInfo{
				"": {MaxVolumeCount: maxVolumes, FreeVolumeCount: maxVolumes},
			},
		},
	}
}

func TestReserveTargetSpreadsAcrossServers(t *testing.T) {
	src := testFixLocation("dc1", "r1", "dn0", 0)
	allLocations := []location{
		src,
		testFixLocation("dc1", "r1", "dn1", 10),
		testFixLocation("dc1", "r1", "dn2", 5),
		testFixLocation("dc1", "r1", "dn3", 3),
	}
	// keepDataNodesSorted reorders allLocations in place, so keep stable
	// per-node copies; the shared dataNode pointers carry the accounting
	byId := make(map[string]*location)
	for _, loc := range allLocations {
		byId[loc.dataNode.Id] = &loc
	}
	replicas := []*VolumeReplica{
		{location: &src, info: &master_pb.VolumeInformationMessage{Id: 1}},
	}
	rp, _ := super_block.NewReplicaPlacementFromString("001")

	// with all copies in flight, consecutive reservations must not converge on
	// the emptiest server
	s := newVolumeCopyScheduler(1)
	var reserved []*location
	got := make(map[string]bool)
	for i := 0; i < 3; i++ {
		dst := s.reserveTarget(rp, replicas, allLocations, "", true)
		if dst == nil {
			t.Fatalf("reservation %d found no destination", i)
		}
		reserved = append(reserved, dst)
		got[dst.dataNode.Id] = true
	}
	for _, id := range []string{"dn1", "dn2", "dn3"} {
		if !got[id] {
			t.Errorf("expected a reservation on %s, got %v", id, got)
		}
	}

	// every eligible destination is at the copy cap: the next reservation
	// waits for a free slot instead of failing
	done := make(chan *location)
	go func() {
		done <- s.reserveTarget(rp, replicas, allLocations, "", true)
	}()
	select {
	case dst := <-done:
		t.Fatalf("reserveTarget should wait while all destinations are at the copy cap, got %s", dst.dataNode.Id)
	case <-time.After(100 * time.Millisecond):
	}
	s.releaseTarget(byId["dn1"], "", true)
	var waited *location
	select {
	case waited = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reserveTarget did not wake up after a copy slot freed")
	}
	if waited == nil || waited.dataNode.Id != "dn1" {
		t.Fatalf("expected the freed dn1 to take the waiting copy, got %+v", waited)
	}

	// a successful copy keeps its volume slot, a failed one returns it
	s.releaseTarget(waited, "", true)
	if count := byId["dn1"].dataNode.DiskInfos[""].VolumeCount; count != 2 {
		t.Errorf("dn1 should keep 2 reserved slots, got %d", count)
	}
	for _, dst := range reserved {
		if dst.dataNode.Id != "dn1" {
			s.releaseTarget(dst, "", false)
			if count := dst.dataNode.DiskInfos[""].VolumeCount; count != 0 {
				t.Errorf("%s should have its failed reservation returned, got volume count %d", dst.dataNode.Id, count)
			}
		}
	}
	if len(s.inflight) != 0 {
		t.Errorf("all copies released, but %d still in flight", len(s.inflight))
	}
}

func TestFixUnderReplicatedVolumesInParallel(t *testing.T) {
	src := testFixLocation("dc1", "r1", "dn0", 0)
	allLocations := []location{
		src,
		testFixLocation("dc1", "r1", "dn1", 4),
		testFixLocation("dc1", "r1", "dn2", 4),
		testFixLocation("dc1", "r1", "dn3", 4),
	}
	rp, _ := super_block.NewReplicaPlacementFromString("001")

	volumeReplicas := make(map[uint32][]*VolumeReplica)
	var volumeIds []uint32
	for vid := uint32(1); vid <= 12; vid++ {
		volumeReplicas[vid] = []*VolumeReplica{
			{location: &src, info: &master_pb.VolumeInformationMessage{Id: vid, ReplicaPlacement: uint32(rp.Byte())}},
		}
		volumeIds = append(volumeIds, vid)
	}

	c := &commandVolumeFixReplication{collectionPattern: new(string)}
	fixedVolumes, err := c.fixUnderReplicatedVolumes(nil, io.Discard, false, volumeIds, volumeReplicas, allLocations, 0, 0, 8, 1)
	if err != nil {
		t.Fatalf("fixUnderReplicatedVolumes: %v", err)
	}
	if len(fixedVolumes) != 0 {
		t.Errorf("simulation should not record fixed volumes, got %d", len(fixedVolumes))
	}

	// 12 volumes must exactly fill the 3x4 free slots without over-reserving
	// any single destination
	for _, loc := range allLocations {
		if loc.dataNode.Id == "dn0" {
			continue
		}
		diskInfo := loc.dataNode.DiskInfos[""]
		if diskInfo.VolumeCount != 4 || diskInfo.FreeVolumeCount != 0 {
			t.Errorf("%s expected exactly 4 reserved slots, got volume count %d, free %d",
				loc.dataNode.Id, diskInfo.VolumeCount, diskInfo.FreeVolumeCount)
		}
	}
}
