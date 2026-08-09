package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func filterTestTopology(t *testing.T) *Topology {
	t.Helper()
	topo := NewTopology("filter", nil, 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: "", Size: 1000, Version: 3, DiskId: 2},
		{Id: 2, Collection: "c", Size: 2000, Version: 3, DiskId: 2},
		{Id: 3, Collection: "other", Size: 3000, Version: 3, DiskId: 2},
	}, dn)
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 8, Collection: "c", EcIndexBits: 0x3fff, DiskId: 2},
		{Id: 9, Collection: "other", EcIndexBits: 0x3fff, DiskId: 2},
	}, dn)
	return topo
}

// listed returns the volume and ec ids a filtered listing carries.
func listed(info *master_pb.TopologyInfo) (volumes []uint32, ecVolumes []uint32) {
	for _, dc := range info.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for _, disk := range node.DiskInfos {
					for _, v := range disk.VolumeInfos {
						volumes = append(volumes, v.Id)
					}
					for _, ec := range disk.EcShardInfos {
						ecVolumes = append(ecVolumes, ec.Id)
					}
				}
			}
		}
	}
	return
}

func equalIds(got, want []uint32) bool {
	if len(got) != len(want) {
		return false
	}
	seen := map[uint32]int{}
	for _, id := range got {
		seen[id]++
	}
	for _, id := range want {
		seen[id]--
	}
	for _, n := range seen {
		if n != 0 {
			return false
		}
	}
	return true
}

func TestVolumeFilterSelects(t *testing.T) {
	topo := filterTestTopology(t)
	collection := func(name string) *string { return &name }
	volume := func(id uint32) *needle.VolumeId { v := needle.VolumeId(id); return &v }

	for _, tc := range []struct {
		name          string
		filter        VolumeFilter
		wantVolumes   []uint32
		wantEcVolumes []uint32
	}{
		{"no filter takes everything", VolumeFilter{}, []uint32{1, 2, 3}, []uint32{8, 9}},
		{"one collection", VolumeFilter{Collection: collection("c")}, []uint32{2}, []uint32{8}},
		// Asking for it must not read as asking for everything.
		{"the default collection", VolumeFilter{Collection: collection("")}, []uint32{1}, nil},
		{"a collection nothing is in", VolumeFilter{Collection: collection("none")}, nil, nil},
		{"one volume", VolumeFilter{VolumeId: volume(3)}, []uint32{3}, nil},
		{"one ec volume", VolumeFilter{VolumeId: volume(9)}, nil, []uint32{9}},
		{"both, agreeing", VolumeFilter{Collection: collection("other"), VolumeId: volume(3)}, []uint32{3}, nil},
		{"both, disagreeing", VolumeFilter{Collection: collection("c"), VolumeId: volume(3)}, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			volumes, ecVolumes := listed(topo.ToTopologyInfo(tc.filter))
			if !equalIds(volumes, tc.wantVolumes) {
				t.Errorf("listed volumes %v, want %v", volumes, tc.wantVolumes)
			}
			if !equalIds(ecVolumes, tc.wantEcVolumes) {
				t.Errorf("listed ec volumes %v, want %v", ecVolumes, tc.wantEcVolumes)
			}
		})
	}
}

// A filter never changes which disks are reported or what they say about
// themselves.
func TestVolumeFilterKeepsTheTopology(t *testing.T) {
	topo := filterTestTopology(t)
	full := topo.ToTopologyInfo(VolumeFilter{})
	name := "c"
	narrowed := topo.ToTopologyInfo(VolumeFilter{Collection: &name})

	for _, info := range []*master_pb.TopologyInfo{full, narrowed} {
		if len(info.DataCenterInfos) != 1 || len(info.DataCenterInfos[0].RackInfos) != 1 {
			t.Fatalf("listing lost its topology: %v", info)
		}
	}

	fullDisk := full.DataCenterInfos[0].RackInfos[0].DataNodeInfos[0].DiskInfos[""]
	narrowedDisk := narrowed.DataCenterInfos[0].RackInfos[0].DataNodeInfos[0].DiskInfos[""]
	if narrowedDisk == nil {
		t.Fatal("the filtered listing dropped the disk")
	}
	if narrowedDisk.VolumeCount != fullDisk.VolumeCount {
		t.Errorf("volume count %d, want the disk's own %d", narrowedDisk.VolumeCount, fullDisk.VolumeCount)
	}
	if narrowedDisk.MaxVolumeCount != fullDisk.MaxVolumeCount {
		t.Errorf("max volume count %d, want %d", narrowedDisk.MaxVolumeCount, fullDisk.MaxVolumeCount)
	}
	if narrowedDisk.DiskId != fullDisk.DiskId {
		t.Errorf("disk id %d, want %d", narrowedDisk.DiskId, fullDisk.DiskId)
	}
}

// So a caller that finds nothing can tell an empty answer from a missing disk.
func TestVolumeFilterKeepsTheDiskIdWhenNothingMatches(t *testing.T) {
	topo := filterTestTopology(t)
	name := "none"
	info := topo.ToTopologyInfo(VolumeFilter{Collection: &name})
	disk := info.DataCenterInfos[0].RackInfos[0].DataNodeInfos[0].DiskInfos[""]
	if len(disk.VolumeInfos) != 0 {
		t.Fatalf("expected nothing listed, got %d volumes", len(disk.VolumeInfos))
	}
	if disk.DiskId != 2 {
		t.Errorf("reported disk id %d, want the one its volumes are on", disk.DiskId)
	}
}

func TestNewVolumeFilterReadsTheRequest(t *testing.T) {
	collectionOf := func(f VolumeFilter) string {
		if f.Collection == nil {
			return "<every>"
		}
		return *f.Collection
	}

	for _, tc := range []struct {
		name    string
		request *master_pb.VolumeListRequest
		want    string
	}{
		// A caller passing through its own "" is answered too much, not wrongly.
		{"an empty request", &master_pb.VolumeListRequest{}, "<every>"},
		{"an empty collection", &master_pb.VolumeListRequest{Collection: ""}, "<every>"},
		{"a named collection", &master_pb.VolumeListRequest{Collection: "c"}, "c"},
		{"the default collection", &master_pb.VolumeListRequest{DefaultCollectionOnly: true}, ""},
		// Contradictory, so the more specific of the two wins.
		{"both", &master_pb.VolumeListRequest{Collection: "c", DefaultCollectionOnly: true}, "c"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := collectionOf(NewVolumeFilter(tc.request)); got != tc.want {
				t.Errorf("selected collection %q, want %q", got, tc.want)
			}
		})
	}

	if f := NewVolumeFilter(&master_pb.VolumeListRequest{}); f.VolumeId != nil {
		t.Error("a zero volume id must not filter")
	}
	f := NewVolumeFilter(&master_pb.VolumeListRequest{VolumeId: 7})
	if f.VolumeId == nil || uint32(*f.VolumeId) != 7 {
		t.Errorf("volume id not carried across: %v", f.VolumeId)
	}
}

// Everything a listing reads off a disk must be read under its lock, including
// how much room to reserve, or a heartbeat writing the map races it.
func TestVolumeFilterListsWhileVolumesChange(t *testing.T) {
	topo := filterTestTopology(t)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
				{Id: uint32(100 + i%50), Collection: "c", Size: 1, Version: 3, DiskId: 2},
			}, dn)
		}
	}()

	name := "c"
	for i := 0; i < 200; i++ {
		topo.ToTopologyInfo(VolumeFilter{})
		topo.ToTopologyInfo(VolumeFilter{Collection: &name})
	}
	close(stop)
	<-done
}
