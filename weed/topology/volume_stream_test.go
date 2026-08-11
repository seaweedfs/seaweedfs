package topology

import (
	"fmt"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func streamTestTopology(t *testing.T) *Topology {
	t.Helper()
	topo := NewTopology("stream", nil, 32*1024*1024*1024, 5, false)
	for dc := 1; dc <= 2; dc++ {
		rack := topo.GetOrCreateDataCenter(fmt.Sprintf("dc%d", dc)).GetOrCreateRack("rack1")
		for n := 1; n <= 2; n++ {
			dn := rack.GetOrCreateDataNode(fmt.Sprintf("10.0.%d.%d", dc, n), 8080, 18080, "", "",
				map[string]uint32{"": 10000})
			var volumes []*master_pb.VolumeInformationMessage
			for i := 0; i < 250; i++ {
				volumes = append(volumes, &master_pb.VolumeInformationMessage{
					Id: uint32(dc*10000 + n*1000 + i), Size: uint64(i) * 100,
					Collection: fmt.Sprintf("c%d", i%3), Version: 3, DiskId: uint32(i % 2),
				})
			}
			topo.SyncDataNodeRegistration(volumes, dn)
			topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
				{Id: uint32(dc*100 + n), Collection: "c1", EcIndexBits: 0x3fff, DiskId: 1},
			}, dn)
		}
	}
	return topo
}

// summarise reduces a listing to what a caller reads off it, so a streamed one
// can be held against an unstreamed one without depending on ordering.
func summarise(info *master_pb.TopologyInfo) []string {
	var lines []string
	for _, dc := range info.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for diskType, disk := range node.DiskInfos {
					where := fmt.Sprintf("%s/%s/%s/%s", dc.Id, rack.Id, node.Id, diskType)
					lines = append(lines,
						fmt.Sprintf("%s disk id=%d max=%d count=%d", where, disk.DiskId, disk.MaxVolumeCount, disk.VolumeCount))
					for _, v := range disk.VolumeInfos {
						lines = append(lines, fmt.Sprintf("%s vol %d size=%d collection=%s disk=%d",
							where, v.Id, v.Size, v.Collection, v.DiskId))
					}
					for _, ec := range disk.EcShardInfos {
						lines = append(lines, fmt.Sprintf("%s ec %d collection=%s bits=%d",
							where, ec.Id, ec.Collection, ec.EcIndexBits))
					}
				}
			}
		}
	}
	sort.Strings(lines)
	return lines
}

// Every caller that moved onto the stream rebuilds the listing from a topology
// sent without volumes plus the batches that follow. That has to come out the
// same as the listing they used to be sent whole.
func TestStreamedListingRebuildsToTheSameThing(t *testing.T) {
	topo := streamTestTopology(t)

	for _, batchSize := range []int{1, 7, 250, 100000} {
		t.Run(fmt.Sprintf("batch=%d", batchSize), func(t *testing.T) {
			rebuilt := topo.ToTopologyInfo(NoVolumes())
			byPlace := map[[4]string]*master_pb.DiskInfo{}
			for _, dc := range rebuilt.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, node := range rack.DataNodeInfos {
						for diskType, disk := range node.DiskInfos {
							byPlace[[4]string{dc.Id, rack.Id, node.Id, diskType}] = disk
							if len(disk.VolumeInfos) != 0 || len(disk.EcShardInfos) != 0 {
								t.Fatalf("the header listed volumes on %s", node.Id)
							}
						}
					}
				}
			}

			err := topo.StreamVolumes(rebuilt, VolumeFilter{}, batchSize, func(b *master_pb.VolumeListStreamResponse) error {
				if batchSize < 100000 && len(b.VolumeInfos) > batchSize {
					t.Errorf("batch carried %d volumes, more than the %d asked for", len(b.VolumeInfos), batchSize)
				}
				disk := byPlace[[4]string{b.DataCenter, b.Rack, b.DataNode, b.DiskType}]
				if disk == nil {
					t.Fatalf("batch named a disk the header did not: %s/%s", b.DataNode, b.DiskType)
				}
				disk.VolumeInfos = append(disk.VolumeInfos, b.VolumeInfos...)
				disk.EcShardInfos = append(disk.EcShardInfos, b.EcShardInfos...)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}

			want := summarise(topo.ToTopologyInfo(VolumeFilter{}))
			got := summarise(rebuilt)
			if len(got) != len(want) {
				t.Fatalf("rebuilt %d lines, want %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("line %d:\n got %q\nwant %q", i, got[i], want[i])
				}
			}
		})
	}
}

// A filter must select the same volumes streamed as it does whole.
func TestStreamedListingHonoursTheFilter(t *testing.T) {
	topo := streamTestTopology(t)
	name := "c1"

	var streamed []uint32
	err := topo.StreamVolumes(topo.ToTopologyInfo(NoVolumes()), VolumeFilter{Collection: &name}, 16, func(b *master_pb.VolumeListStreamResponse) error {
		for _, v := range b.VolumeInfos {
			streamed = append(streamed, v.Id)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	whole, _ := listed(topo.ToTopologyInfo(VolumeFilter{Collection: &name}))
	if !equalIds(streamed, whole) {
		t.Errorf("streamed %d volumes, whole listing had %d", len(streamed), len(whole))
	}
}
