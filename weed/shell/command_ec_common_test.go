package shell

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func TestEcDistribution(t *testing.T) {

	topologyInfo := parseOutput(topoData)

	// find out all volume servers with one slot left.
	ecNodes, totalFreeEcSlots := collectEcVolumeServersByDc(topologyInfo, "")

	sortEcNodesByFreeslotsDescending(ecNodes)

	if totalFreeEcSlots < erasure_coding.TotalShardsCount {
		t.Errorf("not enough free ec shard slots: %d", totalFreeEcSlots)
	}
	allocatedDataNodes := ecNodes
	if len(allocatedDataNodes) > erasure_coding.TotalShardsCount {
		allocatedDataNodes = allocatedDataNodes[:erasure_coding.TotalShardsCount]
	}

	for _, dn := range allocatedDataNodes {
		// fmt.Printf("info %+v %+v\n", dn.info, dn)
		fmt.Printf("=> %+v %+v\n", dn.info.Id, dn.freeEcSlot)
	}
}

func TestVolumeIdToReplicaPlacement(t *testing.T) {
	topo1 := parseOutput(topoData)
	topo2 := parseOutput(topoData2)

	testCases := []struct {
		topology *master_pb.TopologyInfo
		vid      string
		want     string
		wantErr  string
	}{
		{topo1, "", "", "failed to resolve replica placement for volume ID 0"},
		{topo1, "0", "", "failed to resolve replica placement for volume ID 0"},
		{topo1, "1", "100", ""},
		{topo1, "296", "100", ""},
		{topo2, "", "", "failed to resolve replica placement for volume ID 0"},
		{topo2, "19012", "", "failed to resolve replica placement for volume ID 19012"},
		{topo2, "6271", "002", ""},
		{topo2, "17932", "002", ""},
	}

	for _, tc := range testCases {
		vid, _ := needle.NewVolumeId(tc.vid)
		ecNodes, _ := collectEcVolumeServersByDc(tc.topology, "")
		got, gotErr := volumeIdToReplicaPlacement(vid, ecNodes)

		if tc.wantErr == "" && gotErr != nil {
			t.Errorf("expected no error for volume '%s', got '%s'", tc.vid, gotErr.Error())
			continue
		}
		if tc.wantErr != "" {
			if gotErr == nil {
				t.Errorf("got no error for volume '%s', expected '%s'", tc.vid, tc.wantErr)
				continue
			}
			if gotErr.Error() != tc.wantErr {
				t.Errorf("expected error '%s' for volume '%s', got '%s'", tc.wantErr, tc.vid, gotErr.Error())
				continue
			}
		}

		if got == nil {
			if tc.want != "" {
				t.Errorf("expected replica placement '%s' for volume '%s', got nil", tc.want, tc.vid)
			}
			continue
		}
		want, _ := super_block.NewReplicaPlacementFromString(tc.want)
		if !got.Equals(want) {
			t.Errorf("got replica placement '%s' for volune '%s', want '%s'", got.String(), tc.vid, want.String())
		}
	}
}
