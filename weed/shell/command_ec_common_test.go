package shell

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

var (
	topology1  = parseOutput(topoData)
	topology2  = parseOutput(topoData2)
	topologyEc = parseOutput(topoDataEc)
)

func TestEcDistribution(t *testing.T) {

	// find out all volume servers with one slot left.
	ecNodes, totalFreeEcSlots := collectEcVolumeServersByDc(topology1, "")

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
	testCases := []struct {
		topology *master_pb.TopologyInfo
		vid      string
		want     string
		wantErr  string
	}{
		{topology1, "", "", "failed to resolve replica placement for volume ID 0"},
		{topology1, "0", "", "failed to resolve replica placement for volume ID 0"},
		{topology1, "1", "100", ""},
		{topology1, "296", "100", ""},
		{topology2, "", "", "failed to resolve replica placement for volume ID 0"},
		{topology2, "19012", "", "failed to resolve replica placement for volume ID 19012"},
		{topology2, "6271", "002", ""},
		{topology2, "17932", "002", ""},
	}

	for _, tc := range testCases {
		vid, _ := needle.NewVolumeId(tc.vid)
		ecNodes, _ := collectEcVolumeServersByDc(tc.topology, "")
		got, gotErr := volumeIdToReplicaPlacement(vid, ecNodes)

		if tc.wantErr == "" && gotErr != nil {
			t.Errorf("expected no error for volume %q, got %q", tc.vid, gotErr.Error())
			continue
		}
		if tc.wantErr != "" {
			if gotErr == nil {
				t.Errorf("got no error for volume %q, expected %q", tc.vid, tc.wantErr)
				continue
			}
			if gotErr.Error() != tc.wantErr {
				t.Errorf("expected error %q for volume %q, got %q", tc.wantErr, tc.vid, gotErr.Error())
				continue
			}
		}

		if got == nil {
			if tc.want != "" {
				t.Errorf("expected replica placement %q for volume %q, got nil", tc.want, tc.vid)
			}
			continue
		}
		want, _ := super_block.NewReplicaPlacementFromString(tc.want)
		if !got.Equals(want) {
			t.Errorf("got replica placement %q for volune %q, want %q", got.String(), tc.vid, want.String())
		}
	}
}

func TestPickRackToBalanceShardsInto(t *testing.T) {
	testCases := []struct {
		topology  *master_pb.TopologyInfo
		vid       string
		wantOneOf []string
	}{
		// Non-EC volumes. We don't care about these, but the function should return all racks as a safeguard.
		{topologyEc, "", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}},
		{topologyEc, "6225", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}},
		{topologyEc, "6226", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}},
		{topologyEc, "6241", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}},
		{topologyEc, "6242", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}},
		// EC volumes.
		{topologyEc, "9577", []string{"rack1", "rack2", "rack3"}},
		{topologyEc, "10457", []string{"rack1"}},
		{topologyEc, "12737", []string{"rack2"}},
		{topologyEc, "14322", []string{"rack3"}},
	}

	for _, tc := range testCases {
		vid, _ := needle.NewVolumeId(tc.vid)
		ecNodes, _ := collectEcVolumeServersByDc(tc.topology, "")
		racks := collectRacks(ecNodes)

		locations := ecNodes
		rackToShardCount := countShardsByRack(vid, locations)
		averageShardsPerEcRack := ceilDivide(erasure_coding.TotalShardsCount, len(racks))

		got := pickRackToBalanceShardsInto(racks, rackToShardCount, nil, averageShardsPerEcRack)
		if string(got) == "" && len(tc.wantOneOf) == 0 {
			continue
		}
		found := false
		for _, want := range tc.wantOneOf {
			if got := string(got); got == want {
				found = true
				break
			}
		}
		if !(found) {
			t.Errorf("expected one of %v for volume %q, got %q", tc.wantOneOf, tc.vid, got)
		}
	}
}
