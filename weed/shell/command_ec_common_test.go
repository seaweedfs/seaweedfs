package shell

import (
	"fmt"
	"strings"
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

func errorCheck(got error, want string) error {
	if got == nil && want == "" {
		return nil
	}
	if got != nil && want == "" {
		return fmt.Errorf("expected no error, got %q", got.Error())
	}
	if got == nil && want != "" {
		return fmt.Errorf("got no error, expected %q", want)
	}
	if !strings.Contains(got.Error(), want) {
		return fmt.Errorf("expected error %q, got %q", want, got.Error())
	}
	return nil
}

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
	getDefaultReplicaPlacementOrig := getDefaultReplicaPlacement
	getDefaultReplicaPlacement = func(commandEnv *CommandEnv) (*super_block.ReplicaPlacement, error) {
		return super_block.NewReplicaPlacementFromString("123")
	}
	defer func() {
		getDefaultReplicaPlacement = getDefaultReplicaPlacementOrig
	}()

	testCases := []struct {
		topology *master_pb.TopologyInfo
		vid      string
		want     string
		wantErr  string
	}{
		{topology1, "", "", "failed to resolve replica placement"},
		{topology1, "0", "", "failed to resolve replica placement"},
		{topology1, "1", "100", ""},
		{topology1, "296", "100", ""},
		{topology2, "", "", "failed to resolve replica placement"},
		{topology2, "19012", "", "failed to resolve replica placement"},
		{topology2, "6271", "002", ""},
		{topology2, "17932", "002", ""},
		{topologyEc, "", "", "failed to resolve replica placement"},
		{topologyEc, "0", "", "failed to resolve replica placement"},
		{topologyEc, "6225", "002", ""},
		{topologyEc, "6241", "002", ""},
		{topologyEc, "9577", "123", ""},  // EC volume
		{topologyEc, "12737", "123", ""}, // EC volume
	}

	for _, tc := range testCases {
		commandEnv := &CommandEnv{}
		vid, _ := needle.NewVolumeId(tc.vid)
		ecNodes, _ := collectEcVolumeServersByDc(tc.topology, "")
		got, gotErr := volumeIdToReplicaPlacement(commandEnv, vid, ecNodes)

		if err := errorCheck(gotErr, tc.wantErr); err != nil {
			t.Errorf("volume %q: %s", tc.vid, err.Error())
			continue
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

		got, gotErr := pickRackToBalanceShardsInto(racks, rackToShardCount, nil, averageShardsPerEcRack)
		if gotErr != nil {
			t.Errorf("volume %q: %s", tc.vid, gotErr.Error())
			continue
		}

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
func TestPickEcNodeToBalanceShardsInto(t *testing.T) {
	testCases := []struct {
		topology  *master_pb.TopologyInfo
		nodeId    string
		vid       string
		wantOneOf []string
		wantErr   string
	}{
		{topologyEc, "", "", nil, "INTERNAL: missing source nodes"},
		{topologyEc, "idontexist", "12737", nil, "INTERNAL: missing source nodes"},
		// Non-EC nodes. We don't care about these, but the function should return all available target nodes as a safeguard.
		{
			topologyEc, "172.19.0.10:8702", "6225", []string{
				"172.19.0.13:8701", "172.19.0.14:8711", "172.19.0.16:8704", "172.19.0.17:8703",
				"172.19.0.19:8700", "172.19.0.20:8706", "172.19.0.21:8710", "172.19.0.3:8708",
				"172.19.0.4:8707", "172.19.0.5:8705", "172.19.0.6:8713", "172.19.0.8:8709",
				"172.19.0.9:8712"},
			"",
		},
		{
			topologyEc, "172.19.0.8:8709", "6226", []string{
				"172.19.0.10:8702", "172.19.0.13:8701", "172.19.0.14:8711", "172.19.0.16:8704",
				"172.19.0.17:8703", "172.19.0.19:8700", "172.19.0.20:8706", "172.19.0.21:8710",
				"172.19.0.3:8708", "172.19.0.4:8707", "172.19.0.5:8705", "172.19.0.6:8713",
				"172.19.0.9:8712"},
			"",
		},
		// EC volumes.
		{topologyEc, "172.19.0.10:8702", "14322", []string{
			"172.19.0.14:8711", "172.19.0.5:8705", "172.19.0.6:8713"},
			""},
		{topologyEc, "172.19.0.13:8701", "10457", []string{
			"172.19.0.10:8702", "172.19.0.6:8713"},
			""},
		{topologyEc, "172.19.0.17:8703", "12737", []string{
			"172.19.0.13:8701"},
			""},
		{topologyEc, "172.19.0.20:8706", "14322", []string{
			"172.19.0.14:8711", "172.19.0.5:8705", "172.19.0.6:8713"},
			""},
	}

	for _, tc := range testCases {
		vid, _ := needle.NewVolumeId(tc.vid)
		allEcNodes, _ := collectEcVolumeServersByDc(tc.topology, "")

		// Resolve target node by name
		var ecNode *EcNode
		for _, n := range allEcNodes {
			if n.info.Id == tc.nodeId {
				ecNode = n
				break
			}
		}

		averageShardsPerEcNode := 5
		got, gotErr := pickEcNodeToBalanceShardsInto(vid, ecNode, allEcNodes, nil, averageShardsPerEcNode)
		if err := errorCheck(gotErr, tc.wantErr); err != nil {
			t.Errorf("node %q, volume %q: %s", tc.nodeId, tc.vid, err.Error())
			continue
		}

		if got == nil {
			if len(tc.wantOneOf) == 0 {
				continue
			}
			t.Errorf("node %q, volume %q: got no node, want %q", tc.nodeId, tc.vid, tc.wantOneOf)
			continue
		}
		found := false
		for _, want := range tc.wantOneOf {
			if got := got.info.Id; got == want {
				found = true
				break
			}
		}
		if !(found) {
			t.Errorf("expected one of %v for volume %q, got %q", tc.wantOneOf, tc.vid, got.info.Id)
		}
	}
}
