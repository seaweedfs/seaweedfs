package shell

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
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

func TestCollectCollectionsForVolumeIds(t *testing.T) {
	testCases := []struct {
		topology *master_pb.TopologyInfo
		vids     []needle.VolumeId
		want     []string
	}{
		// normal volumes
		{topology1, nil, nil},
		{topology1, []needle.VolumeId{}, nil},
		{topology1, []needle.VolumeId{needle.VolumeId(9999)}, nil},
		{topology1, []needle.VolumeId{needle.VolumeId(2)}, []string{""}},
		{topology1, []needle.VolumeId{needle.VolumeId(2), needle.VolumeId(272)}, []string{"", "collection2"}},
		{topology1, []needle.VolumeId{needle.VolumeId(2), needle.VolumeId(272), needle.VolumeId(299)}, []string{"", "collection2"}},
		{topology1, []needle.VolumeId{needle.VolumeId(272), needle.VolumeId(299), needle.VolumeId(95)}, []string{"collection1", "collection2"}},
		{topology1, []needle.VolumeId{needle.VolumeId(272), needle.VolumeId(299), needle.VolumeId(95), needle.VolumeId(51)}, []string{"collection1", "collection2"}},
		{topology1, []needle.VolumeId{needle.VolumeId(272), needle.VolumeId(299), needle.VolumeId(95), needle.VolumeId(51), needle.VolumeId(15)}, []string{"collection0", "collection1", "collection2"}},
		// EC volumes
		{topology2, []needle.VolumeId{needle.VolumeId(9577)}, []string{"s3qldata"}},
		{topology2, []needle.VolumeId{needle.VolumeId(9577), needle.VolumeId(12549)}, []string{"s3qldata"}},
		// normal + EC volumes
		{topology2, []needle.VolumeId{needle.VolumeId(18111)}, []string{"s3qldata"}},
		{topology2, []needle.VolumeId{needle.VolumeId(8677)}, []string{"s3qldata"}},
		{topology2, []needle.VolumeId{needle.VolumeId(18111), needle.VolumeId(8677)}, []string{"s3qldata"}},
	}

	for _, tc := range testCases {
		got := collectCollectionsForVolumeIds(tc.topology, tc.vids)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("for %v: got %v, want %v", tc.vids, got, tc.want)
		}
	}
}

func TestParseReplicaPlacementArg(t *testing.T) {
	getDefaultReplicaPlacementOrig := getDefaultReplicaPlacement
	getDefaultReplicaPlacement = func(commandEnv *CommandEnv) (*super_block.ReplicaPlacement, error) {
		return super_block.NewReplicaPlacementFromString("123")
	}
	defer func() {
		getDefaultReplicaPlacement = getDefaultReplicaPlacementOrig
	}()

	testCases := []struct {
		argument string
		want     string
		wantErr  string
	}{
		{"lalala", "lal", "unexpected replication type"},
		{"", "123", ""},
		{"021", "021", ""},
	}

	for _, tc := range testCases {
		commandEnv := &CommandEnv{}
		got, gotErr := parseReplicaPlacementArg(commandEnv, tc.argument)

		if err := errorCheck(gotErr, tc.wantErr); err != nil {
			t.Errorf("argument %q: %s", tc.argument, err.Error())
			continue
		}

		want, _ := super_block.NewReplicaPlacementFromString(tc.want)
		if !got.Equals(want) {
			t.Errorf("got replica placement %q, want %q", got.String(), want.String())
		}
	}
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

func TestPickRackToBalanceShardsInto(t *testing.T) {
	testCases := []struct {
		topology         *master_pb.TopologyInfo
		vid              string
		replicaPlacement string
		wantOneOf        []string
		wantErr          string
	}{
		// Non-EC volumes. We don't care about these, but the function should return all racks as a safeguard.
		{topologyEc, "", "123", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}, ""},
		{topologyEc, "6225", "123", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}, ""},
		{topologyEc, "6226", "123", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}, ""},
		{topologyEc, "6241", "123", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}, ""},
		{topologyEc, "6242", "123", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}, ""},
		// EC volumes.
		{topologyEc, "9577", "", nil, "shards 1 > replica placement limit for other racks (0)"},
		{topologyEc, "9577", "111", []string{"rack1", "rack2", "rack3"}, ""},
		{topologyEc, "9577", "222", []string{"rack1", "rack2", "rack3"}, ""},
		{topologyEc, "10457", "222", []string{"rack1"}, ""},
		{topologyEc, "12737", "222", []string{"rack2"}, ""},
		{topologyEc, "14322", "222", []string{"rack3"}, ""},
	}

	for _, tc := range testCases {
		vid, _ := needle.NewVolumeId(tc.vid)
		ecNodes, _ := collectEcVolumeServersByDc(tc.topology, "")
		rp, _ := super_block.NewReplicaPlacementFromString(tc.replicaPlacement)

		ecb := &ecBalancer{
			ecNodes:          ecNodes,
			replicaPlacement: rp,
		}

		racks := ecb.racks()
		rackToShardCount := countShardsByRack(vid, ecNodes)

		got, gotErr := ecb.pickRackToBalanceShardsInto(racks, rackToShardCount)
		if err := errorCheck(gotErr, tc.wantErr); err != nil {
			t.Errorf("volume %q: %s", tc.vid, err.Error())
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

		ecb := &ecBalancer{
			ecNodes: allEcNodes,
		}

		// Resolve target node by name
		var ecNode *EcNode
		for _, n := range allEcNodes {
			if n.info.Id == tc.nodeId {
				ecNode = n
				break
			}
		}

		got, gotErr := ecb.pickEcNodeToBalanceShardsInto(vid, ecNode, allEcNodes)
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

func TestCountFreeShardSlots(t *testing.T) {
	testCases := []struct {
		name     string
		topology *master_pb.TopologyInfo
		diskType types.DiskType
		want     map[string]int
	}{
		{
			name:     "topology #1, free HDD shards",
			topology: topology1,
			diskType: types.HardDriveType,
			want: map[string]int{
				"192.168.1.1:8080": 17330,
				"192.168.1.2:8080": 1540,
				"192.168.1.4:8080": 1900,
				"192.168.1.5:8080": 27010,
				"192.168.1.6:8080": 17420,
			},
		},
		{
			name:     "topology #1, no free SSD shards available",
			topology: topology1,
			diskType: types.SsdType,
			want: map[string]int{
				"192.168.1.1:8080": 0,
				"192.168.1.2:8080": 0,
				"192.168.1.4:8080": 0,
				"192.168.1.5:8080": 0,
				"192.168.1.6:8080": 0,
			},
		},
		{
			name:     "topology #2, no negative free HDD shards",
			topology: topology2,
			diskType: types.HardDriveType,
			want: map[string]int{
				"172.19.0.3:8708":  0,
				"172.19.0.4:8707":  8,
				"172.19.0.5:8705":  58,
				"172.19.0.6:8713":  39,
				"172.19.0.8:8709":  8,
				"172.19.0.9:8712":  0,
				"172.19.0.10:8702": 0,
				"172.19.0.13:8701": 0,
				"172.19.0.14:8711": 0,
				"172.19.0.16:8704": 89,
				"172.19.0.17:8703": 0,
				"172.19.0.19:8700": 9,
				"172.19.0.20:8706": 0,
				"172.19.0.21:8710": 9,
			},
		},
		{
			name:     "topology #2, no free SSD shards available",
			topology: topology2,
			diskType: types.SsdType,
			want: map[string]int{
				"172.19.0.10:8702": 0,
				"172.19.0.13:8701": 0,
				"172.19.0.14:8711": 0,
				"172.19.0.16:8704": 0,
				"172.19.0.17:8703": 0,
				"172.19.0.19:8700": 0,
				"172.19.0.20:8706": 0,
				"172.19.0.21:8710": 0,
				"172.19.0.3:8708":  0,
				"172.19.0.4:8707":  0,
				"172.19.0.5:8705":  0,
				"172.19.0.6:8713":  0,
				"172.19.0.8:8709":  0,
				"172.19.0.9:8712":  0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := map[string]int{}
			eachDataNode(tc.topology, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
				got[dn.Id] = countFreeShardSlots(dn, tc.diskType)
			})

			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
