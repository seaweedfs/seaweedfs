package shell

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

type testMoveCase struct {
	name           string
	replication    string
	replicas       []*VolumeReplica
	sourceLocation location
	targetLocation location
	expected       bool
}

func TestIsGoodMove(t *testing.T) {

	var tests = []testMoveCase{

		{
			name:        "test 100 move to wrong data centers",
			replication: "100",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
			targetLocation: location{"dc2", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:       false,
		},

		{
			name:        "test 100 move to spread into proper data centers",
			replication: "100",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:       true,
		},

		{
			name:        "test move to the same node",
			replication: "001",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			expected:       false,
		},

		{
			name:        "test move to the same rack, but existing node",
			replication: "001",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
			expected:       false,
		},

		{
			name:        "test move to the same rack, a new node",
			replication: "001",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:       true,
		},

		{
			name:        "test 010 move all to the same rack",
			replication: "010",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:       false,
		},

		{
			name:        "test 010 move to spread racks",
			replication: "010",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:       true,
		},

		{
			name:        "test 010 move to spread racks",
			replication: "010",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:       true,
		},

		{
			name:        "test 011 switch which rack has more replicas",
			replication: "011",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
			targetLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:       true,
		},

		{
			name:        "test 011 move the lonely replica to another racks",
			replication: "011",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			sourceLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
			targetLocation: location{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:       true,
		},

		{
			name:        "test 011 move to wrong racks",
			replication: "011",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
			targetLocation: location{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:       false,
		},

		{
			name:        "test 011 move all to the same rack",
			replication: "011",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			sourceLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
			targetLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:       false,
		},

		{
			// rep 001 allows two copies in one rack; replica-placement alone would
			// permit this, but the target shares a host with another replica, so the
			// machine anti-affinity must reject it.
			name:        "test 001 reject move onto a machine already holding a replica",
			replication: "001",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "10.0.0.1:8080"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "10.0.0.2:8080"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "10.0.0.2:8080"}},
			targetLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "10.0.0.1:8081"}},
			expected:       false,
		},

		{
			name:        "test 001 allow move onto a different machine in the rack",
			replication: "001",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "10.0.0.1:8080"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "10.0.0.2:8080"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "10.0.0.2:8080"}},
			targetLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "10.0.0.3:8080"}},
			expected:       true,
		},
	}

	for _, tt := range tests {
		replicaPlacement, _ := super_block.NewReplicaPlacementFromString(tt.replication)
		println("replication:", tt.replication, "expected", tt.expected, "name:", tt.name)
		sourceNode := &Node{
			info: tt.sourceLocation.dataNode,
			dc:   tt.sourceLocation.dc,
			rack: tt.sourceLocation.rack,
		}
		targetNode := &Node{
			info: tt.targetLocation.dataNode,
			dc:   tt.targetLocation.dc,
			rack: tt.targetLocation.rack,
		}
		if isGoodMove(replicaPlacement, tt.replicas, sourceNode, targetNode) != tt.expected {
			t.Errorf("%s: expect %v move from %v to %s, replication:%v",
				tt.name, tt.expected, tt.sourceLocation, tt.targetLocation, tt.replication)
		}
	}

}

func TestBalance(t *testing.T) {
	topologyInfo := parseOutput(topoData)
	volumeServers := collectVolumeServersByDcRackNode(topologyInfo, "", "", "")
	volumeReplicas, _ := collectVolumeReplicaLocations(topologyInfo)
	diskTypes := collectVolumeDiskTypes(topologyInfo)
	c := &commandVolumeBalance{}
	if err := c.balanceVolumeServers(diskTypes, volumeReplicas, volumeServers, nil, "ALL_COLLECTIONS"); err != nil {
		t.Errorf("balance: %v", err)
	}

}

// Regression test: a freshly added empty volume server must end up sharing the
// data roughly evenly, not having every volume drained onto it. Before the fix,
// adjustAfterMove never updated the per-disk VolumeInfos that the density-based
// capacity function reads, so the planner saw a stale topology and moved every
// volume from the full node onto the empty one.
func TestBalanceDoesNotDrainOntoOneNode(t *testing.T) {
	const mb = 1024 * 1024
	volumeSizeLimitMb := uint64(100)

	makeNode := func(id string, volumes []*master_pb.VolumeInformationMessage) *Node {
		return &Node{
			info: &master_pb.DataNodeInfo{
				Id: id,
				DiskInfos: map[string]*master_pb.DiskInfo{
					"": {
						MaxVolumeCount: 10,
						VolumeCount:    int64(len(volumes)),
						VolumeInfos:    volumes,
					},
				},
			},
			dc:   "dc1",
			rack: "rack1",
		}
	}

	var fullVolumes []*master_pb.VolumeInformationMessage
	for id := uint32(1); id <= 6; id++ {
		fullVolumes = append(fullVolumes, &master_pb.VolumeInformationMessage{Id: id, Size: 95 * mb})
	}
	fullNode := makeNode("full", fullVolumes)
	emptyNode := makeNode("empty", nil)
	nodes := []*Node{fullNode, emptyNode}

	volumeReplicas := map[uint32][]*VolumeReplica{}
	for _, v := range fullVolumes {
		loc := newLocation("dc1", "rack1", fullNode.info)
		volumeReplicas[v.Id] = []*VolumeReplica{{location: &loc, info: v}}
	}

	for _, n := range nodes {
		n.selectVolumes(func(v *master_pb.VolumeInformationMessage) bool { return true })
	}

	c := &commandVolumeBalance{volumeSizeLimitMb: volumeSizeLimitMb}
	if err := c.balanceSelectedVolume(types.HardDriveType, volumeReplicas, nodes, sortWritableVolumes); err != nil {
		t.Fatalf("balanceSelectedVolume: %v", err)
	}

	fullCount := len(fullNode.info.DiskInfos[""].VolumeInfos)
	emptyCount := len(emptyNode.info.DiskInfos[""].VolumeInfos)
	if fullCount == 0 || emptyCount == 0 {
		t.Fatalf("expected volumes spread across both nodes, got full=%d empty=%d", fullCount, emptyCount)
	}
	if diff := fullCount - emptyCount; diff > 1 || diff < -1 {
		t.Fatalf("expected balanced distribution within one volume, got full=%d empty=%d", fullCount, emptyCount)
	}
}

// byteFullNode physically holds twice the data of mediumNode but its
// MaxVolumeCount was configured too high for its disk, so the default slot-density
// metric ranks it as the emptiest server and drains mediumNode onto it (verified
// by the default-mode sub-assertion below). With -byDiskUsage the ranking is by
// actual data held, so the fuller server becomes the move source and the two end
// up evenly distributed by data.
func TestBalanceByDiskUsage(t *testing.T) {
	const mb = 1024 * 1024
	volumeSizeLimitMb := uint64(100)

	makeNode := func(id string, maxVolumeCount int64, volumes []*master_pb.VolumeInformationMessage) *Node {
		return &Node{
			info: &master_pb.DataNodeInfo{
				Id: id,
				DiskInfos: map[string]*master_pb.DiskInfo{
					"": {
						MaxVolumeCount: maxVolumeCount,
						VolumeCount:    int64(len(volumes)),
						VolumeInfos:    volumes,
					},
				},
			},
			dc:   "dc1",
			rack: "rack1",
		}
	}

	mkVolumes := func(start, n uint32) []*master_pb.VolumeInformationMessage {
		var vs []*master_pb.VolumeInformationMessage
		for id := start; id < start+n; id++ {
			vs = append(vs, &master_pb.VolumeInformationMessage{Id: id, Size: 95 * mb})
		}
		return vs
	}

	dataBytes := func(n *Node) uint64 {
		var sum uint64
		for _, v := range n.info.DiskInfos[""].VolumeInfos {
			sum += v.Size
		}
		return sum
	}
	volumeCount := func(n *Node) int { return len(n.info.DiskInfos[""].VolumeInfos) }

	setup := func() (*Node, *Node, []*Node, map[uint32][]*VolumeReplica) {
		byteFullNode := makeNode("byte-full", 1000, mkVolumes(1, 20))
		mediumNode := makeNode("half-full", 30, mkVolumes(101, 10))
		nodes := []*Node{byteFullNode, mediumNode}
		volumeReplicas := map[uint32][]*VolumeReplica{}
		for _, n := range nodes {
			for _, v := range n.info.DiskInfos[""].VolumeInfos {
				loc := newLocation("dc1", "rack1", n.info)
				volumeReplicas[v.Id] = []*VolumeReplica{{location: &loc, info: v}}
			}
			n.selectVolumes(func(v *master_pb.VolumeInformationMessage) bool { return true })
		}
		return byteFullNode, mediumNode, nodes, volumeReplicas
	}

	// Default mode: the over-configured MaxVolumeCount makes the fuller server the
	// target, so it gains even more data. This is the reported pathology.
	byteFullNode, _, nodes, volumeReplicas := setup()
	before := dataBytes(byteFullNode)
	c := &commandVolumeBalance{volumeSizeLimitMb: volumeSizeLimitMb}
	if err := c.balanceSelectedVolume(types.HardDriveType, volumeReplicas, nodes, sortWritableVolumes); err != nil {
		t.Fatalf("default balanceSelectedVolume: %v", err)
	}
	if dataBytes(byteFullNode) <= before {
		t.Fatalf("expected default mode to pile onto the fuller server (the bug), but it did not: %d MB -> %d MB", before/mb, dataBytes(byteFullNode)/mb)
	}

	// -byDiskUsage: the fuller server is recognized as full, so it sheds data and
	// the two servers converge to an even data distribution.
	byteFullNode, mediumNode, nodes, volumeReplicas := setup()
	before = dataBytes(byteFullNode)
	c = &commandVolumeBalance{volumeSizeLimitMb: volumeSizeLimitMb, byDiskUsage: true}
	if err := c.balanceSelectedVolume(types.HardDriveType, volumeReplicas, nodes, sortWritableVolumes); err != nil {
		t.Fatalf("byDiskUsage balanceSelectedVolume: %v", err)
	}
	if got := dataBytes(byteFullNode); got >= before {
		t.Fatalf("-byDiskUsage should drain the fuller server, but it did not shrink: %d MB -> %d MB", before/mb, got/mb)
	}
	if diff := volumeCount(byteFullNode) - volumeCount(mediumNode); diff > 1 || diff < -1 {
		t.Fatalf("-byDiskUsage should even out the data, got byte-full=%d half-full=%d volumes",
			volumeCount(byteFullNode), volumeCount(mediumNode))
	}
}

// makeByteNode builds a single-disk Node carrying physical disk bytes, for the
// disk-fullness gate tests.
func makeByteNode(id string, maxVolumeCount int64, totalBytes, freeBytes uint64, volumes []*master_pb.VolumeInformationMessage) *Node {
	return &Node{
		info: &master_pb.DataNodeInfo{
			Id: id,
			DiskInfos: map[string]*master_pb.DiskInfo{
				"": {
					MaxVolumeCount: maxVolumeCount,
					VolumeCount:    int64(len(volumes)),
					VolumeInfos:    volumes,
					DiskTotalBytes: totalBytes,
					DiskFreeBytes:  freeBytes,
				},
			},
		},
		dc:   "dc1",
		rack: "rack1",
	}
}

func mkByteVolumes(start, n uint32, sizeMb uint64) []*master_pb.VolumeInformationMessage {
	const mb = 1024 * 1024
	var vs []*master_pb.VolumeInformationMessage
	for id := start; id < start+n; id++ {
		vs = append(vs, &master_pb.VolumeInformationMessage{Id: id, Size: sizeMb * mb})
	}
	return vs
}

func runBalance(t *testing.T, c *commandVolumeBalance, nodes []*Node) {
	t.Helper()
	volumeReplicas := map[uint32][]*VolumeReplica{}
	for _, n := range nodes {
		for _, v := range n.info.DiskInfos[""].VolumeInfos {
			loc := newLocation("dc1", "rack1", n.info)
			volumeReplicas[v.Id] = []*VolumeReplica{{location: &loc, info: v}}
		}
		n.selectVolumes(func(v *master_pb.VolumeInformationMessage) bool { return true })
	}
	if err := c.balanceSelectedVolume(types.HardDriveType, volumeReplicas, nodes, sortWritableVolumes); err != nil {
		t.Fatalf("balanceSelectedVolume: %v", err)
	}
}

func volCount(n *Node) int { return len(n.info.DiskInfos[""].VolumeInfos) }

// Issue #10160 root-cause guard: a server whose physical disk is near full must
// not be chosen as a move target, even when an over-configured maxVolumeCount
// makes the slot-density metric rank it as the emptiest node. The gate is on by
// default, disables at 0, and falls back to slot-only behavior when the server
// does not report disk bytes.
func TestBalanceSkipsPhysicallyFullTarget(t *testing.T) {
	const gb = 1024 * 1024 * 1024
	volumeSizeLimitMb := uint64(1024) // 1 GiB volumes

	// source: tight slot budget, so it ranks as the move source.
	// fullDisk: huge maxVolumeCount (mis-set) but disk is physically 96% full.
	build := func(fullDiskTotal, fullDiskFree uint64) (*Node, *Node) {
		source := makeByteNode("source", 10, 1000*gb, 200*gb, mkByteVolumes(1, 8, 1000))
		fullDisk := makeByteNode("disk-full", 1000, fullDiskTotal, fullDiskFree, mkByteVolumes(101, 1, 1000))
		return source, fullDisk
	}

	// Gate on (default 90%): the 96%-full server is never a target -> no move.
	source, fullDisk := build(1000*gb, 40*gb)
	c := &commandVolumeBalance{volumeSizeLimitMb: volumeSizeLimitMb, diskUsageHighWaterPercent: 90}
	runBalance(t, c, []*Node{source, fullDisk})
	if got := volCount(fullDisk); got != 1 {
		t.Fatalf("gate on: expected no move onto 96%%-full server, got %d volumes (was 1)", got)
	}

	// Gate off (0): the old behavior piles onto the byte-full server.
	source, fullDisk = build(1000*gb, 40*gb)
	c = &commandVolumeBalance{volumeSizeLimitMb: volumeSizeLimitMb, diskUsageHighWaterPercent: 0}
	runBalance(t, c, []*Node{source, fullDisk})
	if got := volCount(fullDisk); got <= 1 {
		t.Fatalf("gate off: expected moves onto the byte-full server (the bug), got %d volumes", got)
	}

	// Fallback: server reports no disk bytes (DiskTotalBytes==0) -> not gated.
	source, fullDisk = build(0, 0)
	c = &commandVolumeBalance{volumeSizeLimitMb: volumeSizeLimitMb, diskUsageHighWaterPercent: 90}
	runBalance(t, c, []*Node{source, fullDisk})
	if got := volCount(fullDisk); got <= 1 {
		t.Fatalf("fallback: expected slot-only behavior to move onto the server, got %d volumes", got)
	}
}

// With the gate on, balancing steers moves to a physically empty disk and away
// from a physically full one when both look equally empty by slot density.
func TestBalanceGateSteersToEmptierDisk(t *testing.T) {
	const gb = 1024 * 1024 * 1024
	volumeSizeLimitMb := uint64(1024)

	source := makeByteNode("source", 10, 1000*gb, 300*gb, mkByteVolumes(1, 8, 1000))
	fullDisk := makeByteNode("disk-full", 1000, 1000*gb, 40*gb, mkByteVolumes(101, 1, 1000))    // 96% used -> gated
	emptyDisk := makeByteNode("disk-empty", 1000, 1000*gb, 900*gb, mkByteVolumes(201, 1, 1000)) // 10% used -> ok

	c := &commandVolumeBalance{volumeSizeLimitMb: volumeSizeLimitMb, diskUsageHighWaterPercent: 90}
	runBalance(t, c, []*Node{source, fullDisk, emptyDisk})

	if got := volCount(fullDisk); got != 1 {
		t.Fatalf("expected no move onto the physically full disk, got %d volumes (was 1)", got)
	}
	if got := volCount(emptyDisk); got <= 1 {
		t.Fatalf("expected moves onto the physically empty disk, got %d volumes", got)
	}
}

// volumesPerExec caps the number of moves performed in a single execution.
func TestBalanceVolumesPerExec(t *testing.T) {
	const mb = 1024 * 1024
	volumeSizeLimitMb := uint64(100)

	makeNode := func(id string, volumes []*master_pb.VolumeInformationMessage) *Node {
		return &Node{
			info: &master_pb.DataNodeInfo{
				Id: id,
				DiskInfos: map[string]*master_pb.DiskInfo{
					"": {
						MaxVolumeCount: 10,
						VolumeCount:    int64(len(volumes)),
						VolumeInfos:    volumes,
					},
				},
			},
			dc:   "dc1",
			rack: "rack1",
		}
	}

	var fullVolumes []*master_pb.VolumeInformationMessage
	for id := uint32(1); id <= 6; id++ {
		fullVolumes = append(fullVolumes, &master_pb.VolumeInformationMessage{Id: id, Size: 95 * mb})
	}
	fullNode := makeNode("full", fullVolumes)
	emptyNode := makeNode("empty", nil)
	nodes := []*Node{fullNode, emptyNode}

	volumeReplicas := map[uint32][]*VolumeReplica{}
	for _, v := range fullVolumes {
		loc := newLocation("dc1", "rack1", fullNode.info)
		volumeReplicas[v.Id] = []*VolumeReplica{{location: &loc, info: v}}
	}

	for _, n := range nodes {
		n.selectVolumes(func(v *master_pb.VolumeInformationMessage) bool { return true })
	}

	c := &commandVolumeBalance{volumeSizeLimitMb: volumeSizeLimitMb, volumesPerExec: 1}
	if err := c.balanceSelectedVolume(types.HardDriveType, volumeReplicas, nodes, sortWritableVolumes); err != nil {
		t.Fatalf("balanceSelectedVolume: %v", err)
	}

	if c.movedCount != 1 {
		t.Fatalf("expected exactly 1 move with volumesPerExec=1, got %d", c.movedCount)
	}
	if got := len(emptyNode.info.DiskInfos[""].VolumeInfos); got != 1 {
		t.Fatalf("expected empty node to receive exactly 1 volume, got %d", got)
	}
}

func TestVolumeSelection(t *testing.T) {
	topologyInfo := parseOutput(topoData)

	vids, err := collectVolumeIdsForTierChange(topologyInfo, 1000, types.ToDiskType(types.HddType), "", "", 20.0, 0)
	if err != nil {
		t.Errorf("collectVolumeIdsForTierChange: %v", err)
	}
	assert.Equal(t, 378, len(vids))

}

func TestDeleteEmptySelection(t *testing.T) {
	topologyInfo := parseOutput(topoData)

	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				if v.Size <= super_block.SuperBlockSize && v.ModifiedAtSecond > 0 {
					fmt.Printf("empty volume %d from %s\n", v.Id, dn.Id)
				}
			}
		}
	})

}

func TestSplitCSVSet(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want map[string]bool
	}{
		{"empty input is empty set (no filter)", "", map[string]bool{}},
		{"whitespace only is empty set (no filter)", "   ", map[string]bool{}},
		{"commas only is empty set (no filter)", ",,,", map[string]bool{}},
		{"whitespace and commas only is empty set (no filter)", " , , ", map[string]bool{}},
		{"single", "rack1", map[string]bool{"rack1": true}},
		{"multi", "rack1,rack2", map[string]bool{"rack1": true, "rack2": true}},
		{"trims whitespace", " rack1 , rack2 ", map[string]bool{"rack1": true, "rack2": true}},
		{"skips empty items", "rack1,,rack2,", map[string]bool{"rack1": true, "rack2": true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, splitCSVSet(tc.in))
		})
	}
}

// Regression test for the rack/node filter that previously used
// strings.Contains, which falsely matched any id that was a substring of the
// user-supplied flag value (e.g. -racks=rack10 also matched rack1).
func TestCollectVolumeServersByDcRackNode_RackFilter(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{
				{Id: "rack1", DataNodeInfos: []*master_pb.DataNodeInfo{{Id: "n1"}}},
				{Id: "rack10", DataNodeInfos: []*master_pb.DataNodeInfo{{Id: "n10"}}},
				{Id: "rack2", DataNodeInfos: []*master_pb.DataNodeInfo{{Id: "n2"}}},
			},
		}},
	}

	got := collectVolumeServersByDcRackNode(topo, "", "rack10", "")
	if assert.Len(t, got, 1, "-racks=rack10 should not match rack1") {
		assert.Equal(t, "rack10", got[0].rack)
	}

	got = collectVolumeServersByDcRackNode(topo, "", "rack1,rack2", "")
	racks := map[string]bool{}
	for _, n := range got {
		racks[n.rack] = true
	}
	assert.Equal(t, map[string]bool{"rack1": true, "rack2": true}, racks,
		"-racks=rack1,rack2 should match exactly those two, not rack10")
}

// Regression test for the -nodes filter, mirroring the rack-filter case.
// Uses bare ids (no :port suffix) so that "node1" is a true substring of
// "node10": under the old strings.Contains implementation,
// -nodes=node10 wrongly included node1 as well.
func TestCollectVolumeServersByDcRackNode_NodeFilter(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{
					{Id: "node1"},
					{Id: "node10"},
					{Id: "node2"},
				},
			}},
		}},
	}

	got := collectVolumeServersByDcRackNode(topo, "", "", "node10")
	if assert.Len(t, got, 1, "-nodes=node10 should not match node1") {
		assert.Equal(t, "node10", got[0].info.Id)
	}

	got = collectVolumeServersByDcRackNode(topo, "", "", "node1,node2")
	nodes := map[string]bool{}
	for _, n := range got {
		nodes[n.info.Id] = true
	}
	assert.Equal(t, map[string]bool{"node1": true, "node2": true}, nodes,
		"-nodes=node1,node2 should match exactly those two, not node10")
}
