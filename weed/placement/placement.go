package placement

import (
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// PlacementPreference describes where a volume that has to leave its current
// node may land.
type PlacementPreference struct {
	// Source is the node the volume is leaving. It is never a candidate, and
	// its rack and data center are what PreferLocality measures against.
	Source string
	// DiskType is the medium the volume must land on.
	DiskType types.DiskType
	// AnchorDataCenter restricts candidates to one data center when set.
	AnchorDataCenter string
	// Exclude names nodes already spoken for by the same plan, so a caller
	// placing several copies does not stack them on one node.
	Exclude map[string]bool
	// VolumeBytes is what this move will actually consume on the destination.
	// Zero falls back to the tier's average volume size, which is all a caller
	// planning a not-yet-created volume can know.
	VolumeBytes uint64
}

// PickTarget chooses where a volume that has to leave its node should land:
// the emptiest node near it.
//
// Locality ranks first -- same rack as the source, then same data center, then
// anywhere -- because moving a volume past a node that could have held it costs
// cross-rack bandwidth for nothing. Within a locality tier the emptiest wins, so
// a burst of moves still spreads instead of filling whichever node the topology
// happens to list first. Free bytes decide when the cluster reports them and
// free slots break the tie, which also leaves servers too old to report
// filesystem bytes ordered sensibly.
//
// Locality is not optional. A caller that genuinely wants to scatter should
// widen what it passes as Source, not ask placement to forget where the volume
// came from.
//
// The chosen node's volume count is spent in topo before returning. Planning
// several moves from one snapshot otherwise sends them all to the same node,
// since each pick would see the capacity its predecessors already took. Callers
// therefore pass a snapshot they own and may mutate.
func PickTarget(topo *master_pb.TopologyInfo, pref PlacementPreference) *master_pb.DataNodeInfo {
	nodes := candidateNodes(topo, pref.AnchorDataCenter)

	var srcDc, srcRack string
	for _, n := range nodes {
		if n.info.Id == pref.Source {
			srcDc, srcRack = n.dc, n.rack
			break
		}
	}

	type candidate struct {
		node     *node
		locality int // 0 same rack, 1 same dc, 2 elsewhere
		bytes    uint64
		hasBytes bool
		slots    int64
	}

	var candidates []candidate
	for _, n := range nodes {
		if n.info.Id == pref.Source || pref.Exclude[n.info.Id] {
			continue
		}
		d := n.disk(pref.DiskType)
		if d == nil || d.VolumeCount >= d.MaxVolumeCount {
			continue
		}
		c := candidate{node: n}
		c.bytes, c.hasBytes = d.DiskFreeBytes, d.DiskTotalBytes != 0
		c.slots = d.MaxVolumeCount - d.VolumeCount
		switch {
		case srcRack != "" && n.rack == srcRack && n.dc == srcDc:
			c.locality = 0
		case srcDc != "" && n.dc == srcDc:
			c.locality = 1
		default:
			c.locality = 2
		}
		candidates = append(candidates, c)
	}
	if len(candidates) == 0 {
		return nil
	}

	// One metric decides the whole ordering. Comparing some pairs on bytes and
	// others on slots is intransitive -- with A,B reporting bytes and C not, the
	// order of A and C depends on which the sort happens to compare first -- so a
	// single node too old to report filesystem bytes puts every candidate on
	// slots rather than silently mixing the two.
	byBytes := true
	for _, c := range candidates {
		if !c.hasBytes {
			byBytes = false
			break
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.locality != b.locality {
			return a.locality < b.locality
		}
		if byBytes && a.bytes != b.bytes {
			return a.bytes > b.bytes
		}
		return a.slots > b.slots
	})

	chosen := candidates[0].node.info
	if d, ok := chosen.DiskInfos[string(pref.DiskType)]; ok && d != nil {
		d.VolumeCount++
		if d.DiskTotalBytes != 0 && d.DiskFreeBytes != 0 {
			// Spend the space this move actually takes, so the next pick from
			// this snapshot sees the node as fuller by the right amount. A big
			// volume charged the tier average would let a batch overcommit the
			// destination; a small one would divert later moves off a node that
			// still had room.
			spend := pref.VolumeBytes
			if spend == 0 {
				spend = d.DiskTotalBytes / uint64(max64(d.MaxVolumeCount, 1))
			}
			if d.DiskFreeBytes > spend {
				d.DiskFreeBytes -= spend
			} else {
				d.DiskFreeBytes = 0
			}
		}
	}
	return chosen
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// node is a volume server as a placement snapshot sees it: where it sits and
// what it has free. Deliberately smaller than the balancer's Node, which also
// carries the volumes it holds -- placement never needs those.
type node struct {
	info *master_pb.DataNodeInfo
	dc   string
	rack string
}

func (n *node) disk(diskType types.DiskType) *master_pb.DiskInfo {
	d, found := n.info.DiskInfos[string(diskType)]
	if !found {
		return nil
	}
	return d
}

// candidateNodes flattens the topology, keeping one data center when anchored.
func candidateNodes(topo *master_pb.TopologyInfo, anchorDataCenter string) (nodes []*node) {
	for _, dc := range topo.GetDataCenterInfos() {
		if anchorDataCenter != "" && dc.GetId() != anchorDataCenter {
			continue
		}
		for _, rack := range dc.GetRackInfos() {
			for _, dn := range rack.GetDataNodeInfos() {
				nodes = append(nodes, &node{info: dn, dc: dc.GetId(), rack: rack.GetId()})
			}
		}
	}
	return nodes
}
