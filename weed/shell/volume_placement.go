package shell

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
	nodes := collectVolumeServersByDcRackNode(topo, pref.AnchorDataCenter, "", "")

	var srcDc, srcRack string
	for _, n := range nodes {
		if n.info.Id == pref.Source {
			srcDc, srcRack = n.dc, n.rack
			break
		}
	}

	type candidate struct {
		node     *Node
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
		if !n.hasFreeVolumeSlot(pref.DiskType) {
			continue
		}
		c := candidate{node: n}
		_, free, ok := n.diskBytes(pref.DiskType)
		c.bytes, c.hasBytes = free, ok
		if d, found := n.info.DiskInfos[string(pref.DiskType)]; found && d != nil {
			c.slots = d.MaxVolumeCount - d.VolumeCount
		}
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

	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.locality != b.locality {
			return a.locality < b.locality
		}
		// A node that reports filesystem bytes is compared on them; one that does
		// not falls back to slots rather than being read as having zero free.
		if a.hasBytes && b.hasBytes && a.bytes != b.bytes {
			return a.bytes > b.bytes
		}
		return a.slots > b.slots
	})

	chosen := candidates[0].node.info
	if d, ok := chosen.DiskInfos[string(pref.DiskType)]; ok && d != nil {
		d.VolumeCount++
		if d.DiskTotalBytes != 0 && d.DiskFreeBytes != 0 {
			// Spend the slot's worth of space too, so the next pick from this
			// snapshot sees the node as fuller by the same amount.
			perVolume := d.DiskTotalBytes / uint64(max64(d.MaxVolumeCount, 1))
			if d.DiskFreeBytes > perVolume {
				d.DiskFreeBytes -= perVolume
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
