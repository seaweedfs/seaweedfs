package topology

import (
	"errors"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// Volume growth picks data centers and racks weighted by free slots, so an
// emptier location still takes more of the new volumes. The server inside a
// rack is picked differently: among the servers with a free slot, one of
// those holding the fewest writable volumes of the layout per disk, weighted
// by free slots among them. Picking every server independently by free slots
// stacks several writable volumes of one collection on some disks while most
// get none, and their writes pile up on those disks.

// withLayoutLoad returns a copy of option carrying the writable volumes of
// its layout on each data node. The copy keeps the load out of the option the
// caller holds, which grow requests compare with reflect.DeepEqual.
func (o *VolumeGrowOption) withLayoutLoad(topo *Topology) *VolumeGrowOption {
	placed := *o
	placed.layoutLoad = make(map[*DataNode]int)
	if o.ReplicaPlacement == nil {
		return &placed
	}
	if vl := topo.FindVolumeLayout(o.Collection, o.ReplicaPlacement, o.Ttl, o.DiskType); vl != nil {
		placed.layoutLoad = vl.writableCountByNode()
	}
	return &placed
}

// writableCountByNode counts, for each data node, the writable volumes of this
// layout it holds a replica of.
func (vl *VolumeLayout) writableCountByNode() map[*DataNode]int {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()
	counts := make(map[*DataNode]int)
	for _, vid := range vl.writables {
		if locations, ok := vl.vid2location[vid]; ok {
			for _, dn := range locations.list {
				counts[dn]++
			}
		}
	}
	return counts
}

// diskCount is the number of disks of diskType on the node that can take new
// volumes, from the heartbeat DiskTags. An unavailable disk, or one low on
// space with no volumes, reports no volume slots and is left out. A node that
// reported none counts as one disk.
func (dn *DataNode) diskCount(diskType types.DiskType) int {
	dn.RLock()
	defer dn.RUnlock()
	count := 0
	for _, meta := range dn.diskMetas {
		if meta.diskType == diskType && meta.maxVolumeCount > 0 {
			count++
		}
	}
	return max(count, 1)
}

// hasRefused reports whether data node n refused an allocation earlier in
// this grow request.
func (o *VolumeGrowOption) hasRefused(n *NodeImpl) bool {
	if len(o.refused) == 0 {
		return false
	}
	dn, ok := n.value.(*DataNode)
	return ok && o.refused[dn]
}

// lessLoaded reports whether a holds fewer writable volumes of the layout per
// disk than b. Only data nodes carry a load, so for racks and data centers it
// is false and a stable sort keeps their weighted order.
func (o *VolumeGrowOption) lessLoaded(a, b Node) bool {
	if o.layoutLoad == nil {
		return false
	}
	da, okA := a.(*DataNode)
	db, okB := b.(*DataNode)
	if !okA || !okB {
		return false
	}
	return o.layoutLoad[da]*db.diskCount(o.DiskType) < o.layoutLoad[db]*da.diskCount(o.DiskType)
}

// reserveLeastLoaded picks one of this rack's data nodes for a replica: among
// those with a free slot, the least loaded ones, weighted by free slots with r
// the way reserveOneVolumeInternal walks the tree. The caller holds n's lock.
func (n *NodeImpl) reserveLeastLoaded(r int64, option *VolumeGrowOption, freeSpaceOf func(Node) int64) (*DataNode, error) {
	var least []*DataNode
	var weight int64
	for _, node := range n.children {
		dn, ok := node.(*DataNode)
		if !ok || dn.IsTerminating {
			continue
		}
		free := freeSpaceOf(dn)
		if free <= 0 {
			continue
		}
		switch {
		case len(least) == 0 || option.lessLoaded(dn, least[0]):
			least, weight = []*DataNode{dn}, free
		case !option.lessLoaded(least[0], dn):
			least = append(least, dn)
			weight += free
		}
	}
	if len(least) == 0 {
		return nil, errors.New("No free volume slot found!")
	}
	r %= weight
	for _, dn := range least {
		free := freeSpaceOf(dn)
		if r < free {
			return dn, nil
		}
		r -= free
	}
	return least[len(least)-1], nil
}
