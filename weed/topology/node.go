package topology

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type NodeId string

// CapacityReservation represents a temporary reservation of capacity
type CapacityReservation struct {
	reservationId string
	diskType      types.DiskType
	count         int64
	createdAt     time.Time
}

// CapacityReservations manages capacity reservations for a node
type CapacityReservations struct {
	sync.RWMutex
	reservations   map[string]*CapacityReservation
	reservedCounts map[types.DiskType]int64
}

func newCapacityReservations() *CapacityReservations {
	return &CapacityReservations{
		reservations:   make(map[string]*CapacityReservation),
		reservedCounts: make(map[types.DiskType]int64),
	}
}

func (cr *CapacityReservations) addReservation(diskType types.DiskType, count int64) string {
	cr.Lock()
	defer cr.Unlock()

	return cr.doAddReservation(diskType, count)
}

func (cr *CapacityReservations) removeReservation(reservationId string) bool {
	cr.Lock()
	defer cr.Unlock()

	if reservation, exists := cr.reservations[reservationId]; exists {
		delete(cr.reservations, reservationId)
		cr.decrementCount(reservation.diskType, reservation.count)
		return true
	}
	return false
}

func (cr *CapacityReservations) getReservedCount(diskType types.DiskType) int64 {
	cr.RLock()
	defer cr.RUnlock()

	return cr.reservedCounts[diskType]
}

// decrementCount is a helper to decrement reserved count and clean up zero entries
func (cr *CapacityReservations) decrementCount(diskType types.DiskType, count int64) {
	cr.reservedCounts[diskType] -= count
	// Clean up zero counts to prevent map growth
	if cr.reservedCounts[diskType] <= 0 {
		delete(cr.reservedCounts, diskType)
	}
}

// doAddReservation is a helper to add a reservation, assuming the lock is already held
func (cr *CapacityReservations) doAddReservation(diskType types.DiskType, count int64) string {
	now := time.Now()
	reservationId := fmt.Sprintf("%s-%d-%d-%d", diskType, count, now.UnixNano(), rand.Int64())
	cr.reservations[reservationId] = &CapacityReservation{
		reservationId: reservationId,
		diskType:      diskType,
		count:         count,
		createdAt:     now,
	}
	cr.reservedCounts[diskType] += count
	return reservationId
}

// tryReserveAtomic atomically checks available space and reserves if possible
func (cr *CapacityReservations) tryReserveAtomic(diskType types.DiskType, count int64, availableSpaceFunc func() int64) (reservationId string, success bool) {
	cr.Lock()
	defer cr.Unlock()

	// Check available space under lock
	currentReserved := cr.reservedCounts[diskType]
	availableSpace := availableSpaceFunc() - currentReserved

	if availableSpace >= count {
		// Create and add reservation atomically
		return cr.doAddReservation(diskType, count), true
	}

	return "", false
}

func (cr *CapacityReservations) cleanExpiredReservations(expirationDuration time.Duration) {
	cr.Lock()
	defer cr.Unlock()

	now := time.Now()
	for id, reservation := range cr.reservations {
		if now.Sub(reservation.createdAt) > expirationDuration {
			delete(cr.reservations, id)
			cr.decrementCount(reservation.diskType, reservation.count)
			glog.V(1).Infof("Cleaned up expired capacity reservation: %s", id)
		}
	}
}

type Node interface {
	Id() NodeId
	String() string
	AvailableSpaceFor(option *VolumeGrowOption) int64
	ReserveOneVolume(r int64, option *VolumeGrowOption) (*DataNode, error)
	ReserveOneVolumeForReservation(r int64, option *VolumeGrowOption) (*DataNode, error)
	UpAdjustDiskUsageDelta(diskType types.DiskType, diskUsage *DiskUsageCounts)
	UpAdjustMaxVolumeId(vid needle.VolumeId)
	GetDiskUsages() *DiskUsages

	// Capacity reservation methods for avoiding race conditions
	TryReserveCapacity(diskType types.DiskType, count int64) (reservationId string, success bool)
	ReleaseReservedCapacity(reservationId string)
	AvailableSpaceForReservation(option *VolumeGrowOption) int64

	GetMaxVolumeId() needle.VolumeId
	SetParent(Node)
	LinkChildNode(node Node)
	UnlinkChildNode(nodeId NodeId)
	CollectDeadNodeAndFullVolumes(freshThreshHold int64, volumeSizeLimit uint64, growThreshold float64)

	IsDataNode() bool
	IsRack() bool
	IsDataCenter() bool
	IsLocked() bool
	Children() []Node
	Parent() Node

	GetValue() interface{} //get reference to the topology,dc,rack,datanode
}

type NodeImpl struct {
	diskUsages   *DiskUsages
	id           NodeId
	parent       Node
	sync.RWMutex // lock children
	children     map[NodeId]Node
	maxVolumeId  needle.VolumeId

	//for rack, data center, topology
	nodeType string
	value    interface{}

	// capacity reservations to prevent race conditions during volume creation
	capacityReservations *CapacityReservations
}

func (n *NodeImpl) GetDiskUsages() *DiskUsages {
	return n.diskUsages
}

// the first node must satisfy filterFirstNodeFn(), the rest nodes must have one free slot
func (n *NodeImpl) PickNodesByWeight(numberOfNodes int, option *VolumeGrowOption, filterFirstNodeFn func(dn Node) error) (firstNode Node, restNodes []Node, err error) {
	var totalWeights int64
	var errs []string
	n.RLock()
	candidates := make([]Node, 0, len(n.children))
	candidatesWeights := make([]int64, 0, len(n.children))
	//pick nodes which has enough free volumes as candidates, and use free volumes number as node weight.
	for _, node := range n.children {
		if node.AvailableSpaceFor(option) <= 0 {
			continue
		}
		totalWeights += node.AvailableSpaceFor(option)
		candidates = append(candidates, node)
		candidatesWeights = append(candidatesWeights, node.AvailableSpaceFor(option))
	}
	n.RUnlock()
	if len(candidates) < numberOfNodes {
		glog.V(0).Infoln(n.Id(), "failed to pick", numberOfNodes, "from ", len(candidates), "node candidates")
		return nil, nil, errors.New("Not enough data nodes found!")
	}

	//pick nodes randomly by weights, the node picked earlier has higher final weights
	sortedCandidates := make([]Node, 0, len(candidates))
	for i := 0; i < len(candidates); i++ {
		// Break if no more weights available to prevent panic in rand.Int64N
		if totalWeights <= 0 {
			break
		}
		weightsInterval := rand.Int64N(totalWeights)
		lastWeights := int64(0)
		for k, weights := range candidatesWeights {
			if (weightsInterval >= lastWeights) && (weightsInterval < lastWeights+weights) {
				sortedCandidates = append(sortedCandidates, candidates[k])
				candidatesWeights[k] = 0
				totalWeights -= weights
				break
			}
			lastWeights += weights
		}
	}

	restNodes = make([]Node, 0, numberOfNodes-1)
	ret := false
	n.RLock()
	for k, node := range sortedCandidates {
		if err := filterFirstNodeFn(node); err == nil {
			firstNode = node
			if k >= numberOfNodes-1 {
				restNodes = sortedCandidates[:numberOfNodes-1]
			} else {
				restNodes = append(restNodes, sortedCandidates[:k]...)
				restNodes = append(restNodes, sortedCandidates[k+1:numberOfNodes]...)
			}
			ret = true
			break
		} else {
			errs = append(errs, string(node.Id())+":"+err.Error())
		}
	}
	n.RUnlock()
	if !ret {
		return nil, nil, errors.New("No matching data node found! \n" + strings.Join(errs, "\n"))
	}
	return
}

func (n *NodeImpl) IsDataNode() bool {
	return n.nodeType == "DataNode"
}

func (n *NodeImpl) IsRack() bool {
	return n.nodeType == "Rack"
}

func (n *NodeImpl) IsDataCenter() bool {
	return n.nodeType == "DataCenter"
}

func (n *NodeImpl) IsLocked() (isTryLock bool) {
	if isTryLock = n.TryRLock(); isTryLock {
		n.RUnlock()
	}
	return !isTryLock
}

func (n *NodeImpl) String() string {
	if n.parent != nil {
		return n.parent.String() + ":" + string(n.id)
	}
	return string(n.id)
}

func (n *NodeImpl) Id() NodeId {
	return n.id
}

func (n *NodeImpl) getOrCreateDisk(diskType types.DiskType) *DiskUsageCounts {
	return n.diskUsages.getOrCreateDisk(diskType)
}

func (n *NodeImpl) AvailableSpaceFor(option *VolumeGrowOption) int64 {
	t := n.getOrCreateDisk(option.DiskType)
	freeVolumeSlotCount := atomic.LoadInt64(&t.maxVolumeCount) + atomic.LoadInt64(&t.remoteVolumeCount) - atomic.LoadInt64(&t.volumeCount)
	ecShardCount := atomic.LoadInt64(&t.ecShardCount)
	if ecShardCount > 0 {
		freeVolumeSlotCount = freeVolumeSlotCount - ecShardCount/erasure_coding.DataShardsCount - 1
	}
	return freeVolumeSlotCount
}

// AvailableSpaceForReservation returns available space considering existing reservations
func (n *NodeImpl) AvailableSpaceForReservation(option *VolumeGrowOption) int64 {
	baseAvailable := n.AvailableSpaceFor(option)
	reservedCount := n.capacityReservations.getReservedCount(option.DiskType)
	return baseAvailable - reservedCount
}

// TryReserveCapacity attempts to atomically reserve capacity for volume creation
func (n *NodeImpl) TryReserveCapacity(diskType types.DiskType, count int64) (reservationId string, success bool) {
	const reservationTimeout = 5 * time.Minute // TODO: make this configurable

	// Clean up any expired reservations first
	n.capacityReservations.cleanExpiredReservations(reservationTimeout)

	// Atomically check and reserve space
	option := &VolumeGrowOption{DiskType: diskType}
	reservationId, success = n.capacityReservations.tryReserveAtomic(diskType, count, func() int64 {
		return n.AvailableSpaceFor(option)
	})

	if success {
		glog.V(1).Infof("Reserved %d capacity for diskType %s on node %s: %s", count, diskType, n.Id(), reservationId)
	}

	return reservationId, success
}

// ReleaseReservedCapacity releases a previously reserved capacity
func (n *NodeImpl) ReleaseReservedCapacity(reservationId string) {
	if n.capacityReservations.removeReservation(reservationId) {
		glog.V(1).Infof("Released capacity reservation on node %s: %s", n.Id(), reservationId)
	} else {
		glog.V(1).Infof("Attempted to release non-existent reservation on node %s: %s", n.Id(), reservationId)
	}
}
func (n *NodeImpl) SetParent(node Node) {
	n.parent = node
}

func (n *NodeImpl) Children() (ret []Node) {
	n.RLock()
	defer n.RUnlock()
	for _, c := range n.children {
		ret = append(ret, c)
	}
	return ret
}

func (n *NodeImpl) Parent() Node {
	return n.parent
}

func (n *NodeImpl) GetValue() interface{} {
	return n.value
}

func (n *NodeImpl) ReserveOneVolume(r int64, option *VolumeGrowOption) (assignedNode *DataNode, err error) {
	return n.reserveOneVolumeInternal(r, option, false)
}

// ReserveOneVolumeForReservation selects a node using reservation-aware capacity checks
func (n *NodeImpl) ReserveOneVolumeForReservation(r int64, option *VolumeGrowOption) (assignedNode *DataNode, err error) {
	return n.reserveOneVolumeInternal(r, option, true)
}

func (n *NodeImpl) reserveOneVolumeInternal(r int64, option *VolumeGrowOption, useReservations bool) (assignedNode *DataNode, err error) {
	n.RLock()
	defer n.RUnlock()
	for _, node := range n.children {
		var freeSpace int64
		if useReservations {
			freeSpace = node.AvailableSpaceForReservation(option)
		} else {
			freeSpace = node.AvailableSpaceFor(option)
		}
		// fmt.Println("r =", r, ", node =", node, ", freeSpace =", freeSpace)
		if freeSpace <= 0 {
			continue
		}
		if r >= freeSpace {
			r -= freeSpace
		} else {
			var hasSpace bool
			if useReservations {
				hasSpace = node.IsDataNode() && node.AvailableSpaceForReservation(option) > 0
			} else {
				hasSpace = node.IsDataNode() && node.AvailableSpaceFor(option) > 0
			}
			if hasSpace {
				// fmt.Println("vid =", vid, " assigned to node =", node, ", freeSpace =", node.FreeSpace())
				dn := node.(*DataNode)
				if dn.IsTerminating {
					continue
				}
				return dn, nil
			}
			if useReservations {
				assignedNode, err = node.ReserveOneVolumeForReservation(r, option)
			} else {
				assignedNode, err = node.ReserveOneVolume(r, option)
			}
			if err == nil {
				return
			}
		}
	}
	return nil, errors.New("No free volume slot found!")
}

func (n *NodeImpl) UpAdjustDiskUsageDelta(diskType types.DiskType, diskUsage *DiskUsageCounts) { //can be negative
	existingDisk := n.getOrCreateDisk(diskType)
	existingDisk.addDiskUsageCounts(diskUsage)
	if n.parent != nil {
		n.parent.UpAdjustDiskUsageDelta(diskType, diskUsage)
	}
}
func (n *NodeImpl) UpAdjustMaxVolumeId(vid needle.VolumeId) { //can be negative
	if n.maxVolumeId < vid {
		n.maxVolumeId = vid
		if n.parent != nil {
			n.parent.UpAdjustMaxVolumeId(vid)
		}
	}
}
func (n *NodeImpl) GetMaxVolumeId() needle.VolumeId {
	return n.maxVolumeId
}

func (n *NodeImpl) LinkChildNode(node Node) {
	n.Lock()
	defer n.Unlock()
	n.doLinkChildNode(node)
}

func (n *NodeImpl) doLinkChildNode(node Node) {
	if n.children[node.Id()] == nil {
		n.children[node.Id()] = node
		for dt, du := range node.GetDiskUsages().usages {
			n.UpAdjustDiskUsageDelta(dt, du)
		}
		n.UpAdjustMaxVolumeId(node.GetMaxVolumeId())
		node.SetParent(n)
		glog.V(0).Infoln(n, "adds child", node.Id())
	}
}

func (n *NodeImpl) UnlinkChildNode(nodeId NodeId) {
	n.Lock()
	defer n.Unlock()
	node := n.children[nodeId]
	if node != nil {
		node.SetParent(nil)
		delete(n.children, node.Id())
		for dt, du := range node.GetDiskUsages().negative().usages {
			n.UpAdjustDiskUsageDelta(dt, du)
		}
		glog.V(0).Infoln(n, "removes", node.Id())
	}
}

func (n *NodeImpl) CollectDeadNodeAndFullVolumes(freshThreshHoldUnixTime int64, volumeSizeLimit uint64, growThreshold float64) {
	if n.IsRack() {
		for _, c := range n.Children() {
			dn := c.(*DataNode) //can not cast n to DataNode
			for _, v := range dn.GetVolumes() {
				topo := n.GetTopology()
				diskType := types.ToDiskType(v.DiskType)
				vl := topo.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)

				if v.Size >= volumeSizeLimit {
					vl.accessLock.RLock()
					vacuumTime, ok := vl.vacuumedVolumes[v.Id]
					vl.accessLock.RUnlock()

					// If a volume has been vacuumed in the past 20 seconds, we do not check whether it has reached full capacity.
					// After 20s(grpc timeout), theoretically all the heartbeats of the volume server have reached the master,
					// the volume size should be correct, not the size before the vacuum.
					if !ok || time.Now().Add(-20*time.Second).After(vacuumTime) {
						//fmt.Println("volume",v.Id,"size",v.Size,">",volumeSizeLimit)
						topo.chanFullVolumes <- v
					}
				} else if float64(v.Size) > float64(volumeSizeLimit)*growThreshold {
					topo.chanCrowdedVolumes <- v
				}
				copyCount := v.ReplicaPlacement.GetCopyCount()
				if copyCount > 1 {
					if copyCount > len(topo.Lookup(v.Collection, v.Id)) {
						stats.MasterReplicaPlacementMismatch.WithLabelValues(v.Collection, v.Id.String()).Set(1)
					} else {
						stats.MasterReplicaPlacementMismatch.WithLabelValues(v.Collection, v.Id.String()).Set(0)
					}
				}
			}
		}
	} else {
		for _, c := range n.Children() {
			c.CollectDeadNodeAndFullVolumes(freshThreshHoldUnixTime, volumeSizeLimit, growThreshold)
		}
	}
}

func (n *NodeImpl) GetTopology() *Topology {
	var p Node
	p = n
	for p.Parent() != nil {
		p = p.Parent()
	}
	return p.GetValue().(*Topology)
}
