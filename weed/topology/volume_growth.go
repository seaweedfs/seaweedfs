package topology

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/server/constants"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

/*
This package is created to resolve these replica placement issues:
1. growth factor for each replica level, e.g., add 10 volumes for 1 copy, 20 volumes for 2 copies, 30 volumes for 3 copies
2. in time of tight storage, how to reduce replica level
3. optimizing for hot data on faster disk, cold data on cheaper storage,
4. volume allocation for each bucket
*/

type VolumeGrowRequest struct {
	Option *VolumeGrowOption
	Count  uint32
	Force  bool
	Reason string
}

func (vg *VolumeGrowRequest) Equals(req *VolumeGrowRequest) bool {
	return reflect.DeepEqual(vg.Option, req.Option) && vg.Count == req.Count && vg.Force == req.Force
}

type volumeGrowthStrategy struct {
	Copy1Count     uint32
	Copy2Count     uint32
	Copy3Count     uint32
	CopyOtherCount uint32
	Threshold      float64
}

var (
	VolumeGrowStrategy = volumeGrowthStrategy{
		Copy1Count:     7,
		Copy2Count:     6,
		Copy3Count:     3,
		CopyOtherCount: 1,
		Threshold:      0.9,
	}
)

type VolumeGrowOption struct {
	Collection         string                        `json:"collection,omitempty"`
	ReplicaPlacement   *super_block.ReplicaPlacement `json:"replication,omitempty"`
	Ttl                *needle.TTL                   `json:"ttl,omitempty"`
	DiskType           types.DiskType                `json:"disk,omitempty"`
	Preallocate        int64                         `json:"preallocate,omitempty"`
	DataCenter         string                        `json:"dataCenter,omitempty"`
	Rack               string                        `json:"rack,omitempty"`
	DataNode           string                        `json:"dataNode,omitempty"`
	MemoryMapMaxSizeMb uint32                        `json:"memoryMapMaxSizeMb,omitempty"`
	Version            uint32                        `json:"version,omitempty"`
}

type VolumeGrowth struct {
	accessLock sync.Mutex
}

// VolumeGrowReservation tracks capacity reservations for a volume creation operation
type VolumeGrowReservation struct {
	servers        []*DataNode
	reservationIds []string
	diskType       types.DiskType
}

// releaseAllReservations releases all reservations in this volume grow operation
func (vgr *VolumeGrowReservation) releaseAllReservations() {
	for i, server := range vgr.servers {
		if i < len(vgr.reservationIds) && vgr.reservationIds[i] != "" {
			server.ReleaseReservedCapacity(vgr.reservationIds[i])
		}
	}
}

func (o *VolumeGrowOption) String() string {
	blob, _ := json.Marshal(o)
	return string(blob)
}

func NewDefaultVolumeGrowth() *VolumeGrowth {
	return &VolumeGrowth{}
}

// one replication type may need rp.GetCopyCount() actual volumes
// given copyCount, how many logical volumes to create
func (vg *VolumeGrowth) findVolumeCount(copyCount int) (count uint32) {
	switch copyCount {
	case 1:
		count = VolumeGrowStrategy.Copy1Count
	case 2:
		count = VolumeGrowStrategy.Copy2Count
	case 3:
		count = VolumeGrowStrategy.Copy3Count
	default:
		count = VolumeGrowStrategy.CopyOtherCount
	}
	return
}

func (vg *VolumeGrowth) AutomaticGrowByType(option *VolumeGrowOption, grpcDialOption grpc.DialOption, topo *Topology, targetCount uint32) (result []*master_pb.VolumeLocation, err error) {
	if targetCount == 0 {
		targetCount = vg.findVolumeCount(option.ReplicaPlacement.GetCopyCount())
	}
	result, err = vg.GrowByCountAndType(grpcDialOption, targetCount, option, topo)
	if len(result) > 0 && len(result)%option.ReplicaPlacement.GetCopyCount() == 0 {
		return result, nil
	}
	return result, err
}
func (vg *VolumeGrowth) GrowByCountAndType(grpcDialOption grpc.DialOption, targetCount uint32, option *VolumeGrowOption, topo *Topology) (result []*master_pb.VolumeLocation, err error) {
	vg.accessLock.Lock()
	defer vg.accessLock.Unlock()

	for i := uint32(0); i < targetCount; i++ {
		if res, e := vg.findAndGrow(grpcDialOption, topo, option); e == nil {
			result = append(result, res...)
		} else {
			glog.V(0).Infof("create %d volume, created %d: %v", targetCount, len(result), e)
			return result, e
		}
	}
	return
}

func (vg *VolumeGrowth) findAndGrow(grpcDialOption grpc.DialOption, topo *Topology, option *VolumeGrowOption) (result []*master_pb.VolumeLocation, err error) {
	servers, reservation, e := vg.findEmptySlotsForOneVolume(topo, option, true) // use reservations
	if e != nil {
		return nil, e
	}
	// Ensure reservations are released if anything goes wrong
	defer func() {
		if err != nil && reservation != nil {
			reservation.releaseAllReservations()
		}
	}()

	for !topo.LastLeaderChangeTime.Add(constants.VolumePulsePeriod * 2).Before(time.Now()) {
		glog.V(0).Infof("wait for volume servers to join back")
		time.Sleep(constants.VolumePulsePeriod / 2)
	}
	vid, raftErr := topo.NextVolumeId()
	if raftErr != nil {
		return nil, raftErr
	}
	if err = vg.grow(grpcDialOption, topo, vid, option, reservation, servers...); err == nil {
		for _, server := range servers {
			result = append(result, &master_pb.VolumeLocation{
				Url:        server.Url(),
				PublicUrl:  server.PublicUrl,
				DataCenter: server.GetDataCenterId(),
				GrpcPort:   uint32(server.GrpcPort),
				NewVids:    []uint32{uint32(vid)},
			})
		}
	}
	return
}

// 1. find the main data node
// 1.1 collect all data nodes that have 1 slots
// 2.2 collect all racks that have rp.SameRackCount+1
// 2.2 collect all data centers that have DiffRackCount+rp.SameRackCount+1
// 2. find rest data nodes
// If useReservations is true, reserves capacity on each server and returns reservation info
func (vg *VolumeGrowth) findEmptySlotsForOneVolume(topo *Topology, option *VolumeGrowOption, useReservations bool) (servers []*DataNode, reservation *VolumeGrowReservation, err error) {
	//find main datacenter and other data centers
	rp := option.ReplicaPlacement

	// Track tentative reservations to make the process atomic
	var tentativeReservation *VolumeGrowReservation

	// Select appropriate functions based on useReservations flag
	var availableSpaceFunc func(Node, *VolumeGrowOption) int64
	var reserveOneVolumeFunc func(Node, int64, *VolumeGrowOption) (*DataNode, error)

	if useReservations {
		// Initialize tentative reservation tracking
		tentativeReservation = &VolumeGrowReservation{
			servers:        make([]*DataNode, 0),
			reservationIds: make([]string, 0),
			diskType:       option.DiskType,
		}

		// For reservations, we make actual reservations during node selection
		availableSpaceFunc = func(node Node, option *VolumeGrowOption) int64 {
			return node.AvailableSpaceForReservation(option)
		}
		reserveOneVolumeFunc = func(node Node, r int64, option *VolumeGrowOption) (*DataNode, error) {
			return node.ReserveOneVolumeForReservation(r, option)
		}
	} else {
		availableSpaceFunc = func(node Node, option *VolumeGrowOption) int64 {
			return node.AvailableSpaceFor(option)
		}
		reserveOneVolumeFunc = func(node Node, r int64, option *VolumeGrowOption) (*DataNode, error) {
			return node.ReserveOneVolume(r, option)
		}
	}

	// Ensure cleanup of partial reservations on error
	defer func() {
		if err != nil && tentativeReservation != nil {
			tentativeReservation.releaseAllReservations()
		}
	}()
	mainDataCenter, otherDataCenters, dc_err := topo.PickNodesByWeight(rp.DiffDataCenterCount+1, option, func(node Node) error {
		if option.DataCenter != "" && node.IsDataCenter() && node.Id() != NodeId(option.DataCenter) {
			return fmt.Errorf("Not matching preferred data center:%s", option.DataCenter)
		}
		if len(node.Children()) < rp.DiffRackCount+1 {
			return fmt.Errorf("Only has %d racks, not enough for %d.", len(node.Children()), rp.DiffRackCount+1)
		}
		if availableSpaceFunc(node, option) < int64(rp.DiffRackCount+rp.SameRackCount+1) {
			return fmt.Errorf("Free:%d < Expected:%d", availableSpaceFunc(node, option), rp.DiffRackCount+rp.SameRackCount+1)
		}
		possibleRacksCount := 0
		for _, rack := range node.Children() {
			possibleDataNodesCount := 0
			for _, n := range rack.Children() {
				if availableSpaceFunc(n, option) >= 1 {
					possibleDataNodesCount++
				}
			}
			if possibleDataNodesCount >= rp.SameRackCount+1 {
				possibleRacksCount++
			}
		}
		if possibleRacksCount < rp.DiffRackCount+1 {
			return fmt.Errorf("Only has %d racks with more than %d free data nodes, not enough for %d.", possibleRacksCount, rp.SameRackCount+1, rp.DiffRackCount+1)
		}
		return nil
	})
	if dc_err != nil {
		return nil, nil, dc_err
	}

	//find main rack and other racks
	mainRack, otherRacks, rackErr := mainDataCenter.(*DataCenter).PickNodesByWeight(rp.DiffRackCount+1, option, func(node Node) error {
		if option.Rack != "" && node.IsRack() && node.Id() != NodeId(option.Rack) {
			return fmt.Errorf("Not matching preferred rack:%s", option.Rack)
		}
		if availableSpaceFunc(node, option) < int64(rp.SameRackCount+1) {
			return fmt.Errorf("Free:%d < Expected:%d", availableSpaceFunc(node, option), rp.SameRackCount+1)
		}
		if len(node.Children()) < rp.SameRackCount+1 {
			// a bit faster way to test free racks
			return fmt.Errorf("Only has %d data nodes, not enough for %d.", len(node.Children()), rp.SameRackCount+1)
		}
		possibleDataNodesCount := 0
		for _, n := range node.Children() {
			if availableSpaceFunc(n, option) >= 1 {
				possibleDataNodesCount++
			}
		}
		if possibleDataNodesCount < rp.SameRackCount+1 {
			return fmt.Errorf("Only has %d data nodes with a slot, not enough for %d.", possibleDataNodesCount, rp.SameRackCount+1)
		}
		return nil
	})
	if rackErr != nil {
		return nil, nil, rackErr
	}

	//find main server and other servers
	mainServer, otherServers, serverErr := mainRack.(*Rack).PickNodesByWeight(rp.SameRackCount+1, option, func(node Node) error {
		if option.DataNode != "" && node.IsDataNode() && node.Id() != NodeId(option.DataNode) {
			return fmt.Errorf("Not matching preferred data node:%s", option.DataNode)
		}

		if useReservations {
			// For reservations, atomically check and reserve capacity
			if node.IsDataNode() {
				reservationId, success := node.TryReserveCapacity(option.DiskType, 1)
				if !success {
					return fmt.Errorf("Cannot reserve capacity on node %s", node.Id())
				}
				// Track the reservation for later cleanup if needed
				tentativeReservation.servers = append(tentativeReservation.servers, node.(*DataNode))
				tentativeReservation.reservationIds = append(tentativeReservation.reservationIds, reservationId)
			} else if availableSpaceFunc(node, option) < 1 {
				return fmt.Errorf("Free:%d < Expected:%d", availableSpaceFunc(node, option), 1)
			}
		} else if availableSpaceFunc(node, option) < 1 {
			return fmt.Errorf("Free:%d < Expected:%d", availableSpaceFunc(node, option), 1)
		}
		return nil
	})
	if serverErr != nil {
		return nil, nil, serverErr
	}

	servers = append(servers, mainServer.(*DataNode))
	for _, server := range otherServers {
		servers = append(servers, server.(*DataNode))
	}
	for _, rack := range otherRacks {
		r := rand.Int64N(availableSpaceFunc(rack, option))
		if server, e := reserveOneVolumeFunc(rack, r, option); e == nil {
			servers = append(servers, server)

			// If using reservations, also make a reservation on the selected server
			if useReservations {
				reservationId, success := server.TryReserveCapacity(option.DiskType, 1)
				if !success {
					return servers, nil, fmt.Errorf("failed to reserve capacity on server %s from other rack", server.Id())
				}
				tentativeReservation.servers = append(tentativeReservation.servers, server)
				tentativeReservation.reservationIds = append(tentativeReservation.reservationIds, reservationId)
			}
		} else {
			return servers, nil, e
		}
	}
	for _, datacenter := range otherDataCenters {
		r := rand.Int64N(availableSpaceFunc(datacenter, option))
		if server, e := reserveOneVolumeFunc(datacenter, r, option); e == nil {
			servers = append(servers, server)

			// If using reservations, also make a reservation on the selected server
			if useReservations {
				reservationId, success := server.TryReserveCapacity(option.DiskType, 1)
				if !success {
					return servers, nil, fmt.Errorf("failed to reserve capacity on server %s from other datacenter", server.Id())
				}
				tentativeReservation.servers = append(tentativeReservation.servers, server)
				tentativeReservation.reservationIds = append(tentativeReservation.reservationIds, reservationId)
			}
		} else {
			return servers, nil, e
		}
	}

	// If reservations were made, return the tentative reservation
	if useReservations && tentativeReservation != nil {
		reservation = tentativeReservation
		glog.V(1).Infof("Successfully reserved capacity on %d servers for volume creation", len(servers))
	}

	return servers, reservation, nil
}

// grow creates volumes on the provided servers, optionally managing capacity reservations
func (vg *VolumeGrowth) grow(grpcDialOption grpc.DialOption, topo *Topology, vid needle.VolumeId, option *VolumeGrowOption, reservation *VolumeGrowReservation, servers ...*DataNode) (growErr error) {
	var createdVolumes []storage.VolumeInfo
	for _, server := range servers {
		if err := AllocateVolume(server, grpcDialOption, vid, option); err == nil {
			createdVolumes = append(createdVolumes, storage.VolumeInfo{
				Id:               vid,
				Size:             0,
				Collection:       option.Collection,
				ReplicaPlacement: option.ReplicaPlacement,
				Ttl:              option.Ttl,
				Version:          needle.Version(option.Version),
				DiskType:         option.DiskType.String(),
				ModifiedAtSecond: time.Now().Unix(),
			})
			glog.V(0).Infof("Created Volume %d on %s", vid, server.NodeImpl.String())
		} else {
			glog.Warningf("Failed to assign volume %d on %s: %v", vid, server.NodeImpl.String(), err)
			growErr = fmt.Errorf("failed to assign volume %d on %s: %v", vid, server.NodeImpl.String(), err)
			break
		}
	}

	if growErr == nil {
		for i, vi := range createdVolumes {
			server := servers[i]
			server.AddOrUpdateVolume(vi)
			topo.RegisterVolumeLayout(vi, server)
			glog.V(0).Infof("Registered Volume %d on %s", vid, server.NodeImpl.String())
		}
		// Release reservations on success since volumes are now registered
		if reservation != nil {
			reservation.releaseAllReservations()
		}
	} else {
		// cleaning up created volume replicas
		for i, vi := range createdVolumes {
			server := servers[i]
			if err := DeleteVolume(server, grpcDialOption, vi.Id); err != nil {
				glog.Warningf("Failed to clean up volume %d on %s", vid, server.NodeImpl.String())
			}
		}
		// Reservations will be released by the caller in case of failure
	}

	return growErr
}
