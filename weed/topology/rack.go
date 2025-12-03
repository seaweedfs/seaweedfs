package topology

import (
	"net"
	"slices"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type Rack struct {
	NodeImpl
}

func NewRack(id string) *Rack {
	r := &Rack{}
	r.id = NodeId(id)
	r.nodeType = "Rack"
	r.diskUsages = newDiskUsages()
	r.children = make(map[NodeId]Node)
	r.capacityReservations = newCapacityReservations()
	r.NodeImpl.value = r
	return r
}

func (r *Rack) FindDataNode(ip string, port int) *DataNode {
	for _, c := range r.Children() {
		dn := c.(*DataNode)
		if dn.MatchLocation(ip, port) {
			return dn
		}
	}
	return nil
}

// FindDataNodeById finds a DataNode by its ID using O(1) map lookup
func (r *Rack) FindDataNodeById(id string) *DataNode {
	r.RLock()
	defer r.RUnlock()
	if c, ok := r.children[NodeId(id)]; ok {
		return c.(*DataNode)
	}
	return nil
}

func (r *Rack) GetOrCreateDataNode(ip string, port int, grpcPort int, publicUrl string, id string, maxVolumeCounts map[string]uint32) *DataNode {
	r.Lock()
	defer r.Unlock()

	// Determine the node ID: use provided id, or fall back to ip:port for backward compatibility
	nodeId := id
	if nodeId == "" {
		nodeId = util.JoinHostPort(ip, port)
	}

	// First, try to find by node ID using O(1) map lookup (stable identity)
	if c, ok := r.children[NodeId(nodeId)]; ok {
		dn := c.(*DataNode)
		// Update the IP/Port in case they changed (e.g., pod rescheduled in K8s)
		dn.Ip = ip
		dn.Port = port
		dn.GrpcPort = grpcPort
		dn.PublicUrl = publicUrl
		dn.LastSeen = time.Now().Unix()
		return dn
	}

	// For backward compatibility: if id was provided, also check by ip:port
	// to handle transition from old (ip:port) to new (explicit id) behavior
	if id != "" {
		for oldId, c := range r.children {
			dn := c.(*DataNode)
			if dn.MatchLocation(ip, port) {
				// Only transition if the oldId is in ip:port format (legacy identification).
				// If oldId is an explicit id (not ip:port format), this is a different node
				// that happens to reuse the same ip:port - don't incorrectly merge them.
				if _, _, err := net.SplitHostPort(string(oldId)); err != nil {
					// oldId is not in ip:port format, so it's an explicit id from another node
					glog.Warningf("Volume server with id %s has ip:port %s:%d which is used by node %s", id, ip, port, oldId)
					continue
				}
				// Found a node by ip:port, transition it to use the new explicit id
				glog.V(0).Infof("Volume server %s transitioning id from %s to %s", dn.Url(), oldId, id)
				// Re-key the node in the children map with the new id
				delete(r.children, oldId)
				dn.id = NodeId(id)
				r.children[NodeId(id)] = dn
				dn.LastSeen = time.Now().Unix()
				return dn
			}
		}
	}

	dn := NewDataNode(nodeId)
	dn.Ip = ip
	dn.Port = port
	dn.GrpcPort = grpcPort
	dn.PublicUrl = publicUrl
	dn.LastSeen = time.Now().Unix()
	r.doLinkChildNode(dn)
	for diskType, maxVolumeCount := range maxVolumeCounts {
		disk := NewDisk(diskType)
		disk.diskUsages.getOrCreateDisk(types.ToDiskType(diskType)).maxVolumeCount = int64(maxVolumeCount)
		dn.LinkChildNode(disk)
	}
	return dn
}

type RackInfo struct {
	Id        NodeId         `json:"Id"`
	DataNodes []DataNodeInfo `json:"DataNodes"`
}

func (r *Rack) ToInfo() (info RackInfo) {
	info.Id = r.Id()
	var dns []DataNodeInfo
	for _, c := range r.Children() {
		dn := c.(*DataNode)
		dns = append(dns, dn.ToInfo())
	}

	slices.SortFunc(dns, func(a, b DataNodeInfo) int {
		return strings.Compare(a.Url, b.Url)
	})

	info.DataNodes = dns
	return
}

func (r *Rack) ToRackInfo() *master_pb.RackInfo {
	m := &master_pb.RackInfo{
		Id:        string(r.Id()),
		DiskInfos: r.diskUsages.ToDiskInfo(),
	}
	for _, c := range r.Children() {
		dn := c.(*DataNode)
		m.DataNodeInfos = append(m.DataNodeInfos, dn.ToDataNodeInfo())
	}
	return m
}
