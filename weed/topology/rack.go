package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"slices"
	"strings"
	"time"
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
func (r *Rack) GetOrCreateDataNode(ip string, port int, grpcPort int, publicUrl string, maxVolumeCounts map[string]uint32) *DataNode {
	r.Lock()
	defer r.Unlock()
	for _, c := range r.children {
		dn := c.(*DataNode)
		if dn.MatchLocation(ip, port) {
			dn.LastSeen = time.Now().Unix()
			return dn
		}
	}
	dn := NewDataNode(util.JoinHostPort(ip, port))
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
