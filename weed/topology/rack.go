package topology

import (
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"sort"
	"strconv"
	"time"
)

type Rack struct {
	NodeImpl
}

func NewRack(id string) *Rack {
	r := &Rack{}
	r.id = NodeId(id)
	r.nodeType = "Rack"
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
func (r *Rack) GetOrCreateDataNode(ip string, port int, publicUrl string, maxVolumeCount int64) *DataNode {
	for _, c := range r.Children() {
		dn := c.(*DataNode)
		if dn.MatchLocation(ip, port) {
			dn.LastSeen = time.Now().Unix()
			return dn
		}
	}
	dn := NewDataNode(ip + ":" + strconv.Itoa(port))
	dn.Ip = ip
	dn.Port = port
	dn.PublicUrl = publicUrl
	dn.maxVolumeCount = maxVolumeCount
	dn.LastSeen = time.Now().Unix()
	r.LinkChildNode(dn)
	return dn
}

type DataNodePtrSlice []*DataNode
func (p DataNodePtrSlice) Len() int           { return len(p) }
func (p DataNodePtrSlice) Less(i, j int) bool { return p[i].Url() < p[j].Url() }
func (p DataNodePtrSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (r *Rack) ToMap() interface{} {
	m := make(map[string]interface{})
	m["Id"] = r.Id()
	m["Max"] = r.GetMaxVolumeCount()
	m["Free"] = r.FreeSpace()

	var dataNodes DataNodePtrSlice
	for _, c := range r.Children() {
		dataNodes = append(dataNodes, c.(*DataNode))
	}
	sort.Sort(dataNodes)

	var dns []interface{}
	for _, c := range dataNodes {
		dns = append(dns, c.ToMap())
	}

	m["DataNodes"] = dns

	return m
}

func (r *Rack) ToRackInfo() *master_pb.RackInfo {
	m := &master_pb.RackInfo{
		Id:                string(r.Id()),
		VolumeCount:       uint64(r.GetVolumeCount()),
		MaxVolumeCount:    uint64(r.GetMaxVolumeCount()),
		FreeVolumeCount:   uint64(r.FreeSpace()),
		ActiveVolumeCount: uint64(r.GetActiveVolumeCount()),
		RemoteVolumeCount: uint64(r.GetRemoteVolumeCount()),
	}
	for _, c := range r.Children() {
		dn := c.(*DataNode)
		m.DataNodeInfos = append(m.DataNodeInfos, dn.ToDataNodeInfo())
	}
	return m
}
