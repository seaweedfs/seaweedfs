package topology

import (
	"strconv"
	"time"
)

type Rack struct {
	NodeImpl
}

func NewRack(id string) *Rack {
	r := &Rack{}
	r.id = NodeId(id)
	r.nodeType = NodeTypeRack
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
func (r *Rack) GetOrCreateDataNode(ip string, port int, publicUrl string, maxVolumeCount int) *DataNode {
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

func (r *Rack) ToMap() interface{} {
	m := make(map[string]interface{})
	m["Id"] = r.Id()
	m["Max"] = r.GetMaxVolumeCount()
	m["Free"] = r.FreeSpace()
	var dns []interface{}
	for _, c := range r.Children() {
		dn := c.(*DataNode)
		dns = append(dns, dn.ToMap())
	}
	m["DataNodes"] = dns
	return m
}
