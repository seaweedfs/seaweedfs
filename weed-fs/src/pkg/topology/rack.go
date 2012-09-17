package topology

import (
	"strconv"
)

type Rack struct {
	NodeImpl
	ipRange *IpRange
}

func NewRack(id string) *Rack {
	r := &Rack{}
	r.id = NodeId(id)
	r.nodeType = "Rack"
	r.children = make(map[NodeId]Node)
	return r
}

func (r *Rack) MatchLocationRange(ip string) bool {
	if r.ipRange == nil {
		return true
	}
	return r.ipRange.Match(ip)
}

func (r *Rack) GetOrCreateDataNode(ip string, port int, publicUrl string, maxVolumeCount int) *DataNode {
	for _, c := range r.Children() {
		dn := c.(*DataNode)
		if dn.MatchLocation(ip, port) {
			dn.NodeImpl.UpAdjustMaxVolumeCountDelta(maxVolumeCount - dn.maxVolumeCount)
			return dn
		}
	}
	dn := NewDataNode("DataNode" + ip + ":" + strconv.Itoa(port))
	dn.Ip = ip
	dn.Port = port
	dn.PublicUrl = publicUrl
	dn.maxVolumeCount = maxVolumeCount
	r.LinkChildNode(dn)
	return dn
}

func (rack *Rack) ToMap() interface{} {
	m := make(map[string]interface{})
	m["Free"] = rack.FreeSpace()
	var dns []interface{}
	for _, c := range rack.Children() {
		dn := c.(*DataNode)
		dns = append(dns, dn.ToMap())
	}
	m["DataNodes"] = dns
	return m
}
