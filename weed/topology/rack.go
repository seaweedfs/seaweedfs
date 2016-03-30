package topology

import (
	"net"
	"strconv"
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
	n := r.FindChildren(func(c Node) bool {
		dn := c.(*DataNode)
		return dn.MatchLocation(ip, port)
	})
	if n == nil {
		return nil
	}
	return n.(*DataNode)
}

func (r *Rack) GetOrCreateDataNode(ip string, port int, publicUrl string, maxVolumeCount int) *DataNode {
	if dn := r.FindDataNode(ip, port); dn != nil {
		dn.UpdateLastSeen()
		if dn.IsDead() {
			dn.SetDead(false)
			r.GetTopology().chanRecoveredDataNodes <- dn
			dn.UpAdjustMaxVolumeCountDelta(maxVolumeCount - dn.maxVolumeCount)
		}
		return dn
	}

	dn := NewDataNode(net.JoinHostPort(ip, strconv.Itoa(port)))
	dn.Ip = ip
	dn.Port = port
	if publicUrl == "" {
		publicUrl = net.JoinHostPort(ip, strconv.Itoa(port))
	} else if publicUrl[0] == ':' {
		publicUrl = net.JoinHostPort(ip, publicUrl[1:])
	}
	dn.PublicUrl = publicUrl
	dn.maxVolumeCount = maxVolumeCount
	dn.UpdateLastSeen()
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
