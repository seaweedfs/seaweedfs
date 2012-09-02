package topology

import (
)

type Rack struct {
	NodeImpl
	ipRange IpRange
}

func NewRack(id string) *Rack {
	r := &Rack{}
  r.id = NodeId(id)
  r.nodeType = "Rack"
  r.children = make(map[NodeId]Node)
	return r
}
