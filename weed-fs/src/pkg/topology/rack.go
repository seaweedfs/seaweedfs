package topology

import (
)

type Rack struct {
	Node
	ipRange IpRange
}

func NewRack(id NodeId) *Rack {
	r := &Rack{}
	r.Node = *NewNode()
  r.Node.Id = id
	return r
}
