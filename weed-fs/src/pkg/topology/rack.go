package topology

import ()

type RackId uint32
type Rack struct {
	Id      RackId
	nodes   map[NodeId]*Node
	ipRange IpRange
}
