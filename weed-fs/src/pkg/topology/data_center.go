package topology

import ()

type DataCenterId uint32
type DataCenter struct {
	Id      DataCenterId
	racks   map[RackId]*Rack
	ipRange IpRange
}
