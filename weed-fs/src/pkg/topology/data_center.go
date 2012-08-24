package topology

import ()

type DataCenterId string
type DataCenter struct {
	Id      DataCenterId
	racks   map[RackId]*Rack
	ipRange IpRange
}
