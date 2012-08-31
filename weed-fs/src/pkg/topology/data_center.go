package topology

import (
)

type DataCenter struct {
	Node
	ipRange IpRange
}
func NewDataCenter(id NodeId) *DataCenter{
  dc := &DataCenter{}
  dc.Node = *NewNode()
  dc.Node.Id = id
  return dc
}
