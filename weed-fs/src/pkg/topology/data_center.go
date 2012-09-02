package topology

import (
)

type DataCenter struct {
	NodeImpl
	ipRange IpRange
}
func NewDataCenter(id string) *DataCenter{
  dc := &DataCenter{}
  dc.id = NodeId(id)
  dc.nodeType = "DataCenter"
  dc.children = make(map[NodeId]Node)
  return dc
}
