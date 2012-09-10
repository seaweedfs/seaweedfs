package topology

import (
)

type DataNodeLocationList struct {
  list []*DataNode
}

func NewDataNodeLocationList() *DataNodeLocationList {
  return &DataNodeLocationList{}
}

func (dnll *DataNodeLocationList) Add(loc *DataNode){
  for _, dnl := range dnll.list {
    if loc.ip == dnl.ip && loc.port == dnl.port {
      break
    }
  }
  dnll.list = append(dnll.list, loc)
}
