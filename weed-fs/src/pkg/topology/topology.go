package topology

import (
  "pkg/storage"
)

type Topology struct {
  datacenters     map[DataCenterId]*DataCenter
}
//FIXME
func (t *Topology) RandomlyCreateOneVolume() storage.VolumeId{
  return t.findMaxVolumeId()
}
func (t *Topology) findMaxVolumeId() storage.VolumeId{
  return storage.VolumeId(0);
}
