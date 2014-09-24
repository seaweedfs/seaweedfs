package topology

import (
	"github.com/aszxqw/weed-fs/go/glog"
	"github.com/aszxqw/weed-fs/go/storage"
	"strconv"
)

type DataNode struct {
	NodeImpl
	volumes   map[storage.VolumeId]storage.VolumeInfo
	Ip        string
	Port      int
	PublicUrl string
	LastSeen  int64 // unix time in seconds
	Dead      bool
}

func NewDataNode(id string) *DataNode {
	s := &DataNode{}
	s.id = NodeId(id)
	s.nodeType = "DataNode"
	s.volumes = make(map[storage.VolumeId]storage.VolumeInfo)
	s.NodeImpl.value = s
	return s
}

func (dn *DataNode) AddOrUpdateVolume(v storage.VolumeInfo) {
	if _, ok := dn.volumes[v.Id]; !ok {
		dn.volumes[v.Id] = v
		dn.UpAdjustVolumeCountDelta(1)
		if !v.ReadOnly {
			dn.UpAdjustActiveVolumeCountDelta(1)
		}
		dn.UpAdjustMaxVolumeId(v.Id)
	} else {
		dn.volumes[v.Id] = v
	}
}

func (dn *DataNode) UpdateVolumes(actualVolumes []storage.VolumeInfo) (deletedVolumes []storage.VolumeInfo) {
	actualVolumeMap := make(map[storage.VolumeId]storage.VolumeInfo)
	for _, v := range actualVolumes {
		actualVolumeMap[v.Id] = v
	}
	for vid, v := range dn.volumes {
		if _, ok := actualVolumeMap[vid]; !ok {
			glog.V(0).Infoln("Deleting volume id:", vid)
			delete(dn.volumes, vid)
			deletedVolumes = append(deletedVolumes, v)
			dn.UpAdjustVolumeCountDelta(-1)
			dn.UpAdjustActiveVolumeCountDelta(-1)
		}
	} //TODO: adjust max volume id, if need to reclaim volume ids
	for _, v := range actualVolumes {
		dn.AddOrUpdateVolume(v)
	}
	return
}

func (dn *DataNode) GetDataCenter() *DataCenter {
	return dn.Parent().Parent().(*NodeImpl).value.(*DataCenter)
}

func (dn *DataNode) GetRack() *Rack {
	return dn.Parent().(*NodeImpl).value.(*Rack)
}

func (dn *DataNode) GetTopology() *Topology {
	p := dn.Parent()
	for p.Parent() != nil {
		p = p.Parent()
	}
	t := p.(*Topology)
	return t
}

func (dn *DataNode) MatchLocation(ip string, port int) bool {
	return dn.Ip == ip && dn.Port == port
}

func (dn *DataNode) Url() string {
	return dn.Ip + ":" + strconv.Itoa(dn.Port)
}

func (dn *DataNode) ToMap() interface{} {
	ret := make(map[string]interface{})
	ret["Url"] = dn.Url()
	ret["Volumes"] = dn.GetVolumeCount()
	ret["Max"] = dn.GetMaxVolumeCount()
	ret["Free"] = dn.FreeSpace()
	ret["PublicUrl"] = dn.PublicUrl
	return ret
}
