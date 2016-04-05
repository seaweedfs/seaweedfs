package topology

import (
	"fmt"
	"strconv"

	"net"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

type DataNode struct {
	NodeImpl
	volumes   map[storage.VolumeId]*storage.VolumeInfo
	lastSeen  int64 // unix time in seconds
	dead      bool
	Ip        string
	Port      int
	PublicUrl string
}

func NewDataNode(id string) *DataNode {
	s := &DataNode{}
	s.id = NodeId(id)
	s.nodeType = "DataNode"
	s.volumes = make(map[storage.VolumeId]*storage.VolumeInfo)
	s.NodeImpl.value = s
	return s
}

func (dn *DataNode) String() string {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	return fmt.Sprintf("Node:%s, volumes:%v, Ip:%s, Port:%d, PublicUrl:%s, Dead:%v",
		dn.NodeImpl.toString(),
		dn.volumes,
		dn.Ip,
		dn.Port,
		dn.PublicUrl,
		dn.dead)
}

func (dn *DataNode) LastSeen() int64 {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	return dn.lastSeen
}

func (dn *DataNode) UpdateLastSeen() {
	dn.mutex.Lock()
	defer dn.mutex.Unlock()
	dn.lastSeen = time.Now().Unix()
}

func (dn *DataNode) IsDead() bool {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	return dn.dead
}

func (dn *DataNode) SetDead(b bool) {
	dn.mutex.Lock()
	defer dn.mutex.Unlock()
	dn.dead = b
}

func (dn *DataNode) AddOrUpdateVolume(v *storage.VolumeInfo) {
	if dn.GetVolume(v.Id) == nil {
		dn.SetVolume(v)
		dn.UpAdjustVolumeCountDelta(1)
		if !v.ReadOnly {
			dn.UpAdjustActiveVolumeCountDelta(1)
		}
		dn.UpAdjustMaxVolumeId(v.Id)
	} else {
		dn.SetVolume(v)
	}
	return
}

func (dn *DataNode) SetVolume(v *storage.VolumeInfo) {
	dn.mutex.Lock()
	defer dn.mutex.Unlock()
	dn.volumes[v.Id] = v
}

func (dn *DataNode) GetVolume(vid storage.VolumeId) (v *storage.VolumeInfo) {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	return dn.volumes[vid]
}

func (dn *DataNode) DeleteVolume(vid storage.VolumeId) {
	dn.mutex.Lock()
	defer dn.mutex.Unlock()
	delete(dn.volumes, vid)
}

func (dn *DataNode) Volumes() (list []*storage.VolumeInfo) {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	list = make([]*storage.VolumeInfo, 0, len(dn.volumes))
	for _, v := range dn.volumes {
		list = append(list, v)
	}
	return list
}

func (dn *DataNode) UpdateVolumes(actualVolumes []*storage.VolumeInfo) (deletedVolumes []*storage.VolumeInfo) {
	actualVolumeMap := make(map[storage.VolumeId]*storage.VolumeInfo)
	for _, v := range actualVolumes {
		actualVolumeMap[v.Id] = v
	}
	dn.mutex.RLock()
	for vid, v := range dn.volumes {
		if _, ok := actualVolumeMap[vid]; !ok {
			deletedVolumes = append(deletedVolumes, v)
		}
	}
	dn.mutex.RUnlock()

	for _, v := range deletedVolumes {
		glog.V(0).Infoln("Deleting volume id:", v.Id)
		dn.DeleteVolume(v.Id)
		dn.UpAdjustVolumeCountDelta(-1)
		dn.UpAdjustActiveVolumeCountDelta(-1)
	}

	//TODO: adjust max volume id, if need to reclaim volume ids

	for _, v := range actualVolumes {
		dn.AddOrUpdateVolume(v)
	}
	return
}

func (dn *DataNode) GetDataCenter() *DataCenter {
	return dn.Parent().Parent().GetValue().(*DataCenter)
}

func (dn *DataNode) GetRack() *Rack {
	return dn.Parent().GetValue().(*Rack)
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
	return net.JoinHostPort(dn.Ip, strconv.Itoa(dn.Port))
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
