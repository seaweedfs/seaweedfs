package topology

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util"
	"strconv"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

type DataNode struct {
	NodeImpl
	volumes      map[needle.VolumeId]storage.VolumeInfo
	Ip           string
	Port         int
	PublicUrl    string
	LastSeen     int64 // unix time in seconds
	ecShards     map[needle.VolumeId]*erasure_coding.EcVolumeInfo
	ecShardsLock sync.RWMutex
}

func NewDataNode(id string) *DataNode {
	s := &DataNode{}
	s.id = NodeId(id)
	s.nodeType = "DataNode"
	s.volumes = make(map[needle.VolumeId]storage.VolumeInfo)
	s.ecShards = make(map[needle.VolumeId]*erasure_coding.EcVolumeInfo)
	s.NodeImpl.value = s
	return s
}

func (dn *DataNode) String() string {
	dn.RLock()
	defer dn.RUnlock()
	return fmt.Sprintf("Node:%s, volumes:%v, Ip:%s, Port:%d, PublicUrl:%s", dn.NodeImpl.String(), dn.volumes, dn.Ip, dn.Port, dn.PublicUrl)
}

func (dn *DataNode) AddOrUpdateVolume(v storage.VolumeInfo) (isNew, isChangedRO bool) {
	dn.Lock()
	defer dn.Unlock()
	return dn.doAddOrUpdateVolume(v)
}

func (dn *DataNode) doAddOrUpdateVolume(v storage.VolumeInfo) (isNew, isChangedRO bool) {
	if oldV, ok := dn.volumes[v.Id]; !ok {
		dn.volumes[v.Id] = v
		dn.UpAdjustVolumeCountDelta(1)
		if v.IsRemote() {
			dn.UpAdjustRemoteVolumeCountDelta(1)
		}
		if !v.ReadOnly {
			dn.UpAdjustActiveVolumeCountDelta(1)
		}
		dn.UpAdjustMaxVolumeId(v.Id)
		isNew = true
	} else {
		if oldV.IsRemote() != v.IsRemote() {
			if v.IsRemote() {
				dn.UpAdjustRemoteVolumeCountDelta(1)
			}
			if oldV.IsRemote() {
				dn.UpAdjustRemoteVolumeCountDelta(-1)
			}
		}
		isChangedRO = dn.volumes[v.Id].ReadOnly != v.ReadOnly
		dn.volumes[v.Id] = v
	}
	return
}

func (dn *DataNode) UpdateVolumes(actualVolumes []storage.VolumeInfo) (newVolumes, deletedVolumes, changeRO []storage.VolumeInfo) {

	actualVolumeMap := make(map[needle.VolumeId]storage.VolumeInfo)
	for _, v := range actualVolumes {
		actualVolumeMap[v.Id] = v
	}

	dn.Lock()
	defer dn.Unlock()

	for vid, v := range dn.volumes {
		if _, ok := actualVolumeMap[vid]; !ok {
			glog.V(0).Infoln("Deleting volume id:", vid)
			delete(dn.volumes, vid)
			deletedVolumes = append(deletedVolumes, v)
			dn.UpAdjustVolumeCountDelta(-1)
			if v.IsRemote() {
				dn.UpAdjustRemoteVolumeCountDelta(-1)
			}
			if !v.ReadOnly {
				dn.UpAdjustActiveVolumeCountDelta(-1)
			}
		}
	}
	for _, v := range actualVolumes {
		isNew, isChangedRO := dn.doAddOrUpdateVolume(v)
		if isNew {
			newVolumes = append(newVolumes, v)
		}
		if isChangedRO {
			changeRO = append(changeRO, v)
		}
	}
	return
}

func (dn *DataNode) DeltaUpdateVolumes(newVolumes, deletedVolumes []storage.VolumeInfo) {
	dn.Lock()
	defer dn.Unlock()

	for _, v := range deletedVolumes {
		delete(dn.volumes, v.Id)
		dn.UpAdjustVolumeCountDelta(-1)
		if v.IsRemote() {
			dn.UpAdjustRemoteVolumeCountDelta(-1)
		}
		if !v.ReadOnly {
			dn.UpAdjustActiveVolumeCountDelta(-1)
		}
	}
	for _, v := range newVolumes {
		dn.doAddOrUpdateVolume(v)
	}
	return
}

func (dn *DataNode) GetVolumes() (ret []storage.VolumeInfo) {
	dn.RLock()
	for _, v := range dn.volumes {
		ret = append(ret, v)
	}
	dn.RUnlock()
	return ret
}

func (dn *DataNode) GetVolumesById(id needle.VolumeId) (storage.VolumeInfo, error) {
	dn.RLock()
	defer dn.RUnlock()
	vInfo, ok := dn.volumes[id]
	if ok {
		return vInfo, nil
	} else {
		return storage.VolumeInfo{}, fmt.Errorf("volumeInfo not found")
	}
}

func (dn *DataNode) GetDataCenter() *DataCenter {
	rack := dn.Parent()
	dcNode := rack.Parent()
	dcValue := dcNode.GetValue()
	return dcValue.(*DataCenter)
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
	ret["VolumeIds"] = dn.GetVolumeIds()
	ret["EcShards"] = dn.GetEcShardCount()
	ret["Max"] = dn.GetMaxVolumeCount()
	ret["Free"] = dn.FreeSpace()
	ret["PublicUrl"] = dn.PublicUrl
	return ret
}

func (dn *DataNode) ToDataNodeInfo() *master_pb.DataNodeInfo {
	m := &master_pb.DataNodeInfo{
		Id:                string(dn.Id()),
		VolumeCount:       uint64(dn.GetVolumeCount()),
		MaxVolumeCount:    uint64(dn.GetMaxVolumeCount()),
		FreeVolumeCount:   uint64(dn.FreeSpace()),
		ActiveVolumeCount: uint64(dn.GetActiveVolumeCount()),
		RemoteVolumeCount: uint64(dn.GetRemoteVolumeCount()),
	}
	for _, v := range dn.GetVolumes() {
		m.VolumeInfos = append(m.VolumeInfos, v.ToVolumeInformationMessage())
	}
	for _, ecv := range dn.GetEcShards() {
		m.EcShardInfos = append(m.EcShardInfos, ecv.ToVolumeEcShardInformationMessage())
	}
	return m
}

// GetVolumeIds returns the human readable volume ids limited to count of max 100.
func (dn *DataNode) GetVolumeIds() string {
	dn.RLock()
	defer dn.RUnlock()
	ids := make([]int, 0, len(dn.volumes))

	for k := range dn.volumes {
		ids = append(ids, int(k))
	}

	return util.HumanReadableIntsMax(100, ids...)
}
