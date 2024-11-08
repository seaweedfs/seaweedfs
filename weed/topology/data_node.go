package topology

import (
	"fmt"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type DataNode struct {
	NodeImpl
	Ip            string
	Port          int
	GrpcPort      int
	PublicUrl     string
	LastSeen      int64 // unix time in seconds
	Counter       int   // in race condition, the previous dataNode was not dead
	IsTerminating bool
}

func NewDataNode(id string) *DataNode {
	dn := &DataNode{}
	dn.id = NodeId(id)
	dn.nodeType = "DataNode"
	dn.diskUsages = newDiskUsages()
	dn.children = make(map[NodeId]Node)
	dn.NodeImpl.value = dn
	return dn
}

func (dn *DataNode) String() string {
	dn.RLock()
	defer dn.RUnlock()
	return fmt.Sprintf("Node:%s, Ip:%s, Port:%d, PublicUrl:%s", dn.NodeImpl.String(), dn.Ip, dn.Port, dn.PublicUrl)
}

func (dn *DataNode) AddOrUpdateVolume(v storage.VolumeInfo) (isNew, isChangedRO bool) {
	dn.Lock()
	defer dn.Unlock()
	return dn.doAddOrUpdateVolume(v)
}

func (dn *DataNode) getOrCreateDisk(diskType string) *Disk {
	c, found := dn.children[NodeId(diskType)]
	if !found {
		c = NewDisk(diskType)
		dn.doLinkChildNode(c)
	}
	disk := c.(*Disk)
	return disk
}

func (dn *DataNode) doAddOrUpdateVolume(v storage.VolumeInfo) (isNew, isChanged bool) {
	disk := dn.getOrCreateDisk(v.DiskType)
	return disk.AddOrUpdateVolume(v)
}

// UpdateVolumes detects new/deleted/changed volumes on a volume server
// used in master to notify master clients of these changes.
func (dn *DataNode) UpdateVolumes(actualVolumes []storage.VolumeInfo) (newVolumes, deletedVolumes, changedVolumes []storage.VolumeInfo) {

	actualVolumeMap := make(map[needle.VolumeId]storage.VolumeInfo)
	for _, v := range actualVolumes {
		actualVolumeMap[v.Id] = v
	}

	dn.Lock()
	defer dn.Unlock()

	existingVolumes := dn.getVolumes()

	for _, v := range existingVolumes {
		vid := v.Id
		if _, ok := actualVolumeMap[vid]; !ok {
			glog.V(0).Infoln("Deleting volume id:", vid)
			disk := dn.getOrCreateDisk(v.DiskType)
			disk.DeleteVolumeById(vid)
			deletedVolumes = append(deletedVolumes, v)

			deltaDiskUsage := &DiskUsageCounts{}
			deltaDiskUsage.volumeCount = -1
			if v.IsRemote() {
				deltaDiskUsage.remoteVolumeCount = -1
			}
			if !v.ReadOnly {
				deltaDiskUsage.activeVolumeCount = -1
			}
			disk.UpAdjustDiskUsageDelta(types.ToDiskType(v.DiskType), deltaDiskUsage)
		}
	}
	for _, v := range actualVolumes {
		isNew, isChanged := dn.doAddOrUpdateVolume(v)
		if isNew {
			newVolumes = append(newVolumes, v)
		}
		if isChanged {
			changedVolumes = append(changedVolumes, v)
		}
	}
	return
}

func (dn *DataNode) DeltaUpdateVolumes(newVolumes, deletedVolumes []storage.VolumeInfo) {
	dn.Lock()
	defer dn.Unlock()

	for _, v := range deletedVolumes {
		disk := dn.getOrCreateDisk(v.DiskType)

		_, err := disk.GetVolumesById(v.Id)
		if err != nil {
			continue
		}
		disk.DeleteVolumeById(v.Id)

		deltaDiskUsage := &DiskUsageCounts{}
		deltaDiskUsage.volumeCount = -1
		if v.IsRemote() {
			deltaDiskUsage.remoteVolumeCount = -1
		}
		if !v.ReadOnly {
			deltaDiskUsage.activeVolumeCount = -1
		}
		disk.UpAdjustDiskUsageDelta(types.ToDiskType(v.DiskType), deltaDiskUsage)
	}
	for _, v := range newVolumes {
		dn.doAddOrUpdateVolume(v)
	}
	return
}

func (dn *DataNode) AdjustMaxVolumeCounts(maxVolumeCounts map[string]uint32) {
	for diskType, maxVolumeCount := range maxVolumeCounts {
		if maxVolumeCount == 0 {
			// the volume server may have set the max to zero
			continue
		}
		dt := types.ToDiskType(diskType)
		currentDiskUsage := dn.diskUsages.getOrCreateDisk(dt)
		currentDiskUsageMaxVolumeCount := atomic.LoadInt64(&currentDiskUsage.maxVolumeCount)
		if currentDiskUsageMaxVolumeCount == int64(maxVolumeCount) {
			continue
		}
		disk := dn.getOrCreateDisk(dt.String())
		disk.UpAdjustDiskUsageDelta(dt, &DiskUsageCounts{
			maxVolumeCount: int64(maxVolumeCount) - currentDiskUsageMaxVolumeCount,
		})
	}
}

func (dn *DataNode) GetVolumes() (ret []storage.VolumeInfo) {
	dn.RLock()
	for _, c := range dn.children {
		disk := c.(*Disk)
		ret = append(ret, disk.GetVolumes()...)
	}
	dn.RUnlock()
	return ret
}

func (dn *DataNode) GetVolumesById(id needle.VolumeId) (vInfo storage.VolumeInfo, err error) {
	dn.RLock()
	defer dn.RUnlock()
	found := false
	for _, c := range dn.children {
		disk := c.(*Disk)
		vInfo, err = disk.GetVolumesById(id)
		if err == nil {
			found = true
			break
		}
	}
	if found {
		return vInfo, nil
	} else {
		return storage.VolumeInfo{}, fmt.Errorf("volumeInfo not found")
	}
}

func (dn *DataNode) GetDataCenter() *DataCenter {
	rack := dn.Parent()
	if rack == nil {
		return nil
	}
	dcNode := rack.Parent()
	if dcNode == nil {
		return nil
	}
	dcValue := dcNode.GetValue()
	return dcValue.(*DataCenter)
}

func (dn *DataNode) GetDataCenterId() string {
	if dc := dn.GetDataCenter(); dc != nil {
		return string(dc.Id())
	}
	return ""
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
	return util.JoinHostPort(dn.Ip, dn.Port)
}

func (dn *DataNode) ServerAddress() pb.ServerAddress {
	return pb.NewServerAddress(dn.Ip, dn.Port, dn.GrpcPort)
}

type DataNodeInfo struct {
	Url       string `json:"Url"`
	PublicUrl string `json:"PublicUrl"`
	Volumes   int64  `json:"Volumes"`
	EcShards  int64  `json:"EcShards"`
	Max       int64  `json:"Max"`
	VolumeIds string `json:"VolumeIds"`
}

func (dn *DataNode) ToInfo() (info DataNodeInfo) {
	info.Url = dn.Url()
	info.PublicUrl = dn.PublicUrl

	// aggregated volume info
	var volumeCount, ecShardCount, maxVolumeCount int64
	var volumeIds string
	for _, diskUsage := range dn.diskUsages.usages {
		volumeCount += diskUsage.volumeCount
		ecShardCount += diskUsage.ecShardCount
		maxVolumeCount += diskUsage.maxVolumeCount
	}

	for _, disk := range dn.Children() {
		d := disk.(*Disk)
		volumeIds += " " + d.GetVolumeIds()
	}

	info.Volumes = volumeCount
	info.EcShards = ecShardCount
	info.Max = maxVolumeCount
	info.VolumeIds = volumeIds

	return
}

func (dn *DataNode) ToDataNodeInfo() *master_pb.DataNodeInfo {
	m := &master_pb.DataNodeInfo{
		Id:        string(dn.Id()),
		DiskInfos: make(map[string]*master_pb.DiskInfo),
		GrpcPort:  uint32(dn.GrpcPort),
	}
	for _, c := range dn.Children() {
		disk := c.(*Disk)
		m.DiskInfos[string(disk.Id())] = disk.ToDiskInfo()
	}
	return m
}

// GetVolumeIds returns the human readable volume ids limited to count of max 100.
func (dn *DataNode) GetVolumeIds() string {
	dn.RLock()
	defer dn.RUnlock()
	existingVolumes := dn.getVolumes()
	ids := make([]int, 0, len(existingVolumes))

	for k := range existingVolumes {
		ids = append(ids, int(k))
	}

	return util.HumanReadableIntsMax(100, ids...)
}

func (dn *DataNode) getVolumes() []storage.VolumeInfo {
	var existingVolumes []storage.VolumeInfo
	for _, c := range dn.children {
		disk := c.(*Disk)
		existingVolumes = append(existingVolumes, disk.GetVolumes()...)
	}
	return existingVolumes
}
