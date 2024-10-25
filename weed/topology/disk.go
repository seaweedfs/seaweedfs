package topology

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"github.com/seaweedfs/seaweedfs/weed/storage"
)

type Disk struct {
	NodeImpl
	volumes      map[needle.VolumeId]storage.VolumeInfo
	ecShards     map[needle.VolumeId]*erasure_coding.EcVolumeInfo
	ecShardsLock sync.RWMutex
}

func NewDisk(diskType string) *Disk {
	s := &Disk{}
	s.id = NodeId(diskType)
	s.nodeType = "Disk"
	s.diskUsages = newDiskUsages()
	s.volumes = make(map[needle.VolumeId]storage.VolumeInfo, 2)
	s.ecShards = make(map[needle.VolumeId]*erasure_coding.EcVolumeInfo, 2)
	s.NodeImpl.value = s
	return s
}

type DiskUsages struct {
	sync.RWMutex
	usages map[types.DiskType]*DiskUsageCounts
}

func newDiskUsages() *DiskUsages {
	return &DiskUsages{
		usages: make(map[types.DiskType]*DiskUsageCounts),
	}
}

func (d *DiskUsages) negative() *DiskUsages {
	d.RLock()
	defer d.RUnlock()
	t := newDiskUsages()
	for diskType, b := range d.usages {
		a := t.getOrCreateDisk(diskType)
		a.volumeCount = -b.volumeCount
		a.remoteVolumeCount = -b.remoteVolumeCount
		a.activeVolumeCount = -b.activeVolumeCount
		a.ecShardCount = -b.ecShardCount
		a.maxVolumeCount = -b.maxVolumeCount

	}
	return t
}

func (d *DiskUsages) ToDiskInfo() map[string]*master_pb.DiskInfo {
	ret := make(map[string]*master_pb.DiskInfo)
	for diskType, diskUsageCounts := range d.usages {
		m := &master_pb.DiskInfo{
			VolumeCount:       diskUsageCounts.volumeCount,
			MaxVolumeCount:    diskUsageCounts.maxVolumeCount,
			FreeVolumeCount:   diskUsageCounts.maxVolumeCount - (diskUsageCounts.volumeCount - diskUsageCounts.remoteVolumeCount) - (diskUsageCounts.ecShardCount+1)/erasure_coding.DataShardsCount,
			ActiveVolumeCount: diskUsageCounts.activeVolumeCount,
			RemoteVolumeCount: diskUsageCounts.remoteVolumeCount,
		}
		ret[string(diskType)] = m
	}
	return ret
}

func (d *DiskUsages) FreeSpace() (freeSpace int64) {
	d.RLock()
	defer d.RUnlock()
	for _, diskUsage := range d.usages {
		freeSpace += diskUsage.FreeSpace()
	}
	return
}

func (d *DiskUsages) GetMaxVolumeCount() (maxVolumeCount int64) {
	d.RLock()
	defer d.RUnlock()
	for _, diskUsage := range d.usages {
		maxVolumeCount += diskUsage.maxVolumeCount
	}
	return
}

type DiskUsageCounts struct {
	volumeCount       int64
	remoteVolumeCount int64
	activeVolumeCount int64
	ecShardCount      int64
	maxVolumeCount    int64
}

func (a *DiskUsageCounts) addDiskUsageCounts(b *DiskUsageCounts) {
	atomic.AddInt64(&a.volumeCount, b.volumeCount)
	atomic.AddInt64(&a.remoteVolumeCount, b.remoteVolumeCount)
	atomic.AddInt64(&a.activeVolumeCount, b.activeVolumeCount)
	atomic.AddInt64(&a.ecShardCount, b.ecShardCount)
	atomic.AddInt64(&a.maxVolumeCount, b.maxVolumeCount)
}

func (a *DiskUsageCounts) FreeSpace() int64 {
	freeVolumeSlotCount := a.maxVolumeCount + a.remoteVolumeCount - a.volumeCount
	if a.ecShardCount > 0 {
		freeVolumeSlotCount = freeVolumeSlotCount - a.ecShardCount/erasure_coding.DataShardsCount - 1
	}
	return freeVolumeSlotCount
}

func (a *DiskUsageCounts) minus(b *DiskUsageCounts) *DiskUsageCounts {
	return &DiskUsageCounts{
		volumeCount:       a.volumeCount - b.volumeCount,
		remoteVolumeCount: a.remoteVolumeCount - b.remoteVolumeCount,
		activeVolumeCount: a.activeVolumeCount - b.activeVolumeCount,
		ecShardCount:      a.ecShardCount - b.ecShardCount,
		maxVolumeCount:    a.maxVolumeCount - b.maxVolumeCount,
	}
}

func (du *DiskUsages) getOrCreateDisk(diskType types.DiskType) *DiskUsageCounts {
	du.Lock()
	defer du.Unlock()
	t, found := du.usages[diskType]
	if found {
		return t
	}
	t = &DiskUsageCounts{}
	du.usages[diskType] = t
	return t
}

func (d *Disk) String() string {
	d.RLock()
	defer d.RUnlock()
	return fmt.Sprintf("Disk:%s, volumes:%v, ecShards:%v", d.NodeImpl.String(), d.volumes, d.ecShards)
}

func (d *Disk) AddOrUpdateVolume(v storage.VolumeInfo) (isNew, isChanged bool) {
	d.Lock()
	defer d.Unlock()
	return d.doAddOrUpdateVolume(v)
}

func (d *Disk) doAddOrUpdateVolume(v storage.VolumeInfo) (isNew, isChanged bool) {
	deltaDiskUsage := &DiskUsageCounts{}
	if oldV, ok := d.volumes[v.Id]; !ok {
		d.volumes[v.Id] = v
		deltaDiskUsage.volumeCount = 1
		if v.IsRemote() {
			deltaDiskUsage.remoteVolumeCount = 1
		}
		if !v.ReadOnly {
			deltaDiskUsage.activeVolumeCount = 1
		}
		d.UpAdjustMaxVolumeId(v.Id)
		d.UpAdjustDiskUsageDelta(types.ToDiskType(v.DiskType), deltaDiskUsage)
		isNew = true
	} else {
		if oldV.IsRemote() != v.IsRemote() {
			if v.IsRemote() {
				deltaDiskUsage.remoteVolumeCount = 1
			}
			if oldV.IsRemote() {
				deltaDiskUsage.remoteVolumeCount = -1
			}
			d.UpAdjustDiskUsageDelta(types.ToDiskType(v.DiskType), deltaDiskUsage)
		}
		isChanged = d.volumes[v.Id].ReadOnly != v.ReadOnly
		d.volumes[v.Id] = v
	}
	return
}

func (d *Disk) GetVolumes() (ret []storage.VolumeInfo) {
	d.RLock()
	for _, v := range d.volumes {
		ret = append(ret, v)
	}
	d.RUnlock()
	return ret
}

func (d *Disk) GetVolumesById(id needle.VolumeId) (storage.VolumeInfo, error) {
	d.RLock()
	defer d.RUnlock()
	vInfo, ok := d.volumes[id]
	if ok {
		return vInfo, nil
	} else {
		return storage.VolumeInfo{}, fmt.Errorf("volumeInfo not found")
	}
}

func (d *Disk) DeleteVolumeById(id needle.VolumeId) {
	d.Lock()
	defer d.Unlock()
	delete(d.volumes, id)
}

func (d *Disk) GetDataCenter() *DataCenter {
	dn := d.Parent()
	rack := dn.Parent()
	dcNode := rack.Parent()
	dcValue := dcNode.GetValue()
	return dcValue.(*DataCenter)
}

func (d *Disk) GetRack() *Rack {
	return d.Parent().Parent().(*NodeImpl).value.(*Rack)
}

func (d *Disk) GetTopology() *Topology {
	p := d.Parent()
	for p.Parent() != nil {
		p = p.Parent()
	}
	t := p.(*Topology)
	return t
}

func (d *Disk) ToMap() interface{} {
	ret := make(map[string]interface{})
	diskUsage := d.diskUsages.getOrCreateDisk(types.ToDiskType(string(d.Id())))
	ret["Volumes"] = diskUsage.volumeCount
	ret["VolumeIds"] = d.GetVolumeIds()
	ret["EcShards"] = diskUsage.ecShardCount
	ret["Max"] = diskUsage.maxVolumeCount
	ret["Free"] = d.FreeSpace()
	return ret
}

func (d *Disk) FreeSpace() int64 {
	t := d.diskUsages.getOrCreateDisk(types.ToDiskType(string(d.Id())))
	return t.FreeSpace()
}

func (d *Disk) ToDiskInfo() *master_pb.DiskInfo {
	diskUsage := d.diskUsages.getOrCreateDisk(types.ToDiskType(string(d.Id())))
	m := &master_pb.DiskInfo{
		Type:              string(d.Id()),
		VolumeCount:       diskUsage.volumeCount,
		MaxVolumeCount:    diskUsage.maxVolumeCount,
		FreeVolumeCount:   diskUsage.maxVolumeCount - (diskUsage.volumeCount - diskUsage.remoteVolumeCount) - (diskUsage.ecShardCount+1)/erasure_coding.DataShardsCount,
		ActiveVolumeCount: diskUsage.activeVolumeCount,
		RemoteVolumeCount: diskUsage.remoteVolumeCount,
	}
	for _, v := range d.GetVolumes() {
		m.VolumeInfos = append(m.VolumeInfos, v.ToVolumeInformationMessage())
	}
	for _, ecv := range d.GetEcShards() {
		m.EcShardInfos = append(m.EcShardInfos, ecv.ToVolumeEcShardInformationMessage())
	}
	return m
}

// GetVolumeIds returns the human readable volume ids limited to count of max 100.
func (d *Disk) GetVolumeIds() string {
	d.RLock()
	defer d.RUnlock()
	ids := make([]int, 0, len(d.volumes))

	for k := range d.volumes {
		ids = append(ids, int(k))
	}

	return util.HumanReadableIntsMax(100, ids...)
}
