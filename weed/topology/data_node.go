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

	MaintenanceMode bool
	// lookupDigest covers the volumes reachable through this node in the volume
	// layouts, for comparison against what its disks actually hold.
	lookupDigest atomic.Uint64
	// duplicateVolumeIds records that the node last reported one volume id more
	// than once, which the master cannot represent.
	duplicateVolumeIds atomic.Bool
	// diskMetas holds each physical disk's tags, type, and capacity from the
	// heartbeat DiskTags, including disks with no volumes or EC shards.
	diskMetas map[uint32]diskMeta
}

type diskMeta struct {
	tags           []string
	diskType       types.DiskType
	maxVolumeCount int64
}

func NewDataNode(id string) *DataNode {
	dn := &DataNode{}
	dn.id = NodeId(id)
	dn.nodeType = "DataNode"
	dn.diskUsages = newDiskUsages()
	dn.children = make(map[NodeId]Node)
	dn.capacityReservations = newCapacityReservations()
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

// AddProvisionalVolume records a volume the master registered on its own,
// ahead of any server report naming it. See Disk.AddProvisionalVolume.
func (dn *DataNode) AddProvisionalVolume(v storage.VolumeInfo) (isNew, isChanged bool) {
	dn.Lock()
	defer dn.Unlock()
	disk := dn.getOrCreateDisk(v.DiskType)
	return disk.AddProvisionalVolume(v)
}

// UpdateVolumes detects new/deleted/changed volumes on a volume server
// used in master to notify master clients of these changes.
func (dn *DataNode) UpdateVolumes(actualVolumes []storage.VolumeInfo) (newVolumes, deletedVolumes, changedVolumes []storage.VolumeInfo) {

	reported := newReportedVolumes(len(actualVolumes))
	for _, v := range actualVolumes {
		reported.add(v.Id, v.DiskType)
	}

	// A volume id mounted on two disks of one server -- a stale twin re-attached
	// after a disk repair -- is reported twice, but the master keys volumes by
	// id alone and keeps only the last copy. Its digest can then never equal the
	// server's however often the list is resent, so record it and let the
	// heartbeat fall back to the full list for this node.
	dn.duplicateVolumeIds.Store(reported.duplicated)

	dn.Lock()
	defer dn.Unlock()

	keptCount := 0
	for _, c := range dn.children {
		disk := c.(*Disk)
		for _, v := range disk.RemoveVolumesNotIn(reported) {
			glog.V(0).Infoln("Deleting volume id:", v.Id)
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
		keptCount += disk.VolumeCount()
	}
	// Everything still on the node is also in this heartbeat, so the remainder
	// is what the node is about to gain. A steady-state heartbeat gains nothing
	// and must not allocate here; a reconnecting server gains all of them.
	if addedCount := reported.count() - keptCount; addedCount > 0 {
		newVolumes = make([]storage.VolumeInfo, 0, addedCount)
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

// AdjustDiskUsageBytes records the physical filesystem capacity a volume server
// reports per disk type, applied as a delta so it flows through the same
// aggregation as the volume counts. Mirrors AdjustMaxVolumeCounts; entries with a
// zero total are treated as "not reported" and skipped.
func (dn *DataNode) AdjustDiskUsageBytes(diskTotalBytes, diskFreeBytes map[string]uint64) {
	for diskType, totalBytes := range diskTotalBytes {
		// Unlike maxVolumeCount, a 0 here is not "unset" but "not reported": let it
		// flow through so a later heartbeat that drops physical-capacity reporting
		// (e.g. statfs starts failing) clears the stale bytes and the gate falls
		// back to slot-only instead of trusting outdated capacity.
		dt := types.ToDiskType(diskType)
		currentDiskUsage := dn.diskUsages.getOrCreateDisk(dt)
		currentTotal := atomic.LoadInt64(&currentDiskUsage.diskTotalBytes)
		currentFree := atomic.LoadInt64(&currentDiskUsage.diskFreeBytes)
		newTotal := int64(totalBytes)
		newFree := int64(diskFreeBytes[diskType])
		if currentTotal == newTotal && currentFree == newFree {
			continue
		}
		disk := dn.getOrCreateDisk(dt.String())
		disk.UpAdjustDiskUsageDelta(dt, &DiskUsageCounts{
			diskTotalBytes: newTotal - currentTotal,
			diskFreeBytes:  newFree - currentFree,
		})
	}
}

func (dn *DataNode) GetVolumes() (ret []storage.VolumeInfo) {
	dn.RLock()
	defer dn.RUnlock()
	total := 0
	for _, c := range dn.children {
		total += c.(*Disk).VolumeCount()
	}
	ret = make([]storage.VolumeInfo, 0, total)
	for _, c := range dn.children {
		ret = c.(*Disk).AppendVolumes(ret)
	}
	return ret
}

// HasDuplicateVolumeIds reports whether the node's last full report named one
// volume id more than once. While it does, the node's digest is not meaningful.
func (dn *DataNode) HasDuplicateVolumeIds() bool {
	return dn.duplicateVolumeIds.Load()
}

// VolumeDigest summarises every volume the master believes this node holds. A
// volume server that reports a different digest has drifted from the master and
// needs to resend its volume list.
func (dn *DataNode) VolumeDigest() uint64 {
	dn.RLock()
	defer dn.RUnlock()
	var digest uint64
	for _, c := range dn.children {
		digest ^= c.(*Disk).VolumeDigest()
	}
	return digest
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

func (dn *DataNode) ToDataNodeInfo(filter VolumeFilter) *master_pb.DataNodeInfo {
	m := &master_pb.DataNodeInfo{
		Id: string(dn.Id()),
		// Start from disk usage counters so empty disks are still represented
		// even when there are no volumes/EC shards on this data node yet.
		DiskInfos: dn.diskUsages.ToDiskInfo(),
		GrpcPort:  uint32(dn.GrpcPort),
		Address:   dn.Url(), // ip:port for connecting to the volume server
	}
	if m.DiskInfos == nil {
		m.DiskInfos = make(map[string]*master_pb.DiskInfo)
	}
	for diskType, diskInfo := range m.DiskInfos {
		if diskInfo == nil {
			m.DiskInfos[diskType] = &master_pb.DiskInfo{Type: diskType}
			continue
		}
		diskInfo.Type = diskType
	}

	for _, c := range dn.Children() {
		disk := c.(*Disk)
		m.DiskInfos[string(disk.Id())] = disk.ToDiskInfo(filter)
	}

	dn.RLock()
	metas := make(map[uint32]diskMeta, len(dn.diskMetas))
	for diskID, meta := range dn.diskMetas {
		metas[diskID] = meta
	}
	dn.RUnlock()
	for _, diskInfo := range m.DiskInfos {
		if diskInfo == nil {
			continue
		}
		if meta, found := metas[diskInfo.DiskId]; found {
			diskInfo.Tags = append([]string(nil), meta.tags...)
		}
		// Max per physical disk of this type, empty and unavailable (max 0) ones
		// included. Emit only when some disk reports capacity, so an older server
		// sending all zeros leaves the map nil and falls back.
		diskType := types.ToDiskType(diskInfo.Type)
		maxByDisk := make(map[uint32]int64)
		anyCapacity := false
		for diskID, meta := range metas {
			if meta.diskType != diskType {
				continue
			}
			if meta.maxVolumeCount > 0 {
				anyCapacity = true
			}
			maxByDisk[diskID] = meta.maxVolumeCount
		}
		if anyCapacity {
			diskInfo.MaxVolumeCountByDisk = maxByDisk
		}
	}
	return m
}

func (dn *DataNode) UpdateDiskTags(tags []*master_pb.DiskTag) {
	if len(tags) == 0 {
		return
	}
	// DiskTags is the full list on each full heartbeat; rebuild fresh to drop
	// removed disks.
	metas := make(map[uint32]diskMeta, len(tags))
	for _, tagInfo := range tags {
		if tagInfo == nil {
			continue
		}
		metas[tagInfo.DiskId] = diskMeta{
			tags:           append([]string(nil), tagInfo.Tags...),
			diskType:       types.ToDiskType(tagInfo.Type),
			maxVolumeCount: tagInfo.MaxVolumeCount,
		}
	}
	dn.Lock()
	dn.diskMetas = metas
	dn.Unlock()
}
