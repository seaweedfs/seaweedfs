package topology

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"github.com/seaweedfs/seaweedfs/weed/storage"
)

type Disk struct {
	NodeImpl
	volumes map[needle.VolumeId]*storage.VolumeInfo
	// ecShards is nested so the same volume can retain separate entries per
	// physical disk id. A single topology Disk represents one DiskType on a
	// DataNode and may front multiple physical disks of that type, so EC
	// shards of one volume can legitimately live on several of them. The
	// outer key is the volume id; the inner key is the physical disk id.
	ecShards     map[needle.VolumeId]map[types.DiskId]*erasure_coding.EcVolumeInfo
	ecShardsLock sync.RWMutex
	// volumeDigest is the xor of every volume's ReportHash. Order-independent
	// and its own inverse, so it stays current by xoring a volume out before
	// its old state is dropped and back in after the new one lands.
	volumeDigest uint64
	// volumeIdDigest covers which volumes are on the disk, ignoring their
	// state, so it can be compared against the lookup index the master serves
	// reads from. The two indexes are maintained separately and have been seen
	// to drift.
	volumeIdDigest uint64
	// volumeAddedAt remembers when each volume reached this view of the disk
	// without a server report having confirmed it yet. Registration by the
	// master itself -- volume growth -- races the heartbeat in flight, which
	// cannot name a volume created after it was collected.
	volumeAddedAt map[needle.VolumeId]time.Time
}

// volumeRemovalGracePeriod is how long an unconfirmed volume survives a report
// that does not name it. Removing a just-grown volume strands its collection
// without writable volumes, so the report that raced the grow does not get to
// erase it; the cap keeps a registration that never materializes server-side
// from lingering forever.
const volumeRemovalGracePeriod = 10 * time.Second

func NewDisk(diskType string) *Disk {
	s := &Disk{}
	s.id = NodeId(diskType)
	s.nodeType = "Disk"
	s.diskUsages = newDiskUsages()
	s.volumes = make(map[needle.VolumeId]*storage.VolumeInfo, 2)
	s.volumeAddedAt = make(map[needle.VolumeId]time.Time, 2)
	s.ecShards = make(map[needle.VolumeId]map[types.DiskId]*erasure_coding.EcVolumeInfo, 2)
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
		a.diskTotalBytes = -b.diskTotalBytes
		a.diskFreeBytes = -b.diskFreeBytes

	}
	return t
}

func (d *DiskUsages) ToDiskInfo() map[string]*master_pb.DiskInfo {
	d.RLock()
	defer d.RUnlock()
	ret := make(map[string]*master_pb.DiskInfo)
	for diskType, diskUsageCounts := range d.usages {
		usage := diskUsageCounts.snapshot()
		m := &master_pb.DiskInfo{
			VolumeCount:       usage.volumeCount,
			MaxVolumeCount:    usage.maxVolumeCount,
			FreeVolumeCount:   usage.maxVolumeCount - (usage.volumeCount - usage.remoteVolumeCount) - erasure_coding.VolumeSlots(usage.ecShardCount),
			ActiveVolumeCount: usage.activeVolumeCount,
			RemoteVolumeCount: usage.remoteVolumeCount,
			DiskTotalBytes:    uint64(max(0, usage.diskTotalBytes)),
			DiskFreeBytes:     uint64(max(0, usage.diskFreeBytes)),
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
	// Physical filesystem capacity reported by the volume server, in bytes.
	// 0 means the volume server did not report it (e.g. an older build).
	diskTotalBytes int64
	diskFreeBytes  int64
}

func (a *DiskUsageCounts) addDiskUsageCounts(b *DiskUsageCounts) {
	atomic.AddInt64(&a.volumeCount, b.volumeCount)
	atomic.AddInt64(&a.remoteVolumeCount, b.remoteVolumeCount)
	atomic.AddInt64(&a.activeVolumeCount, b.activeVolumeCount)
	atomic.AddInt64(&a.ecShardCount, b.ecShardCount)
	atomic.AddInt64(&a.maxVolumeCount, b.maxVolumeCount)
	atomic.AddInt64(&a.diskTotalBytes, b.diskTotalBytes)
	atomic.AddInt64(&a.diskFreeBytes, b.diskFreeBytes)
}

// snapshot reads each counter atomically, so a reader sees whole values rather
// than ones a concurrent heartbeat is halfway through writing. They are still
// read one at a time, so they need not all describe the same instant.
func (a *DiskUsageCounts) snapshot() DiskUsageCounts {
	return DiskUsageCounts{
		volumeCount:       atomic.LoadInt64(&a.volumeCount),
		remoteVolumeCount: atomic.LoadInt64(&a.remoteVolumeCount),
		activeVolumeCount: atomic.LoadInt64(&a.activeVolumeCount),
		ecShardCount:      atomic.LoadInt64(&a.ecShardCount),
		maxVolumeCount:    atomic.LoadInt64(&a.maxVolumeCount),
		diskTotalBytes:    atomic.LoadInt64(&a.diskTotalBytes),
		diskFreeBytes:     atomic.LoadInt64(&a.diskFreeBytes),
	}
}

func (a *DiskUsageCounts) FreeSpace() int64 {
	u := a.snapshot()
	return u.maxVolumeCount + u.remoteVolumeCount - u.volumeCount - erasure_coding.VolumeSlots(u.ecShardCount)
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
	return d.doAddOrUpdateVolume(v, true)
}

// AddProvisionalVolume records a volume the master registered on its own --
// volume growth -- before any server report has named it. Until one does, the
// volume is protected from removal by a report that raced its creation.
func (d *Disk) AddProvisionalVolume(v storage.VolumeInfo) (isNew, isChanged bool) {
	d.Lock()
	defer d.Unlock()
	return d.doAddOrUpdateVolume(v, false)
}

func (d *Disk) doAddOrUpdateVolume(v storage.VolumeInfo, fromReport bool) (isNew, isChanged bool) {
	deltaDiskUsage := &DiskUsageCounts{}
	if oldV, ok := d.volumes[v.Id]; !ok {
		stored := v
		d.volumes[v.Id] = &stored
		if !fromReport {
			d.volumeAddedAt[v.Id] = time.Now()
		}
		d.volumeDigest ^= v.ReportHash()
		d.volumeIdDigest ^= VolumeIdDigestHash(v.Id)
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
		if !fromReport && v.DiskId == 0 && oldV.DiskId != 0 {
			// A provisional (grow-time) record carries no disk id -- the
			// master cannot know which directory the server chose. Keep the
			// one the server's report already named, before the digest below
			// is computed, or the stored record would drift from what the
			// server keeps reporting.
			v.DiskId = oldV.DiskId
		}
		if oldV.IsRemote() != v.IsRemote() {
			if v.IsRemote() {
				deltaDiskUsage.remoteVolumeCount = 1
			}
			if oldV.IsRemote() {
				deltaDiskUsage.remoteVolumeCount = -1
			}
			d.UpAdjustDiskUsageDelta(types.ToDiskType(v.DiskType), deltaDiskUsage)
		}
		d.volumeDigest ^= oldV.ReportHash() ^ v.ReportHash()
		if fromReport {
			delete(d.volumeAddedAt, v.Id)
		}
		isChanged = oldV.ReadOnly != v.ReadOnly
		if isChanged {
			// Adjust active volume count when ReadOnly status changes
			// Use a separate delta object to avoid affecting other metric adjustments
			readOnlyDelta := &DiskUsageCounts{}
			if v.ReadOnly {
				// Changed from writable to read-only
				readOnlyDelta.activeVolumeCount = -1
			} else {
				// Changed from read-only to writable
				readOnlyDelta.activeVolumeCount = 1
			}
			d.UpAdjustDiskUsageDelta(types.ToDiskType(v.DiskType), readOnlyDelta)
		}
		// Written through the pointer the map already holds, and only after
		// everything above has read the old value off it.
		*oldV = v
	}
	return
}

func (d *Disk) GetVolumes() []storage.VolumeInfo {
	return d.AppendVolumes(make([]storage.VolumeInfo, 0, d.VolumeCount()))
}

// AppendVolumeIds appends the ids of the disk's volumes to dst. Callers that
// only need to name volumes use this rather than AppendVolumes, which copies
// a whole record per volume to be read for four bytes of it.
func (d *Disk) AppendVolumeIds(dst []uint32) []uint32 {
	d.RLock()
	defer d.RUnlock()
	for id := range d.volumes {
		dst = append(dst, uint32(id))
	}
	return dst
}

// AppendVolumes appends the disk's volumes to dst, so a caller gathering
// several disks fills one slice instead of concatenating a copy per disk.
func (d *Disk) AppendVolumes(dst []storage.VolumeInfo) []storage.VolumeInfo {
	d.RLock()
	defer d.RUnlock()
	for _, v := range d.volumes {
		dst = append(dst, *v)
	}
	return dst
}

func (d *Disk) VolumeCount() int {
	d.RLock()
	defer d.RUnlock()
	return len(d.volumes)
}

// RemoveVolumesNotIn drops the volumes the heartbeat did not name on this disk
// and returns them, so a heartbeat can be diffed without copying the volume map
// out. A volume named on another disk has moved, and counts as absent here.
func (d *Disk) RemoveVolumesNotIn(reported *reportedVolumes) (removed []storage.VolumeInfo) {
	diskTypeIndex := reported.diskTypeIndex(string(d.Id()))
	d.Lock()
	defer d.Unlock()
	now := time.Now()
	for vid, v := range d.volumes {
		if reported.namedOn(vid, diskTypeIndex) {
			// The server confirmed this volume; from here on its absence from
			// a report is meaningful.
			delete(d.volumeAddedAt, vid)
			continue
		}
		// A volume the master registered itself and no report has confirmed
		// yet is likely racing the list being applied, which was collected
		// before the grow finished. Explicitly reported deletions still
		// remove immediately through DeleteVolumeById.
		if addedAt, unconfirmed := d.volumeAddedAt[vid]; unconfirmed && now.Sub(addedAt) < volumeRemovalGracePeriod {
			continue
		}
		removed = append(removed, *v)
		delete(d.volumes, vid)
		delete(d.volumeAddedAt, vid)
		d.volumeDigest ^= v.ReportHash()
		d.volumeIdDigest ^= VolumeIdDigestHash(vid)
	}
	return removed
}

func (d *Disk) GetVolumesById(id needle.VolumeId) (storage.VolumeInfo, error) {
	d.RLock()
	defer d.RUnlock()
	vInfo, ok := d.volumes[id]
	if ok {
		return *vInfo, nil
	} else {
		return storage.VolumeInfo{}, fmt.Errorf("volumeInfo not found")
	}
}

func (d *Disk) DeleteVolumeById(id needle.VolumeId) {
	d.Lock()
	defer d.Unlock()
	if v, ok := d.volumes[id]; ok {
		d.volumeDigest ^= v.ReportHash()
		d.volumeIdDigest ^= VolumeIdDigestHash(id)
		delete(d.volumes, id)
		delete(d.volumeAddedAt, id)
	}
}

// VolumeDigest returns the disk's running volume digest.
func (d *Disk) VolumeDigest() uint64 {
	d.RLock()
	defer d.RUnlock()
	return d.volumeDigest
}

// VolumeIdDigest returns the digest of which volumes the disk holds.
func (d *Disk) VolumeIdDigest() uint64 {
	d.RLock()
	defer d.RUnlock()
	return d.volumeIdDigest
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

func (d *Disk) ToDiskInfo(filter VolumeFilter) *master_pb.DiskInfo {
	diskUsage := d.diskUsages.getOrCreateDisk(types.ToDiskType(string(d.Id()))).snapshot()

	// Built under the read lock rather than from a copy as large as the
	// messages it fed. Nothing here re-enters the topology, so the hold is safe.
	d.RLock()
	// Reserving room for every volume would keep what a filter set out not to
	// build.
	capacity := 0
	if filter.SelectsEverything() {
		capacity = len(d.volumes)
	}
	volumeInfos := make([]*master_pb.VolumeInformationMessage, 0, capacity)
	var diskId uint32
	var haveDiskId bool
	for _, v := range d.volumes {
		// Any volume names the disk, including one filtered out. The smallest
		// rather than whichever the map yields first, so that two listings of
		// an unchanged disk agree when it fronts several physical disks.
		if !haveDiskId || v.DiskId < diskId {
			diskId, haveDiskId = v.DiskId, true
		}
		if !filter.matches(v.Collection, v.Id) {
			continue
		}
		volumeInfos = append(volumeInfos, v.ToVolumeInformationMessage())
	}
	d.RUnlock()

	ecShards := d.GetEcShards()
	if !haveDiskId {
		for _, ecv := range ecShards {
			if !haveDiskId || ecv.DiskId < diskId {
				diskId, haveDiskId = ecv.DiskId, true
			}
		}
	}

	m := &master_pb.DiskInfo{
		Type:              string(d.Id()),
		VolumeCount:       diskUsage.volumeCount,
		MaxVolumeCount:    diskUsage.maxVolumeCount,
		FreeVolumeCount:   diskUsage.maxVolumeCount - (diskUsage.volumeCount - diskUsage.remoteVolumeCount) - erasure_coding.VolumeSlots(diskUsage.ecShardCount),
		ActiveVolumeCount: diskUsage.activeVolumeCount,
		RemoteVolumeCount: diskUsage.remoteVolumeCount,
		DiskId:            diskId,
		DiskTotalBytes:    uint64(max(0, diskUsage.diskTotalBytes)),
		DiskFreeBytes:     uint64(max(0, diskUsage.diskFreeBytes)),
	}
	m.VolumeInfos = volumeInfos
	ecCapacity := 0
	if filter.SelectsEverything() {
		ecCapacity = len(ecShards)
	}
	m.EcShardInfos = make([]*master_pb.VolumeEcShardInformationMessage, 0, ecCapacity)
	for _, ecv := range ecShards {
		if !filter.matches(ecv.Collection, ecv.VolumeId) {
			continue
		}
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

	slices.Sort(ids)

	return util.HumanReadableIntsMax(100, ids...)
}
