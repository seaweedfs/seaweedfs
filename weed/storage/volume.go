package storage

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

type Volume struct {
	Id                 needle.VolumeId
	dir                string
	dirIdx             string
	Collection         string
	DataBackend        backend.BackendStorageFile
	nm                 NeedleMapper
	tmpNm              TempNeedleMapper
	needleMapKind      NeedleMapKind
	noWriteOrDelete    bool // if readonly, either noWriteOrDelete or noWriteCanDelete
	noWriteCanDelete   bool // if readonly, either noWriteOrDelete or noWriteCanDelete
	noWriteLock        sync.RWMutex
	hasRemoteFile      bool // if the volume has a remote file
	MemoryMapMaxSizeMb uint32

	super_block.SuperBlock

	dataFileAccessLock    sync.RWMutex
	superBlockAccessLock  sync.Mutex
	asyncRequestsChan     chan *needle.AsyncRequest
	lastModifiedTsSeconds uint64 // unix time in seconds
	lastAppendAtNs        uint64 // unix time in nanoseconds

	lastCompactIndexOffset uint64
	lastCompactRevision    uint16
	ldbTimeout             int64

	isCompactionInProgress atomic.Bool
	lastDiskCheckNs        atomic.Int64 // unix time in nanoseconds for phantom volume detection

	volumeInfoRWLock sync.RWMutex
	volumeInfo       *volume_server_pb.VolumeInfo
	location         *DiskLocation
	diskId           uint32 // ID of this volume's disk in Store.Locations array

	// lastIoError is the most recent EIO from a read/write/delete; cleared
	// on the next successful or non-EIO op. lastIoErrorCount tracks
	// consecutive EIOs so CollectHeartbeat can require a sustained failure
	// before unmounting the replica — protects against a transient
	// hardware/network blip hitting multiple replicas at once and
	// stranding the only good copy.
	//
	// ioErrorQuarantined is sticky: once CollectHeartbeat sees the streak
	// cross IoErrorTolerance it sets this and never clears it on its own.
	// A subsequent successful read clears the streak counter but must NOT
	// un-quarantine the volume — only MarkVolumeWritable does that, after
	// an operator has decided the disk is healthy. Without the sticky
	// bit, one good read between heartbeats would silently put a known-
	// bad replica back into rotation.
	//
	// All four fields are guarded together so the heartbeat reader sees
	// a consistent snapshot.
	lastIoError        error
	lastIoErrorCount   int32
	ioErrorQuarantined bool
	lastIoErrorLock    sync.RWMutex
}

// noteIoError records an EIO and increments the consecutive-error
// counter. Caller has already verified errors.Is(err, syscall.EIO).
func (v *Volume) noteIoError(err error) {
	v.lastIoErrorLock.Lock()
	defer v.lastIoErrorLock.Unlock()
	v.lastIoError = err
	v.lastIoErrorCount++
}

// clearIoError resets the EIO streak counter only. The sticky quarantine
// bit set by CollectHeartbeat is intentionally left alone — recovery is
// an operator decision via MarkVolumeWritable. Called on any successful
// op or on a non-EIO error (which still breaks the EIO streak; only
// sustained EIOs are diagnostic of a failing volume).
func (v *Volume) clearIoError() {
	v.lastIoErrorLock.Lock()
	defer v.lastIoErrorLock.Unlock()
	v.lastIoError = nil
	v.lastIoErrorCount = 0
}

// resetIoErrorState clears both the EIO streak and the sticky quarantine
// flag. Used by MarkVolumeWritable to rejoin a previously-quarantined
// replica; if the disk is still bad, the next failed op re-arms the
// streak.
func (v *Volume) resetIoErrorState() {
	v.lastIoErrorLock.Lock()
	defer v.lastIoErrorLock.Unlock()
	v.lastIoError = nil
	v.lastIoErrorCount = 0
	v.ioErrorQuarantined = false
}

// markIoQuarantined sets the sticky quarantine flag. Idempotent; safe
// to call from CollectHeartbeat each pass while the volume remains
// quarantined.
func (v *Volume) markIoQuarantined() {
	v.lastIoErrorLock.Lock()
	defer v.lastIoErrorLock.Unlock()
	v.ioErrorQuarantined = true
}

// getIoErrorState returns the latest EIO, the consecutive-EIO count,
// and the sticky quarantine flag as one consistent snapshot.
func (v *Volume) getIoErrorState() (error, int32, bool) {
	v.lastIoErrorLock.RLock()
	defer v.lastIoErrorLock.RUnlock()
	return v.lastIoError, v.lastIoErrorCount, v.ioErrorQuarantined
}

func NewVolume(dirname string, dirIdx string, collection string, id needle.VolumeId, needleMapKind NeedleMapKind, replicaPlacement *super_block.ReplicaPlacement, ttl *needle.TTL, preallocate int64, ver needle.Version, memoryMapMaxSizeMb uint32, ldbTimeout int64) (v *Volume, e error) {
	// if replicaPlacement is nil, the superblock will be loaded from disk
	v = &Volume{dir: dirname, dirIdx: dirIdx, Collection: collection, Id: id, MemoryMapMaxSizeMb: memoryMapMaxSizeMb,
		asyncRequestsChan: make(chan *needle.AsyncRequest, 128)}
	v.SuperBlock = super_block.SuperBlock{ReplicaPlacement: replicaPlacement, Ttl: ttl}
	v.needleMapKind = needleMapKind
	v.ldbTimeout = ldbTimeout
	e = v.load(true, true, needleMapKind, preallocate, ver)
	v.startWorker()
	return
}

func (v *Volume) String() string {
	v.noWriteLock.RLock()
	defer v.noWriteLock.RUnlock()
	return fmt.Sprintf("Id:%v dir:%s dirIdx:%s Collection:%s dataFile:%v nm:%v noWrite:%v canDelete:%v", v.Id, v.dir, v.dirIdx, v.Collection, v.DataBackend, v.nm, v.noWriteOrDelete || v.noWriteCanDelete, v.noWriteCanDelete)
}

func VolumeFileName(dir string, collection string, id int) (fileName string) {
	idString := strconv.Itoa(id)
	if collection == "" {
		fileName = path.Join(dir, idString)
	} else {
		fileName = path.Join(dir, collection+"_"+idString)
	}
	return
}

func (v *Volume) DataFileName() (fileName string) {
	return VolumeFileName(v.dir, v.Collection, int(v.Id))
}

func (v *Volume) IndexFileName() (fileName string) {
	return VolumeFileName(v.dirIdx, v.Collection, int(v.Id))
}

func (v *Volume) FileName(ext string) (fileName string) {
	switch ext {
	case ".idx", ".cpx", ".ldb", ".cpldb", ".rdb":
		return VolumeFileName(v.dirIdx, v.Collection, int(v.Id)) + ext
	}
	// .dat, .cpd, .vif
	return VolumeFileName(v.dir, v.Collection, int(v.Id)) + ext
}

func (v *Volume) Version() needle.Version {
	v.superBlockAccessLock.Lock()
	defer v.superBlockAccessLock.Unlock()
	if v.volumeInfo.Version != 0 {
		v.SuperBlock.Version = needle.Version(v.volumeInfo.Version)
	}
	return v.SuperBlock.Version
}

func (v *Volume) FileStat() (datSize uint64, idxSize uint64, modTime time.Time) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	if v.DataBackend == nil {
		return
	}

	datFileSize, modTime, e := v.DataBackend.GetStat()
	if e == nil {
		return uint64(datFileSize), v.nm.IndexFileSize(), modTime
	}
	glog.V(0).Infof("Failed to read file size %s %v", v.DataBackend.Name(), e)
	return // -1 causes integer overflow and the volume to become unwritable.
}

func (v *Volume) ContentSize() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return v.nm.ContentSize()
}

func (v *Volume) doIsEmpty() (bool, error) {
	// check v.DataBackend.GetStat()
	if v.DataBackend == nil {
		return false, fmt.Errorf("v.DataBackend is nil")
	} else {
		datFileSize, _, e := v.DataBackend.GetStat()
		if e != nil {
			glog.V(0).Infof("Failed to read file size %s %v", v.DataBackend.Name(), e)
			return false, fmt.Errorf("v.DataBackend.GetStat(): %v", e)
		}
		if datFileSize > super_block.SuperBlockSize {
			return false, nil
		}
	}
	// check v.nm.ContentSize()
	if v.nm != nil {
		if v.nm.ContentSize() > 0 {
			return false, nil
		}
	}
	return true, nil
}

func (v *Volume) DeletedSize() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return v.nm.DeletedSize()
}

func (v *Volume) FileCount() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return uint64(v.nm.FileCount())
}

func (v *Volume) DeletedCount() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return uint64(v.nm.DeletedCount())
}

func (v *Volume) MaxFileKey() types.NeedleId {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return v.nm.MaxFileKey()
}

func (v *Volume) IndexFileSize() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return v.nm.IndexFileSize()
}

func (v *Volume) DiskType() types.DiskType {
	return v.location.DiskType
}

func (v *Volume) SyncToDisk() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	if v.nm != nil {
		if err := v.nm.Sync(); err != nil {
			glog.Warningf("Volume Close fail to sync volume idx %d", v.Id)
		}
	}
	if v.DataBackend != nil {
		if err := v.DataBackend.Sync(); err != nil {
			glog.Warningf("Volume Close fail to sync volume %d", v.Id)
		}
	}
}

// Close cleanly shuts down this volume
func (v *Volume) Close() {
	// Wait for any in-progress compaction to finish and claim the flag so no
	// new compaction can start. This must happen BEFORE acquiring
	// dataFileAccessLock to avoid deadlocking with CommitCompact which holds
	// the flag while waiting for the lock.
	for !v.isCompactionInProgress.CompareAndSwap(false, true) {
		time.Sleep(521 * time.Millisecond)
		glog.Warningf("Volume Close wait for compaction %d", v.Id)
	}
	defer v.isCompactionInProgress.Store(false)

	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	v.doClose()
}

// SwapDataBackend atomically replaces the data backend (e.g. swapping a
// remote-tier backend for a freshly downloaded local .dat), closing the old
// one. Held under dataFileAccessLock so a concurrent read/write never observes
// a half-swapped or closed backend.
func (v *Volume) SwapDataBackend(newBackend backend.BackendStorageFile) {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	if v.DataBackend != nil {
		v.DataBackend.Close()
	}
	v.DataBackend = newBackend
}

func (v *Volume) doClose() {
	if v.nm != nil {
		if err := v.nm.Sync(); err != nil {
			glog.Warningf("Volume Close fail to sync volume idx %d", v.Id)
		}
		v.nm.Close()
		v.nm = nil
	}
	if v.DataBackend != nil {
		if err := v.DataBackend.Close(); err != nil {
			glog.Warningf("Volume Close fail to sync volume %d", v.Id)
		}
		v.DataBackend = nil
		stats.VolumeServerVolumeGauge.WithLabelValues(v.Collection, "volume").Dec()
	}
}

func (v *Volume) NeedToReplicate() bool {
	return v.ReplicaPlacement.GetCopyCount() > 1
}

// volume is expired if modified time + volume ttl < now
// except when volume is empty
// or when the volume does not have a ttl
// or when volumeSizeLimit is 0 when server just starts
func (v *Volume) expired(contentSize uint64, volumeSizeLimit uint64) bool {
	if volumeSizeLimit == 0 {
		// skip if we don't know size limit
		return false
	}
	if contentSize <= super_block.SuperBlockSize {
		return false
	}
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	glog.V(2).Infof("volume %d now:%v lastModified:%v", v.Id, time.Now().Unix(), v.lastModifiedTsSeconds)
	livedMinutes := (time.Now().Unix() - int64(v.lastModifiedTsSeconds)) / 60
	glog.V(2).Infof("volume %d ttl:%v lived:%v", v.Id, v.Ttl, livedMinutes)
	if int64(v.Ttl.Minutes()) < livedMinutes {
		return true
	}
	return false
}

// wait either maxDelayMinutes or 10% of ttl minutes
func (v *Volume) expiredLongEnough(maxDelayMinutes uint32) bool {
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	removalDelay := v.Ttl.Minutes() / 10
	if removalDelay > maxDelayMinutes {
		removalDelay = maxDelayMinutes
	}

	if uint64(v.Ttl.Minutes()+removalDelay)*60+v.lastModifiedTsSeconds < uint64(time.Now().Unix()) {
		return true
	}
	return false
}

func (v *Volume) collectStatus() (maxFileKey types.NeedleId, datFileSize int64, modTime time.Time, fileCount, deletedCount, deletedSize uint64, ok bool) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	glog.V(4).Infof("collectStatus volume %d", v.Id)

	if v.nm == nil || v.DataBackend == nil {
		return
	}

	ok = true

	maxFileKey = v.nm.MaxFileKey()
	datFileSize, modTime, _ = v.DataBackend.GetStat()
	fileCount = uint64(v.nm.FileCount())
	deletedCount = uint64(v.nm.DeletedCount())
	deletedSize = v.nm.DeletedSize()

	return
}

func (v *Volume) ToVolumeInformationMessage() (types.NeedleId, *master_pb.VolumeInformationMessage) {

	maxFileKey, volumeSize, modTime, fileCount, deletedCount, deletedSize, ok := v.collectStatus()

	if !ok {
		return 0, nil
	}

	// Detect phantom volumes: the .dat was unlinked from disk but is still held
	// open as a deleted FD, so the volume keeps serving and heartbeating while no
	// disk-path operation can ever succeed. Skip remote-tiered volumes, whose .dat
	// legitimately lives in cloud storage. Only a present .dat is cached for 30s; a
	// missing one is re-checked every heartbeat so the volume stays suppressed until
	// the file returns. See github.com/seaweedfs/seaweedfs/issues/10004
	if fileCount > 0 && !v.HasRemoteFile() {
		const diskCheckIntervalNs = 30 * int64(time.Second)
		now := time.Now().UnixNano()
		if now-v.lastDiskCheckNs.Load() > diskCheckIntervalNs {
			if _, err := os.Stat(v.FileName(".dat")); os.IsNotExist(err) {
				glog.Warningf("Volume %d: data file %s missing (held open as deleted FD) - not reporting to master", v.Id, v.FileName(".dat"))
				return 0, nil
			}
			v.lastDiskCheckNs.Store(now)
		}
	}

	volumeInfo := &master_pb.VolumeInformationMessage{
		Id:               uint32(v.Id),
		Size:             uint64(volumeSize),
		Collection:       v.Collection,
		FileCount:        fileCount,
		DeleteCount:      deletedCount,
		DeletedByteCount: deletedSize,
		ReadOnly:         v.IsReadOnly(),
		ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
		Version:          uint32(v.Version()),
		Ttl:              v.Ttl.ToUint32(),
		CompactRevision:  uint32(v.SuperBlock.CompactionRevision),
		ModifiedAtSecond: modTime.Unix(),
		DiskType:         string(v.location.DiskType),
		DiskId:           v.diskId,
	}

	volumeInfo.RemoteStorageName, volumeInfo.RemoteStorageKey = v.RemoteStorageNameKey()

	return maxFileKey, volumeInfo
}

func (v *Volume) RemoteStorageNameKey() (storageName, storageKey string) {
	if v.volumeInfo == nil {
		return
	}
	if len(v.volumeInfo.GetFiles()) == 0 {
		return
	}
	return v.volumeInfo.GetFiles()[0].BackendName(), v.volumeInfo.GetFiles()[0].GetKey()
}

func (v *Volume) IsReadOnly() bool {
	v.noWriteLock.RLock()
	defer v.noWriteLock.RUnlock()
	return v.noWriteOrDelete || v.noWriteCanDelete || v.location.isDiskSpaceLow.Load()
}

func (v *Volume) PersistReadOnly(readOnly bool) {
	v.volumeInfoRWLock.RLock()
	defer v.volumeInfoRWLock.RUnlock()
	v.volumeInfo.ReadOnly = readOnly
	v.SaveVolumeInfo()
}
