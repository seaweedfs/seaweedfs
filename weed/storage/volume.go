package storage

import (
	"fmt"
	"path"
	"strconv"
	"sync"
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

	isCompacting       bool
	isCommitCompacting bool

	volumeInfoRWLock sync.RWMutex
	volumeInfo       *volume_server_pb.VolumeInfo
	location         *DiskLocation

	lastIoError error
}

func NewVolume(dirname string, dirIdx string, collection string, id needle.VolumeId, needleMapKind NeedleMapKind, replicaPlacement *super_block.ReplicaPlacement, ttl *needle.TTL, preallocate int64, memoryMapMaxSizeMb uint32, ldbTimeout int64) (v *Volume, e error) {
	// if replicaPlacement is nil, the superblock will be loaded from disk
	v = &Volume{dir: dirname, dirIdx: dirIdx, Collection: collection, Id: id, MemoryMapMaxSizeMb: memoryMapMaxSizeMb,
		asyncRequestsChan: make(chan *needle.AsyncRequest, 128)}
	v.SuperBlock = super_block.SuperBlock{ReplicaPlacement: replicaPlacement, Ttl: ttl}
	v.needleMapKind = needleMapKind
	v.ldbTimeout = ldbTimeout
	e = v.load(true, true, needleMapKind, preallocate)
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
	case ".idx", ".cpx", ".ldb", ".cpldb":
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
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	v.doClose()
}

func (v *Volume) doClose() {
	for v.isCommitCompacting {
		time.Sleep(521 * time.Millisecond)
		glog.Warningf("Volume Close wait for compaction %d", v.Id)
	}

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
	return v.noWriteOrDelete || v.noWriteCanDelete || v.location.isDiskSpaceLow
}

func (v *Volume) PersistReadOnly(readOnly bool) {
	v.volumeInfoRWLock.RLock()
	defer v.volumeInfoRWLock.RUnlock()
	v.volumeInfo.ReadOnly = readOnly
	v.SaveVolumeInfo()
}
