package storage

import (
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

type Volume struct {
	Id                 needle.VolumeId
	dir                string
	Collection         string
	DataBackend        backend.BackendStorageFile
	nm                 NeedleMapper
	needleMapKind      NeedleMapType
	noWriteOrDelete    bool // if readonly, either noWriteOrDelete or noWriteCanDelete
	noWriteCanDelete   bool // if readonly, either noWriteOrDelete or noWriteCanDelete
	hasRemoteFile      bool // if the volume has a remote file
	MemoryMapMaxSizeMb uint32

	super_block.SuperBlock

	dataFileAccessLock    sync.RWMutex
	asyncRequestsChan     chan *needle.AsyncRequest
	lastModifiedTsSeconds uint64 //unix time in seconds
	lastAppendAtNs        uint64 //unix time in nanoseconds

	lastCompactIndexOffset uint64
	lastCompactRevision    uint16

	isCompacting bool

	volumeInfo *volume_server_pb.VolumeInfo
}

func NewVolume(dirname string, collection string, id needle.VolumeId, needleMapKind NeedleMapType, replicaPlacement *super_block.ReplicaPlacement, ttl *needle.TTL, preallocate int64, memoryMapMaxSizeMb uint32) (v *Volume, e error) {
	// if replicaPlacement is nil, the superblock will be loaded from disk
	v = &Volume{dir: dirname, Collection: collection, Id: id, MemoryMapMaxSizeMb: memoryMapMaxSizeMb,
		asyncRequestsChan: make(chan *needle.AsyncRequest, 128)}
	v.SuperBlock = super_block.SuperBlock{ReplicaPlacement: replicaPlacement, Ttl: ttl}
	v.needleMapKind = needleMapKind
	e = v.load(true, true, needleMapKind, preallocate)
	v.startWorker()
	return
}

func (v *Volume) String() string {
	return fmt.Sprintf("Id:%v, dir:%s, Collection:%s, dataFile:%v, nm:%v, noWrite:%v canDelete:%v", v.Id, v.dir, v.Collection, v.DataBackend, v.nm, v.noWriteOrDelete || v.noWriteCanDelete, v.noWriteCanDelete)
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

func (v *Volume) FileName() (fileName string) {
	return VolumeFileName(v.dir, v.Collection, int(v.Id))
}

func (v *Volume) Version() needle.Version {
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

// Close cleanly shuts down this volume
func (v *Volume) Close() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	if v.nm != nil {
		v.nm.Close()
		v.nm = nil
	}
	if v.DataBackend != nil {
		_ = v.DataBackend.Close()
		v.DataBackend = nil
		stats.VolumeServerVolumeCounter.WithLabelValues(v.Collection, "volume").Dec()
	}
}

func (v *Volume) NeedToReplicate() bool {
	return v.ReplicaPlacement.GetCopyCount() > 1
}

// volume is expired if modified time + volume ttl < now
// except when volume is empty
// or when the volume does not have a ttl
// or when volumeSizeLimit is 0 when server just starts
func (v *Volume) expired(volumeSizeLimit uint64) bool {
	if volumeSizeLimit == 0 {
		//skip if we don't know size limit
		return false
	}
	if v.ContentSize() == 0 {
		return false
	}
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	glog.V(2).Infof("now:%v lastModified:%v", time.Now().Unix(), v.lastModifiedTsSeconds)
	livedMinutes := (time.Now().Unix() - int64(v.lastModifiedTsSeconds)) / 60
	glog.V(2).Infof("ttl:%v lived:%v", v.Ttl, livedMinutes)
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

func (v *Volume) ToVolumeInformationMessage() *master_pb.VolumeInformationMessage {
	size, _, modTime := v.FileStat()

	volumInfo := &master_pb.VolumeInformationMessage{
		Id:               uint32(v.Id),
		Size:             size,
		Collection:       v.Collection,
		FileCount:        v.FileCount(),
		DeleteCount:      v.DeletedCount(),
		DeletedByteCount: v.DeletedSize(),
		ReadOnly:         v.IsReadOnly(),
		ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
		Version:          uint32(v.Version()),
		Ttl:              v.Ttl.ToUint32(),
		CompactRevision:  uint32(v.SuperBlock.CompactionRevision),
		ModifiedAtSecond: modTime.Unix(),
	}

	volumInfo.RemoteStorageName, volumInfo.RemoteStorageKey = v.RemoteStorageNameKey()

	return volumInfo
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
	return v.noWriteOrDelete || v.noWriteCanDelete
}
