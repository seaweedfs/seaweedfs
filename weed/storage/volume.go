package storage

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"

	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

type Volume struct {
	Id            needle.VolumeId
	dir           string
	Collection    string
	dataFile      *os.File
	nm            NeedleMapper
	compactingWg  sync.WaitGroup
	needleMapKind NeedleMapType
	readOnly      bool

	SuperBlock

	dataFileAccessLock    sync.Mutex
	lastModifiedTsSeconds uint64 //unix time in seconds
	lastAppendAtNs        uint64 //unix time in nanoseconds

	lastCompactIndexOffset uint64
	lastCompactRevision    uint16
}

func NewVolume(dirname string, collection string, id needle.VolumeId, needleMapKind NeedleMapType, replicaPlacement *ReplicaPlacement, ttl *needle.TTL, preallocate int64) (v *Volume, e error) {
	// if replicaPlacement is nil, the superblock will be loaded from disk
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.SuperBlock = SuperBlock{ReplicaPlacement: replicaPlacement, Ttl: ttl}
	v.needleMapKind = needleMapKind
	e = v.load(true, true, needleMapKind, preallocate)
	return
}
func (v *Volume) String() string {
	return fmt.Sprintf("Id:%v, dir:%s, Collection:%s, dataFile:%v, nm:%v, readOnly:%v", v.Id, v.dir, v.Collection, v.dataFile, v.nm, v.readOnly)
}

func VolumeFileName(collection string, dir string, id int) (fileName string) {
	idString := strconv.Itoa(id)
	if collection == "" {
		fileName = path.Join(dir, idString)
	} else {
		fileName = path.Join(dir, collection+"_"+idString)
	}
	return
}
func (v *Volume) FileName() (fileName string) {
	return VolumeFileName(v.Collection, v.dir, int(v.Id))
}
func (v *Volume) DataFile() *os.File {
	return v.dataFile
}

func (v *Volume) Version() needle.Version {
	return v.SuperBlock.Version()
}

func (v *Volume) FileStat() (datSize uint64, idxSize uint64, modTime time.Time) {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if v.dataFile == nil {
		return
	}

	stat, e := v.dataFile.Stat()
	if e == nil {
		return uint64(stat.Size()), v.nm.IndexFileSize(), stat.ModTime()
	}
	glog.V(0).Infof("Failed to read file size %s %v", v.dataFile.Name(), e)
	return // -1 causes integer overflow and the volume to become unwritable.
}

func (v *Volume) IndexFileSize() uint64 {
	return v.nm.IndexFileSize()
}

func (v *Volume) FileCount() uint64 {
	return uint64(v.nm.FileCount())
}

// Close cleanly shuts down this volume
func (v *Volume) Close() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	if v.nm != nil {
		v.nm.Close()
		v.nm = nil
	}
	if v.dataFile != nil {
		_ = v.dataFile.Close()
		v.dataFile = nil
	}
}

func (v *Volume) NeedToReplicate() bool {
	return v.ReplicaPlacement.GetCopyCount() > 1
}

func (v *Volume) ContentSize() uint64 {
	return v.nm.ContentSize()
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
	glog.V(1).Infof("now:%v lastModified:%v", time.Now().Unix(), v.lastModifiedTsSeconds)
	livedMinutes := (time.Now().Unix() - int64(v.lastModifiedTsSeconds)) / 60
	glog.V(1).Infof("ttl:%v lived:%v", v.Ttl, livedMinutes)
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
	return &master_pb.VolumeInformationMessage{
		Id:               uint32(v.Id),
		Size:             size,
		Collection:       v.Collection,
		FileCount:        uint64(v.nm.FileCount()),
		DeleteCount:      uint64(v.nm.DeletedCount()),
		DeletedByteCount: v.nm.DeletedSize(),
		ReadOnly:         v.readOnly,
		ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
		Version:          uint32(v.Version()),
		Ttl:              v.Ttl.ToUint32(),
		CompactRevision:  uint32(v.SuperBlock.CompactionRevision),
		ModifiedAtSecond: modTime.Unix(),
	}
}
