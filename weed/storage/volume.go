package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

type Volume struct {
	Id            VolumeId
	dir           string
	Collection    string
	dataFile      *os.File
	nm            NeedleMapper
	needleMapKind NeedleMapType
	readOnly      bool

	SuperBlock

	mutex            sync.RWMutex
	lastModifiedTime uint64 //unix time in seconds
}

func NewVolume(dirname string, collection string, id VolumeId, needleMapKind NeedleMapType, ttl *TTL) (v *Volume, e error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.SuperBlock = SuperBlock{Ttl: ttl}
	v.needleMapKind = needleMapKind
	e = v.load(true, true, needleMapKind)
	return
}
func (v *Volume) String() string {
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	return fmt.Sprintf("Id:%v, dir:%s, Collection:%s, dataFile:%v, nm:%v, readOnly:%v",
		v.Id,
		v.dir,
		v.Collection,
		v.dataFile,
		v.nm,
		v.readOnly)
}

func loadVolumeWithoutIndex(dirname string, collection string, id VolumeId, needleMapKind NeedleMapType) (v *Volume, e error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.SuperBlock = SuperBlock{}
	v.needleMapKind = needleMapKind
	e = v.load(false, false, needleMapKind)
	return
}
func (v *Volume) FileName() (fileName string) {
	if v.Collection == "" {
		fileName = path.Join(v.dir, v.Id.String())
	} else {
		fileName = path.Join(v.dir, v.Collection+"_"+v.Id.String())
	}
	return
}
func (v *Volume) DataFile() *os.File {
	return v.dataFile
}
func (v *Volume) load(alsoLoadIndex bool, createDatIfMissing bool, needleMapKind NeedleMapType) error {
	var e error
	fileName := v.FileName()

	if exists, canRead, canWrite, modifiedTime := checkFile(fileName + ".dat"); exists {
		if !canRead {
			return fmt.Errorf("cannot read Volume Data file %s.dat", fileName)
		}
		if canWrite {
			v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
			v.lastModifiedTime = uint64(modifiedTime.Unix())
		} else {
			glog.V(0).Infoln("opening " + fileName + ".dat in READONLY mode")
			v.dataFile, e = os.Open(fileName + ".dat")
			v.readOnly = true
		}
	} else {
		if createDatIfMissing {
			v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
		} else {
			return fmt.Errorf("Volume Data file %s.dat does not exist.", fileName)
		}
	}

	if e != nil {
		if !os.IsPermission(e) {
			return fmt.Errorf("cannot load Volume Data %s.dat: %v", fileName, e)
		}
	}

	if e = v.maybeWriteSuperBlock(); e != nil {
		return e
	}
	if e = v.readSuperBlock(); e != nil {
		return e
	}

	if alsoLoadIndex {
		var indexFile *os.File
		if v.readOnly {
			glog.V(1).Infoln("open to read file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDONLY, 0644); e != nil {
				return fmt.Errorf("cannot read Volume Index %s.idx: %v", fileName, e)
			}
		} else {
			glog.V(1).Infoln("open to write file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDWR|os.O_CREATE, 0644); e != nil {
				return fmt.Errorf("cannot write Volume Index %s.idx: %v", fileName, e)
			}
		}
		switch needleMapKind {
		case NeedleMapInMemory:
			glog.V(1).Infoln("loading index file", fileName+".idx", "readonly", v.readOnly)
			if v.nm, e = LoadNeedleMap(indexFile); e != nil {
				glog.V(0).Infof("loading index %s error: %v", fileName+".idx", e)
			}
		case NeedleMapLevelDb:
			glog.V(1).Infoln("loading leveldb file", fileName+".ldb")
			if v.nm, e = NewLevelDbNeedleMap(fileName+".ldb", indexFile); e != nil {
				glog.V(0).Infof("loading leveldb %s error: %v", fileName+".ldb", e)
			}
		case NeedleMapBoltDb:
			glog.V(1).Infoln("loading boltdb file", fileName+".bdb")
			if v.nm, e = NewBoltDbNeedleMap(fileName+".bdb", indexFile); e != nil {
				glog.V(0).Infof("loading boltdb %s error: %v", fileName+".bdb", e)
			}
		}
	}
	return e
}
func (v *Volume) Version() Version {
	return v.SuperBlock.Version()
}
func (v *Volume) Size() int64 {
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	stat, e := v.dataFile.Stat()
	if e == nil {
		return stat.Size()
	}
	glog.V(2).Infof("Failed to read file size %s %v", v.dataFile.Name(), e)
	return -1
}

// Close cleanly shuts down this volume
func (v *Volume) Close() {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	v.nm.Close()
	v.dataFile.Close()
}

// isFileUnchanged checks whether this needle to write is same as last one.
// It requires serialized access in the same volume.
func (v *Volume) isFileUnchanged(n *Needle) bool {
	if v.Ttl.String() != "" {
		return false
	}
	nv, ok := v.nm.Get(n.Id)
	if ok && nv.Offset > 0 {
		oldNeedle := new(Needle)
		err := oldNeedle.ReadData(v.dataFile, int64(nv.Offset)*NeedlePaddingSize, nv.Size, v.Version())
		if err != nil {
			glog.V(0).Infof("Failed to check updated file %v", err)
			return false
		}
		if oldNeedle.Checksum == n.Checksum && bytes.Equal(oldNeedle.Data, n.Data) {
			n.DataSize = oldNeedle.DataSize
			return true
		}
	}
	return false
}

// Destroy removes everything related to this volume
func (v *Volume) Destroy() (err error) {
	if v.IsReadOnly() {
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	v.Close()
	err = os.Remove(v.dataFile.Name())
	if err != nil {
		return
	}
	err = v.nm.Destroy()
	return
}

// AppendBlob append a blob to end of the data file, used in replication
func (v *Volume) AppendBlob(b []byte) (offset int64, err error) {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	if v.readOnly {
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	if offset, err = v.dataFile.Seek(0, 2); err != nil {
		glog.V(0).Infof("failed to seek the end of file: %v", err)
		return
	}
	//ensure file writing starting from aligned positions
	if offset%NeedlePaddingSize != 0 {
		offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
		if offset, err = v.dataFile.Seek(offset, 0); err != nil {
			glog.V(0).Infof("failed to align in datafile %s: %v", v.dataFile.Name(), err)
			return
		}
	}
	v.dataFile.Write(b)
	return
}

func (v *Volume) write(n *Needle) (size uint32, err error) {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	glog.V(4).Infof("writing needle %s", NewFileIdFromNeedle(v.Id, n).String())
	if v.readOnly {
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	if v.isFileUnchanged(n) {
		size = n.DataSize
		glog.V(4).Infof("needle is unchanged!")
		return
	}
	var offset int64
	if offset, err = v.dataFile.Seek(0, 2); err != nil {
		glog.V(0).Infof("failed to seek the end of file: %v", err)
		return
	}

	//ensure file writing starting from aligned positions
	if offset%NeedlePaddingSize != 0 {
		offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
		if offset, err = v.dataFile.Seek(offset, 0); err != nil {
			glog.V(0).Infof("failed to align in datafile %s: %v", v.dataFile.Name(), err)
			return
		}
	}

	if size, err = n.Append(v.dataFile, v.Version()); err != nil {
		if e := v.dataFile.Truncate(offset); e != nil {
			err = fmt.Errorf("%s\ncannot truncate %s: %v", err, v.dataFile.Name(), e)
		}
		return
	}
	nv, ok := v.nm.Get(n.Id)
	if !ok || int64(nv.Offset)*NeedlePaddingSize < offset {
		if err = v.nm.Put(n.Id, uint32(offset/NeedlePaddingSize), n.Size); err != nil {
			glog.V(4).Infof("failed to save in needle map %d: %v", n.Id, err)
		}
	}
	if v.lastModifiedTime < n.LastModified {
		v.lastModifiedTime = n.LastModified
	}
	return
}

func (v *Volume) delete(n *Needle) (uint32, error) {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	glog.V(4).Infof("delete needle %s", NewFileIdFromNeedle(v.Id, n).String())
	if v.readOnly {
		return 0, fmt.Errorf("%s is read-only", v.dataFile.Name())
	}
	nv, ok := v.nm.Get(n.Id)
	//fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok {
		size := nv.Size
		if err := v.nm.Delete(n.Id); err != nil {
			return size, err
		}
		if _, err := v.dataFile.Seek(0, 2); err != nil {
			return size, err
		}
		n.Data = nil
		_, err := n.Append(v.dataFile, v.Version())
		return size, err
	}
	return 0, nil
}

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) readNeedle(n *Needle) (int, error) {
	nv, ok := v.nm.Get(n.Id)
	if !ok || nv.Offset == 0 {
		return -1, errors.New("Not Found")
	}
	v.mutex.RLock()
	err := n.ReadData(v.dataFile, int64(nv.Offset)*NeedlePaddingSize, nv.Size, v.Version())
	v.mutex.RUnlock()
	if err != nil {
		return 0, err
	}
	bytesRead := len(n.Data)
	if !n.HasTtl() {
		return bytesRead, nil
	}
	ttlMinutes := n.Ttl.Minutes()
	if ttlMinutes == 0 {
		return bytesRead, nil
	}
	if !n.HasLastModifiedDate() {
		return bytesRead, nil
	}
	if uint64(time.Now().Unix()) < n.LastModified+uint64(ttlMinutes*60) {
		return bytesRead, nil
	}
	return -1, errors.New("Not Found")
}

func ScanVolumeFile(dirname string, collection string, id VolumeId,
	needleMapKind NeedleMapType,
	visitSuperBlock func(SuperBlock) error,
	readNeedleBody bool,
	visitNeedle func(n *Needle, offset int64) error) (err error) {
	var v *Volume
	if v, err = loadVolumeWithoutIndex(dirname, collection, id, needleMapKind); err != nil {
		return fmt.Errorf("Failed to load volume %d: %v", id, err)
	}
	if err = visitSuperBlock(v.SuperBlock); err != nil {
		return fmt.Errorf("Failed to process volume %d super block: %v", id, err)
	}

	version := v.Version()

	offset := int64(SuperBlockSize)
	n, rest, e := ReadNeedleHeader(v.dataFile, version, offset)
	if e != nil {
		err = fmt.Errorf("cannot read needle header: %v", e)
		return
	}
	for n != nil {
		if readNeedleBody {
			if err = n.ReadNeedleBody(v.dataFile, version, offset+int64(NeedleHeaderSize), rest); err != nil {
				glog.V(0).Infof("cannot read needle body: %v", err)
				//err = fmt.Errorf("cannot read needle body: %v", err)
				//return
			}
			if n.DataSize >= n.Size {
				// this should come from a bug reported on #87 and #93
				// fixed in v0.69
				// remove this whole "if" clause later, long after 0.69
				oldRest, oldSize := rest, n.Size
				padding := NeedlePaddingSize - ((n.Size + NeedleHeaderSize + NeedleChecksumSize) % NeedlePaddingSize)
				n.Size = 0
				rest = n.Size + NeedleChecksumSize + padding
				if rest%NeedlePaddingSize != 0 {
					rest += (NeedlePaddingSize - rest%NeedlePaddingSize)
				}
				glog.V(4).Infof("Adjusting n.Size %d=>0 rest:%d=>%d %+v", oldSize, oldRest, rest, n)
			}
		}
		if err = visitNeedle(n, offset); err != nil {
			glog.V(0).Infof("visit needle error: %v", err)
		}
		offset += int64(NeedleHeaderSize) + int64(rest)
		glog.V(4).Infof("==> new entry offset %d", offset)
		if n, rest, err = ReadNeedleHeader(v.dataFile, version, offset); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("cannot read needle header: %v", err)
		}
		glog.V(4).Infof("new entry needle size:%d rest:%d", n.Size, rest)
	}

	return
}

func (v *Volume) ContentSize() uint64 {
	return v.nm.ContentSize()
}

func checkFile(filename string) (exists, canRead, canWrite bool, modTime time.Time) {
	exists = true
	fi, err := os.Stat(filename)
	if os.IsNotExist(err) {
		exists = false
		return
	}
	if fi.Mode()&0400 != 0 {
		canRead = true
	}
	if fi.Mode()&0200 != 0 {
		canWrite = true
	}
	modTime = fi.ModTime()
	return
}

// volume is expired if modified time + volume ttl < now
// except when volume is empty
// or when the volume does not have a ttl
// or when volumeSizeLimit is 0 when server just starts
func (v *Volume) expired(volumeSizeLimit uint64) bool {
	v.mutex.RLock()
	defer v.mutex.RUnlock()
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
	glog.V(0).Infof("now:%v lastModified:%v", time.Now().Unix(), v.lastModifiedTime)
	livedMinutes := (time.Now().Unix() - int64(v.lastModifiedTime)) / 60
	glog.V(0).Infof("ttl:%v lived:%v", v.Ttl, livedMinutes)
	if int64(v.Ttl.Minutes()) < livedMinutes {
		return true
	}
	return false
}

// wait either maxDelayMinutes or 10% of ttl minutes
func (v *Volume) expiredLongEnough(maxDelayMinutes uint32) bool {
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	removalDelay := v.Ttl.Minutes() / 10
	if removalDelay > maxDelayMinutes {
		removalDelay = maxDelayMinutes
	}

	if uint64(v.Ttl.Minutes()+removalDelay)*60+v.lastModifiedTime < uint64(time.Now().Unix()) {
		return true
	}
	return false
}

func (v *Volume) SetReadOnly(isReadOnly bool) error {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	if isReadOnly == false {
		if fi, e := v.dataFile.Stat(); e != nil {
			return e
		} else {
			if fi.Mode()&0200 == 0 {
				return errors.New(v.FileName() + ".dat is READONLY")
			}
		}
	}
	v.readOnly = isReadOnly
	return nil
}

func (v *Volume) IsReadOnly() bool {
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	return v.readOnly
}
