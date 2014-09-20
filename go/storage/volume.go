package storage

import (
	"bytes"
	"code.google.com/p/weed-fs/go/glog"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"
)

type Volume struct {
	Id         VolumeId
	dir        string
	Collection string
	dataFile   *os.File
	nm         NeedleMapper
	readOnly   bool

	SuperBlock

	accessLock       sync.Mutex
	lastModifiedTime uint64 //unix time in seconds
}

func NewVolume(dirname string, collection string, id VolumeId, replicaPlacement *ReplicaPlacement, ttl *TTL) (v *Volume, e error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.SuperBlock = SuperBlock{ReplicaPlacement: replicaPlacement, Ttl: ttl}
	e = v.load(true, true)
	return
}
func loadVolumeWithoutIndex(dirname string, collection string, id VolumeId) (v *Volume, e error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.SuperBlock = SuperBlock{}
	e = v.load(false, false)
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
func (v *Volume) load(alsoLoadIndex bool, createDatIfMissing bool) error {
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
			return fmt.Errorf("cannot load Volume Data %s.dat: %s", fileName, e.Error())
		}
	}

	if v.ReplicaPlacement == nil {
		e = v.readSuperBlock()
	} else {
		e = v.maybeWriteSuperBlock()
	}
	if e == nil && alsoLoadIndex {
		if v.readOnly {
			if v.ensureConvertIdxToCdb(fileName) {
				v.nm, e = OpenCdbMap(fileName + ".cdb")
				return e
			}
		}
		var indexFile *os.File
		if v.readOnly {
			glog.V(1).Infoln("open to read file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDONLY, 0644); e != nil {
				return fmt.Errorf("cannot read Volume Index %s.idx: %s", fileName, e.Error())
			}
		} else {
			glog.V(1).Infoln("open to write file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDWR|os.O_CREATE, 0644); e != nil {
				return fmt.Errorf("cannot write Volume Index %s.idx: %s", fileName, e.Error())
			}
		}
		glog.V(0).Infoln("loading file", fileName+".idx", "readonly", v.readOnly)
		if v.nm, e = LoadNeedleMap(indexFile); e != nil {
			glog.V(0).Infoln("loading error:", e)
		}
	}
	return e
}
func (v *Volume) Version() Version {
	return v.SuperBlock.Version()
}
func (v *Volume) Size() int64 {
	stat, e := v.dataFile.Stat()
	if e == nil {
		return stat.Size()
	}
	glog.V(0).Infof("Failed to read file size %s %s", v.dataFile.Name(), e.Error())
	return -1
}
func (v *Volume) Close() {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	v.nm.Close()
	_ = v.dataFile.Close()
}
func (v *Volume) NeedToReplicate() bool {
	return v.ReplicaPlacement.GetCopyCount() > 1
}

func (v *Volume) isFileUnchanged(n *Needle) bool {
	nv, ok := v.nm.Get(n.Id)
	if ok && nv.Offset > 0 {
		oldNeedle := new(Needle)
		oldNeedle.Read(v.dataFile, int64(nv.Offset)*NeedlePaddingSize, nv.Size, v.Version())
		if oldNeedle.Checksum == n.Checksum && bytes.Equal(oldNeedle.Data, n.Data) {
			n.Size = oldNeedle.Size
			return true
		}
	}
	return false
}

func (v *Volume) Destroy() (err error) {
	if v.readOnly {
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

func (v *Volume) write(n *Needle) (size uint32, err error) {
	glog.V(4).Infof("writing needle %s", NewFileIdFromNeedle(v.Id, n).String())
	if v.readOnly {
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	if v.isFileUnchanged(n) {
		size = n.Size
		glog.V(4).Infof("needle is unchanged!")
		return
	}
	var offset int64
	if offset, err = v.dataFile.Seek(0, 2); err != nil {
		return
	}

	//ensure file writing starting from aligned positions
	if offset%NeedlePaddingSize != 0 {
		offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
		if offset, err = v.dataFile.Seek(offset, 0); err != nil {
			glog.V(4).Infof("failed to align in datafile %s: %s", v.dataFile.Name(), err.Error())
			return
		}
	}

	if size, err = n.Append(v.dataFile, v.Version()); err != nil {
		if e := v.dataFile.Truncate(offset); e != nil {
			err = fmt.Errorf("%s\ncannot truncate %s: %s", err, v.dataFile.Name(), e.Error())
		}
		return
	}
	nv, ok := v.nm.Get(n.Id)
	if !ok || int64(nv.Offset)*NeedlePaddingSize < offset {
		if _, err = v.nm.Put(n.Id, uint32(offset/NeedlePaddingSize), n.Size); err != nil {
			glog.V(4).Infof("failed to save in needle map %d: %s", n.Id, err.Error())
		}
	}
	if v.lastModifiedTime < n.LastModified {
		v.lastModifiedTime = n.LastModified
	}
	return
}

func (v *Volume) delete(n *Needle) (uint32, error) {
	glog.V(4).Infof("delete needle %s", NewFileIdFromNeedle(v.Id, n).String())
	if v.readOnly {
		return 0, fmt.Errorf("%s is read-only", v.dataFile.Name())
	}
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
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
		n.Data = make([]byte, 0)
		_, err := n.Append(v.dataFile, v.Version())
		return size, err
	}
	return 0, nil
}

func (v *Volume) read(n *Needle) (int, error) {
	nv, ok := v.nm.Get(n.Id)
	if !ok || nv.Offset == 0 {
		return -1, errors.New("Not Found")
	}
	bytesRead, err := n.Read(v.dataFile, int64(nv.Offset)*NeedlePaddingSize, nv.Size, v.Version())
	if err != nil {
		return bytesRead, err
	}
	if !n.HasTtl() {
		return bytesRead, err
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

func (v *Volume) freeze() error {
	if v.readOnly {
		return nil
	}
	nm, ok := v.nm.(*NeedleMap)
	if !ok {
		return nil
	}
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	bn, _ := baseFilename(v.dataFile.Name())
	cdbFn := bn + ".cdb"
	glog.V(0).Infof("converting %s to %s", nm.indexFile.Name(), cdbFn)
	err := DumpNeedleMapToCdb(cdbFn, nm)
	if err != nil {
		return err
	}
	if v.nm, err = OpenCdbMap(cdbFn); err != nil {
		return err
	}
	nm.indexFile.Close()
	os.Remove(nm.indexFile.Name())
	v.readOnly = true
	return nil
}

func ScanVolumeFile(dirname string, collection string, id VolumeId,
	visitSuperBlock func(SuperBlock) error,
	readNeedleBody bool,
	visitNeedle func(n *Needle, offset int64) error) (err error) {
	var v *Volume
	if v, err = loadVolumeWithoutIndex(dirname, collection, id); err != nil {
		return errors.New("Failed to load volume:" + err.Error())
	}
	if err = visitSuperBlock(v.SuperBlock); err != nil {
		return errors.New("Failed to read super block:" + err.Error())
	}

	version := v.Version()

	offset := int64(SuperBlockSize)
	n, rest, e := ReadNeedleHeader(v.dataFile, version, offset)
	if e != nil {
		err = fmt.Errorf("cannot read needle header: %s", e)
		return
	}
	for n != nil {
		if readNeedleBody {
			if err = n.ReadNeedleBody(v.dataFile, version, offset+int64(NeedleHeaderSize), rest); err != nil {
				err = fmt.Errorf("cannot read needle body: %s", err)
				return
			}
		}
		if err = visitNeedle(n, offset); err != nil {
			return
		}
		offset += int64(NeedleHeaderSize) + int64(rest)
		if n, rest, err = ReadNeedleHeader(v.dataFile, version, offset); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("cannot read needle header: %s", err)
		}
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
func (v *Volume) ensureConvertIdxToCdb(fileName string) (cdbCanRead bool) {
	var indexFile *os.File
	var e error
	_, cdbCanRead, cdbCanWrite, cdbModTime := checkFile(fileName + ".cdb")
	_, idxCanRead, _, idxModeTime := checkFile(fileName + ".idx")
	if cdbCanRead && cdbModTime.After(idxModeTime) {
		return true
	}
	if !cdbCanWrite {
		return false
	}
	if !idxCanRead {
		glog.V(0).Infoln("Can not read file", fileName+".idx!")
		return false
	}
	glog.V(2).Infoln("opening file", fileName+".idx")
	if indexFile, e = os.Open(fileName + ".idx"); e != nil {
		glog.V(0).Infoln("Failed to read file", fileName+".idx !")
		return false
	}
	defer indexFile.Close()
	glog.V(0).Infof("converting %s.idx to %s.cdb", fileName, fileName)
	if e = ConvertIndexToCdb(fileName+".cdb", indexFile); e != nil {
		glog.V(0).Infof("error converting %s.idx to %s.cdb: %s", fileName, fileName, e.Error())
		return false
	}
	return true
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
	glog.V(0).Infof("now:%v lastModified:%v", time.Now().Unix(), v.lastModifiedTime)
	livedMinutes := (time.Now().Unix() - int64(v.lastModifiedTime)) / 60
	glog.V(0).Infof("ttl:%v lived:%v", v.Ttl, livedMinutes)
	if int64(v.Ttl.Minutes()) < livedMinutes {
		return true
	}
	return false
}

// wait either maxDelayMinutes or 10% of ttl minutes
func (v *Volume) exiredLongEnough(maxDelayMinutes uint32) bool {
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
