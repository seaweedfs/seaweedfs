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

const (
	SuperBlockSize = 8
)

type SuperBlock struct {
	Version          Version
	ReplicaPlacement *ReplicaPlacement
}

func (s *SuperBlock) Bytes() []byte {
	header := make([]byte, SuperBlockSize)
	header[0] = byte(s.Version)
	header[1] = s.ReplicaPlacement.Byte()
	return header
}

type Volume struct {
	Id         VolumeId
	dir        string
	Collection string
	dataFile   *os.File
	nm         NeedleMapper
	readOnly   bool

	SuperBlock

	accessLock sync.Mutex
}

func NewVolume(dirname string, collection string, id VolumeId, replicaPlacement *ReplicaPlacement) (v *Volume, e error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.SuperBlock = SuperBlock{ReplicaPlacement: replicaPlacement}
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

	if exists, canRead, canWrite, _ := checkFile(fileName + ".dat"); exists {
		if !canRead {
			return fmt.Errorf("cannot read Volume Data file %s.dat", fileName)
		}
		if canWrite {
			v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
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
				return fmt.Errorf("cannot read Volume Data %s.dat: %s", fileName, e.Error())
			}
		} else {
			glog.V(1).Infoln("open to write file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDWR|os.O_CREATE, 0644); e != nil {
				return fmt.Errorf("cannot write Volume Data %s.dat: %s", fileName, e.Error())
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
	return v.SuperBlock.Version
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
func (v *Volume) maybeWriteSuperBlock() error {
	stat, e := v.dataFile.Stat()
	if e != nil {
		glog.V(0).Infof("failed to stat datafile %s: %s", v.dataFile, e.Error())
		return e
	}
	if stat.Size() == 0 {
		v.SuperBlock.Version = CurrentVersion
		_, e = v.dataFile.Write(v.SuperBlock.Bytes())
		if e != nil && os.IsPermission(e) {
			//read-only, but zero length - recreate it!
			if v.dataFile, e = os.Create(v.dataFile.Name()); e == nil {
				if _, e = v.dataFile.Write(v.SuperBlock.Bytes()); e == nil {
					v.readOnly = false
				}
			}
		}
	}
	return e
}
func (v *Volume) readSuperBlock() (err error) {
	if _, err = v.dataFile.Seek(0, 0); err != nil {
		return fmt.Errorf("cannot seek to the beginning of %s: %s", v.dataFile.Name(), err.Error())
	}
	header := make([]byte, SuperBlockSize)
	if _, e := v.dataFile.Read(header); e != nil {
		return fmt.Errorf("cannot read superblock: %s", e.Error())
	}
	v.SuperBlock, err = ParseSuperBlock(header)
	return err
}
func ParseSuperBlock(header []byte) (superBlock SuperBlock, err error) {
	superBlock.Version = Version(header[0])
	if superBlock.ReplicaPlacement, err = NewReplicaPlacementFromByte(header[1]); err != nil {
		err = fmt.Errorf("cannot read replica type: %s", err.Error())
	}
	return
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
		err = fmt.Errorf("%s is read-only", v.dataFile)
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
	if v.readOnly {
		err = fmt.Errorf("%s is read-only", v.dataFile)
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
			err = fmt.Errorf("%s\ncannot truncate %s: %s", err, v.dataFile, e.Error())
		}
		return
	}
	nv, ok := v.nm.Get(n.Id)
	if !ok || int64(nv.Offset)*NeedlePaddingSize < offset {
		if _, err = v.nm.Put(n.Id, uint32(offset/NeedlePaddingSize), n.Size); err != nil {
			glog.V(4).Infof("failed to save in needle map %d: %s", n.Id, err.Error())
		}
	}
	return
}

func (v *Volume) delete(n *Needle) (uint32, error) {
	if v.readOnly {
		return 0, fmt.Errorf("%s is read-only", v.dataFile)
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
	if ok && nv.Offset > 0 {
		return n.Read(v.dataFile, int64(nv.Offset)*NeedlePaddingSize, nv.Size, v.Version())
	}
	return -1, errors.New("Not Found")
}

func (v *Volume) garbageLevel() float64 {
	return float64(v.nm.DeletedSize()) / float64(v.ContentSize())
}

func (v *Volume) Compact() error {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()

	filePath := path.Join(v.dir, v.Id.String())
	return v.copyDataAndGenerateIndexFile(filePath+".cpd", filePath+".cpx")
}
func (v *Volume) commitCompact() error {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	_ = v.dataFile.Close()
	var e error
	if e = os.Rename(path.Join(v.dir, v.Id.String()+".cpd"), path.Join(v.dir, v.Id.String()+".dat")); e != nil {
		return e
	}
	if e = os.Rename(path.Join(v.dir, v.Id.String()+".cpx"), path.Join(v.dir, v.Id.String()+".idx")); e != nil {
		return e
	}
	if e = v.load(true, false); e != nil {
		return e
	}
	return nil
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
	visitNeedle func(n *Needle, offset int64) error) (err error) {
	var v *Volume
	if v, err = loadVolumeWithoutIndex(dirname, collection, id); err != nil {
		return
	}
	if err = visitSuperBlock(v.SuperBlock); err != nil {
		return
	}

	version := v.Version()

	offset := int64(SuperBlockSize)
	n, rest, e := ReadNeedleHeader(v.dataFile, version, offset)
	if e != nil {
		err = fmt.Errorf("cannot read needle header: %s", e)
		return
	}
	for n != nil {
		offset += int64(NeedleHeaderSize)
		if err = n.ReadNeedleBody(v.dataFile, version, offset, rest); err != nil {
			err = fmt.Errorf("cannot read needle body: %s", err)
			return
		}
		if err = visitNeedle(n, offset); err != nil {
			return
		}
		offset += int64(rest)
		if n, rest, err = ReadNeedleHeader(v.dataFile, version, offset); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("cannot read needle header: %s", err)
		}
	}

	return
}

func (v *Volume) copyDataAndGenerateIndexFile(dstName, idxName string) (err error) {
	var (
		dst, idx *os.File
	)
	if dst, err = os.OpenFile(dstName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return
	}
	defer dst.Close()

	if idx, err = os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return
	}
	defer idx.Close()

	new_offset := int64(SuperBlockSize)

	err = ScanVolumeFile(v.dir, v.Collection, v.Id, func(superBlock SuperBlock) error {
		_, err = dst.Write(superBlock.Bytes())
		return err
	}, func(n *Needle, offset int64) error {
		nv, ok := v.nm.Get(n.Id)
		//glog.V(0).Infoln("file size is", n.Size, "rest", rest)
		if ok && int64(nv.Offset)*NeedlePaddingSize == offset && nv.Size > 0 {
			if _, err = n.Append(dst, v.Version()); err != nil {
				return fmt.Errorf("cannot append needle: %s", err)
			}
			new_offset += n.DiskSize()
			//glog.V(0).Infoln("saving key", n.Id, "volume offset", old_offset, "=>", new_offset, "data_size", n.Size, "rest", rest)
		}
		return nil
	})

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
