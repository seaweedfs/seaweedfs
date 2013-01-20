package storage

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
)

const (
	SuperBlockSize = 8
)

type Volume struct {
	Id       VolumeId
	dir      string
	dataFile *os.File
	nm       *NeedleMap

	version     Version
	replicaType ReplicationType

	accessLock sync.Mutex
}

func NewVolume(dirname string, id VolumeId, replicationType ReplicationType) (v *Volume, e error) {
	v = &Volume{dir: dirname, Id: id, replicaType: replicationType}
	e = v.load()
	return
}
func (v *Volume) load() error {
	var e error
	fileName := path.Join(v.dir, v.Id.String())
	v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
	if e != nil {
		return fmt.Errorf("cannot create Volume Data %s.dat: %s", fileName, e)
	}
	if v.replicaType == CopyNil {
		if e = v.readSuperBlock(); e != nil {
			return e
		}
	} else {
		v.maybeWriteSuperBlock()
	}
	indexFile, ie := os.OpenFile(fileName+".idx", os.O_RDWR|os.O_CREATE, 0644)
	if ie != nil {
		return fmt.Errorf("cannot create Volume Data %s.dat: %s", fileName, e)
	}
	v.nm = LoadNeedleMap(indexFile)
	return nil
}
func (v *Volume) Version() Version {
	return v.version
}
func (v *Volume) Size() int64 {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	stat, e := v.dataFile.Stat()
	if e == nil {
		return stat.Size()
	}
	fmt.Printf("Failed to read file size %s %s\n", v.dataFile.Name(), e.Error())
	return -1
}
func (v *Volume) Close() {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	v.nm.Close()
	v.dataFile.Close()
}
func (v *Volume) maybeWriteSuperBlock() {
	stat, e := v.dataFile.Stat()
	if e != nil {
		fmt.Printf("failed to stat datafile %s: %s", v.dataFile, e)
		return
	}
	if stat.Size() == 0 {
		v.version = CurrentVersion
		header := make([]byte, SuperBlockSize)
		header[0] = byte(v.version)
		header[1] = v.replicaType.Byte()
		v.dataFile.Write(header)
	}
}
func (v *Volume) readSuperBlock() (err error) {
	v.dataFile.Seek(0, 0)
	header := make([]byte, SuperBlockSize)
	if _, e := v.dataFile.Read(header); e != nil {
		return fmt.Errorf("cannot read superblock: %s", e)
	}
	v.version, v.replicaType, err = ParseSuperBlock(header)
	return err
}
func ParseSuperBlock(header []byte) (version Version, replicaType ReplicationType, err error) {
	version = Version(header[0])
	if replicaType, err = NewReplicationTypeFromByte(header[1]); err != nil {
		err = fmt.Errorf("cannot read replica type: %s", err)
	}
	return
}
func (v *Volume) NeedToReplicate() bool {
	return v.replicaType.GetCopyCount() > 1
}

func (v *Volume) write(n *Needle) (size uint32, err error) {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	var offset int64
	if offset, err = v.dataFile.Seek(0, 2); err != nil {
		return
	}
	if size, err = n.Append(v.dataFile, v.version); err != nil {
		return
	}
	nv, ok := v.nm.Get(n.Id)
	if !ok || int64(nv.Offset)*NeedlePaddingSize < offset {
		_, err = v.nm.Put(n.Id, uint32(offset/NeedlePaddingSize), n.Size)
	}
	return
}
func (v *Volume) delete(n *Needle) (uint32, error) {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	nv, ok := v.nm.Get(n.Id)
	//fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok {
		v.nm.Delete(n.Id)
		v.dataFile.Seek(int64(nv.Offset*NeedlePaddingSize), 0)
		_, err := n.Append(v.dataFile, v.version)
		return nv.Size, err
	}
	return 0, nil
}

func (v *Volume) read(n *Needle) (int, error) {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	nv, ok := v.nm.Get(n.Id)
	if ok && nv.Offset > 0 {
		v.dataFile.Seek(int64(nv.Offset)*NeedlePaddingSize, 0)
		return n.Read(v.dataFile, nv.Size, v.version)
	}
	return -1, errors.New("Not Found")
}

func (v *Volume) garbageLevel() float64 {
	return float64(v.nm.deletionByteCounter) / float64(v.ContentSize())
}

func (v *Volume) compact() error {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()

	filePath := path.Join(v.dir, v.Id.String())
	return v.copyDataAndGenerateIndexFile(filePath+".dat", filePath+".cpd", filePath+".cpx")
}
func (v *Volume) commitCompact() error {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	v.dataFile.Close()
	var e error
	if e = os.Rename(path.Join(v.dir, v.Id.String()+".cpd"), path.Join(v.dir, v.Id.String()+".dat")); e != nil {
		return e
	}
	if e = os.Rename(path.Join(v.dir, v.Id.String()+".cpx"), path.Join(v.dir, v.Id.String()+".idx")); e != nil {
		return e
	}
	if e = v.load(); e != nil {
		return e
	}
	return nil
}

func (v *Volume) copyDataAndGenerateIndexFile(srcName, dstName, idxName string) (err error) {
	var (
		src, dst, idx *os.File
	)
	if src, err = os.OpenFile(srcName, os.O_RDONLY, 0644); err != nil {
		return
	}
	defer src.Close()

	if dst, err = os.OpenFile(dstName, os.O_WRONLY|os.O_CREATE, 0644); err != nil {
		return
	}
	defer dst.Close()

	if idx, err = os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE, 0644); err != nil {
		return
	}
	defer idx.Close()

	src.Seek(0, 0)
	header := make([]byte, SuperBlockSize)
	if _, err = src.Read(header); err == nil {
		_, err = dst.Write(header)
	}
	if err != nil {
		return err
	}

	version, _, e := ParseSuperBlock(header)
	if e != nil {
		err = fmt.Errorf("cannot parse superblock: %s", e)
		return
	}

	n, rest, e := ReadNeedleHeader(src, version)
	if e != nil {
		err = fmt.Errorf("cannot read needle header: %s", e)
		return
	}
	nm := NewNeedleMap(idx)
	old_offset := uint32(SuperBlockSize)
	new_offset := uint32(SuperBlockSize)
	for n != nil {
		nv, ok := v.nm.Get(n.Id)
		//log.Println("file size is", n.Size, "rest", rest)
		if !ok || nv.Offset*NeedlePaddingSize != old_offset {
			src.Seek(int64(rest), 1)
		} else {
			if nv.Size > 0 {
				if _, err = nm.Put(n.Id, new_offset/NeedlePaddingSize, n.Size); err != nil {
					return fmt.Errorf("cannot put needle: %s", err)
				}
				if err = n.ReadNeedleBody(src, version, rest); err != nil {
					return fmt.Errorf("cannot read needle body: %s", err)
				}
				if _, err = n.Append(dst, v.version); err != nil {
					return fmt.Errorf("cannot append needle: %s", err)
				}
				new_offset += rest + NeedleHeaderSize
				//log.Println("saving key", n.Id, "volume offset", old_offset, "=>", new_offset, "data_size", n.Size, "rest", rest)
			} else {
				src.Seek(int64(rest), 1)
			}
		}
		old_offset += rest + NeedleHeaderSize
		if n, rest, err = ReadNeedleHeader(src, version); err != nil {
			return fmt.Errorf("cannot read needle header: %s", err)
		}
	}

	return nil
}
func (v *Volume) ContentSize() uint64 {
	return v.nm.fileByteCounter
}
