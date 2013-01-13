package storage

import (
	"errors"
	"fmt"
	"log"
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
	// TODO: if .idx not exists, but .cdb exists, then use (but don't load!) that
	indexFile, ie := os.OpenFile(fileName+".idx", os.O_RDWR|os.O_CREATE, 0644)
	if ie != nil {
		return fmt.Errorf("cannot create Volume Data %s.dat: %s", fileName, e)
	}
	v.nm, e = LoadNeedleMap(indexFile)
	return e
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

// a volume is writable, if its data file is writable and the index is not frozen
func (v *Volume) IsWritable() bool {
	stat, e := v.dataFile.Stat()
	if e != nil {
		log.Printf("Failed to read file permission %s %s\n", v.dataFile.Name(), e.Error())
		return false
	}
	//  4 for r, 2 for w, 1 for x
	return stat.Mode().Perm()&0222 > 0 && !v.nm.IsFrozen()
}

func (v *Volume) Close() {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	v.nm.Close()
	v.dataFile.Close()
}
func (v *Volume) maybeWriteSuperBlock() {
	stat, _ := v.dataFile.Stat()
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
	if version == 0 {
		err = errors.New("Zero version impossible - bad superblock!")
		return
	}
	if replicaType, err = NewReplicationTypeFromByte(header[1]); err != nil {
		err = fmt.Errorf("cannot read replica type: %s", err)
	}
	return
}
func (v *Volume) NeedToReplicate() bool {
	return v.replicaType.GetCopyCount() > 1
}

func (v *Volume) write(n *Needle) uint32 {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	offset, _ := v.dataFile.Seek(0, 2)
	ret := n.Append(v.dataFile, v.version)
	nv, ok := v.nm.Get(n.Id)
	if !ok || int64(nv.Offset)*8 < offset {
		v.nm.Put(n.Id, uint32(offset/8), n.Size)
	}
	return ret
}
func (v *Volume) delete(n *Needle) uint32 {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	nv, ok := v.nm.Get(n.Id)
	//fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok {
		v.nm.Delete(n.Id)
		v.dataFile.Seek(int64(nv.Offset*NeedlePaddingSize), 0)
		n.Append(v.dataFile, v.version)
		return nv.Size
	}
	return 0
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
	src, err := os.OpenFile(srcName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(dstName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer dst.Close()

	idx, err := os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer idx.Close()

	src.Seek(0, 0)
	header := make([]byte, SuperBlockSize)
	if _, error := src.Read(header); error == nil {
		dst.Write(header)
	}

	version, _, _ := ParseSuperBlock(header)

	n, rest := ReadNeedleHeader(src, version)
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
				nm.Put(n.Id, new_offset/NeedlePaddingSize, n.Size)
				n.ReadNeedleBody(src, version, rest)
				n.Append(dst, v.version)
				new_offset += rest + NeedleHeaderSize
				//log.Println("saving key", n.Id, "volume offset", old_offset, "=>", new_offset, "data_size", n.Size, "rest", rest)
			} else {
				src.Seek(int64(rest), 1)
			}
		}
		old_offset += rest + NeedleHeaderSize
		n, rest = ReadNeedleHeader(src, version)
	}

	return nil
}
func (v *Volume) ContentSize() uint64 {
	return v.nm.fileByteCounter
}

// Walk over the contained needles (call the function with each NeedleValue till error is returned)
func (v *Volume) WalkValues(pedestrian func(*Needle) error) error {
	pedplus := func(nv *NeedleValue) (err error) {
		n := new(Needle)
		if nv.Offset > 0 {
			v.dataFile.Seek(int64(nv.Offset)*NeedlePaddingSize, 0)
			if _, err = n.Read(v.dataFile, nv.Size, v.version); err != nil {
				return
			}
			if err = pedestrian(n); err != nil {
				return
			}
		}
		return nil
	}
	return v.nm.Walk(pedplus)
}

// Walk over the keys
func (v *Volume) WalkKeys(pedestrian func(Key) error) error {
	pedplus := func(nv *NeedleValue) (err error) {
		if nv.Offset > 0 && nv.Key > 0 {
			if err = pedestrian(nv.Key); err != nil {
				return
			}
		}
		return nil
	}
	return v.nm.Walk(pedplus)
}

func (v *Volume) String() string {
	return fmt.Sprintf("%d@%s:v%d:r%s", v.Id, v.dataFile.Name(),
		v.Version(), v.replicaType)
}
