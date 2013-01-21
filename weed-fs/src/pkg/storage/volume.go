package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
)

const (
	SuperBlockSize = 8
)

type SuperBlock struct {
	Version     Version
	ReplicaType ReplicationType
}

func (s *SuperBlock) Bytes() []byte {
	header := make([]byte, SuperBlockSize)
	header[0] = byte(s.Version)
	header[1] = s.ReplicaType.Byte()
	return header
}

type Volume struct {
	Id       VolumeId
	dir      string
	dataFile *os.File
	nm       *NeedleMap

	SuperBlock

	accessLock sync.Mutex
}

func NewVolume(dirname string, id VolumeId, replicationType ReplicationType) (v *Volume, e error) {
	v = &Volume{dir: dirname, Id: id}
	v.SuperBlock = SuperBlock{ReplicaType: replicationType}
	e = v.load(true)
	return
}
func LoadVolumeOnly(dirname string, id VolumeId) (v *Volume, e error) {
	v = &Volume{dir: dirname, Id: id}
	v.SuperBlock = SuperBlock{ReplicaType: CopyNil}
	e = v.load(false)
	return
}
func (v *Volume) load(alsoLoadIndex bool) error {
	var e error
	fileName := path.Join(v.dir, v.Id.String())
	v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
	if e != nil {
		return fmt.Errorf("cannot create Volume Data %s.dat: %s", fileName, e)
	}
	if v.ReplicaType == CopyNil {
		if e = v.readSuperBlock(); e != nil {
			return e
		}
	} else {
		v.maybeWriteSuperBlock()
	}
	if alsoLoadIndex {
		indexFile, ie := os.OpenFile(fileName+".idx", os.O_RDWR|os.O_CREATE, 0644)
		if ie != nil {
			return fmt.Errorf("cannot create Volume Data %s.dat: %s", fileName, e)
		}
		v.nm = LoadNeedleMap(indexFile)
	}
	return nil
}
func (v *Volume) Version() Version {
	return v.SuperBlock.Version
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
		v.SuperBlock.Version = CurrentVersion
		v.dataFile.Write(v.SuperBlock.Bytes())
	}
}
func (v *Volume) readSuperBlock() (err error) {
	v.dataFile.Seek(0, 0)
	header := make([]byte, SuperBlockSize)
	if _, e := v.dataFile.Read(header); e != nil {
		return fmt.Errorf("cannot read superblock: %s", e)
	}
	v.SuperBlock, err = ParseSuperBlock(header)
	return err
}
func ParseSuperBlock(header []byte) (superBlock SuperBlock, err error) {
	superBlock.Version = Version(header[0])
	if superBlock.ReplicaType, err = NewReplicationTypeFromByte(header[1]); err != nil {
		err = fmt.Errorf("cannot read replica type: %s", err)
	}
	return
}
func (v *Volume) NeedToReplicate() bool {
	return v.ReplicaType.GetCopyCount() > 1
}

func (v *Volume) write(n *Needle) (size uint32, err error) {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	var offset int64
	if offset, err = v.dataFile.Seek(0, 2); err != nil {
		return
	}
	if size, err = n.Append(v.dataFile, v.Version()); err != nil {
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
		_, err := n.Append(v.dataFile, v.Version())
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
		return n.Read(v.dataFile, nv.Size, v.Version())
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
	return v.copyDataAndGenerateIndexFile(filePath+".cpd", filePath+".cpx")
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
	if e = v.load(true); e != nil {
		return e
	}
	return nil
}

func ScanVolumeFile(dirname string, id VolumeId,
	visitSuperBlock func(SuperBlock) error,
	visitNeedle func(n *Needle, offset uint32) error) (err error) {
	var v *Volume
	if v, err = LoadVolumeOnly(dirname, id); err != nil {
		return
	}
	if err = visitSuperBlock(v.SuperBlock); err != nil {
		return
	}

	version := v.Version()

	offset := uint32(SuperBlockSize)
	n, rest, e := ReadNeedleHeader(v.dataFile, version)
	if e != nil {
		err = fmt.Errorf("cannot read needle header: %s", e)
		return
	}
	for n != nil {
		if err = n.ReadNeedleBody(v.dataFile, version, rest); err != nil {
			err = fmt.Errorf("cannot read needle body: %s", err)
			return
		}
		if err = visitNeedle(n, offset); err != nil {
			return
		}
		offset += NeedleHeaderSize + rest
		if n, rest, err = ReadNeedleHeader(v.dataFile, version); err != nil {
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
	if dst, err = os.OpenFile(dstName, os.O_WRONLY|os.O_CREATE, 0644); err != nil {
		return
	}
	defer dst.Close()

	if idx, err = os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE, 0644); err != nil {
		return
	}
	defer idx.Close()

	nm := NewNeedleMap(idx)
	new_offset := uint32(SuperBlockSize)

	err = ScanVolumeFile(v.dir, v.Id, func(superBlock SuperBlock) error {
		_, err = dst.Write(superBlock.Bytes())
		return err
	}, func(n *Needle, offset uint32) error {
		nv, ok := v.nm.Get(n.Id)
		//log.Println("file size is", n.Size, "rest", rest)
		if ok && nv.Offset*NeedlePaddingSize == offset {
			if nv.Size > 0 {
				if _, err = nm.Put(n.Id, new_offset/NeedlePaddingSize, n.Size); err != nil {
					return fmt.Errorf("cannot put needle: %s", err)
				}
				if _, err = n.Append(dst, v.Version()); err != nil {
					return fmt.Errorf("cannot append needle: %s", err)
				}
				new_offset += n.DiskSize()
				//log.Println("saving key", n.Id, "volume offset", old_offset, "=>", new_offset, "data_size", n.Size, "rest", rest)
			}
		}
		return nil
	})

	return
}
func (v *Volume) ContentSize() uint64 {
	return v.nm.fileByteCounter
}
