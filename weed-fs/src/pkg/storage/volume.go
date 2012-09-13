package storage

import (
	"io"
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

	replicaType ReplicationType

	accessLock sync.Mutex

	//transient
	locations []string
}

func NewVolume(dirname string, id VolumeId, replicationType ReplicationType) (v *Volume) {
	var e error
	v = &Volume{dir: dirname, Id: id, replicaType: replicationType}
	fileName := id.String()
	v.dataFile, e = os.OpenFile(path.Join(v.dir, fileName+".dat"), os.O_RDWR|os.O_CREATE, 0644)
	if e != nil {
		log.Fatalf("New Volume [ERROR] %s\n", e)
	}
	if replicationType == CopyNil {
		v.readSuperBlock()
	} else {
		v.maybeWriteSuperBlock()
	}
	indexFile, ie := os.OpenFile(path.Join(v.dir, fileName+".idx"), os.O_RDWR|os.O_CREATE, 0644)
	if ie != nil {
		log.Fatalf("Write Volume Index [ERROR] %s\n", ie)
	}
	v.nm = LoadNeedleMap(indexFile)

	return
}
func (v *Volume) Size() int64 {
	stat, e := v.dataFile.Stat()
	if e == nil {
		return stat.Size()
	}
	return -1
}
func (v *Volume) Close() {
	v.nm.Close()
	v.dataFile.Close()
}
func (v *Volume) maybeWriteSuperBlock() {
	stat, _ := v.dataFile.Stat()
	if stat.Size() == 0 {
		header := make([]byte, SuperBlockSize)
		header[0] = 1
		header[1] = byte(v.replicaType)
		v.dataFile.Write(header)
	}
}
func (v *Volume) readSuperBlock() {
	v.dataFile.Seek(0, 0)
	header := make([]byte, SuperBlockSize)
	if _, error := v.dataFile.Read(header); error == nil {
		v.replicaType = ReplicationType(header[1])
	}
}

func (v *Volume) write(n *Needle) uint32 {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	offset, _ := v.dataFile.Seek(0, 2)
	ret := n.Append(v.dataFile)
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
	//log.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok {
		v.nm.Delete(n.Id)
		v.dataFile.Seek(int64(nv.Offset*8), 0)
		n.Append(v.dataFile)
		return nv.Size
	}
	return 0
}
func (v *Volume) read(n *Needle) (int, error) {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	nv, ok := v.nm.Get(n.Id)
	if ok && nv.Offset > 0 {
		v.dataFile.Seek(int64(nv.Offset)*8, 0)
		return n.Read(v.dataFile, nv.Size)
	}
	return -1, io.EOF
}
