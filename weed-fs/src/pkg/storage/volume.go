package storage

import (
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"sync"
)

const (
	SuperBlockSize = 8
)

type Volume struct {
	Id       uint32
	dir      string
	dataFile *os.File
	nm       *NeedleMap

	accessLock sync.Mutex
}

func NewVolume(dirname string, id uint32) (v *Volume) {
	var e error
	v = &Volume{dir: dirname, Id: id}
	fileName := strconv.FormatUint(uint64(v.Id), 10)
	v.dataFile, e = os.OpenFile(path.Join(v.dir, fileName+".dat"), os.O_RDWR|os.O_CREATE, 0644)
	if e != nil {
		log.Fatalf("New Volume [ERROR] %s\n", e)
	}
	v.maybeWriteSuperBlock()
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
		v.dataFile.Write(header)
	}
}

func (v *Volume) write(n *Needle) uint32 {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	offset, _ := v.dataFile.Seek(0, 2)
	ret := n.Append(v.dataFile)
	nv, ok := v.nm.Get(n.Key)
	if !ok || int64(nv.Offset)*8 < offset {
		v.nm.Put(n.Key, uint32(offset/8), n.Size)
	}
	return ret
}
func (v *Volume) delete(n *Needle) uint32 {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	nv, ok := v.nm.Get(n.Key)
	//log.Println("key", n.Key, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok {
		v.nm.Delete(n.Key)
		v.dataFile.Seek(int64(nv.Offset*8), 0)
		n.Append(v.dataFile)
		return nv.Size
	}
	return 0
}
func (v *Volume) read(n *Needle) (int, error) {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	nv, ok := v.nm.Get(n.Key)
	if ok && nv.Offset > 0 {
		v.dataFile.Seek(int64(nv.Offset)*8, 0)
		return n.Read(v.dataFile, nv.Size)
	}
	return -1, io.EOF
}
