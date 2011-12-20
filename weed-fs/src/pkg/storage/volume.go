package storage

import (
	"os"
	"path"
	"strconv"
	"log"
)

type Volume struct {
	Id                  uint32
	dir                 string
	dataFile, indexFile *os.File
	nm                  *NeedleMap

	accessChannel chan int
}

func NewVolume(dirname string, id uint32) (v *Volume) {
	var e os.Error
	v = &Volume{dir:dirname,Id:id, nm:NewNeedleMap()}
	fileName := strconv.Uitoa64(uint64(v.Id))
	v.dataFile, e = os.OpenFile(path.Join(v.dir,fileName+".dat"), os.O_RDWR|os.O_CREATE, 0644)
	if e != nil {
		log.Fatalf("New Volume [ERROR] %s\n", e)
	}
	v.indexFile, e = os.OpenFile(path.Join(v.dir,fileName+".idx"), os.O_RDWR|os.O_CREATE, 0644)
	if e != nil {
		log.Fatalf("New Volume [ERROR] %s\n", e)
	}
	v.nm.load(v.indexFile)

	v.accessChannel = make(chan int, 1)
	v.accessChannel <- 0

	return
}
func (v *Volume) Size() int64 {
    stat, e:=v.dataFile.Stat()
    if e!=nil{
       return stat.Size
    }
    return -1
}
func (v *Volume) Close() {
	close(v.accessChannel)
	v.dataFile.Close()
	v.indexFile.Close()
}

func (v *Volume) write(n *Needle) {
	counter := <-v.accessChannel
	offset, _ := v.dataFile.Seek(0, 2)
	n.Append(v.dataFile)
	nv, ok := v.nm.get(n.Key)
	if !ok || int64(nv.Offset)*8 < offset {
		v.nm.put(n.Key, uint32(offset/8), n.Size)
	}
	v.accessChannel <- counter + 1
}
func (v *Volume) read(n *Needle) {
	counter := <-v.accessChannel
	nv, ok := v.nm.get(n.Key)
	if ok && nv.Offset > 0 {
		v.dataFile.Seek(int64(nv.Offset)*8, 0)
		n.Read(v.dataFile, nv.Size)
	}
	v.accessChannel <- counter + 1
}
