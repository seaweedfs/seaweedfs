package storage

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
	"log"
	"os"
	"testing"
)

func TestMemoryUsage(t *testing.T) {

	indexFile, ie := os.OpenFile("../../test/sample.idx", os.O_RDWR|os.O_RDONLY, 0644)
	if ie != nil {
		log.Fatalln(ie)
	}
	LoadNewNeedleMap(indexFile)

}

func LoadNewNeedleMap(file *os.File) CompactMap {
	m := NewCompactMap()
	bytes := make([]byte, 16*1024)
	count, e := file.Read(bytes)
	if count > 0 {
		fstat, _ := file.Stat()
		glog.V(0).Infoln("Loading index file", fstat.Name(), "size", fstat.Size())
	}
	for count > 0 && e == nil {
		for i := 0; i < count; i += 16 {
			key := util.BytesToUint64(bytes[i : i+8])
			offset := util.BytesToUint32(bytes[i+8 : i+12])
			size := util.BytesToUint32(bytes[i+12 : i+16])
			if offset > 0 {
				m.Set(Key(key), offset, size)
			} else {
				//delete(m, key)
			}
		}

		count, e = file.Read(bytes)
	}
	return m
}
