package needle

import (
	"testing"
	"os"
	"log"

	"github.com/chrislusf/seaweedfs/weed/util"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

/*

To see the memory usage:

go test -run TestMemoryUsage -memprofile=mem.out
go tool pprof needle.test mem.out

 */

func TestMemoryUsage(t *testing.T) {

	indexFile, ie := os.OpenFile("../../../test/sample.idx", os.O_RDWR|os.O_RDONLY, 0644)
	if ie != nil {
		log.Fatalln(ie)
	}
	loadNewNeedleMap(indexFile)

	indexFile.Close()

}

func loadNewNeedleMap(file *os.File) {
	m := NewCompactMap()
	bytes := make([]byte, NeedleEntrySize*1024)
	count, e := file.Read(bytes)
	for count > 0 && e == nil {
		for i := 0; i < count; i += NeedleEntrySize {
			key := BytesToNeedleId(bytes[i : i+NeedleIdSize])
			offset := BytesToOffset(bytes[i+NeedleIdSize : i+NeedleIdSize+OffsetSize])
			size := util.BytesToUint32(bytes[i+NeedleIdSize+OffsetSize : i+NeedleIdSize+OffsetSize+SizeSize])

			if offset > 0 {
				m.Set(NeedleId(key), offset, size)
			} else {
				//delete(m, key)
			}
		}

		count, e = file.Read(bytes)
	}

	m.report()

}

// report memory usage
func (cm *CompactMap) report() {
	overFlowCount := 0;
	overwrittenByOverflow := 0;
	entryCount := 0
	compactSectionCount := 0
	compactSectionEntryCount := 0
	for _, cs := range cm.list {
		compactSectionCount++
		cs.RLock()
		for range cs.overflow {
			overFlowCount++
			entryCount++
		}
		for _, v := range cs.values {
			compactSectionEntryCount++
			if _, found := cs.overflow[v.Key]; !found {
				entryCount++
			} else {
				overwrittenByOverflow++
			}
		}
		cs.RUnlock()
	}
	println("overFlowCount", overFlowCount)
	println("overwrittenByOverflow", overwrittenByOverflow)
	println("entryCount", entryCount)
	println("compactSectionCount", compactSectionCount)
	println("compactSectionEntryCount", compactSectionEntryCount)
}
