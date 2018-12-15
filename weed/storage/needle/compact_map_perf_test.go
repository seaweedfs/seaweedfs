package needle

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"testing"
	"time"

	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

/*

To see the memory usage:

go test -run TestMemoryUsage
The TotalAlloc section shows the memory increase for each iteration.

go test -run TestMemoryUsage -memprofile=mem.out
go tool pprof --alloc_space needle.test mem.out


*/

func TestMemoryUsage(t *testing.T) {

	var maps []*CompactMap

	startTime := time.Now()
	for i := 0; i < 10; i++ {
		indexFile, ie := os.OpenFile("../../../test/sample.idx", os.O_RDWR|os.O_RDONLY, 0644)
		if ie != nil {
			log.Fatalln(ie)
		}
		maps = append(maps, loadNewNeedleMap(indexFile))

		indexFile.Close()

		PrintMemUsage()
		now := time.Now()
		fmt.Printf("\tTaken = %v\n", now.Sub(startTime))
		startTime = now
	}

}

func loadNewNeedleMap(file *os.File) *CompactMap {
	m := NewCompactMap()
	bytes := make([]byte, NeedleEntrySize)
	count, e := file.Read(bytes)
	for count > 0 && e == nil {
		for i := 0; i < count; i += NeedleEntrySize {
			key := BytesToNeedleId(bytes[i : i+NeedleIdSize])
			offset := BytesToOffset(bytes[i+NeedleIdSize : i+NeedleIdSize+OffsetSize])
			size := util.BytesToUint32(bytes[i+NeedleIdSize+OffsetSize : i+NeedleIdSize+OffsetSize+SizeSize])

			if offset > 0 {
				m.Set(NeedleId(key), offset, size)
			} else {
				m.Delete(key)
			}
		}

		count, e = file.Read(bytes)
	}

	return m

}

func PrintMemUsage() {

	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v", m.NumGC)
}
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
