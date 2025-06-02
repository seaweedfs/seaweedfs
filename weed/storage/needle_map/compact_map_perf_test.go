package needle_map

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"testing"
	"time"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

/*

To see the memory usage:

go test -run TestMemoryUsage
The Alloc section shows the in-use memory increase for each iteration.

go test -run TestMemoryUsage -memprofile=mem.out
go tool pprof --alloc_space needle.test mem.out


*/

func TestMemoryUsage(t *testing.T) {

	var maps []*CompactMap
	totalRowCount := uint64(0)

	startTime := time.Now()
	for i := 0; i < 10; i++ {
		indexFile, ie := os.OpenFile("../../../test/data/sample.idx", os.O_RDWR|os.O_RDONLY, 0644)
		if ie != nil {
			log.Fatalln(ie)
		}
		m, rowCount := loadNewNeedleMap(indexFile)
		maps = append(maps, m)
		totalRowCount += rowCount

		indexFile.Close()

		PrintMemUsage(totalRowCount)
		now := time.Now()
		fmt.Printf("\tTaken = %v\n", now.Sub(startTime))
		startTime = now
	}

}

func loadNewNeedleMap(file *os.File) (*CompactMap, uint64) {
	m := NewCompactMap()
	bytes := make([]byte, NeedleMapEntrySize)
	rowCount := uint64(0)
	count, e := file.Read(bytes)
	for count > 0 && e == nil {
		for i := 0; i < count; i += NeedleMapEntrySize {
			rowCount++
			key := BytesToNeedleId(bytes[i : i+NeedleIdSize])
			offset := BytesToOffset(bytes[i+NeedleIdSize : i+NeedleIdSize+OffsetSize])
			size := BytesToSize(bytes[i+NeedleIdSize+OffsetSize : i+NeedleIdSize+OffsetSize+SizeSize])

			if !offset.IsZero() {
				m.Set(NeedleId(key), offset, size)
			} else {
				m.Delete(key)
			}
		}

		count, e = file.Read(bytes)
	}

	return m, rowCount

}

func PrintMemUsage(totalRowCount uint64) {

	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Each %.02f Bytes", float64(m.Alloc)/float64(totalRowCount))
	fmt.Printf("\tAlloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v", m.NumGC)
}
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
