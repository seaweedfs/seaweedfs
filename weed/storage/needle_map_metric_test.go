package storage

import (
	"math/rand"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util/log"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestFastLoadingNeedleMapMetrics(t *testing.T) {

	idxFile, _ := os.CreateTemp("", "tmp.idx")
	nm := NewCompactNeedleMap(idxFile)

	for i := 0; i < 10000; i++ {
		nm.Put(Uint64ToNeedleId(uint64(i+1)), Uint32ToOffset(uint32(0)), Size(1))
		if rand.Float32() < 0.2 && i > 0 {
			nm.Delete(Uint64ToNeedleId(uint64(rand.Int63n(int64(i))+1)), Uint32ToOffset(uint32(0)))
		}
	}

	mm, _ := newNeedleMapMetricFromIndexFile(idxFile)

	log.V(3).Infof("FileCount expected %d actual %d", nm.FileCount(), mm.FileCount())
	log.V(3).Infof("DeletedSize expected %d actual %d", nm.DeletedSize(), mm.DeletedSize())
	log.V(3).Infof("ContentSize expected %d actual %d", nm.ContentSize(), mm.ContentSize())
	log.V(3).Infof("DeletedCount expected %d actual %d", nm.DeletedCount(), mm.DeletedCount())
	log.V(3).Infof("MaxFileKey expected %d actual %d", nm.MaxFileKey(), mm.MaxFileKey())
}
