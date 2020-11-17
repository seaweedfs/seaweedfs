package storage

import (
	"io/ioutil"
	"math/rand"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/util/log"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

func TestFastLoadingNeedleMapMetrics(t *testing.T) {

	idxFile, _ := ioutil.TempFile("", "tmp.idx")
	nm := NewCompactNeedleMap(idxFile)

	for i := 0; i < 10000; i++ {
		nm.Put(Uint64ToNeedleId(uint64(i+1)), Uint32ToOffset(uint32(0)), Size(1))
		if rand.Float32() < 0.2 {
			nm.Delete(Uint64ToNeedleId(uint64(rand.Int63n(int64(i))+1)), Uint32ToOffset(uint32(0)))
		}
	}

	mm, _ := newNeedleMapMetricFromIndexFile(idxFile)

	log.Infof("FileCount expected %d actual %d", nm.FileCount(), mm.FileCount())
	log.Infof("DeletedSize expected %d actual %d", nm.DeletedSize(), mm.DeletedSize())
	log.Infof("ContentSize expected %d actual %d", nm.ContentSize(), mm.ContentSize())
	log.Infof("DeletedCount expected %d actual %d", nm.DeletedCount(), mm.DeletedCount())
	log.Infof("MaxFileKey expected %d actual %d", nm.MaxFileKey(), mm.MaxFileKey())
}
