package storage

import (
	"math/rand"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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

	glog.V(0).Infof("FileCount expected %d actual %d", nm.FileCount(), mm.FileCount())
	glog.V(0).Infof("DeletedSize expected %d actual %d", nm.DeletedSize(), mm.DeletedSize())
	glog.V(0).Infof("ContentSize expected %d actual %d", nm.ContentSize(), mm.ContentSize())
	glog.V(0).Infof("DeletedCount expected %d actual %d", nm.DeletedCount(), mm.DeletedCount())
	glog.V(0).Infof("MaxFileKey expected %d actual %d", nm.MaxFileKey(), mm.MaxFileKey())
}
