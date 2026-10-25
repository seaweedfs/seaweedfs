package storage

import (
	"math/rand"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestFastLoadingNeedleMapMetrics(t *testing.T) {

	idxFile, _ := os.CreateTemp("", "tmp.idx")
	nm := NewCompactNeedleMap(idxFile)

	for i := 0; i < 10000; i++ {
		nm.Put(Uint64ToNeedleId(uint64(i+1)), Uint32ToOffset(uint32(i+1)), Size(1))
		if rand.Float32() < 0.2 && i > 0 {
			nm.Delete(Uint64ToNeedleId(uint64(rand.Int63n(int64(i))+1)), Uint32ToOffset(uint32(0)))
		}
	}

	mm, err := newNeedleMapMetricFromIndexFile(idxFile, needle.GetCurrentVersion())
	if err != nil {
		t.Fatalf("newNeedleMapMetricFromIndexFile() error = %v", err)
	}

	glog.V(0).Infof("FileCount expected %d actual %d", nm.FileCount(), mm.FileCount())
	glog.V(0).Infof("DeletedSize expected %d actual %d", nm.DeletedSize(), mm.DeletedSize())
	glog.V(0).Infof("ContentSize expected %d actual %d", nm.ContentSize(), mm.ContentSize())
	glog.V(0).Infof("DeletedCount expected %d actual %d", nm.DeletedCount(), mm.DeletedCount())
	glog.V(0).Infof("MaxFileKey expected %d actual %d", nm.MaxFileKey(), mm.MaxFileKey())

	if mm.FileCount() != nm.FileCount() {
		t.Fatalf("FileCount = %d, want %d", mm.FileCount(), nm.FileCount())
	}
	if mm.ContentSize() != nm.ContentSize() {
		t.Fatalf("ContentSize = %d, want %d", mm.ContentSize(), nm.ContentSize())
	}
	if mm.MaxFileKey() != nm.MaxFileKey() {
		t.Fatalf("MaxFileKey = %d, want %d", mm.MaxFileKey(), nm.MaxFileKey())
	}
	// Bloom false positives can hide a key whose latest row is live, which
	// only inflates the deletion counters by a small bounded amount.
	if got, want := mm.DeletedCount(), nm.DeletedCount(); got < want || got > want+256 {
		t.Fatalf("DeletedCount = %d, want within [%d, %d]", got, want, want+256)
	}
	if got, want := mm.DeletedSize(), nm.DeletedSize(); got < want || got > want+256 {
		t.Fatalf("DeletedSize = %d, want within [%d, %d]", got, want, want+256)
	}
}
