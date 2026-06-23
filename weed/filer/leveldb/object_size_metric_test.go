package leveldb

import (
	"context"
	"os"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func histogramState(t *testing.T) (count uint64, perBucket map[float64]uint64) {
	t.Helper()
	m := &dto.Metric{}
	if err := stats.FilerObjectSizeBytesHistogram.Write(m); err != nil {
		t.Fatalf("write histogram: %v", err)
	}
	perBucket = make(map[float64]uint64)
	for _, b := range m.GetHistogram().GetBucket() {
		perBucket[b.GetUpperBound()] = b.GetCumulativeCount()
	}
	return m.GetHistogram().GetSampleCount(), perBucket
}

func TestCreateEntryRecordsObjectSize(t *testing.T) {
	testFiler := filer.NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	store := &LevelDBStore{}
	if err := store.initialize(t.TempDir()); err != nil {
		t.Fatalf("init store: %v", err)
	}
	testFiler.SetStore(store)
	defer testFiler.Shutdown()
	ctx := context.Background()

	file := func(path string, size uint64) *filer.Entry {
		return &filer.Entry{
			FullPath: util.FullPath(path),
			Attr:     filer.Attr{Mode: 0640, FileSize: size},
		}
	}

	before, _ := histogramState(t)

	if err := testFiler.CreateEntry(ctx, file("/data/small.txt", 500), nil, false, false, nil, false, 255); err != nil {
		t.Fatalf("create small: %v", err)
	}
	if err := testFiler.CreateEntry(ctx, file("/data/big.bin", 5*1024*1024), nil, false, false, nil, false, 255); err != nil {
		t.Fatalf("create big: %v", err)
	}
	dir := &filer.Entry{FullPath: util.FullPath("/data/sub"), Attr: filer.Attr{Mode: os.ModeDir | 0755}}
	if err := testFiler.CreateEntry(ctx, dir, nil, false, false, nil, false, 255); err != nil {
		t.Fatalf("create dir: %v", err)
	}
	// overwrite is an update, not a new object
	if err := testFiler.CreateEntry(ctx, file("/data/small.txt", 800), nil, false, false, nil, false, 255); err != nil {
		t.Fatalf("overwrite small: %v", err)
	}

	after, buckets := histogramState(t)

	// only the two new files are sampled, into their respective ranges
	if got := after - before; got != 2 {
		t.Fatalf("expected 2 sampled objects, got %d", got)
	}
	if buckets[1024] < 1 {
		t.Errorf("expected the 500-byte object in the <=1024 bucket, buckets=%v", buckets)
	}
	if buckets[104857600]-buckets[1048576] < 1 {
		t.Errorf("expected the 5MB object in the (1MB,100MB] range, buckets=%v", buckets)
	}
}
