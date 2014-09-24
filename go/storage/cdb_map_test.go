package storage

import (
	"github.com/aszxqw/weed-fs/go/glog"
	"math/rand"
	"os"
	"runtime"
	"testing"
)

var testIndexFilename string = "../../test/sample.idx"

func TestCdbMap0Convert(t *testing.T) {
	indexFile, err := os.Open(testIndexFilename)
	if err != nil {
		t.Fatalf("cannot open %s: %s", testIndexFilename, err)
	}
	defer indexFile.Close()

	cdbFn := testIndexFilename + ".cdb"
	t.Logf("converting %s to %s", cdbFn, cdbFn)
	if err = ConvertIndexToCdb(cdbFn, indexFile); err != nil {
		t.Fatalf("error while converting: %s", err)
	}
}

func TestCdbMap1Mem(t *testing.T) {
	var nm NeedleMapper
	i := 0
	visit := func(nv NeedleValue) error {
		i++
		return nil
	}

	a := getMemStats()
	t.Logf("opening %s.cdb", testIndexFilename)
	nm, err := OpenCdbMap(testIndexFilename + ".cdb")
	if err != nil {
		t.Fatalf("error opening cdb: %s", err)
	}
	b := getMemStats()
	glog.V(0).Infof("opening cdb consumed %d bytes", b-a)
	defer nm.Close()

	a = getMemStats()
	if err = nm.Visit(visit); err != nil {
		t.Fatalf("error visiting %s: %s", nm, err)
	}
	b = getMemStats()
	glog.V(0).Infof("visit cdb %d consumed %d bytes", i, b-a)
	nm.Close()

	indexFile, err := os.Open(testIndexFilename)
	if err != nil {
		t.Fatalf("error opening idx: %s", err)
	}
	a = getMemStats()
	nm, err = LoadNeedleMap(indexFile)
	if err != nil {
		t.Fatalf("error loading idx: %s", err)
	}
	defer nm.Close()
	b = getMemStats()
	glog.V(0).Infof("opening idx consumed %d bytes", b-a)

	i = 0
	a = getMemStats()
	if err = nm.Visit(visit); err != nil {
		t.Fatalf("error visiting %s: %s", nm, err)
	}
	b = getMemStats()
	glog.V(0).Infof("visit idx %d consumed %d bytes", i, b-a)
}

func BenchmarkCdbMap9List(t *testing.B) {
	t.StopTimer()
	indexFile, err := os.Open(testIndexFilename)
	if err != nil {
		t.Fatalf("cannot open %s: %s", testIndexFilename, err)
	}
	defer indexFile.Close()

	a := getMemStats()
	t.Logf("opening %s", indexFile)
	idx, err := LoadNeedleMap(indexFile)
	if err != nil {
		t.Fatalf("cannot load %s: %s", indexFile.Name(), err)
	}
	defer idx.Close()
	b := getMemStats()
	glog.V(0).Infof("LoadNeedleMap consumed %d bytes", b-a)

	cdbFn := testIndexFilename + ".cdb"
	a = getMemStats()
	t.Logf("opening %s", cdbFn)
	m, err := OpenCdbMap(cdbFn)
	if err != nil {
		t.Fatalf("error opening %s: %s", cdbFn, err)
	}
	defer m.Close()
	b = getMemStats()
	glog.V(0).Infof("OpenCdbMap consumed %d bytes", b-a)

	i := 0
	glog.V(0).Infoln("checking whether the cdb contains every key")
	t.StartTimer()
	err = idx.Visit(func(nv NeedleValue) error {
		if i > t.N || rand.Intn(10) < 9 {
			return nil
		}
		i++
		if i%1000 == 0 {
			glog.V(0).Infof("%d. %s", i, nv)
		}
		if nv2, ok := m.Get(uint64(nv.Key)); !ok || nv2 == nil {
			t.Errorf("%d in index, not in cdb", nv.Key)
		} else if nv2.Key != nv.Key {
			t.Errorf("requested key %d from cdb, got %d", nv.Key, nv2.Key)
		} else if nv2.Offset != nv.Offset {
			t.Errorf("offset is %d in index, %d in cdb", nv.Offset, nv2.Offset)
		} else if nv2.Size != nv.Size {
			t.Errorf("size is %d in index, %d in cdb", nv.Size, nv2.Size)
		}
		t.SetBytes(int64(nv.Size))
		return nil
	})
	t.StopTimer()
	if err != nil {
		t.Errorf("error visiting index: %s", err)
	}

	i = 0
	glog.V(0).Infoln("checking wheter the cdb contains no stray keys")
	t.StartTimer()
	err = m.Visit(func(nv NeedleValue) error {
		if i > t.N || rand.Intn(10) < 9 {
			return nil
		}
		if nv2, ok := m.Get(uint64(nv.Key)); !ok || nv2 == nil {
			t.Errorf("%d in cdb, not in index", nv.Key)
		} else if nv2.Key != nv.Key {
			t.Errorf("requested key %d from index, got %d", nv.Key, nv2.Key)
		} else if nv2.Offset != nv.Offset {
			t.Errorf("offset is %d in cdb, %d in index", nv.Offset, nv2.Offset)
		} else if nv2.Size != nv.Size {
			t.Errorf("size is %d in cdb, %d in index", nv.Size, nv2.Size)
		}
		i++
		if i%1000 == 0 {
			glog.V(0).Infof("%d. %s", i, nv)
		}
		t.SetBytes(int64(nv.Size))
		return nil
	})
	t.StopTimer()
	if err != nil {
		t.Errorf("error visiting index: %s", err)
	}
}

var mem = new(runtime.MemStats)

// returns MemStats.Alloc after a GC
func getMemStats() int64 {
	runtime.GC()
	runtime.ReadMemStats(mem)
	return int64(mem.Alloc)
}
