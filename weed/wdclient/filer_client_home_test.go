package wdclient

import (
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// An embedded S3 gateway is seeded with its local filer as "ip:port" while the
// filer registers in the master as "ip:port.grpcPort". Both name the same filer,
// so discovery must keep it in place, with its health, as the current filer.
func TestDiscoveryKeepsSeededFilerUnderRegisteredForm(t *testing.T) {
	fc := newTestFilerClient("10.0.0.1:8888")
	seededHealth := fc.filerHealth[0]
	atomic.StoreInt32(&seededHealth.failureCount, 2)

	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{
		"10.0.0.1:8888.18888": {},
		"10.0.0.2:8888.18888": {},
		"10.0.0.3:8888.18888": {},
	})

	want := []pb.ServerAddress{"10.0.0.1:8888.18888", "10.0.0.2:8888.18888", "10.0.0.3:8888.18888"}
	if got := filerAddressList(fc); !reflect.DeepEqual(got, want) {
		t.Fatalf("filer list: want %v, got %v", want, got)
	}
	if fc.filerHealth[0] != seededHealth {
		t.Fatalf("seeded filer lost its health entry")
	}
	if got := atomic.LoadInt32(&fc.filerHealth[0].failureCount); got != 2 {
		t.Fatalf("seeded filer health reset: want failureCount 2, got %d", got)
	}
	if got := fc.GetCurrentFiler(); got != "10.0.0.1:8888.18888" {
		t.Fatalf("current filer: want the seeded local filer, got %s", got)
	}
}

func TestDiscoveryOrderIsDeterministic(t *testing.T) {
	discovered := map[pb.ServerAddress]struct{}{
		"10.0.0.1:8888.18888": {},
		"10.0.0.2:8888.18888": {},
		"10.0.0.3:8888.18888": {},
		"10.0.0.4:8888.18888": {},
	}
	var first []pb.ServerAddress
	for i := 0; i < 200; i++ {
		fc := newTestFilerClient("10.0.0.9:8888")
		fc.applyDiscoveredFilers(discovered)
		got := filerAddressList(fc)
		if first == nil {
			first = got
			continue
		}
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("run %d: list order changed: %v vs %v", i, got, first)
		}
	}
}

func TestDiscoveryCurrentGoneFallsToFirstSorted(t *testing.T) {
	a := pb.ServerAddress("10.0.0.1:18888")
	b := pb.ServerAddress("10.0.0.2:18888")
	c := pb.ServerAddress("10.0.0.3:18888")
	fc := newTestFilerClient(a, b)
	atomic.StoreInt32(&fc.filerIndex, 1)

	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{a: {}, c: {}})

	if got := fc.GetCurrentFiler(); got != a {
		t.Fatalf("current filer: want %s, got %s", a, got)
	}
}
