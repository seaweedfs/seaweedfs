package topology

import (
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestGetVolumeLayoutOfAbsentKey(t *testing.T) {
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	c := NewCollection("c", 32*1024, false)

	vl, found := c.GetVolumeLayout(rp, needle.EMPTY_TTL, types.HardDriveType)
	if found || vl != nil {
		t.Fatalf("absent layout: got (%v, %v), want (nil, false)", vl, found)
	}
}

// Volume servers dropping the last replica of several volumes that share one
// layout all see the layout go empty and all delete it. The losers used to
// crash the master on a nil type assertion.
func TestConcurrentLastReplicaRemoval(t *testing.T) {
	const nodeCount = 8
	ttl, _ := needle.ReadTTL("5m")
	for round := 0; round < 500; round++ {
		topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
		rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")

		// A second layout keeps the collection alive, so every remover reaches
		// the layout deletion instead of stopping at a vanished collection.
		keeper := rack.GetOrCreateDataNode("127.0.0.1", 9000, 0, "", "", map[string]uint32{"": 100})
		topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{{
			Id: 999, Collection: "c", Version: uint32(needle.GetCurrentVersion()), Ttl: ttl.ToUint32(),
		}}, keeper)

		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < nodeCount; i++ {
			dn := rack.GetOrCreateDataNode("127.0.0.1", 8080+i, 0, "", "", map[string]uint32{"": 100})
			m := &master_pb.VolumeInformationMessage{
				Id: uint32(i + 1), Collection: "c", Version: uint32(needle.GetCurrentVersion()),
			}
			topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{m}, dn)
			vi, err := storage.NewVolumeInfo(m)
			if err != nil {
				t.Fatalf("NewVolumeInfo: %v", err)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				topo.UnRegisterVolumeLayout(vi, dn)
			}()
		}
		close(start)
		wg.Wait()

		c, found := topo.FindCollection("c")
		if !found {
			t.Fatalf("round %d: collection dropped while a layout still had volumes", round)
		}
		if layouts := c.GetAllVolumeLayouts(); len(layouts) != 1 {
			t.Fatalf("round %d: got %d layouts, want only the one still holding a volume", round, len(layouts))
		}
	}
}
