package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitialLockRingUpdateReturnsLastBroadcastForFilers(t *testing.T) {
	ms := &MasterServer{
		LockRingManager: cluster.NewLockRingManager(nil),
	}

	ms.LockRingManager.AddServer("group-a", "filer1:8888")
	ms.LockRingManager.AddServer("group-a", "filer2:8888")
	ms.LockRingManager.FlushPending("group-a")

	resp := ms.initialLockRingUpdate(cluster.FilerType, "group-a")
	require.NotNil(t, resp)
	require.NotNil(t, resp.LockRingUpdate)
	assert.Equal(t, "group-a", resp.LockRingUpdate.FilerGroup)
	assert.ElementsMatch(t, []string{"filer1:8888", "filer2:8888"}, resp.LockRingUpdate.Servers)
	assert.Greater(t, resp.LockRingUpdate.Version, int64(0))
}

func TestInitialLockRingUpdateSkipsNonFilers(t *testing.T) {
	ms := &MasterServer{
		LockRingManager: cluster.NewLockRingManager(nil),
	}

	ms.LockRingManager.AddServer("group-a", "filer1:8888")
	ms.LockRingManager.FlushPending("group-a")

	assert.Nil(t, ms.initialLockRingUpdate(cluster.BrokerType, "group-a"))
}

func TestEnsureCollectionDeleteSafeRejectsDuplicateVolumeIds(t *testing.T) {
	topo := topology.NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	maxVolumeCounts := map[string]uint32{"": 25}

	dn1 := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", maxVolumeCounts)
	dn2 := rack.GetOrCreateDataNode("127.0.0.2", 34535, 0, "127.0.0.2", "", maxVolumeCounts)

	replicaPlacement := &super_block.ReplicaPlacement{}
	collectionA := storage.VolumeInfo{
		Id:               needle.VolumeId(100),
		Collection:       "collection-a",
		Version:          needle.GetCurrentVersion(),
		ReplicaPlacement: replicaPlacement,
		Ttl:              needle.EMPTY_TTL,
	}
	collectionB := storage.VolumeInfo{
		Id:               needle.VolumeId(100),
		Collection:       "collection-b",
		Version:          needle.GetCurrentVersion(),
		ReplicaPlacement: replicaPlacement,
		Ttl:              needle.EMPTY_TTL,
	}

	dn1.UpdateVolumes([]storage.VolumeInfo{collectionA})
	dn2.UpdateVolumes([]storage.VolumeInfo{collectionB})
	topo.RegisterVolumeLayout(collectionA, dn1)
	topo.RegisterVolumeLayout(collectionB, dn2)

	ms := &MasterServer{Topo: topo}

	err := ms.ensureCollectionDeleteSafe("collection-a")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to delete collection")
	assert.Contains(t, err.Error(), "100:[collection-a, collection-b]")
}
