package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
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

// TestBroadcastVolumeLocationsToClients verifies grown volume locations are sent to registered clients.
func TestBroadcastVolumeLocationsToClients(t *testing.T) {
	clientChan := make(chan *master_pb.KeepConnectedResponse, 2)
	ms := &MasterServer{
		clientChans: map[string]chan *master_pb.KeepConnectedResponse{
			"default.filer@127.0.0.1:8888": clientChan,
		},
	}

	ms.broadcastVolumeLocationsToClients([]*master_pb.VolumeLocation{
		{Url: "volume-a:8080", NewVids: []uint32{7}},
		{Url: "volume-b:8080", NewVids: []uint32{8}},
	})

	var first *master_pb.KeepConnectedResponse
	select {
	case first = <-clientChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first broadcast")
	}
	require.NotNil(t, first.GetVolumeLocation())
	assert.Equal(t, []uint32{7}, first.GetVolumeLocation().GetNewVids())
	assert.Equal(t, "volume-a:8080", first.GetVolumeLocation().GetUrl())

	var second *master_pb.KeepConnectedResponse
	select {
	case second = <-clientChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second broadcast")
	}
	require.NotNil(t, second.GetVolumeLocation())
	assert.Equal(t, []uint32{8}, second.GetVolumeLocation().GetNewVids())
	assert.Equal(t, "volume-b:8080", second.GetVolumeLocation().GetUrl())
}
