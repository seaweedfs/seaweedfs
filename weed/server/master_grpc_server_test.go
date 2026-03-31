package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
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
