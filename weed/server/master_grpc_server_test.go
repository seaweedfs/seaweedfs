package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
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

func TestLeaveLockRingDropsOnlyTheLeavingFiler(t *testing.T) {
	ms := &MasterServer{
		LockRingManager: cluster.NewLockRingManager(nil),
	}
	ms.LockRingManager.AddServer("group-a", "filer1:8888")
	ms.LockRingManager.AddServer("group-a", "filer2:8888")
	ms.LockRingManager.FlushPending("group-a")

	registered := &master_pb.KeepConnectedRequest{ClientType: cluster.FilerType, FilerGroup: "group-a"}
	ms.onKeepConnectedMessage(registered, "filer1:8888", &master_pb.KeepConnectedRequest{})
	ms.LockRingManager.FlushPending("group-a")
	assert.ElementsMatch(t, []string{"filer1:8888", "filer2:8888"}, ms.LockRingManager.GetServers("group-a"))

	ms.onKeepConnectedMessage(registered, "filer1:8888", &master_pb.KeepConnectedRequest{LeaveLockRing: true})
	ms.LockRingManager.FlushPending("group-a")
	assert.ElementsMatch(t, []string{"filer2:8888"}, ms.LockRingManager.GetServers("group-a"))

	s3 := &master_pb.KeepConnectedRequest{ClientType: cluster.S3Type, FilerGroup: "group-a"}
	ms.onKeepConnectedMessage(s3, "filer2:8888", &master_pb.KeepConnectedRequest{LeaveLockRing: true})
	ms.LockRingManager.FlushPending("group-a")
	assert.ElementsMatch(t, []string{"filer2:8888"}, ms.LockRingManager.GetServers("group-a"))
}

// A filer's replacement can register under the same address before the master
// ends the old stream; that stream's cleanup must leave the live filer in the ring.
func TestOverlappedFilerRegistrationStaysInLockRing(t *testing.T) {
	ms := &MasterServer{
		Cluster:         cluster.NewCluster(),
		LockRingManager: cluster.NewLockRingManager(nil),
	}
	registered := &master_pb.KeepConnectedRequest{ClientType: cluster.FilerType, FilerGroup: "group-a"}
	register := func(address pb.ServerAddress) {
		ms.Cluster.AddClusterNode(registered.FilerGroup, registered.ClientType, "", "", address, "")
		ms.updateLockRing(registered, address)
	}
	ringServers := func() []string {
		ms.LockRingManager.FlushPending("group-a")
		return ms.LockRingManager.GetServers("group-a")
	}

	register("filer0:8888")
	register("filer1:8888")
	register("filer0:8888")

	ms.removeClusterNode(registered, "filer0:8888")
	assert.True(t, ms.Cluster.IsKnownNode(cluster.FilerType, "filer0:8888"))
	assert.ElementsMatch(t, []string{"filer0:8888", "filer1:8888"}, ringServers())

	ms.removeClusterNode(registered, "filer0:8888")
	assert.False(t, ms.Cluster.IsKnownNode(cluster.FilerType, "filer0:8888"))
	assert.ElementsMatch(t, []string{"filer1:8888"}, ringServers())
}

// A leaving filer's reconnect can register before the master ends its broken
// stream; the filer must leave the ring without waiting for either stream.
func TestLeavingFilerReconnectLeavesLockRing(t *testing.T) {
	ms := &MasterServer{
		Cluster:         cluster.NewCluster(),
		LockRingManager: cluster.NewLockRingManager(nil),
	}
	registered := &master_pb.KeepConnectedRequest{ClientType: cluster.FilerType, FilerGroup: "group-a"}
	leaving := &master_pb.KeepConnectedRequest{ClientType: cluster.FilerType, FilerGroup: "group-a", LeaveLockRing: true}
	register := func(req *master_pb.KeepConnectedRequest, address pb.ServerAddress) {
		ms.Cluster.AddClusterNode(req.FilerGroup, req.ClientType, "", "", address, "")
		ms.updateLockRing(req, address)
	}
	ringServers := func() []string {
		ms.LockRingManager.FlushPending("group-a")
		return ms.LockRingManager.GetServers("group-a")
	}

	register(registered, "filer0:8888")
	register(registered, "filer1:8888")
	register(leaving, "filer0:8888")
	assert.ElementsMatch(t, []string{"filer1:8888"}, ringServers())

	ms.removeClusterNode(registered, "filer0:8888")
	assert.True(t, ms.Cluster.IsKnownNode(cluster.FilerType, "filer0:8888"))
	assert.ElementsMatch(t, []string{"filer1:8888"}, ringServers())

	ms.removeClusterNode(leaving, "filer0:8888")
	assert.False(t, ms.Cluster.IsKnownNode(cluster.FilerType, "filer0:8888"))
	assert.ElementsMatch(t, []string{"filer1:8888"}, ringServers())
}

// A filer that left the ring and is replaced before its stream ends must not
// take the replacement out of the ring with it.
func TestLeftFilerStreamEndKeepsReplacementInLockRing(t *testing.T) {
	ms := &MasterServer{
		Cluster:         cluster.NewCluster(),
		LockRingManager: cluster.NewLockRingManager(nil),
	}
	registered := &master_pb.KeepConnectedRequest{ClientType: cluster.FilerType, FilerGroup: "group-a"}
	for _, address := range []pb.ServerAddress{"filer0:8888", "filer1:8888"} {
		ms.Cluster.AddClusterNode(registered.FilerGroup, registered.ClientType, "", "", address, "")
		ms.LockRingManager.AddServer(cluster.FilerGroupName(registered.FilerGroup), address)
	}

	ms.onKeepConnectedMessage(registered, "filer0:8888", &master_pb.KeepConnectedRequest{LeaveLockRing: true})
	ms.LockRingManager.FlushPending("group-a")
	assert.ElementsMatch(t, []string{"filer1:8888"}, ms.LockRingManager.GetServers("group-a"))

	ms.Cluster.AddClusterNode(registered.FilerGroup, registered.ClientType, "", "", "filer0:8888", "")
	ms.LockRingManager.AddServer(cluster.FilerGroupName(registered.FilerGroup), "filer0:8888")
	ms.removeClusterNode(registered, "filer0:8888")
	ms.LockRingManager.FlushPending("group-a")
	assert.ElementsMatch(t, []string{"filer0:8888", "filer1:8888"}, ms.LockRingManager.GetServers("group-a"))
}

func TestInitialLockRingUpdateSkipsNonFilers(t *testing.T) {
	ms := &MasterServer{
		LockRingManager: cluster.NewLockRingManager(nil),
	}

	ms.LockRingManager.AddServer("group-a", "filer1:8888")
	ms.LockRingManager.FlushPending("group-a")

	assert.Nil(t, ms.initialLockRingUpdate(cluster.BrokerType, "group-a"))
}

// TestReconnectedClientSurvivesOldHandlerCleanup covers a client reconnecting
// KeepConnected under the same name before the old handler has exited: the old
// handler's deferred cleanup must not close the channel the reconnected stream
// registered, or that stream reads nil from its closed channel forever and
// floods the client with empty responses.
func TestReconnectedClientSurvivesOldHandlerCleanup(t *testing.T) {
	ms := &MasterServer{
		clientChans: make(map[string]chan *master_pb.KeepConnectedResponse),
	}

	clientName, oldChan := ms.addClient("", cluster.MasterType, "peer:19333")
	_, newChan := ms.addClient("", cluster.MasterType, "peer:19333")

	// the old handler's deferred cleanup runs after the reconnect registered
	ms.deleteClient(clientName, oldChan)

	// the old channel is closed so its drain goroutine can exit
	select {
	case _, ok := <-oldChan:
		assert.False(t, ok, "old channel should be closed")
	default:
		t.Fatal("old channel left open")
	}

	// the reconnected stream's channel is untouched and still receives broadcasts
	ms.broadcastToClients(&master_pb.KeepConnectedResponse{
		VolumeLocation: &master_pb.VolumeLocation{Url: "volume-a:8080"},
	})
	select {
	case message, ok := <-newChan:
		require.True(t, ok, "reconnected client's channel was closed by the old handler's cleanup")
		require.NotNil(t, message.GetVolumeLocation())
	default:
		t.Fatal("no broadcast reached the reconnected client")
	}

	// the reconnected handler's own cleanup still removes the registration
	ms.deleteClient(clientName, newChan)
	ms.clientChansLock.RLock()
	_, found := ms.clientChans[clientName]
	ms.clientChansLock.RUnlock()
	assert.False(t, found)
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
