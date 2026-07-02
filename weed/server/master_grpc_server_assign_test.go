package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// leaderRaftServer answers only State(); the embedded nil interface panics if any
// other method is called.
type leaderRaftServer struct {
	raft.Server
}

func (leaderRaftServer) State() string { return raft.Leader }

func newLeaderMaster() *MasterServer {
	topo := topology.NewTopology("test", sequence.NewMemorySequencer(), 1<<20, 5, false)
	topo.RaftServer = leaderRaftServer{}
	return &MasterServer{
		Topo:                    topo,
		option:                  &MasterOption{},
		guard:                   security.NewGuard(nil, "", 0, "", 0),
		volumeGrowthRequestChan: make(chan *topology.VolumeGrowRequest, 1<<6),
	}
}

// markGrowthInFlight flags growth on the same VolumeLayout the handler resolves,
// by mirroring its exact lookup.
func markGrowthInFlight(t *testing.T, topo *topology.Topology, req *master_pb.AssignRequest) {
	t.Helper()
	rp, err := super_block.NewReplicaPlacementFromString(req.Replication)
	require.NoError(t, err)
	ttl, err := needle.ReadTTL(req.Ttl)
	require.NoError(t, err)
	topo.GetVolumeLayout(req.Collection, rp, ttl, types.ToDiskType(req.DiskType)).AddGrowRequestIfAbsent()
}

// With free space but no writable volume and growth already in flight, Assign
// sheds with a retryable ResourceExhausted immediately instead of spinning to
// timeout. ResourceExhausted (not Unavailable) so the shed isn't mistaken for a
// dead channel and doesn't tear down the shared master connection.
func TestAssignShedsLoadWhenGrowthInFlight(t *testing.T) {
	ms := newLeaderMaster()
	// Free volume slots but no writable volume yet, so growth is the remedy.
	ms.Topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "127.0.0.1", "dn1", map[string]uint32{"": 100})

	req := &master_pb.AssignRequest{Count: 1, Replication: "000"}
	markGrowthInFlight(t, ms.Topo, req)

	start := time.Now()
	resp, err := ms.Assign(context.Background(), req)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Nil(t, resp)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())
	// Shed immediately rather than spinning out the 10s retry budget.
	assert.Less(t, elapsed, 2*time.Second)
	// Growth was already pending, so we must not enqueue another grow request.
	assert.Len(t, ms.volumeGrowthRequestChan, 0)
}

// The assign that triggers growth must not shed itself: on a cold cluster there
// is no other request to pick up the volume, so it waits for the growth it
// started and returns the fresh volume once it lands.
func TestAssignInitiatorWaitsForItsOwnGrowth(t *testing.T) {
	ms := newLeaderMaster()
	dn := ms.Topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "127.0.0.1", "dn1", map[string]uint32{"": 100})

	req := &master_pb.AssignRequest{Count: 1, Replication: "000"}
	rp, err := super_block.NewReplicaPlacementFromString(req.Replication)
	require.NoError(t, err)

	// Simulate growth landing shortly after it is requested by registering a
	// writable volume, then draining the request the initiator enqueued.
	go func() {
		req := <-ms.volumeGrowthRequestChan
		v := storage.VolumeInfo{
			Id:               needle.VolumeId(1),
			Version:          needle.GetCurrentVersion(),
			ReplicaPlacement: rp,
			Ttl:              needle.EMPTY_TTL,
		}
		dn.UpdateVolumes([]storage.VolumeInfo{v})
		ms.Topo.RegisterVolumeLayout(v, dn)
		ms.Topo.GetVolumeLayout(req.Option.Collection, rp, needle.EMPTY_TTL, types.ToDiskType("")).DoneGrowRequest()
	}()

	start := time.Now()
	resp, err := ms.Assign(context.Background(), req)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotEmpty(t, resp.Fid)
	// It waited for growth rather than shedding, but well inside the retry budget.
	assert.Less(t, elapsed, 5*time.Second)
}

// An initiator whose growth concludes without yielding a writable volume
// (failed or discarded growth) sheds retryably instead of re-triggering
// growth for the rest of its budget.
func TestAssignInitiatorShedsWhenGrowthConcludesUnfulfilled(t *testing.T) {
	ms := newLeaderMaster()
	ms.Topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "127.0.0.1", "dn1", map[string]uint32{"": 100})

	req := &master_pb.AssignRequest{Count: 1, Replication: "000"}
	rp, err := super_block.NewReplicaPlacementFromString(req.Replication)
	require.NoError(t, err)
	vl := ms.Topo.GetVolumeLayout("", rp, needle.EMPTY_TTL, types.ToDiskType(""))

	// Growth consumer that fails: clears the flag without registering volumes.
	go func() {
		<-ms.volumeGrowthRequestChan
		vl.DoneGrowRequest()
	}()

	start := time.Now()
	resp, err := ms.Assign(context.Background(), req)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Nil(t, resp)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())
	assert.Less(t, elapsed, 2*time.Second)
	// Growth was triggered exactly once, not re-enqueued after the failure.
	assert.Len(t, ms.volumeGrowthRequestChan, 0)
}

// A cancelled request stops waiting instead of sleeping out the 10s budget.
func TestAssignAbortsOnCancel(t *testing.T) {
	ms := newLeaderMaster()
	ms.Topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "127.0.0.1", "dn1", map[string]uint32{"": 100})

	// Initiator with its growth never concluding: nobody drains the chan.
	req := &master_pb.AssignRequest{Count: 1, Replication: "000"}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := ms.Assign(ctx, req)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, elapsed, 2*time.Second)
}

// Out of space, Assign fails fast with the real error rather than masking it as
// a retryable "growth in progress".
func TestAssignFailsFastWhenOutOfSpace(t *testing.T) {
	ms := newLeaderMaster() // no data nodes -> no free space

	req := &master_pb.AssignRequest{Count: 1, Replication: "000"}

	start := time.Now()
	resp, err := ms.Assign(context.Background(), req)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Nil(t, resp)
	if st, ok := status.FromError(err); ok {
		assert.NotEqual(t, codes.Unavailable, st.Code())
	}
	assert.Contains(t, err.Error(), "no free volumes left")
	assert.Less(t, elapsed, 2*time.Second)
}
