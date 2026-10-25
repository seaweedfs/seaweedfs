package weed_server

import (
	"io"
	"testing"

	"github.com/seaweedfs/raft"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// admitTestRaftServer answers as leader and swallows Do so a growth freeze
// lands its local flag without needing a real raft log.
type admitTestRaftServer struct {
	raft.Server
}

func (admitTestRaftServer) State() string                        { return raft.Leader }
func (admitTestRaftServer) Do(raft.Command) (interface{}, error) { return nil, nil }

type fakeHeartbeatStream struct {
	grpc.ServerStream
	heartbeats []*master_pb.Heartbeat
	sent       []*master_pb.HeartbeatResponse
}

func (s *fakeHeartbeatStream) Recv() (*master_pb.Heartbeat, error) {
	if len(s.heartbeats) == 0 {
		return nil, io.EOF
	}
	hb := s.heartbeats[0]
	s.heartbeats = s.heartbeats[1:]
	return hb, nil
}

func (s *fakeHeartbeatStream) Send(resp *master_pb.HeartbeatResponse) error {
	s.sent = append(s.sent, resp)
	return nil
}

// A server the single-reporter admission refuses must not leave any applied
// state behind: its stale topology id cannot freeze growth and its seen bound
// cannot raise the master's.
func TestSendHeartbeatRejectsForeignReporterBeforeApplyingState(t *testing.T) {
	ms := newLeaderMaster()
	ms.Topo.RaftServer = admitTestRaftServer{}
	ms.Topo.SetGrowthFreezeConfig(true, 0)
	ms.Topo.SetTopologyId("cluster-b")
	ms.Topo.ReleaseAllocationOnFirstReport("10.0.0.1:8080")

	stream := &fakeHeartbeatStream{heartbeats: []*master_pb.Heartbeat{{
		Ip:                 "10.0.0.2",
		Port:               8080,
		MaxVolumeIdSeen:    7,
		LastSeenTopologyId: "cluster-a",
		Volumes:            []*master_pb.VolumeInformationMessage{{Id: 3}},
	}}}

	err := ms.SendHeartbeat(stream)
	require.ErrorContains(t, err, "rejected")
	assert.False(t, ms.Topo.IsGrowthFrozen())
	assert.Zero(t, uint32(ms.Topo.GetMaxVolumeId()))
	assert.Empty(t, stream.sent)
}

// The pinned reporter is still subject to the mismatch freeze: admission gates
// who may report, not what a legitimate report can do.
func TestSendHeartbeatAdmittedReporterStillFreezesOnMismatch(t *testing.T) {
	ms := newLeaderMaster()
	ms.Topo.RaftServer = admitTestRaftServer{}
	ms.Topo.SetGrowthFreezeConfig(true, 0)
	ms.Topo.SetTopologyId("cluster-b")
	ms.Topo.ReleaseAllocationOnFirstReport("10.0.0.1:8080")

	stream := &fakeHeartbeatStream{heartbeats: []*master_pb.Heartbeat{{
		Ip:                 "10.0.0.1",
		Port:               8080,
		MaxVolumeIdSeen:    7,
		LastSeenTopologyId: "cluster-a",
		Volumes:            []*master_pb.VolumeInformationMessage{{Id: 3}},
	}}}

	_ = ms.SendHeartbeat(stream)
	assert.True(t, ms.Topo.IsGrowthFrozen())
}

// An unregistered heartbeat with no address can never be the pinned reporter:
// its stale topology id and seen bound must not reach the topology on the way
// to the delta-heartbeat skip.
func TestSendHeartbeatRejectsAddresslessFirstHeartbeat(t *testing.T) {
	ms := newLeaderMaster()
	ms.Topo.RaftServer = admitTestRaftServer{}
	ms.Topo.SetGrowthFreezeConfig(true, 0)
	ms.Topo.SetTopologyId("cluster-b")
	ms.Topo.ReleaseAllocationOnFirstReport("10.0.0.1:8080")

	stream := &fakeHeartbeatStream{heartbeats: []*master_pb.Heartbeat{{
		MaxVolumeIdSeen:    7,
		LastSeenTopologyId: "cluster-a",
		Volumes:            []*master_pb.VolumeInformationMessage{{Id: 3}},
	}}}

	err := ms.SendHeartbeat(stream)
	require.ErrorContains(t, err, "rejected")
	assert.False(t, ms.Topo.IsGrowthFrozen())
	assert.Zero(t, uint32(ms.Topo.GetMaxVolumeId()))
	assert.Empty(t, stream.sent)
}
