package weed_server

import (
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type chanHeartbeatStream struct {
	grpc.ServerStream
	in chan *master_pb.Heartbeat
}

func (s *chanHeartbeatStream) Recv() (*master_pb.Heartbeat, error) {
	hb, ok := <-s.in
	if !ok {
		return nil, io.EOF
	}
	return hb, nil
}

func (s *chanHeartbeatStream) Send(*master_pb.HeartbeatResponse) error { return nil }

func reportedLastHeartbeatNs(ms *MasterServer) int64 {
	for _, dc := range ms.Topo.ToTopologyInfo(topology.VolumeFilter{}).DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				return dn.LastHeartbeatNs
			}
		}
	}
	return 0
}

func TestSendHeartbeatReportsEachHeartbeatTime(t *testing.T) {
	ms := newLeaderMaster()
	stream := &chanHeartbeatStream{in: make(chan *master_pb.Heartbeat)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ms.SendHeartbeat(stream)
	}()

	before := time.Now().UnixNano()
	stream.in <- &master_pb.Heartbeat{Ip: "10.0.0.1", Port: 8080, HasNoVolumes: true}
	var first int64
	require.Eventually(t, func() bool {
		first = reportedLastHeartbeatNs(ms)
		return first >= before
	}, 5*time.Second, time.Millisecond, "first heartbeat time not reported")

	time.Sleep(2 * time.Millisecond)
	stream.in <- &master_pb.Heartbeat{HasNoVolumes: true}
	require.Eventually(t, func() bool {
		return reportedLastHeartbeatNs(ms) > first
	}, 5*time.Second, time.Millisecond, "a later heartbeat must move the reported time")

	close(stream.in)
	<-done
}
