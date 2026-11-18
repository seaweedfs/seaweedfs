package weed_server

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// silentHeartbeatStream hands out its heartbeats and then blocks in Recv until
// its context ends, as a stream from a volume server that went silent.
type silentHeartbeatStream struct {
	grpc.ServerStream
	ctx        context.Context
	heartbeats chan *master_pb.Heartbeat
}

func (s *silentHeartbeatStream) Context() context.Context { return s.ctx }

func (s *silentHeartbeatStream) Recv() (*master_pb.Heartbeat, error) {
	select {
	case hb := <-s.heartbeats:
		return hb, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *silentHeartbeatStream) Send(*master_pb.HeartbeatResponse) error { return nil }

func newSilentHeartbeatStream(t *testing.T, heartbeats ...*master_pb.Heartbeat) *silentHeartbeatStream {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch := make(chan *master_pb.Heartbeat, len(heartbeats))
	for _, hb := range heartbeats {
		ch <- hb
	}
	return &silentHeartbeatStream{ctx: ctx, heartbeats: ch}
}

func TestHeartbeatReceiverPassesHeartbeatsAndErrors(t *testing.T) {
	stream := &fakeHeartbeatStream{heartbeats: []*master_pb.Heartbeat{{Ip: "10.0.0.1"}, {Ip: "10.0.0.2"}}}
	recv, stop := heartbeatReceiver(stream, time.Second)
	defer stop()

	for _, ip := range []string{"10.0.0.1", "10.0.0.2"} {
		hb, err := recv()
		require.NoError(t, err)
		assert.Equal(t, ip, hb.Ip)
	}
	_, err := recv()
	assert.ErrorIs(t, err, io.EOF)
}

func TestHeartbeatReceiverTimesOutOnSilentStream(t *testing.T) {
	stream := newSilentHeartbeatStream(t, &master_pb.Heartbeat{Ip: "10.0.0.1"})
	recv, stop := heartbeatReceiver(stream, 50*time.Millisecond)
	defer stop()

	_, err := recv()
	require.NoError(t, err)
	errc := make(chan error, 1)
	go func() {
		_, err := recv()
		errc <- err
	}()
	select {
	case err = <-errc:
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("recv still waiting on a silent stream")
	}
}

// A volume server that stops sending heartbeats is unregistered once
// volumeHeartbeatTimeout passes, although its stream is still open.
func TestSendHeartbeatUnregistersSilentVolumeServer(t *testing.T) {
	saved := volumeHeartbeatTimeout
	volumeHeartbeatTimeout = 500 * time.Millisecond
	defer func() { volumeHeartbeatTimeout = saved }()

	ms := newLeaderMaster()
	ms.clientChans = make(map[string]chan *master_pb.KeepConnectedResponse)
	_, clientChan := ms.addClient("", "filer", "10.0.0.9:8888")
	stream := newSilentHeartbeatStream(t, &master_pb.Heartbeat{
		Ip:              "10.0.0.1",
		Port:            8080,
		MaxVolumeCounts: map[string]uint32{"": 10},
		Volumes:         []*master_pb.VolumeInformationMessage{{Id: 7, Version: 3}},
	})

	errc := make(chan error, 1)
	go func() { errc <- ms.SendHeartbeat(stream) }()
	require.Eventually(t, func() bool { return ms.Topo.DataNodeExists("", "", "10.0.0.1:8080") },
		5*time.Second, 10*time.Millisecond, "the first heartbeat registers the server")
	select {
	case err := <-errc:
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(10 * time.Second):
		t.Fatal("SendHeartbeat still waiting on a silent stream")
	}
	assert.False(t, ms.Topo.DataNodeExists("", "", "10.0.0.1:8080"))
	for {
		select {
		case message := <-clientChan:
			if location := message.GetVolumeLocation(); len(location.GetDeletedVids()) > 0 {
				assert.Equal(t, []uint32{7}, location.DeletedVids)
				return
			}
		default:
			t.Fatal("clients were not told the silent server's volumes are gone")
		}
	}
}
