package dash

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type volumeAccessTestMaster struct {
	master_pb.UnimplementedSeaweedServer
	topology *master_pb.TopologyInfo
}

type volumeAccessTestServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	rpcError error
	calls    chan any
}

func (s *volumeAccessTestMaster) KeepConnected(stream master_pb.Seaweed_KeepConnectedServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(&master_pb.KeepConnectedResponse{}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *volumeAccessTestMaster) VolumeList(context.Context, *master_pb.VolumeListRequest) (*master_pb.VolumeListResponse, error) {
	// Return unfiltered topology to exercise compatibility with older masters.
	return &master_pb.VolumeListResponse{TopologyInfo: s.topology}, nil
}

func (s *volumeAccessTestServer) VolumeMarkReadonly(_ context.Context, req *volume_server_pb.VolumeMarkReadonlyRequest) (*volume_server_pb.VolumeMarkReadonlyResponse, error) {
	s.calls <- req
	return &volume_server_pb.VolumeMarkReadonlyResponse{}, s.rpcError
}

func (s *volumeAccessTestServer) VolumeMarkWritable(_ context.Context, req *volume_server_pb.VolumeMarkWritableRequest) (*volume_server_pb.VolumeMarkWritableResponse, error) {
	s.calls <- req
	return &volume_server_pb.VolumeMarkWritableResponse{}, s.rpcError
}

func volumeAccessTestTopology(nodes ...*master_pb.DataNodeInfo) *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{DataCenterInfos: []*master_pb.DataCenterInfo{{
		RackInfos: []*master_pb.RackInfo{{DataNodeInfos: nodes}},
	}}}
}

func TestSetVolumeReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name     string
		readOnly bool
		volumeID uint32
		server   string
		rpcError error
		wantCall bool
	}{
		{name: "persist read-only", readOnly: true, volumeID: 7, server: "node-a", wantCall: true},
		{name: "restore read-write", volumeID: 7, server: "node-a", wantCall: true},
		{name: "propagate volume server error", volumeID: 7, server: "node-a", wantCall: true, rpcError: status.Error(codes.PermissionDenied, "server is in maintenance mode")},
		{name: "reject unknown volume", readOnly: true, volumeID: 8, server: "node-a"},
		{name: "reject unknown server", readOnly: true, volumeID: 7, server: "127.0.0.1:9999"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			port := listener.Addr().(*net.TCPAddr).Port
			master := &volumeAccessTestMaster{
				topology: volumeAccessTestTopology(&master_pb.DataNodeInfo{
					Id: "node-a", Address: "127.0.0.1:8080", GrpcPort: uint32(port),
					DiskInfos: map[string]*master_pb.DiskInfo{"hdd": {
						VolumeInfos: []*master_pb.VolumeInformationMessage{{Id: 7}},
					}},
				}),
			}
			fake := &volumeAccessTestServer{
				rpcError: tc.rpcError,
				calls:    make(chan any, 1),
			}
			grpcServer := grpc.NewServer()
			master_pb.RegisterSeaweedServer(grpcServer, master)
			volume_server_pb.RegisterVolumeServerServer(grpcServer, fake)
			go grpcServer.Serve(listener)
			t.Cleanup(grpcServer.Stop)

			dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
			address := pb.NewServerAddress("127.0.0.1", 9333, port)
			discovery := pb.NewServiceDiscoveryFromMap(map[string]pb.ServerAddress{"master": address})
			masterClient := wdclient.NewMasterClient(dialOption, "", "admin", "", "", "", *discovery)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			go masterClient.KeepConnectedToMaster(ctx)
			admin := &AdminServer{masterClient: masterClient, grpcDialOption: dialOption}

			err = admin.SetVolumeReadOnly(ctx, tc.volumeID, tc.server, tc.readOnly)
			if !tc.wantCall {
				require.ErrorContains(t, err, "not found on server")
				require.Empty(t, fake.calls, "an unknown replica must not receive a mutation")
				return
			}
			if tc.rpcError != nil {
				require.Equal(t, status.Code(tc.rpcError), status.Code(err))
			} else {
				require.NoError(t, err)
			}
			select {
			case call := <-fake.calls:
				if tc.readOnly {
					req, ok := call.(*volume_server_pb.VolumeMarkReadonlyRequest)
					require.True(t, ok)
					require.Equal(t, tc.volumeID, req.VolumeId)
					require.True(t, req.Persist, "operator changes must survive a restart")
					require.False(t, req.CanDelete)
				} else {
					req, ok := call.(*volume_server_pb.VolumeMarkWritableRequest)
					require.True(t, ok)
					require.Equal(t, tc.volumeID, req.VolumeId)
				}
			default:
				t.Fatal("volume server did not receive an access mode change")
			}
		})
	}
}

func TestVolumeReplicaAddress(t *testing.T) {
	topology := volumeAccessTestTopology(
		&master_pb.DataNodeInfo{Id: "legacy:8080", DiskInfos: map[string]*master_pb.DiskInfo{
			"hdd": {VolumeInfos: []*master_pb.VolumeInformationMessage{{Id: 7}}},
		}},
		&master_pb.DataNodeInfo{Id: "node-b", Address: "host-b:8081", GrpcPort: 18082, DiskInfos: map[string]*master_pb.DiskInfo{
			"ssd": {VolumeInfos: []*master_pb.VolumeInformationMessage{{Id: 7}}, EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{Id: 8}}},
		}},
	)
	address, err := volumeReplicaAddress(topology, 7, "legacy:8080")
	require.NoError(t, err)
	require.Equal(t, "legacy:18080", address.ToGrpcAddress())
	address, err = volumeReplicaAddress(topology, 7, "node-b")
	require.NoError(t, err)
	require.Equal(t, "host-b:18082", address.ToGrpcAddress())
	_, err = volumeReplicaAddress(topology, 8, "node-b")
	require.Error(t, err, "EC shards must not be treated as regular volumes")
	_, err = volumeReplicaAddress(nil, 7, "node-b")
	require.Error(t, err)
}
