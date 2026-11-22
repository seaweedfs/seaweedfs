package erasure_coding

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/raft"
	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fakeEcTeardownServer struct {
	volume_server_pb.UnimplementedVolumeServerServer

	mu        sync.Mutex
	unmounted []uint32
	deleted   []uint32
	noVolume  bool
}

func (f *fakeEcTeardownServer) ReadVolumeFileStatus(_ context.Context, req *volume_server_pb.ReadVolumeFileStatusRequest) (*volume_server_pb.ReadVolumeFileStatusResponse, error) {
	if f.noVolume {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}
	return &volume_server_pb.ReadVolumeFileStatusResponse{VolumeId: req.VolumeId, DatFileSize: 8, IdxFileSize: 16}, nil
}

func (f *fakeEcTeardownServer) VolumeEcShardsUnmount(_ context.Context, req *volume_server_pb.VolumeEcShardsUnmountRequest) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unmounted = append(f.unmounted, req.VolumeId)
	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

func (f *fakeEcTeardownServer) VolumeEcShardsDelete(_ context.Context, req *volume_server_pb.VolumeEcShardsDeleteRequest) (*volume_server_pb.VolumeEcShardsDeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, req.VolumeId)
	return &volume_server_pb.VolumeEcShardsDeleteResponse{FullTeardownDone: true}, nil
}

func (f *fakeEcTeardownServer) calls() (unmounted, deleted []uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint32(nil), f.unmounted...), append([]uint32(nil), f.deleted...)
}

type fakeEcLookupMaster struct {
	master_pb.UnimplementedSeaweedServer

	mu      sync.Mutex
	holders []*master_pb.Location
	err     error
	lookups int
}

func (f *fakeEcLookupMaster) LookupEcVolume(_ context.Context, req *master_pb.LookupEcVolumeRequest) (*master_pb.LookupEcVolumeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	if f.err != nil {
		return nil, f.err
	}
	if len(f.holders) == 0 {
		return &master_pb.LookupEcVolumeResponse{}, fmt.Errorf("ec volume %d not found", req.VolumeId)
	}
	resp := &master_pb.LookupEcVolumeResponse{VolumeId: req.VolumeId}
	for i, holder := range f.holders {
		resp.ShardIdLocations = append(resp.ShardIdLocations, &master_pb.LookupEcVolumeResponse_EcShardIdLocation{
			ShardId:   uint32(i),
			Locations: []*master_pb.Location{holder},
		})
	}
	return resp, nil
}

func (f *fakeEcLookupMaster) lookupCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lookups
}

func listenLoopback(t *testing.T) (net.Listener, int) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	return lis, lis.Addr().(*net.TCPAddr).Port
}

// Equals compares the http part of a server address, so each fake uses its
// gRPC port as its http port too.
func startFakeEcTeardownServer(t *testing.T) (*fakeEcTeardownServer, *master_pb.Location) {
	t.Helper()
	server := &fakeEcTeardownServer{}
	lis, port := listenLoopback(t)
	grpcServer := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(grpcServer, server)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)
	return server, &master_pb.Location{Url: fmt.Sprintf("127.0.0.1:%d", port), GrpcPort: uint32(port)}
}

func startFakeEcLookupMaster(t *testing.T, master *fakeEcLookupMaster) string {
	t.Helper()
	lis, port := listenLoopback(t)
	grpcServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(grpcServer, master)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func newMasterHolderCleanupTask(masterAddresses []string, server string, volumeID uint32, collection, targetNode string) *ErasureCodingTask {
	return &ErasureCodingTask{
		BaseTask:        base.NewBaseTask("master-holder-cleanup", types.TaskTypeErasureCoding),
		server:          server,
		volumeID:        volumeID,
		collection:      collection,
		dataShards:      erasure_coding.DataShardsCount,
		parityShards:    erasure_coding.ParityShardsCount,
		grpcDialOption:  grpc.WithTransportCredentials(insecure.NewCredentials()),
		masterAddresses: masterAddresses,
		targets:         []*worker_pb.TaskTarget{{Node: targetNode, VolumeId: volumeID, ShardIds: []uint32{0}}},
	}
}

func newFakeMasterHolderCleanupTask(target *master_pb.Location, masterAddresses ...string) *ErasureCodingTask {
	node := string(pb.NewServerAddressFromLocation(target))
	return newMasterHolderCleanupTask(masterAddresses, node, 147, "ec", node)
}

// A server that came back after detection and remounted a failed attempt's
// shards is not in the plan; the preflight still has to clear it, or the new
// encode completes beside the old shards.
func TestEnsureCleanEcStartClearsMasterListedHolderOutsidePlan(t *testing.T) {
	target, targetLocation := startFakeEcTeardownServer(t)
	outside, outsideLocation := startFakeEcTeardownServer(t)
	master := &fakeEcLookupMaster{holders: []*master_pb.Location{targetLocation, outsideLocation}}
	task := newFakeMasterHolderCleanupTask(targetLocation, startFakeEcLookupMaster(t, master))

	require.NoError(t, task.ensureCleanEcStart(context.Background()))

	for name, server := range map[string]*fakeEcTeardownServer{"target": target, "outside holder": outside} {
		unmounted, deleted := server.calls()
		require.Equal(t, []uint32{147}, unmounted, name)
		require.Equal(t, []uint32{147}, deleted, name)
	}
	require.Equal(t, 1, master.lookupCount())
}

// A replayed job whose earlier run committed the EC copy and deleted the
// source must not wipe the shards, wherever they moved since.
func TestEnsureCleanEcStartRefusesWhenSourceReplicaIsGone(t *testing.T) {
	source, sourceLocation := startFakeEcTeardownServer(t)
	source.noVolume = true
	relocated, relocatedLocation := startFakeEcTeardownServer(t)
	master := &fakeEcLookupMaster{holders: []*master_pb.Location{relocatedLocation}}
	task := newFakeMasterHolderCleanupTask(sourceLocation, startFakeEcLookupMaster(t, master))

	err := task.ensureCleanEcStart(context.Background())
	require.ErrorContains(t, err, "refusing to clear EC shards")

	for name, server := range map[string]*fakeEcTeardownServer{"source": source, "relocated holder": relocated} {
		unmounted, deleted := server.calls()
		require.Empty(t, unmounted, name)
		require.Empty(t, deleted, name)
	}
	require.Zero(t, master.lookupCount())
}

// Detection may have reached the leader through a later entry of the master
// list, so the cleanup cannot stop at a follower's refusal.
func TestCleanupStaleEcShardsAsksNextMasterAfterFollower(t *testing.T) {
	target, targetLocation := startFakeEcTeardownServer(t)
	outside, outsideLocation := startFakeEcTeardownServer(t)
	follower := &fakeEcLookupMaster{err: raft.NotLeaderError}
	leader := &fakeEcLookupMaster{holders: []*master_pb.Location{outsideLocation}}
	task := newFakeMasterHolderCleanupTask(targetLocation, startFakeEcLookupMaster(t, follower), startFakeEcLookupMaster(t, leader))

	require.NoError(t, task.cleanupStaleEcShards(context.Background()))

	for name, server := range map[string]*fakeEcTeardownServer{"target": target, "outside holder": outside} {
		unmounted, deleted := server.calls()
		require.Equal(t, []uint32{147}, unmounted, name)
		require.Equal(t, []uint32{147}, deleted, name)
	}
	require.Equal(t, 1, follower.lookupCount())
	require.Equal(t, 1, leader.lookupCount())
}

func TestCleanupStaleEcShardsTreatsMasterNotFoundAsNoHolders(t *testing.T) {
	target, targetLocation := startFakeEcTeardownServer(t)
	master := &fakeEcLookupMaster{}
	task := newFakeMasterHolderCleanupTask(targetLocation, startFakeEcLookupMaster(t, master))

	require.NoError(t, task.cleanupStaleEcShards(context.Background()))

	unmounted, deleted := target.calls()
	require.Equal(t, []uint32{147}, unmounted)
	require.Equal(t, []uint32{147}, deleted)
	require.Equal(t, 1, master.lookupCount())
}

// Without the master's answer the cleanup cannot know every holder, so it
// reports a failure, but still clears the planned nodes.
func TestCleanupStaleEcShardsFailsWhenMasterLookupFails(t *testing.T) {
	target, targetLocation := startFakeEcTeardownServer(t)
	master := &fakeEcLookupMaster{err: fmt.Errorf("raft: not leader")}
	task := newFakeMasterHolderCleanupTask(targetLocation, startFakeEcLookupMaster(t, master))

	err := task.cleanupStaleEcShards(context.Background())
	require.ErrorContains(t, err, "lookup EC shard holders")
	require.Equal(t, 1, master.lookupCount())

	unmounted, deleted := target.calls()
	require.Equal(t, []uint32{147}, unmounted)
	require.Equal(t, []uint32{147}, deleted)
}

func TestCleanupStaleEcShardsSkipsMasterLookupWithoutMasterAddress(t *testing.T) {
	target, targetLocation := startFakeEcTeardownServer(t)
	task := newFakeMasterHolderCleanupTask(targetLocation)

	require.NoError(t, task.cleanupStaleEcShards(context.Background()))

	unmounted, _ := target.calls()
	require.Equal(t, []uint32{147}, unmounted)
}

// Real master and volume servers: the leftover shards sit on a server the
// plan does not name, and only the master's view reaches them.
func TestEnsureCleanEcStartClearsHolderOutsidePlanViaMaster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartDualVolumeCluster(t, matrix.P1())
	serverAddress := func(index int) string {
		_, grpcPort, err := net.SplitHostPort(cluster.VolumeGRPCAddress(index))
		require.NoError(t, err)
		return cluster.VolumeAdminAddress(index) + "." + grpcPort
	}
	conn, holderClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer conn.Close()

	const (
		volumeID   = uint32(94785)
		collection = "ec-master-holder"
	)
	framework.AllocateVolume(t, holderClient, volumeID, collection)
	fid := framework.NewFileID(volumeID, 9478500, 0x9478ABCD)
	upResp := framework.UploadBytes(t, framework.NewHTTPClient(), cluster.VolumeAdminURL(1), fid, []byte("payload-for-master-listed-holder"))
	_ = framework.ReadAllAndClose(t, upResp)
	require.Equal(t, http.StatusCreated, upResp.StatusCode)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := holderClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{VolumeId: volumeID, Collection: collection})
	require.NoError(t, err)
	_, err = holderClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{VolumeId: volumeID, Collection: collection, ShardIds: []uint32{0, 8}})
	require.NoError(t, err)

	holder := pb.ServerAddress(serverAddress(1))
	require.Eventually(t, func() bool {
		holders, lookupErr := pluginworker.LookupEcVolumeLocations(ctx, []string{cluster.MasterAddress()}, grpc.WithTransportCredentials(insecure.NewCredentials()), volumeID)
		return lookupErr == nil && slices.ContainsFunc(holders, holder.Equals)
	}, 30*time.Second, 200*time.Millisecond, "master never listed the EC shard holder")

	task := newMasterHolderCleanupTask([]string{cluster.MasterAddress()}, string(holder), volumeID, collection, serverAddress(0))

	require.NoError(t, task.ensureCleanEcStart(ctx))

	_, infoErr := holderClient.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{VolumeId: volumeID})
	require.Error(t, infoErr, "the holder outside the plan must lose its EC shards")
	_, statusErr := holderClient.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	require.NoError(t, statusErr, "the plain replica must survive the cleanup")
}
