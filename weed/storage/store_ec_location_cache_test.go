package storage

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// countingMaster answers LookupEcVolume with one holder per shard and counts
// how often it was asked.
type countingMaster struct {
	master_pb.UnimplementedSeaweedServer
	lookups atomic.Int64
	// onLookup runs inside the RPC, standing in for whatever else the volume
	// server is doing while it waits on the master.
	onLookup func()
	// failLookup answers with an error instead of a shard map.
	failLookup atomic.Bool
	// located, when set, answers every shard slot as the master does but
	// places only the first located shards on a server.
	located atomic.Int64
	// partialPort is the port of the server those shards are placed on; 0 is 1.
	partialPort atomic.Int64
}

func partialLocation(port int64) *master_pb.Location {
	if port == 0 {
		port = 1
	}
	return &master_pb.Location{Url: fmt.Sprintf("127.0.0.1:%d", port), GrpcPort: 2}
}

func partialHolder(port int64) pb.ServerAddress {
	return pb.NewServerAddressFromLocation(partialLocation(port))
}

func (m *countingMaster) LookupEcVolume(_ context.Context, req *master_pb.LookupEcVolumeRequest) (*master_pb.LookupEcVolumeResponse, error) {
	m.lookups.Add(1)
	if m.onLookup != nil {
		m.onLookup()
	}
	if m.failLookup.Load() {
		return nil, status.Error(codes.Unavailable, "master is having a moment")
	}
	resp := &master_pb.LookupEcVolumeResponse{VolumeId: req.VolumeId}
	if located := int(m.located.Load()); located > 0 {
		for shardId := 0; shardId < erasure_coding.MaxShardCount; shardId++ {
			entry := &master_pb.LookupEcVolumeResponse_EcShardIdLocation{ShardId: uint32(shardId)}
			if shardId < located {
				entry.Locations = []*master_pb.Location{partialLocation(m.partialPort.Load())}
			}
			resp.ShardIdLocations = append(resp.ShardIdLocations, entry)
		}
		return resp, nil
	}
	for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
		resp.ShardIdLocations = append(resp.ShardIdLocations, &master_pb.LookupEcVolumeResponse_EcShardIdLocation{
			ShardId:   uint32(shardId),
			Locations: []*master_pb.Location{{Url: "127.0.0.1:1", GrpcPort: 2}},
		})
	}
	return resp, nil
}

func startCountingMaster(t *testing.T) (*countingMaster, pb.ServerAddress) {
	t.Helper()
	master := &countingMaster{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	master_pb.RegisterSeaweedServer(srv, master)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return master, pb.NewServerAddressWithGrpcPort("127.0.0.1:1", lis.Addr().(*net.TCPAddr).Port)
}

// rewindShardLocationsRefresh ages the cache by d so a test can reach a TTL
// without waiting one out.
func rewindShardLocationsRefresh(ecVolume *erasure_coding.EcVolume, d time.Duration) {
	ecVolume.ShardLocationsLock.Lock()
	ecVolume.ShardLocationsRefreshTime = ecVolume.ShardLocationsRefreshTime.Add(-d)
	ecVolume.ShardLocationsLock.Unlock()
}

// A read that fails against a cached location leaves the map claiming something
// it has just disproved, so the next lookup goes back to the master within
// seconds rather than serving the hole for the rest of a 7-minute window.
func TestCachedLookupEcShardLocationsRefreshesAfterAFailedRead(t *testing.T) {
	master, masterAddr := startCountingMaster(t)
	store := &Store{
		MasterFn:       func() pb.ServerAddress { return masterAddr },
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ecVolume := &erasure_coding.EcVolume{
		VolumeId:       7,
		ShardLocations: make(map[erasure_coding.ShardId][]pb.ServerAddress),
	}

	if err := store.cachedLookupEcShardLocations(ecVolume); err != nil {
		t.Fatalf("first lookup: %v", err)
	}
	if got := master.lookups.Load(); got != 1 {
		t.Fatalf("cold cache asked the master %d times, want 1", got)
	}

	// A map nothing has disproved is trusted for minutes, whether or not it is
	// complete -- dropping a shard by hand is not the same as a read failing.
	rewindShardLocationsRefresh(ecVolume, 12*time.Second)
	ecVolume.ShardLocationsLock.Lock()
	delete(ecVolume.ShardLocations, erasure_coding.ShardId(2))
	ecVolume.ShardLocationsLock.Unlock()
	if err := store.cachedLookupEcShardLocations(ecVolume); err != nil {
		t.Fatalf("second lookup: %v", err)
	}
	if got := master.lookups.Load(); got != 1 {
		t.Errorf("a 12s-old map asked the master %d times, want it left alone", got)
	}

	forgetShardId(ecVolume, erasure_coding.ShardId(3))
	if err := store.cachedLookupEcShardLocations(ecVolume); err != nil {
		t.Fatalf("lookup after a failed read: %v", err)
	}
	if got := master.lookups.Load(); got != 2 {
		t.Errorf("a map disproved by a failed read asked the master %d times, want 2", got)
	}

	ecVolume.ShardLocationsLock.RLock()
	_, restored := ecVolume.ShardLocations[erasure_coding.ShardId(3)]
	ecVolume.ShardLocationsLock.RUnlock()
	if !restored {
		t.Error("shard 3 is still missing from the map after the refresh")
	}
}

// A failed read buys one prompt re-check, not a lookup on every read that
// follows: the refresh clears the mark and the map is trusted again.
func TestCachedLookupEcShardLocationsSettlesAfterRefresh(t *testing.T) {
	master, masterAddr := startCountingMaster(t)
	store := &Store{
		MasterFn:       func() pb.ServerAddress { return masterAddr },
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ecVolume := &erasure_coding.EcVolume{
		VolumeId:       8,
		ShardLocations: make(map[erasure_coding.ShardId][]pb.ServerAddress),
	}
	if err := store.cachedLookupEcShardLocations(ecVolume); err != nil {
		t.Fatalf("first lookup: %v", err)
	}
	rewindShardLocationsRefresh(ecVolume, 12*time.Second)
	forgetShardId(ecVolume, erasure_coding.ShardId(3))
	if err := store.cachedLookupEcShardLocations(ecVolume); err != nil {
		t.Fatalf("lookup after a failed read: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := store.cachedLookupEcShardLocations(ecVolume); err != nil {
			t.Fatalf("settled lookup: %v", err)
		}
	}
	if got := master.lookups.Load(); got != 2 {
		t.Errorf("asked the master %d times, want 2: one cold, one after the failed read", got)
	}
}

// The master answers every shard slot, so a reply naming 32 shards can still
// place fewer than the data shards on a server, as while several holders are
// down. That reply is a partial and must leave a complete cache as it was.
func TestCachedLookupEcShardLocationsRefusesAReplyWithFewLocatedShards(t *testing.T) {
	master, masterAddr := startCountingMaster(t)
	store := &Store{
		MasterFn:       func() pb.ServerAddress { return masterAddr },
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ecVolume := &erasure_coding.EcVolume{
		VolumeId:       9,
		ShardLocations: make(map[erasure_coding.ShardId][]pb.ServerAddress),
	}
	if err := store.cachedLookupEcShardLocations(ecVolume); err != nil {
		t.Fatalf("first lookup: %v", err)
	}

	master.located.Store(7)
	master.partialPort.Store(3)
	rewindShardLocationsRefresh(ecVolume, 12*time.Second)
	ecVolume.ShardLocationsLock.Lock()
	ecVolume.ShardLocationsStale = true
	refreshedAt := ecVolume.ShardLocationsRefreshTime
	ecVolume.ShardLocationsLock.Unlock()
	if err := store.cachedLookupEcShardLocations(ecVolume); err == nil {
		t.Error("a reply placing 7 of 14 shards was accepted")
	}
	ecVolume.ShardLocationsLock.RLock()
	cached := len(ecVolume.ShardLocations)
	named := ecVolume.ShardLocations[0]
	unnamed := ecVolume.ShardLocations[7]
	refreshed := ecVolume.ShardLocationsRefreshTime
	ecVolume.ShardLocationsLock.RUnlock()
	if cached != erasure_coding.TotalShardsCount {
		t.Errorf("the cache holds %d shards after the partial reply, want all %d kept", cached, erasure_coding.TotalShardsCount)
	}
	if want := []pb.ServerAddress{partialHolder(3), partialHolder(1)}; !slices.Equal(named, want) {
		t.Errorf("a shard the partial reply places holds %v, want its new holder first and the cached one kept: %v", named, want)
	}
	if want := []pb.ServerAddress{partialHolder(1)}; !slices.Equal(unnamed, want) {
		t.Errorf("a shard the partial reply places nowhere holds %v, want its cached %v", unnamed, want)
	}
	if !refreshed.Equal(refreshedAt) {
		t.Errorf("the partial reply moved the refresh time from %v to %v", refreshedAt, refreshed)
	}
}

// A partial reply is no map to trust, but the cache takes the holders it names,
// so a read can still go straight to a shard whose holder is up, including one
// that moved while the replies stayed partial.
func TestCachedLookupEcShardLocationsFillsAColdCacheFromAPartialReply(t *testing.T) {
	master, masterAddr := startCountingMaster(t)
	master.located.Store(7)
	store := &Store{
		MasterFn:       func() pb.ServerAddress { return masterAddr },
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ecVolume := &erasure_coding.EcVolume{
		VolumeId:       9,
		ShardLocations: make(map[erasure_coding.ShardId][]pb.ServerAddress),
	}
	if err := store.cachedLookupEcShardLocations(ecVolume); err == nil {
		t.Error("a reply placing 7 of 14 shards was accepted")
	}
	ecVolume.ShardLocationsLock.RLock()
	cached := len(ecVolume.ShardLocations)
	refreshed := !ecVolume.ShardLocationsRefreshTime.IsZero()
	stale := ecVolume.ShardLocationsStale
	ecVolume.ShardLocationsLock.RUnlock()
	if cached != 7 {
		t.Errorf("the cold cache holds %d shards after the partial reply, want the 7 it placed", cached)
	}
	if refreshed || !stale {
		t.Errorf("a partial reply must leave the map due a lookup: refreshed=%v stale=%v", refreshed, stale)
	}

	master.partialPort.Store(4)
	if err := store.cachedLookupEcShardLocations(ecVolume); err == nil {
		t.Error("a second reply placing 7 of 14 shards was accepted")
	}
	if got := master.lookups.Load(); got != 2 {
		t.Fatalf("asked the master %d times, want 2: a partial reply leaves the map due", got)
	}
	ecVolume.ShardLocationsLock.RLock()
	moved := ecVolume.ShardLocations[0]
	ecVolume.ShardLocationsLock.RUnlock()
	if want := []pb.ServerAddress{partialHolder(4), partialHolder(1)}; !slices.Equal(moved, want) {
		t.Errorf("after the shard moved, the cache holds %v, want %v", moved, want)
	}
}

func TestEcShardLocationsTTL(t *testing.T) {
	ecCtx := erasure_coding.NewDefaultECContext("", 1)
	for _, tc := range []struct {
		name       string
		shardCount int
		stale      bool
		want       time.Duration
	}{
		{"complete", ecCtx.Total(), false, 37 * time.Minute},
		{"one shard short", ecCtx.Total() - 1, false, 7 * time.Minute},
		{"below the data shards", ecCtx.DataShards - 1, false, 11 * time.Second},
		{"complete but disproved by a read", ecCtx.Total(), true, 11 * time.Second},
		{"one shard short and disproved", ecCtx.Total() - 1, true, 11 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ecShardLocationsTTL(tc.shardCount, tc.stale, ecCtx); got != tc.want {
				t.Errorf("ttl = %v, want %v", got, tc.want)
			}
		})
	}
}

// oneBadShardPeer serves every shard out of an EC volume's files except one,
// standing in for a shard that has moved off the address the map still names.
type oneBadShardPeer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	baseFileName string
	badShard     erasure_coding.ShardId
}

func (p *oneBadShardPeer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {
	if erasure_coding.ShardId(req.ShardId) == p.badShard {
		return status.Errorf(codes.NotFound, "shard %d is not here", req.ShardId)
	}
	f, err := os.Open(p.baseFileName + erasure_coding.ToExt(int(req.ShardId)))
	if err != nil {
		return err
	}
	defer f.Close()
	data := make([]byte, req.Size)
	if _, err := f.ReadAt(data, req.Offset); err != nil {
		return err
	}
	return stream.Send(&volume_server_pb.VolumeEcShardReadResponse{Data: data, EncodeTsNs: req.EncodeTsNs})
}

// Recovery skips the shard whose direct read failed, so that failure is the one
// thing that never invalidates the map. The read still succeeds off the other
// shards, and the location it just disproved has to be re-checked anyway.
func TestReadOneEcShardIntervalMarksTheMapAfterADirectReadFails(t *testing.T) {
	const badShard = erasure_coding.ShardId(3)
	baseFileName, n := writeEcVolumeFiles(t, t.TempDir(), 7)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(srv, &oneBadShardPeer{baseFileName: baseFileName, badShard: badShard})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	store := &Store{grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials())}
	ecVolume := &erasure_coding.EcVolume{
		VolumeId:       7,
		ShardLocations: make(map[erasure_coding.ShardId][]pb.ServerAddress),
	}
	seedShardLocations(ecVolume, pb.NewServerAddressWithGrpcPort("127.0.0.1:1", lis.Addr().(*net.TCPAddr).Port))

	interval := erasure_coding.Interval{BlockIndex: int(badShard), Size: 4096, IsLargeBlock: true}
	got := make([]byte, interval.Size)
	if _, err := store.readOneEcShardInterval(n.Id, ecVolume, interval, got); err != nil {
		t.Fatalf("read interval: %v", err)
	}

	ecVolume.ShardLocationsLock.RLock()
	stale := ecVolume.ShardLocationsStale
	_, stillMapped := ecVolume.ShardLocations[badShard]
	ecVolume.ShardLocationsLock.RUnlock()
	if !stale {
		t.Error("a failed direct read left the map trusted, so the moved shard stays hidden until the window expires")
	}
	if !stillMapped {
		t.Error("shard 3 was dropped from the map; the direct read is worth retrying")
	}

	want := make([]byte, len(got))
	f, err := os.Open(baseFileName + erasure_coding.ToExt(int(badShard)))
	if err != nil {
		t.Fatalf("open shard %d: %v", badShard, err)
	}
	defer f.Close()
	if _, err := f.ReadAt(want, 0); err != nil {
		t.Fatalf("read shard %d: %v", badShard, err)
	}
	if !bytes.Equal(got, want) {
		t.Error("the interval recovered from the other shards does not match the shard on disk")
	}
}

// A read that fails while the master is answering has disproved the very map
// that answer is about to install, so the refresh must not clear its mark.
func TestCachedLookupEcShardLocationsKeepsAMarkRaisedDuringTheLookup(t *testing.T) {
	master, masterAddr := startCountingMaster(t)
	store := &Store{
		MasterFn:       func() pb.ServerAddress { return masterAddr },
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ecVolume := &erasure_coding.EcVolume{
		VolumeId:       9,
		ShardLocations: make(map[erasure_coding.ShardId][]pb.ServerAddress),
	}

	master.onLookup = func() { markShardLocationsStale(ecVolume) }
	if err := store.cachedLookupEcShardLocations(ecVolume); err != nil {
		t.Fatalf("first lookup: %v", err)
	}
	master.onLookup = nil

	ecVolume.ShardLocationsLock.RLock()
	stale := ecVolume.ShardLocationsStale
	ecVolume.ShardLocationsLock.RUnlock()
	if !stale {
		t.Fatal("the refresh swallowed a mark raised while it was in flight")
	}

	// And the mark shortens the window, so the next read re-checks in seconds
	// rather than trusting a map already disproved.
	rewindShardLocationsRefresh(ecVolume, 12*time.Second)
	if err := store.cachedLookupEcShardLocations(ecVolume); err != nil {
		t.Fatalf("lookup after the swallowed mark: %v", err)
	}
	if got := master.lookups.Load(); got != 2 {
		t.Errorf("asked the master %d times, want 2", got)
	}
}

// A refresh that never answered cannot be treated as one that did: the mark it
// consumed goes back, or the map it was meant to correct is trusted for its
// whole window on the strength of a lookup that failed.
func TestCachedLookupEcShardLocationsKeepsTheMarkWhenTheLookupFails(t *testing.T) {
	master, masterAddr := startCountingMaster(t)
	store := &Store{
		MasterFn:       func() pb.ServerAddress { return masterAddr },
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ecVolume := &erasure_coding.EcVolume{
		VolumeId:       10,
		ShardLocations: make(map[erasure_coding.ShardId][]pb.ServerAddress),
	}
	if err := store.cachedLookupEcShardLocations(ecVolume); err != nil {
		t.Fatalf("first lookup: %v", err)
	}

	rewindShardLocationsRefresh(ecVolume, 12*time.Second)
	markShardLocationsStale(ecVolume)
	master.failLookup.Store(true)
	if err := store.cachedLookupEcShardLocations(ecVolume); err == nil {
		t.Fatal("expected the lookup to fail")
	}

	ecVolume.ShardLocationsLock.RLock()
	stale := ecVolume.ShardLocationsStale
	ecVolume.ShardLocationsLock.RUnlock()
	if !stale {
		t.Error("the failed lookup kept the mark it consumed")
	}

	master.failLookup.Store(false)
	if err := store.cachedLookupEcShardLocations(ecVolume); err != nil {
		t.Fatalf("retry after the failed lookup: %v", err)
	}
	if got := master.lookups.Load(); got != 3 {
		t.Errorf("asked the master %d times, want 3: a failed lookup must not leave the disproved map trusted", got)
	}
}
