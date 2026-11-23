package storage

import (
	"bytes"
	"context"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// countingEcShardPeer stands in for a peer volume server that is having a
// transient problem: every VolumeEcShardRead is counted and then held until the
// test releases it, so a stalled fan-out is observable while it is stalled.
type countingEcShardPeer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	requests    atomic.Int64
	inFlight    atomic.Int64
	peak        atomic.Int64
	release     chan struct{}
	releaseOnce sync.Once
}

func (p *countingEcShardPeer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {
	p.requests.Add(1)
	inFlight := p.inFlight.Add(1)
	for peak := p.peak.Load(); inFlight > peak; peak = p.peak.Load() {
		if p.peak.CompareAndSwap(peak, inFlight) {
			break
		}
	}
	defer p.inFlight.Add(-1)
	<-p.release
	return status.Error(codes.Unavailable, "transient failure")
}

func (p *countingEcShardPeer) releaseAll() {
	p.releaseOnce.Do(func() { close(p.release) })
}

func startCountingEcShardPeer(t *testing.T) (*countingEcShardPeer, pb.ServerAddress) {
	t.Helper()
	peer := &countingEcShardPeer{release: make(chan struct{})}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(srv, peer)
	go srv.Serve(lis)
	t.Cleanup(func() {
		peer.releaseAll()
		srv.Stop()
	})
	return peer, pb.NewServerAddressWithGrpcPort("127.0.0.1:1", lis.Addr().(*net.TCPAddr).Port)
}

// writeEcVolumeFiles writes one needle into a volume in dir and EC-encodes it in
// place, leaving the .ec?? / .ecx / .vif set behind.
func writeEcVolumeFiles(t *testing.T, dir string, vid needle.VolumeId) (baseFileName string, n *needle.Needle) {
	t.Helper()

	v, err := NewVolume(dir, dir, "", vid, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("new volume: %v", err)
	}
	n = new(needle.Needle)
	n.Id = types.Uint64ToNeedleId(42)
	n.Data = make([]byte, 3*erasure_coding.ErasureCodingSmallBlockSize+1234)
	rand.New(rand.NewSource(42)).Read(n.Data)
	n.Checksum = needle.NewCRC(n.Data)
	if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
		t.Fatalf("write needle: %v", err)
	}
	baseFileName = v.DataFileName()
	v.Close()

	datSize, err := os.Stat(baseFileName + ".dat")
	if err != nil {
		t.Fatalf("stat .dat: %v", err)
	}
	ecCtx := erasure_coding.BackgroundECContext()
	if _, err := erasure_coding.WriteEcFiles(baseFileName, ecCtx); err != nil {
		t.Fatalf("write ec files: %v", err)
	}
	if err := erasure_coding.WriteSortedFileFromIdx(baseFileName, ".ecx"); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := os.WriteFile(baseFileName+".ecj", nil, 0o644); err != nil {
		t.Fatalf("write .ecj: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(baseFileName+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.GetCurrentVersion()),
		DatFileSize: datSize.Size(),
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   erasure_coding.DataShardsCount,
			ParityShards: erasure_coding.ParityShardsCount,
			// The encode writes the uniform layout; a .vif that omitted its
			// block size would mount these shards as legacy and read every
			// interval at the wrong offset.
			BlockSize: ecCtx.BlockSize,
		},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}
	for _, ext := range []string{".dat", ".idx"} {
		if err := os.Remove(baseFileName + ext); err != nil {
			t.Fatalf("remove %s: %v", ext, err)
		}
	}
	return baseFileName, n
}

// mountLocalEcVolume writes an EC volume into the store's own disk and mounts
// every shard, so a recovery can be served entirely from local shards.
func mountLocalEcVolume(t *testing.T, store *Store, vid needle.VolumeId) (*erasure_coding.EcVolume, *needle.Needle) {
	t.Helper()
	_, n := writeEcVolumeFiles(t, store.Locations[0].Directory, vid)
	for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
		if err := store.MountEcShards("", vid, erasure_coding.ShardId(shardId), ""); err != nil {
			t.Fatalf("mount shard %d: %v", shardId, err)
		}
	}
	ecVolume, found := store.Locations[0].FindEcVolume(vid)
	if !found {
		t.Fatal("ec volume not mounted")
	}
	return ecVolume, n
}

// seedShardLocations points every shard of the volume at one address and marks
// the cache fresh, so a read never reaches for the master this test does not run.
func seedShardLocations(ecVolume *erasure_coding.EcVolume, addr pb.ServerAddress) {
	ecVolume.ShardLocationsLock.Lock()
	for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
		ecVolume.ShardLocations[erasure_coding.ShardId(shardId)] = []pb.ServerAddress{addr}
	}
	ecVolume.ShardLocationsRefreshTime = time.Now()
	ecVolume.ShardLocationsLock.Unlock()
}

// Recovery reconstructs a shard from the shards this server already holds, so a
// store holding the whole volume never asks a peer for one.
func TestRecoverOneRemoteEcShardIntervalUsesLocalShards(t *testing.T) {
	store := newTestStore(t, 1)
	peer, addr := startCountingEcShardPeer(t)
	ecVolume, n := mountLocalEcVolume(t, store, 7)
	seedShardLocations(ecVolume, addr)

	_, _, intervals, err := ecVolume.LocateEcShardNeedle(n.Id, ecVolume.Version)
	if err != nil {
		t.Fatalf("locate needle: %v", err)
	}
	shardIdToRecover, actualOffset := ecVolume.IntervalToShardIdAndOffset(intervals[0])

	got := make([]byte, intervals[0].Size)
	nRead, _, err := store.recoverOneRemoteEcShardInterval(n.Id, ecVolume, shardIdToRecover, got, actualOffset)
	if err != nil {
		t.Fatalf("recover ec shard interval: %v", err)
	}
	if nRead != len(got) {
		t.Fatalf("recovered %d bytes, want %d", nRead, len(got))
	}
	if requests := peer.requests.Load(); requests != 0 {
		t.Errorf("recovery asked peers for %d shard intervals, want 0 with every shard local", requests)
	}

	want := make([]byte, len(got))
	if err := store.readLocalEcShardInterval(ecVolume, shardIdToRecover, want, actualOffset, false); err != nil {
		t.Fatalf("read local ec shard %d: %v", shardIdToRecover, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("recovered shard %d bytes differ from the shard on disk", shardIdToRecover)
	}
}

// shardServingPeer answers VolumeEcShardRead out of an EC volume's shard files
// and counts how many shard intervals the caller asked for.
type shardServingPeer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	baseFileName string
	requests     atomic.Int64
}

func (p *shardServingPeer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {
	p.requests.Add(1)
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

// Reed-Solomon needs DataShards shards, so the fan-out stops there instead of
// pulling every surviving shard off peers that may already be struggling.
func TestRecoverOneRemoteEcShardIntervalFetchesOnlyWhatItNeeds(t *testing.T) {
	baseFileName, n := writeEcVolumeFiles(t, t.TempDir(), 7)

	peer := &shardServingPeer{baseFileName: baseFileName}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(srv, peer)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	addr := pb.NewServerAddressWithGrpcPort("127.0.0.1:1", lis.Addr().(*net.TCPAddr).Port)

	store := &Store{grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials())}
	ecVolume := &erasure_coding.EcVolume{
		VolumeId:       7,
		ShardLocations: make(map[erasure_coding.ShardId][]pb.ServerAddress),
	}
	seedShardLocations(ecVolume, addr)

	const shardIdToRecover = erasure_coding.ShardId(0)
	const offset = 0
	got := make([]byte, 4096)
	if _, _, err := store.recoverOneRemoteEcShardInterval(n.Id, ecVolume, shardIdToRecover, got, offset); err != nil {
		t.Fatalf("recover ec shard interval: %v", err)
	}
	if requests := peer.requests.Load(); requests != int64(erasure_coding.DataShardsCount) {
		t.Errorf("recovery asked peers for %d shard intervals, want %d", requests, erasure_coding.DataShardsCount)
	}

	want := make([]byte, len(got))
	f, err := os.Open(baseFileName + erasure_coding.ToExt(int(shardIdToRecover)))
	if err != nil {
		t.Fatalf("open shard %d: %v", shardIdToRecover, err)
	}
	defer f.Close()
	if _, err := f.ReadAt(want, offset); err != nil {
		t.Fatalf("read shard %d: %v", shardIdToRecover, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("recovered shard %d bytes differ from the shard on disk", shardIdToRecover)
	}
}

// setEcRecoverBudget narrows the process-wide recovery budget for one test.
func setEcRecoverBudget(t *testing.T, budget int64) {
	t.Helper()
	oldBudget, oldSem := ecRecoverBudget, ecRecoverSem
	ecRecoverBudget, ecRecoverSem = budget, semaphore.NewWeighted(budget)
	t.Cleanup(func() { ecRecoverBudget, ecRecoverSem = oldBudget, oldSem })
}

// A burst of reads against a peer that has stopped answering queues on the
// recovery budget instead of piling interval-sized buffers onto the heap.
func TestRecoverOneRemoteEcShardIntervalBoundsInFlightBytes(t *testing.T) {
	const intervalSize = 1 << 20
	const readers = 8
	const budget = 24 << 20

	setEcRecoverBudget(t, budget)
	peer, addr := startCountingEcShardPeer(t)

	store := &Store{grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials())}
	ecVolume := &erasure_coding.EcVolume{
		VolumeId:       7,
		ShardLocations: make(map[erasure_coding.ShardId][]pb.ServerAddress),
	}
	seedShardLocations(ecVolume, addr)

	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.recoverOneRemoteEcShardInterval(types.Uint64ToNeedleId(42), ecVolume, 0, make([]byte, intervalSize), 0)
		}()
	}

	// let the fan-out reach the peer and settle there
	for deadline, settled, last := time.Now().Add(10*time.Second), 0, int64(-1); time.Now().Before(deadline) && settled < 5; {
		time.Sleep(50 * time.Millisecond)
		inFlight := peer.inFlight.Load()
		if inFlight > 0 && inFlight == last {
			settled++
		} else {
			settled = 0
		}
		last = inFlight
	}

	if peak, allowed := peer.peak.Load(), int64(budget/intervalSize); peak > allowed {
		t.Errorf("%d shard reads of %dMB in flight at once, more than the %dMB budget allows", peak, intervalSize>>20, budget>>20)
	}

	peer.releaseAll()
	wg.Wait()
}

// deletingEcShardPeer answers every shard read with the needle's deletion.
type deletingEcShardPeer struct {
	volume_server_pb.UnimplementedVolumeServerServer
}

func (p *deletingEcShardPeer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {
	return stream.Send(&volume_server_pb.VolumeEcShardReadResponse{IsDeleted: true, EncodeTsNs: req.EncodeTsNs})
}

// A needle the peers report deleted reads as deleted -- a 404 -- even when too
// few shards came back to reconstruct it.
func TestReadEcShardNeedleAnswersDeletedWhenRecoveryFallsShort(t *testing.T) {
	store := newTestStore(t, 1)
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	const vid = needle.VolumeId(7)
	_, n := writeEcVolumeFiles(t, store.Locations[0].Directory, vid)

	// Too few local shards to reconstruct, and none for the shard the needle's
	// first interval lands on, so that interval has to recover from peers.
	for shardId := 1; shardId <= 5; shardId++ {
		if err := store.MountEcShards("", vid, erasure_coding.ShardId(shardId), ""); err != nil {
			t.Fatalf("mount shard %d: %v", shardId, err)
		}
	}
	ecVolume, found := store.Locations[0].FindEcVolume(vid)
	if !found {
		t.Fatal("ec volume not mounted")
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(srv, &deletingEcShardPeer{})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	seedShardLocations(ecVolume, pb.NewServerAddressWithGrpcPort("127.0.0.1:1", lis.Addr().(*net.TCPAddr).Port))
	ecVolume.ShardLocationsLock.Lock()
	delete(ecVolume.ShardLocations, erasure_coding.ShardId(0))
	ecVolume.ShardLocationsLock.Unlock()

	got := new(needle.Needle)
	got.Id = n.Id
	if _, err := store.ReadEcShardNeedle(vid, got, nil); err != ErrorDeleted {
		t.Errorf("read a deleted needle returned %v, want %v", err, ErrorDeleted)
	}
}

// partlyDeletedEcShardPeer reports the needle deleted on its shard reads and
// counts the deletes forwarded to it.
type partlyDeletedEcShardPeer struct {
	deletingEcShardPeer
	deletes atomic.Int64
	fileKey atomic.Uint64
}

func (p *partlyDeletedEcShardPeer) VolumeEcBlobDelete(ctx context.Context, req *volume_server_pb.VolumeEcBlobDeleteRequest) (*volume_server_pb.VolumeEcBlobDeleteResponse, error) {
	p.deletes.Add(1)
	p.fileKey.Store(req.FileKey)
	return &volume_server_pb.VolumeEcBlobDeleteResponse{}, nil
}

// A holder whose delete stopped part way reports the needle deleted from the
// disk that recorded it, so a retry still forwards the delete for the rest.
func TestDeleteEcShardNeedleForwardsWhenReadSaysDeleted(t *testing.T) {
	store := newTestStore(t, 1)
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	const vid = needle.VolumeId(8)
	_, n := writeEcVolumeFiles(t, store.Locations[0].Directory, vid)
	for shardId := 1; shardId < erasure_coding.TotalShardsCount; shardId++ {
		if err := store.MountEcShards("", vid, erasure_coding.ShardId(shardId), ""); err != nil {
			t.Fatalf("mount shard %d: %v", shardId, err)
		}
	}
	ecVolume, found := store.Locations[0].FindEcVolume(vid)
	if !found {
		t.Fatal("ec volume not mounted")
	}

	peer := &partlyDeletedEcShardPeer{}
	seedShardLocations(ecVolume, serveEcShardPeer(t, peer))

	got := new(needle.Needle)
	got.Id = n.Id
	if _, err := store.DeleteEcShardNeedle(ecVolume, got, n.Cookie); err != ErrorDeleted {
		t.Fatalf("delete of a needle read as deleted returned %v, want %v", err, ErrorDeleted)
	}
	if peer.deletes.Load() != 1 {
		t.Errorf("forwarded %d deletes to the holder, want 1", peer.deletes.Load())
	}
	if got := types.NeedleId(peer.fileKey.Load()); got != n.Id {
		t.Errorf("forwarded the delete of needle %s, want %s", got, n.Id)
	}
}

// unreadableEcShardPeer fails its shard reads, as a quarantined copy does, and
// counts the deletes forwarded to it.
type unreadableEcShardPeer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	deletes atomic.Int64
}

func (p *unreadableEcShardPeer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {
	return status.Error(codes.Unavailable, "shard unreadable")
}

func (p *unreadableEcShardPeer) VolumeEcBlobDelete(ctx context.Context, req *volume_server_pb.VolumeEcBlobDeleteRequest) (*volume_server_pb.VolumeEcBlobDeleteResponse, error) {
	p.deletes.Add(1)
	return &volume_server_pb.VolumeEcBlobDeleteResponse{}, nil
}

func serveEcShardPeer(t *testing.T, peer volume_server_pb.VolumeServerServer) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(srv, peer)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return pb.NewServerAddressWithGrpcPort("127.0.0.1:1", lis.Addr().(*net.TCPAddr).Port)
}

// The retry goes to the holder that answered the read deleted, not to the
// first holder that takes a delete.
func TestDeleteEcShardNeedleForwardsToTheHolderReadingDeleted(t *testing.T) {
	store := newTestStore(t, 1)
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	const vid = needle.VolumeId(9)
	_, n := writeEcVolumeFiles(t, store.Locations[0].Directory, vid)
	for shardId := 1; shardId < erasure_coding.TotalShardsCount; shardId++ {
		if err := store.MountEcShards("", vid, erasure_coding.ShardId(shardId), ""); err != nil {
			t.Fatalf("mount shard %d: %v", shardId, err)
		}
	}
	ecVolume, found := store.Locations[0].FindEcVolume(vid)
	if !found {
		t.Fatal("ec volume not mounted")
	}

	unreadable := &unreadableEcShardPeer{}
	reporter := &partlyDeletedEcShardPeer{}
	reporterAddr := serveEcShardPeer(t, reporter)
	seedShardLocations(ecVolume, reporterAddr)
	ecVolume.ShardLocationsLock.Lock()
	ecVolume.ShardLocations[0] = []pb.ServerAddress{serveEcShardPeer(t, unreadable), reporterAddr}
	ecVolume.ShardLocationsLock.Unlock()

	got := new(needle.Needle)
	got.Id = n.Id
	if _, err := store.DeleteEcShardNeedle(ecVolume, got, n.Cookie); err != ErrorDeleted {
		t.Fatalf("delete of a needle read as deleted returned %v, want %v", err, ErrorDeleted)
	}
	if reporter.deletes.Load() != 1 {
		t.Errorf("forwarded %d deletes to the holder reading it deleted, want 1", reporter.deletes.Load())
	}
	if unreadable.deletes.Load() != 0 {
		t.Errorf("forwarded %d deletes to the unreadable holder, want 0", unreadable.deletes.Load())
	}
}

// A holder that stops answering after the reporter took the delete does not
// hold the delete's answer.
func TestDeleteEcShardNeedleProbeSkipsAStalledHolder(t *testing.T) {
	store := newTestStore(t, 1)
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	const vid = needle.VolumeId(10)
	_, n := writeEcVolumeFiles(t, store.Locations[0].Directory, vid)
	for shardId := 1; shardId < erasure_coding.TotalShardsCount; shardId++ {
		if err := store.MountEcShards("", vid, erasure_coding.ShardId(shardId), ""); err != nil {
			t.Fatalf("mount shard %d: %v", shardId, err)
		}
	}
	ecVolume, found := store.Locations[0].FindEcVolume(vid)
	if !found {
		t.Fatal("ec volume not mounted")
	}
	defer func(old time.Duration) { ecReadHintTimeout = old }(ecReadHintTimeout)
	ecReadHintTimeout = 200 * time.Millisecond

	reporter := &partlyDeletedEcShardPeer{}
	reporterAddr := serveEcShardPeer(t, reporter)
	seedShardLocations(ecVolume, reporterAddr)
	ecVolume.ShardLocationsLock.Lock()
	ecVolume.ShardLocations[0] = []pb.ServerAddress{reporterAddr, serveEcShardPeer(t, stalledPeer{})}
	ecVolume.ShardLocationsLock.Unlock()

	done := make(chan error, 1)
	go func() {
		got := new(needle.Needle)
		got.Id = n.Id
		_, err := store.DeleteEcShardNeedle(ecVolume, got, n.Cookie)
		done <- err
	}()
	select {
	case err := <-done:
		if err != ErrorDeleted {
			t.Fatalf("delete of a needle read as deleted returned %v, want %v", err, ErrorDeleted)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("delete still waiting on the stalled holder")
	}
	if reporter.deletes.Load() != 1 {
		t.Errorf("forwarded %d deletes to the holder reading it deleted, want 1", reporter.deletes.Load())
	}
}

// stalledDeleteEcShardPeer reads the needle as deleted but never answers a
// delete.
type stalledDeleteEcShardPeer struct {
	deletingEcShardPeer
}

func (p *stalledDeleteEcShardPeer) VolumeEcBlobDelete(ctx context.Context, req *volume_server_pb.VolumeEcBlobDeleteRequest) (*volume_server_pb.VolumeEcBlobDeleteResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// A holder that reads the needle deleted but stalls on the delete holds
// neither the reply nor the holders after it.
func TestDeleteEcShardNeedleRetrySkipsAStalledDelete(t *testing.T) {
	store := newTestStore(t, 1)
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	const vid = needle.VolumeId(11)
	_, n := writeEcVolumeFiles(t, store.Locations[0].Directory, vid)
	for shardId := 1; shardId < erasure_coding.TotalShardsCount; shardId++ {
		if err := store.MountEcShards("", vid, erasure_coding.ShardId(shardId), ""); err != nil {
			t.Fatalf("mount shard %d: %v", shardId, err)
		}
	}
	ecVolume, found := store.Locations[0].FindEcVolume(vid)
	if !found {
		t.Fatal("ec volume not mounted")
	}
	defer func(old time.Duration) { ecDeleteRetryTimeout = old }(ecDeleteRetryTimeout)
	ecDeleteRetryTimeout = 200 * time.Millisecond

	reporter := &partlyDeletedEcShardPeer{}
	reporterAddr := serveEcShardPeer(t, reporter)
	seedShardLocations(ecVolume, reporterAddr)
	ecVolume.ShardLocationsLock.Lock()
	ecVolume.ShardLocations[0] = []pb.ServerAddress{serveEcShardPeer(t, &stalledDeleteEcShardPeer{}), reporterAddr}
	ecVolume.ShardLocationsLock.Unlock()

	done := make(chan error, 1)
	go func() {
		got := new(needle.Needle)
		got.Id = n.Id
		_, err := store.DeleteEcShardNeedle(ecVolume, got, n.Cookie)
		done <- err
	}()
	select {
	case err := <-done:
		if err != ErrorDeleted {
			t.Fatalf("delete of a needle read as deleted returned %v, want %v", err, ErrorDeleted)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("delete still waiting on the holder stalled in its delete")
	}
	if reporter.deletes.Load() != 1 {
		t.Errorf("forwarded %d deletes to the holder after the stalled one, want 1", reporter.deletes.Load())
	}
}
