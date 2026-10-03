package topology

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestIsEmptyVolumeDeleteCandidate(t *testing.T) {
	now := time.Now().Unix()
	old := now - 7200
	base := storage.VolumeInfo{
		Size:             0,
		ModifiedAtSecond: old,
	}
	for _, tc := range []struct {
		name  string
		v     storage.VolumeInfo
		quiet int64
		want  bool
	}{
		{"empty and quiet", base, 3600, true},
		{"all garbage and quiet", storage.VolumeInfo{Size: 1 << 20, FileCount: 100, DeleteCount: 100, ModifiedAtSecond: old}, 3600, true},
		{"remote-backed", storage.VolumeInfo{Size: 0, ModifiedAtSecond: old, RemoteStorageName: "s3"}, 3600, false},
		{"read-only without delete permission", storage.VolumeInfo{Size: 0, ModifiedAtSecond: old, ReadOnly: true}, 3600, false},
		{"read-only can delete", storage.VolumeInfo{Size: 0, ModifiedAtSecond: old, ReadOnly: true, ReadOnlyCanDelete: true}, 3600, true},
		{"live files", storage.VolumeInfo{Size: 1 << 20, FileCount: 100, DeleteCount: 50, ModifiedAtSecond: old}, 3600, false},
		{"no live files but oversized is fine", storage.VolumeInfo{Size: 1 << 20, FileCount: 100, DeleteCount: 100, ModifiedAtSecond: old}, 3600, true},
		{"unreported mtime", storage.VolumeInfo{Size: 0}, 3600, false},
		{"still quiet-recent", storage.VolumeInfo{Size: 0, ModifiedAtSecond: now - 60}, 3600, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isEmptyVolumeDeleteCandidate(tc.v, tc.quiet, now); got != tc.want {
				t.Errorf("isEmptyVolumeDeleteCandidate() = %v, want %v", got, tc.want)
			}
		})
	}
}

type fakeVolumeDeleteServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	mu      sync.Mutex
	deletes []*volume_server_pb.VolumeDeleteRequest
}

func (f *fakeVolumeDeleteServer) VolumeDelete(ctx context.Context, req *volume_server_pb.VolumeDeleteRequest) (*volume_server_pb.VolumeDeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, req)
	return &volume_server_pb.VolumeDeleteResponse{}, nil
}

func startFakeVolumeServer(t *testing.T, vs volume_server_pb.VolumeServerServer) (grpcPort int, dialOption grpc.DialOption) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(srv, vs)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("fake volume server: %v", err)
		}
	})
	return lis.Addr().(*net.TCPAddr).Port, grpc.WithTransportCredentials(insecure.NewCredentials())
}

func TestDeleteEmptyVolumesDeletesOnlyQuietEmptyCopies(t *testing.T) {
	fake := &fakeVolumeDeleteServer{}
	grpcPort, dialOption := startFakeVolumeServer(t, fake)

	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 8080, grpcPort, "127.0.0.1", fmt.Sprintf("dn-%d", grpcPort), map[string]uint32{"": 10})

	now := time.Now().Unix()
	old := now - 7200
	volume := func(id int, size uint64, files, deleted uint32, mtime int64) storage.VolumeInfo {
		return storage.VolumeInfo{
			Id:               needle.VolumeId(id),
			Size:             size,
			Collection:       "c",
			FileCount:        files,
			DeleteCount:      deleted,
			ModifiedAtSecond: mtime,
			Version:          needle.GetCurrentVersion(),
			ReplicaPlacement: &super_block.ReplicaPlacement{},
			Ttl:              needle.EMPTY_TTL,
		}
	}
	quietEmpty := volume(1, 0, 0, 0, old)
	quietGarbage := volume(2, 1<<20, 100, 100, old)
	recentEmpty := volume(3, 0, 0, 0, now)
	live := volume(4, 1<<20, 100, 50, old)

	todo := make(map[needle.VolumeId]*VolumeLocationList)
	dn.UpdateVolumes([]storage.VolumeInfo{quietEmpty, quietGarbage, recentEmpty, live})
	for _, v := range []storage.VolumeInfo{quietEmpty, quietGarbage, recentEmpty, live} {
		topo.RegisterVolumeLayout(v, dn)
		ll := NewVolumeLocationList()
		ll.list = append(ll.list, dn)
		todo[v.Id] = ll
	}

	vl := topo.GetVolumeLayout("c", &super_block.ReplicaPlacement{}, needle.EMPTY_TTL, types.ToDiskType(""))
	topo.deleteEmptyVolumes(dialOption, vl, todo, time.Hour)

	if _, ok := todo[quietEmpty.Id]; ok {
		t.Error("quiet empty volume stayed in the sweep map")
	}
	if _, ok := todo[quietGarbage.Id]; ok {
		t.Error("quiet all-garbage volume stayed in the sweep map")
	}
	for _, v := range []storage.VolumeInfo{recentEmpty, live} {
		if _, ok := todo[v.Id]; !ok {
			t.Errorf("volume %d was deleted though not an empty-quiet candidate", v.Id)
		}
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.deletes) != 2 {
		t.Fatalf("VolumeDelete calls = %d, want 2", len(fake.deletes))
	}
	got := map[uint32]*volume_server_pb.VolumeDeleteRequest{}
	for _, d := range fake.deletes {
		if !d.OnlyEmpty {
			t.Errorf("VolumeDelete %d missing the onlyEmpty guard", d.VolumeId)
		}
		got[d.VolumeId] = d
	}
	if got[uint32(quietEmpty.Id)].OnlyGarbage {
		t.Error("truly empty volume deleted with onlyGarbage")
	}
	if !got[uint32(quietGarbage.Id)].OnlyGarbage {
		t.Error("all-garbage volume should carry onlyGarbage")
	}
}

// A volume whose sibling replica holds live files is not "empty": deleting
// the empty copy would silently cut the live copy's replica count, so the
// whole vid is left alone.
func TestDeleteEmptyVolumesSkipsVidWithLiveReplica(t *testing.T) {
	fake := &fakeVolumeDeleteServer{}
	grpcPort, dialOption := startFakeVolumeServer(t, fake)

	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	emptyDn := rack.GetOrCreateDataNode("127.0.0.1", 8080, grpcPort, "127.0.0.1", "dn-empty", map[string]uint32{"": 10})
	liveDn := rack.GetOrCreateDataNode("127.0.0.2", 8080, 1, "127.0.0.2", "dn-live", map[string]uint32{"": 10})

	now := time.Now().Unix()
	v := storage.VolumeInfo{
		Id:               needle.VolumeId(1),
		Size:             0,
		Collection:       "c",
		ModifiedAtSecond: now - 7200,
		Version:          needle.GetCurrentVersion(),
		ReplicaPlacement: &super_block.ReplicaPlacement{},
		Ttl:              needle.EMPTY_TTL,
	}
	live := storage.VolumeInfo{
		Id:               v.Id,
		Size:             1 << 20,
		Collection:       "c",
		FileCount:        100,
		DeleteCount:      50,
		ModifiedAtSecond: now - 7200,
		Version:          needle.GetCurrentVersion(),
		ReplicaPlacement: &super_block.ReplicaPlacement{},
		Ttl:              needle.EMPTY_TTL,
	}
	emptyDn.UpdateVolumes([]storage.VolumeInfo{v})
	liveDn.UpdateVolumes([]storage.VolumeInfo{live})
	topo.RegisterVolumeLayout(v, emptyDn)
	topo.RegisterVolumeLayout(live, liveDn)

	ll := NewVolumeLocationList()
	ll.list = append(ll.list, emptyDn, liveDn)
	todo := map[needle.VolumeId]*VolumeLocationList{v.Id: ll}

	vl := topo.GetVolumeLayout("c", &super_block.ReplicaPlacement{}, needle.EMPTY_TTL, types.ToDiskType(""))
	topo.deleteEmptyVolumes(dialOption, vl, todo, time.Hour)

	if _, ok := todo[v.Id]; !ok {
		t.Fatal("vid left the sweep map while a live replica keeps it")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.deletes) != 0 {
		t.Fatalf("VolumeDelete calls = %d, want 0 (a live sibling keeps the whole vid)", len(fake.deletes))
	}
}

// With every copy eligible a copy whose delete RPC fails keeps the vid in the
// sweep map; the deleted copy is gone but the vid falls through to the normal
// compaction path for its surviving replicas.
func TestDeleteEmptyVolumesKeepsVidWhenCopyDeleteFails(t *testing.T) {
	fake := &fakeVolumeDeleteServer{}
	grpcPort, dialOption := startFakeVolumeServer(t, fake)

	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	emptyDn := rack.GetOrCreateDataNode("127.0.0.1", 8080, grpcPort, "127.0.0.1", "dn-empty", map[string]uint32{"": 10})
	deadDn := rack.GetOrCreateDataNode("127.0.0.2", 8080, 1, "127.0.0.2", "dn-dead", map[string]uint32{"": 10})

	now := time.Now().Unix()
	v := storage.VolumeInfo{
		Id:               needle.VolumeId(1),
		Size:             0,
		Collection:       "c",
		ModifiedAtSecond: now - 7200,
		Version:          needle.GetCurrentVersion(),
		ReplicaPlacement: &super_block.ReplicaPlacement{},
		Ttl:              needle.EMPTY_TTL,
	}
	emptyDn.UpdateVolumes([]storage.VolumeInfo{v})
	deadDn.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, emptyDn)
	topo.RegisterVolumeLayout(v, deadDn)

	ll := NewVolumeLocationList()
	ll.list = append(ll.list, emptyDn, deadDn)
	todo := map[needle.VolumeId]*VolumeLocationList{v.Id: ll}

	vl := topo.GetVolumeLayout("c", &super_block.ReplicaPlacement{}, needle.EMPTY_TTL, types.ToDiskType(""))
	topo.deleteEmptyVolumes(dialOption, vl, todo, time.Hour)

	if _, ok := todo[v.Id]; !ok {
		t.Fatal("vid left the sweep map while a replica delete failed")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.deletes) != 1 {
		t.Fatalf("VolumeDelete calls = %d, want 1 (only the reachable copy)", len(fake.deletes))
	}
}

type fakeVacuumServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	mu         sync.Mutex
	checks     map[uint32]*volume_server_pb.VacuumVolumeCheckResponse
	committed  []uint32
	compactErr error
	hang       string
	entered    chan string
	cancelled  chan struct{}
}

func (f *fakeVacuumServer) stall(ctx context.Context, phase string) error {
	select {
	case f.entered <- phase:
	default:
	}
	<-ctx.Done()
	f.mu.Lock()
	if f.cancelled != nil {
		select {
		case <-f.cancelled:
		default:
			close(f.cancelled)
		}
	}
	f.mu.Unlock()
	return ctx.Err()
}

func (f *fakeVacuumServer) VacuumVolumeCheck(ctx context.Context, req *volume_server_pb.VacuumVolumeCheckRequest) (*volume_server_pb.VacuumVolumeCheckResponse, error) {
	if f.hang == "check" {
		return nil, f.stall(ctx, "check")
	}
	resp, ok := f.checks[req.VolumeId]
	if !ok {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}
	return resp, nil
}

func (f *fakeVacuumServer) VacuumVolumeCompact(_ *volume_server_pb.VacuumVolumeCompactRequest, stream volume_server_pb.VolumeServer_VacuumVolumeCompactServer) error {
	if f.hang == "compact" {
		return f.stall(stream.Context(), "compact")
	}
	return f.compactErr
}

func (f *fakeVacuumServer) VacuumVolumeCommit(ctx context.Context, req *volume_server_pb.VacuumVolumeCommitRequest) (*volume_server_pb.VacuumVolumeCommitResponse, error) {
	if f.hang == "commit" {
		return nil, f.stall(ctx, "commit")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = append(f.committed, req.VolumeId)
	return &volume_server_pb.VacuumVolumeCommitResponse{IsReadOnly: true}, nil
}

func (f *fakeVacuumServer) VacuumVolumeCleanup(ctx context.Context, _ *volume_server_pb.VacuumVolumeCleanupRequest) (*volume_server_pb.VacuumVolumeCleanupResponse, error) {
	if f.hang == "cleanup" {
		return nil, f.stall(ctx, "cleanup")
	}
	return &volume_server_pb.VacuumVolumeCleanupResponse{}, nil
}

func (f *fakeVacuumServer) VolumeStatus(ctx context.Context, _ *volume_server_pb.VolumeStatusRequest) (*volume_server_pb.VolumeStatusResponse, error) {
	if f.hang == "status" {
		return nil, f.stall(ctx, "status")
	}
	return &volume_server_pb.VolumeStatusResponse{}, nil
}

// A sweep keeps a read-only replica eligible only when the volume server
// reports disk_space_low; other read-only causes stay skipped unless the
// request names the volume explicitly.
func TestVacuumReadOnlyDiskLowVolume(t *testing.T) {
	fake := &fakeVacuumServer{
		checks: map[uint32]*volume_server_pb.VacuumVolumeCheckResponse{
			1: {GarbageRatio: 0.9, DiskSpaceLow: true},
			2: {GarbageRatio: 0.9},
			3: {GarbageRatio: 0.9},
			4: {GarbageRatio: 0.1, DiskSpaceLow: true},
			5: {GarbageRatio: 0.9},
		},
	}
	grpcPort, dialOption := startFakeVolumeServer(t, fake)

	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, grpcPort, "127.0.0.1", "dn", map[string]uint32{"": 10})

	newVolumeInfo := func(vid needle.VolumeId, readOnly bool) storage.VolumeInfo {
		return storage.VolumeInfo{
			Id:               vid,
			Size:             1 << 20,
			Collection:       "c",
			ReadOnly:         readOnly,
			ModifiedAtSecond: time.Now().Unix(),
			Version:          needle.GetCurrentVersion(),
			ReplicaPlacement: &super_block.ReplicaPlacement{},
			Ttl:              needle.EMPTY_TTL,
		}
	}
	vl := topo.GetVolumeLayout("c", &super_block.ReplicaPlacement{}, needle.EMPTY_TTL, types.ToDiskType(""))
	volumes := []storage.VolumeInfo{
		newVolumeInfo(1, true),
		newVolumeInfo(2, true),
		newVolumeInfo(3, false),
		newVolumeInfo(4, true),
		newVolumeInfo(5, true),
	}
	dn.UpdateVolumes(volumes)
	for _, v := range volumes {
		topo.RegisterVolumeLayout(v, dn)
	}
	c := NewCollection("c", topo.volumeSizeLimit, false)

	vacuum := func(vid needle.VolumeId, skipReadOnly bool) {
		vl.accessLock.RLock()
		ll := vl.vid2location[vid].Copy()
		vl.accessLock.RUnlock()
		topo.vacuumOneVolumeId(dialOption, vl, c, 0.3, ll, vid, 0, skipReadOnly)
	}

	vacuum(1, true)  // read-only but disk_low: compacted
	vacuum(2, true)  // read-only otherwise: skipped by sweep
	vacuum(3, true)  // writable: compacted
	vacuum(4, true)  // disk_low but below threshold: skipped
	vacuum(5, false) // read-only, explicit request: compacted

	fake.mu.Lock()
	defer fake.mu.Unlock()
	want := map[uint32]bool{1: true, 3: true, 5: true}
	got := map[uint32]bool{}
	for _, vid := range fake.committed {
		got[vid] = true
	}
	for vid := range want {
		if !got[vid] {
			t.Errorf("volume %d was not vacuum-committed", vid)
		}
	}
	for _, vid := range fake.committed {
		if !want[vid] {
			t.Errorf("volume %d vacuum-committed unexpectedly", vid)
		}
	}
}

// A volume server that keeps a vacuum RPC pending must not hold the
// topology-wide vacuum guard forever: the phase deadline cancels the call,
// Vacuum returns, and the next request is not skipped.
func TestVacuumStalledVolumeServerReleasesGuard(t *testing.T) {
	for _, phase := range []string{"check", "compact", "commit", "status", "cleanup"} {
		t.Run(phase, func(t *testing.T) {
			old := vacuumPhaseTimeout
			vacuumPhaseTimeout = 50 * time.Millisecond
			defer func() { vacuumPhaseTimeout = old }()

			hangFake := &fakeVacuumServer{
				checks:    map[uint32]*volume_server_pb.VacuumVolumeCheckResponse{1: {GarbageRatio: 0.9}},
				hang:      phase,
				entered:   make(chan string, 1),
				cancelled: make(chan struct{}),
			}
			if phase == "cleanup" {
				hangFake.compactErr = errors.New("compact failed")
			}
			if phase == "status" {
				hangFake.checks[1] = &volume_server_pb.VacuumVolumeCheckResponse{GarbageRatio: 0.1}
			}

			topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 1, 5, true)
			rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")

			var dns []*DataNode
			if phase == "status" {
				okFake := &fakeVacuumServer{
					checks: map[uint32]*volume_server_pb.VacuumVolumeCheckResponse{1: {GarbageRatio: 0.9}},
				}
				okPort, _ := startFakeVolumeServer(t, okFake)
				dns = append(dns, rack.GetOrCreateDataNode("127.0.0.1", 8080, okPort, "127.0.0.1", "dn-ok", map[string]uint32{"": 10}))
			}
			hangPort, dialOption := startFakeVolumeServer(t, hangFake)
			dns = append(dns, rack.GetOrCreateDataNode("127.0.0.1", 8081, hangPort, "127.0.0.1", "dn-hang", map[string]uint32{"": 10}))

			v := storage.VolumeInfo{
				Id:               needle.VolumeId(1),
				Size:             1 << 20,
				Collection:       "c",
				ModifiedAtSecond: time.Now().Unix(),
				Version:          needle.GetCurrentVersion(),
				ReplicaPlacement: &super_block.ReplicaPlacement{},
				Ttl:              needle.EMPTY_TTL,
			}
			for _, dn := range dns {
				dn.UpdateVolumes([]storage.VolumeInfo{v})
				topo.RegisterVolumeLayout(v, dn)
			}

			done := make(chan struct{})
			go func() {
				topo.Vacuum(dialOption, 0.3, 1, 1, "c", 0, false, 0)
				close(done)
			}()

			select {
			case <-hangFake.entered:
			case <-time.After(15 * time.Second):
				t.Fatalf("vacuum never reached the %s RPC", phase)
			}
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				t.Fatalf("vacuum did not return while the %s RPC was pending", phase)
			}
			select {
			case <-hangFake.cancelled:
			case <-time.After(15 * time.Second):
				t.Fatalf("the %s RPC was not cancelled", phase)
			}
			if c := atomic.LoadInt64(&topo.vacuumLockCounter); c != 0 {
				t.Fatalf("vacuumLockCounter = %d, want 0", c)
			}
		})
	}
}
