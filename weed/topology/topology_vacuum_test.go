package topology

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
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

type fakeVacuumCheckServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	mu      sync.Mutex
	checked []uint32
}

func (f *fakeVacuumCheckServer) VacuumVolumeCheck(ctx context.Context, req *volume_server_pb.VacuumVolumeCheckRequest) (*volume_server_pb.VacuumVolumeCheckResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checked = append(f.checked, req.VolumeId)
	// no garbage: the sweep stops after the check, which is all this test needs
	return &volume_server_pb.VacuumVolumeCheckResponse{GarbageRatio: 0}, nil
}

// The sweep skips read-only volumes because the flag usually means a failing
// disk. A volume that is read-only only because its disk is low on space is
// healthy and must still be checked, or a full disk can never be reclaimed.
func TestSweepChecksVolumesReadOnlyOnlyForLowDisk(t *testing.T) {
	fake := &fakeVacuumCheckServer{}
	grpcPort, dialOption := startFakeVolumeServer(t, fake)

	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, grpcPort, "127.0.0.1", fmt.Sprintf("dn-%d", grpcPort), map[string]uint32{"": 10})

	volume := func(id int, lowDisk bool) storage.VolumeInfo {
		return storage.VolumeInfo{
			Id:               needle.VolumeId(id),
			Size:             1 << 20,
			Collection:       "c",
			FileCount:        100,
			DeleteCount:      100,
			ReadOnly:         true,
			ReadOnlyLowDisk:  lowDisk,
			Version:          needle.GetCurrentVersion(),
			ReplicaPlacement: &super_block.ReplicaPlacement{},
			Ttl:              needle.EMPTY_TTL,
		}
	}
	lowDisk := volume(1, true)
	otherReason := volume(2, false)
	dn.UpdateVolumes([]storage.VolumeInfo{lowDisk, otherReason})
	topo.RegisterVolumeLayout(lowDisk, dn)
	topo.RegisterVolumeLayout(otherReason, dn)

	vl := topo.GetVolumeLayout("c", &super_block.ReplicaPlacement{}, needle.EMPTY_TTL, types.ToDiskType(""))
	c, ok := topo.FindCollection("c")
	if !ok {
		t.Fatal("collection c not found")
	}
	for _, v := range []storage.VolumeInfo{lowDisk, otherReason} {
		vl.accessLock.RLock()
		locations := vl.vid2location[v.Id].Copy()
		vl.accessLock.RUnlock()
		if !locations.AnyReadOnly() {
			t.Fatalf("volume %d is not flagged read-only in the layout; the test would not exercise the skip", v.Id)
		}
		topo.vacuumOneVolumeId(dialOption, vl, c, 0.3, locations, v.Id, 0, true)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.checked) != 1 || fake.checked[0] != uint32(lowDisk.Id) {
		t.Fatalf("sweep checked volumes %v, want only the low-disk volume [%d]", fake.checked, lowDisk.Id)
	}
}

// volume.mark -readonly on a volume that was read-only for low disk space
// must not leave it looking merely low on space until the next heartbeat.
func TestMarkReadOnlyClearsLowDiskReason(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 0, "127.0.0.1", "", map[string]uint32{"": 10})
	v := storage.VolumeInfo{
		Id: 1, Collection: "c", Size: 1 << 20, ReadOnly: true, ReadOnlyLowDisk: true,
		Version: needle.GetCurrentVersion(), ReplicaPlacement: &super_block.ReplicaPlacement{}, Ttl: needle.EMPTY_TTL,
	}
	dn.UpdateVolumes([]storage.VolumeInfo{v})

	// a node that was told the final state outright is what the digest must match
	marked := v
	marked.ReadOnlyLowDisk = false
	other := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.2", 8080, 0, "127.0.0.2", "", map[string]uint32{"": 10})
	other.UpdateVolumes([]storage.VolumeInfo{marked})
	if dn.VolumeDigest() == other.VolumeDigest() {
		t.Fatal("test setup: the low-disk bit does not show in the digest")
	}

	dn.SetVolumeReadOnly(v.Id, true)

	stored, err := dn.GetVolumesById(v.Id)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.ReadOnly || stored.ReadOnlyLowDisk {
		t.Fatalf("after an explicit mark: ReadOnly=%t ReadOnlyLowDisk=%t, want true false", stored.ReadOnly, stored.ReadOnlyLowDisk)
	}
	if dn.VolumeDigest() != other.VolumeDigest() {
		t.Fatal("the mark cleared the low-disk bit without moving the digest, so the master would keep asking for the full list")
	}
}
