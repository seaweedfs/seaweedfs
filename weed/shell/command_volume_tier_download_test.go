package shell

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

const (
	tierReplicaRemote = "remote"
	tierReplicaLocal  = "local"
)

// tierDownloadCluster models the replicas of one tiered volume across fake
// volume servers sharing one remote object.
type tierDownloadCluster struct {
	mu           sync.Mutex
	servers      []pb.ServerAddress
	ports        map[pb.ServerAddress]int
	replicas     map[pb.ServerAddress]string
	diskTypes    map[pb.ServerAddress]string
	objectExists bool
	downloads    int
	dangling     []string
	// hiddenOnRead hides servers from the n-th topology read (0-based)
	hiddenOnRead map[int]map[pb.ServerAddress]bool
	reads        int
	frozen       map[pb.ServerAddress]bool
	writable     map[pb.ServerAddress]bool
	remounts     map[pb.ServerAddress]int
	placement    uint32
	// staleTwin lists a remote-backed twin of the volume on another disk
	staleTwin map[pb.ServerAddress]bool
	// markFails applies the readonly mark, then fails as a lost master notification does
	markFails  map[pb.ServerAddress]bool
	mountFails map[pb.ServerAddress]bool
}

type tierDownloadTestServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	cluster *tierDownloadCluster
	addr    pb.ServerAddress
}

func (c *tierDownloadCluster) deleteObjectLocked(deleter pb.ServerAddress) {
	c.objectExists = false
	for server, state := range c.replicas {
		if server != deleter && state == tierReplicaRemote {
			c.dangling = append(c.dangling, string(server))
		}
	}
}

func (s *tierDownloadTestServer) VolumeTierMoveDatFromRemote(req *volume_server_pb.VolumeTierMoveDatFromRemoteRequest, _ volume_server_pb.VolumeServer_VolumeTierMoveDatFromRemoteServer) error {
	c := s.cluster
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.replicas[s.addr] != tierReplicaRemote {
		return fmt.Errorf("volume %d is already on local disk", req.VolumeId)
	}
	if !c.objectExists {
		return fmt.Errorf("remote object missing")
	}
	c.downloads++
	c.replicas[s.addr] = tierReplicaLocal
	if !req.KeepRemoteDatFile {
		c.deleteObjectLocked(s.addr)
	}
	return nil
}

func (s *tierDownloadTestServer) VolumeUnmount(context.Context, *volume_server_pb.VolumeUnmountRequest) (*volume_server_pb.VolumeUnmountResponse, error) {
	return &volume_server_pb.VolumeUnmountResponse{}, nil
}

func (s *tierDownloadTestServer) VolumeMount(context.Context, *volume_server_pb.VolumeMountRequest) (*volume_server_pb.VolumeMountResponse, error) {
	c := s.cluster
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mountFails[s.addr] {
		return nil, fmt.Errorf("mount volume: disk error")
	}
	c.frozen[s.addr] = false
	c.remounts[s.addr]++
	return &volume_server_pb.VolumeMountResponse{}, nil
}

func (s *tierDownloadTestServer) VolumeStatus(context.Context, *volume_server_pb.VolumeStatusRequest) (*volume_server_pb.VolumeStatusResponse, error) {
	c := s.cluster
	c.mu.Lock()
	defer c.mu.Unlock()
	return &volume_server_pb.VolumeStatusResponse{IsReadOnly: !c.writable[s.addr] || c.frozen[s.addr]}, nil
}

func (s *tierDownloadTestServer) VolumeMarkReadonly(_ context.Context, req *volume_server_pb.VolumeMarkReadonlyRequest) (*volume_server_pb.VolumeMarkReadonlyResponse, error) {
	c := s.cluster
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frozen[s.addr] = !req.CanDelete
	if c.markFails[s.addr] {
		return nil, fmt.Errorf("notify master: leader changed")
	}
	return &volume_server_pb.VolumeMarkReadonlyResponse{}, nil
}

func (s *tierDownloadTestServer) VolumeMarkWritable(context.Context, *volume_server_pb.VolumeMarkWritableRequest) (*volume_server_pb.VolumeMarkWritableResponse, error) {
	c := s.cluster
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frozen[s.addr] = false
	c.writable[s.addr] = true
	return &volume_server_pb.VolumeMarkWritableResponse{}, nil
}

func (s *tierDownloadTestServer) VolumeCopy(req *volume_server_pb.VolumeCopyRequest, _ volume_server_pb.VolumeServer_VolumeCopyServer) error {
	c := s.cluster
	c.mu.Lock()
	defer c.mu.Unlock()
	// an empty disk type means the source's, as on the real servers
	want, got := types.ToDiskType(c.diskTypes[s.addr]), types.ToDiskType(c.diskTypes[pb.ServerAddress(req.SourceDataNode)])
	if req.DiskType != "" {
		got = types.ToDiskType(req.DiskType)
	}
	if got != want {
		return fmt.Errorf("copy volume %d onto disk type %q, replica is on %q", req.VolumeId, got, want)
	}
	if !c.frozen[pb.ServerAddress(req.SourceDataNode)] {
		return fmt.Errorf("volume %d on %s still accepts deletes during the copy", req.VolumeId, req.SourceDataNode)
	}
	switch c.replicas[pb.ServerAddress(req.SourceDataNode)] {
	case tierReplicaLocal:
		c.replicas[s.addr] = tierReplicaLocal
	case tierReplicaRemote:
		c.replicas[s.addr] = tierReplicaRemote
	default:
		return fmt.Errorf("volume %d not found on %s", req.VolumeId, req.SourceDataNode)
	}
	return nil
}

func (s *tierDownloadTestServer) VolumeDelete(_ context.Context, req *volume_server_pb.VolumeDeleteRequest) (*volume_server_pb.VolumeDeleteResponse, error) {
	c := s.cluster
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.replicas[s.addr] == tierReplicaRemote && !req.KeepRemoteData {
		c.deleteObjectLocked(s.addr)
	}
	delete(c.replicas, s.addr)
	return &volume_server_pb.VolumeDeleteResponse{}, nil
}

func newTierDownloadCluster(t *testing.T, states ...string) *tierDownloadCluster {
	t.Helper()
	c := &tierDownloadCluster{
		ports:        make(map[pb.ServerAddress]int),
		replicas:     make(map[pb.ServerAddress]string),
		diskTypes:    make(map[pb.ServerAddress]string),
		objectExists: true,
		hiddenOnRead: make(map[int]map[pb.ServerAddress]bool),
		frozen:       make(map[pb.ServerAddress]bool),
		writable:     make(map[pb.ServerAddress]bool),
		remounts:     make(map[pb.ServerAddress]int),
		staleTwin:    make(map[pb.ServerAddress]bool),
		markFails:    make(map[pb.ServerAddress]bool),
		mountFails:   make(map[pb.ServerAddress]bool),
	}
	for _, state := range states {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		port := lis.Addr().(*net.TCPAddr).Port
		addr := pb.NewServerAddress("127.0.0.1", port, port)
		srv := grpc.NewServer()
		volume_server_pb.RegisterVolumeServerServer(srv, &tierDownloadTestServer{cluster: c, addr: addr})
		go srv.Serve(lis)
		t.Cleanup(srv.Stop)
		c.servers = append(c.servers, addr)
		c.ports[addr] = port
		c.replicas[addr] = state
		if len(c.servers)%2 == 0 {
			c.diskTypes[addr] = "ssd"
		}
	}
	return c
}

func (c *tierDownloadCluster) hide(read int, servers ...pb.ServerAddress) {
	if c.hiddenOnRead[read] == nil {
		c.hiddenOnRead[read] = make(map[pb.ServerAddress]bool)
	}
	for _, server := range servers {
		c.hiddenOnRead[read][server] = true
	}
}

func (c *tierDownloadCluster) readTopology() (*master_pb.TopologyInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	hidden := c.hiddenOnRead[c.reads]
	c.reads++
	var dns []*master_pb.DataNodeInfo
	for _, server := range c.servers {
		state, found := c.replicas[server]
		if !found || hidden[server] {
			continue
		}
		vi := &master_pb.VolumeInformationMessage{Id: 7, Collection: "c", DiskType: c.diskTypes[server], ReplicaPlacement: c.placement}
		if state == tierReplicaRemote {
			vi.RemoteStorageName = "s3.default"
			vi.RemoteStorageKey = "key"
		}
		disks := map[string]*master_pb.DiskInfo{"": {VolumeInfos: []*master_pb.VolumeInformationMessage{vi}}}
		if c.staleTwin[server] {
			disks["twin"] = &master_pb.DiskInfo{VolumeInfos: []*master_pb.VolumeInformationMessage{{
				Id: 7, Collection: "c", RemoteStorageName: "s3.default", RemoteStorageKey: "key", ReplicaPlacement: c.placement,
			}}}
		}
		dns = append(dns, &master_pb.DataNodeInfo{
			Id:        fmt.Sprintf("127.0.0.1:%d", c.ports[server]),
			GrpcPort:  uint32(c.ports[server]),
			DiskInfos: disks,
		})
	}
	return &master_pb.TopologyInfo{DataCenterInfos: []*master_pb.DataCenterInfo{{
		RackInfos: []*master_pb.RackInfo{{DataNodeInfos: dns}},
	}}}, nil
}

func (c *tierDownloadCluster) run(t *testing.T) error {
	t.Helper()
	return downloadAndReplicateFromRemoteTier(grpc.WithTransportCredentials(insecure.NewCredentials()), io.Discard, c.readTopology, needle.VolumeId(7), "c", 0)
}

func (c *tierDownloadCluster) assertAllLocal(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, server := range c.servers {
		if state := c.replicas[server]; state != tierReplicaLocal {
			t.Errorf("replica on %s: want local, got %q", server, state)
		}
	}
	if len(c.dangling) > 0 {
		t.Errorf("remote object deleted while still referenced by %s", strings.Join(c.dangling, ","))
	}
}

func TestTierDownloadReplicatesBeforeDeletingRemoteObject(t *testing.T) {
	c := newTierDownloadCluster(t, tierReplicaRemote, tierReplicaRemote, tierReplicaRemote)
	if err := c.run(t); err != nil {
		t.Fatalf("tier download: %v", err)
	}
	c.assertAllLocal(t)
	if c.objectExists {
		t.Errorf("remote object should be deleted once every replica is local")
	}
	if c.downloads != 1 {
		t.Errorf("want one download from the remote tier, got %d", c.downloads)
	}
}

func TestTierDownloadReplacesReplicaMissingFromFirstTopology(t *testing.T) {
	c := newTierDownloadCluster(t, tierReplicaRemote, tierReplicaRemote, tierReplicaRemote)
	c.hide(0, c.servers[2])
	if err := c.run(t); err != nil {
		t.Fatalf("tier download: %v", err)
	}
	c.assertAllLocal(t)
	if c.objectExists {
		t.Errorf("remote object should be deleted once every replica is local")
	}
}

func TestTierDownloadKeepsRemoteObjectWhenLastReplicaVanishes(t *testing.T) {
	c := newTierDownloadCluster(t, tierReplicaRemote, tierReplicaRemote, tierReplicaRemote)
	c.hide(1, c.servers[2])
	if err := c.run(t); err != nil {
		t.Fatalf("tier download: %v", err)
	}
	if !c.objectExists {
		t.Fatalf("remote object must be kept while an unlisted replica may still reference it")
	}
	if len(c.dangling) > 0 {
		t.Errorf("remote object deleted while still referenced by %v", c.dangling)
	}
}

func TestTierDownloadSingleReplicaDeletesRemoteObject(t *testing.T) {
	c := newTierDownloadCluster(t, tierReplicaRemote)
	if err := c.run(t); err != nil {
		t.Fatalf("tier download: %v", err)
	}
	c.assertAllLocal(t)
	if c.objectExists {
		t.Errorf("remote object should be deleted")
	}
}

func TestTierDownloadRerunReusesLocalReplica(t *testing.T) {
	c := newTierDownloadCluster(t, tierReplicaLocal, tierReplicaRemote, tierReplicaRemote)
	if err := c.run(t); err != nil {
		t.Fatalf("tier download: %v", err)
	}
	c.assertAllLocal(t)
	if c.objectExists {
		t.Errorf("remote object should be deleted once every replica is local")
	}
	if c.downloads != 0 {
		t.Errorf("want no download when a replica is already local, got %d", c.downloads)
	}
}

func TestTierDownloadCopiesFromSsdSourceOntoHddReplica(t *testing.T) {
	c := newTierDownloadCluster(t, tierReplicaRemote, tierReplicaRemote, tierReplicaRemote)
	c.hide(0, c.servers[0])
	if err := c.run(t); err != nil {
		t.Fatalf("tier download: %v", err)
	}
	c.assertAllLocal(t)
}

func TestTierDownloadRestoresSourceAfterBlockingDeletes(t *testing.T) {
	for _, writable := range []bool{false, true} {
		c := newTierDownloadCluster(t, tierReplicaLocal, tierReplicaRemote, tierReplicaRemote)
		source := c.servers[0]
		c.writable[source] = writable
		if err := c.run(t); err != nil {
			t.Fatalf("tier download (writable=%v): %v", writable, err)
		}
		c.assertAllLocal(t)
		if c.frozen[source] {
			t.Errorf("writable=%v: source still blocks deletes after the command", writable)
		}
		if c.writable[source] != writable {
			t.Errorf("writable=%v: source writable state changed to %v", writable, c.writable[source])
		}
		if !writable && c.remounts[source] != 1 {
			t.Errorf("readonly source should reload its persisted state once, got %d remounts", c.remounts[source])
		}
	}
}

func TestTierDownloadKeepsRemoteObjectWhileReplicasAreUnlisted(t *testing.T) {
	for _, tc := range []struct {
		name   string
		states []string
	}{
		{"lone listed replica", []string{tierReplicaRemote, tierReplicaRemote}},
		{"local source and one remote replica", []string{tierReplicaLocal, tierReplicaRemote, tierReplicaRemote}},
		{"three remote replicas", []string{tierReplicaRemote, tierReplicaRemote, tierReplicaRemote, tierReplicaRemote}},
	} {
		c := newTierDownloadCluster(t, tc.states...)
		c.placement = uint32(len(tc.states) - 1) // "00n": n+1 copies, one more than listed
		offline := c.servers[len(c.servers)-1]
		for read := 0; read < 4; read++ {
			c.hide(read, offline)
		}
		if err := c.run(t); err != nil {
			t.Fatalf("%s: tier download: %v", tc.name, err)
		}
		if !c.objectExists {
			t.Errorf("%s: remote object deleted while the replica on %s is unlisted", tc.name, offline)
		}
	}
}

func TestTierDownloadFailsWhenNoReplicaIsListed(t *testing.T) {
	c := newTierDownloadCluster(t, tierReplicaRemote)
	c.hide(0, c.servers[0])
	if err := c.run(t); err == nil {
		t.Fatalf("tier download should fail when no replica is listed")
	}
}

func TestTierDownloadKeepsRemoteObjectForStaleTwinOnSource(t *testing.T) {
	c := newTierDownloadCluster(t, tierReplicaLocal, tierReplicaRemote)
	c.staleTwin[c.servers[0]] = true
	if err := c.run(t); err != nil {
		t.Fatalf("tier download: %v", err)
	}
	if !c.objectExists {
		t.Errorf("remote object deleted while a twin on %s still references it", c.servers[0])
	}
}

func TestTierDownloadRestoresSourceWhenMarkFailsAfterApplying(t *testing.T) {
	for _, writable := range []bool{false, true} {
		c := newTierDownloadCluster(t, tierReplicaLocal, tierReplicaRemote, tierReplicaRemote)
		source := c.servers[0]
		c.writable[source] = writable
		c.markFails[source] = true
		if err := c.run(t); err == nil {
			t.Fatalf("writable=%v: tier download should fail when the readonly mark fails", writable)
		}
		if c.frozen[source] {
			t.Errorf("writable=%v: source still blocks deletes after the failed mark", writable)
		}
		if c.writable[source] != writable {
			t.Errorf("writable=%v: source writable state changed to %v", writable, c.writable[source])
		}
		if !c.objectExists {
			t.Errorf("writable=%v: remote object deleted although no replica was replaced", writable)
		}
	}
}

func TestTierDownloadReportsSourceRestoreFailure(t *testing.T) {
	c := newTierDownloadCluster(t, tierReplicaLocal, tierReplicaRemote, tierReplicaRemote)
	source := c.servers[0]
	c.mountFails[source] = true
	err := c.run(t)
	if err == nil || !strings.Contains(err.Error(), "restore volume 7 state") {
		t.Fatalf("tier download should report the failed source restore, got %v", err)
	}
	c.assertAllLocal(t)
}
