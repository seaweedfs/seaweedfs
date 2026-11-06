package weed_server

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type replicaLookupMaster struct {
	master_pb.UnimplementedSeaweedServer
	mu        sync.Mutex
	locations []*master_pb.Location
}

func (m *replicaLookupMaster) LookupVolume(_ context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	resp := &master_pb.LookupVolumeResponse{}
	for _, vid := range req.VolumeOrFileIds {
		resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeOrFileId: vid,
			Locations:      m.locations,
		})
	}
	return resp, nil
}

func (m *replicaLookupMaster) setLocations(locations ...*master_pb.Location) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.locations = locations
}

// A replica the master no longer lists would miss the delete and keep the
// needle when it returns, so the delete waits until every replica is back.
func TestBatchDelete_WaitsForEveryReplica(t *testing.T) {
	const vid = needle.VolumeId(9)
	const cookie = uint32(0x12345678)

	store := newVolumeCopyTestStore(t, t.TempDir())
	if err := store.AddVolume(vid, "", storage.NeedleMapInMemory, "001", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("add volume: %v", err)
	}
	data := []byte("hello")
	if _, err := store.WriteVolumeNeedle(vid, &needle.Needle{Id: 1, Cookie: types.Cookie(cookie), Data: data, Checksum: needle.NewCRC(data)}, true, false); err != nil {
		t.Fatalf("write needle: %v", err)
	}

	self := &master_pb.Location{Url: "127.0.0.1:0"}
	peerReplica := &master_pb.Location{Url: "127.0.0.2:8080"}
	master := &replicaLookupMaster{}
	master.setLocations(self)
	masterGrpcPort := serveGrpc(t, func(s *grpc.Server) {
		master_pb.RegisterSeaweedServer(s, master)
	})

	vs := &VolumeServer{
		store:          store,
		guard:          security.NewGuard(nil, "", 0, "", 0),
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	vs.setCurrentMaster(pb.NewServerAddress("127.0.0.1", masterGrpcPort-10000, masterGrpcPort))

	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345},
	})
	req := &volume_server_pb.BatchDeleteRequest{
		FileIds:         []string{needle.NewFileId(vid, 1, cookie).String()},
		SkipCookieCheck: true,
	}

	resp, err := vs.BatchDelete(ctx, req)
	if err != nil {
		t.Fatalf("BatchDelete: %v", err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("got %d results, want 1", len(resp.Results))
	}
	if got := resp.Results[0]; got.Status != http.StatusServiceUnavailable || !strings.Contains(got.Error, "replication copy count") {
		t.Fatalf("with a replica offline: status %d error %q, want %d and a replica count error", got.Status, got.Error, http.StatusServiceUnavailable)
	}
	if _, err := store.ReadVolumeNeedle(vid, &needle.Needle{Id: 1}, nil, nil); err != nil {
		t.Fatalf("needle deleted while a replica was offline: %v", err)
	}

	master.setLocations(self, peerReplica)
	resp, err = vs.BatchDelete(ctx, req)
	if err != nil {
		t.Fatalf("BatchDelete: %v", err)
	}
	if got := resp.Results[0]; got.Status != http.StatusAccepted || got.Error != "" {
		t.Fatalf("with every replica back: status %d error %q, want %d", got.Status, got.Error, http.StatusAccepted)
	}
}
