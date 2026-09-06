package topology

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestDistributedOperationCancelsSiblingsOnFirstError verifies that once one
// replica fails, an outstanding replica still stalled in a dial timeout is
// cancelled rather than gating the caller until it times out.
func TestDistributedOperationCancelsSiblingsOnFirstError(t *testing.T) {
	locations := []operation.Location{{Url: "fast"}, {Url: "slow"}}
	cancelled := make(chan struct{}, 1)

	start := time.Now()
	err := DistributedOperation(context.Background(), locations, func(ctx context.Context, location operation.Location) error {
		if location.Url == "fast" {
			return errors.New("connection refused")
		}
		// slow: a replica stalled in a dial timeout
		select {
		case <-ctx.Done():
			cancelled <- struct{}{}
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return nil
		}
	})

	if err == nil {
		t.Fatal("expected an error from the fast-failing replica")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("did not fail fast: took %v", elapsed)
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("slow replica was not cancelled after the first error")
	}
}

func TestDistributedOperationEmpty(t *testing.T) {
	err := DistributedOperation(context.Background(), nil, func(ctx context.Context, location operation.Location) error {
		t.Fatal("op should not be called when there are no locations")
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil for no locations, got %v", err)
	}
}

type mockMasterServer struct {
	master_pb.UnimplementedSeaweedServer
	mu        sync.Mutex
	calls     int
	locations []*master_pb.Location
}

func (m *mockMasterServer) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	var vls []*master_pb.LookupVolumeResponse_VolumeIdLocation
	for _, vid := range req.VolumeOrFileIds {
		vls = append(vls, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeOrFileId: vid,
			Locations:      m.locations,
		})
	}
	return &master_pb.LookupVolumeResponse{VolumeIdLocations: vls}, nil
}

func startMockMasterServer(t *testing.T, master *mockMasterServer) (operation.GetMasterFn, grpc.DialOption) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(grpcServer, master)
	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(lis) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("mock master serve: %v", err)
		}
	})

	grpcPort := lis.Addr().(*net.TCPAddr).Port
	return func(_ context.Context) pb.ServerAddress {
		return pb.NewServerAddressWithGrpcPort(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpcPort)
	}, grpc.WithTransportCredentials(insecure.NewCredentials())
}

func TestGetWritableRemoteReplicationsRefreshesReadOnlyReplicas(t *testing.T) {
	master := &mockMasterServer{locations: []*master_pb.Location{
		{Url: "127.0.0.1:8080"},
		{Url: "127.0.0.2:8080"},
	}}
	masterFn, dialOption := startMockMasterServer(t, master)
	store := &storage.Store{Ip: "127.0.0.1", Port: 8080}
	volumeId := needle.VolumeId(1)
	operation.InvalidateVolumeIdLocationCache(volumeId.String())

	locations, err := GetWritableRemoteReplications(store, dialOption, volumeId, masterFn)
	if err != nil {
		t.Fatalf("first lookup: %v", err)
	}
	if len(locations) != 1 || locations[0].Url != "127.0.0.2:8080" {
		t.Fatalf("first lookup locations = %v", locations)
	}

	master.mu.Lock()
	master.locations = []*master_pb.Location{
		{Url: "127.0.0.1:8080"},
		{Url: "127.0.0.2:8080", ReadOnly: true},
	}
	master.mu.Unlock()

	locations, err = GetWritableRemoteReplications(store, dialOption, volumeId, masterFn)
	if err != nil {
		t.Fatalf("second lookup: %v", err)
	}
	if len(locations) != 0 {
		t.Fatalf("read-only replica remained a write target: %v", locations)
	}
	master.mu.Lock()
	calls := master.calls
	master.mu.Unlock()
	if calls != 2 {
		t.Fatalf("master lookup calls = %d, want 2", calls)
	}

	locations, err = GetRemoteReplications(store, dialOption, volumeId, masterFn)
	if err != nil {
		t.Fatalf("delete lookup: %v", err)
	}
	if len(locations) != 1 || !locations[0].ReadOnly {
		t.Fatalf("read-only-can-delete target was dropped: %v", locations)
	}
}

// TestReplicatedWriteForwardsFsyncToReplicas verifies that the fsync=true
// request parameter is forwarded to replica volume servers in the fan-out
// request, so a durable write means every replica has flushed to disk.
func TestReplicatedWriteForwardsFsyncToReplicas(t *testing.T) {
	replicaQueries := make(chan url.Values, 4)
	replica := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		replicaQueries <- r.URL.Query()
		w.WriteHeader(http.StatusCreated)
		if _, err := w.Write([]byte(`{"size":1}`)); err != nil {
			t.Errorf("replica write response: %v", err)
		}
	}))
	defer replica.Close()
	replicaHost := strings.TrimPrefix(replica.URL, "http://")

	masterFn, dialOption := startMockMasterServer(t, &mockMasterServer{
		locations: []*master_pb.Location{{Url: replicaHost}},
	})

	store := &storage.Store{}
	volumeId := needle.VolumeId(1)

	for _, tc := range []struct {
		name      string
		fsync     string
		wantFsync bool
	}{
		{name: "fsync requested", fsync: "true", wantFsync: true},
		{name: "no fsync requested", fsync: "", wantFsync: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operation.InvalidateVolumeIdLocationCache(volumeId.String())

			path := "http://127.0.0.1:8080/1,01637037d6"
			if tc.fsync != "" {
				path += "?fsync=" + tc.fsync
			}
			r := httptest.NewRequest(http.MethodPost, path, nil)
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}

			n := &needle.Needle{
				Id:   1,
				Data: []byte("test data"),
				Ttl:  needle.EMPTY_TTL,
			}
			if _, err := ReplicatedWrite(context.Background(), masterFn, dialOption, store, volumeId, n, r, ""); err != nil {
				t.Fatalf("ReplicatedWrite: %v", err)
			}

			select {
			case q := <-replicaQueries:
				got := q.Get("fsync")
				if tc.wantFsync && got != "true" {
					t.Errorf("expected fsync=true in replica query, got %q (query: %v)", got, q)
				}
				if !tc.wantFsync && got != "" {
					t.Errorf("expected no fsync in replica query, got %q (query: %v)", got, q)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("replica never received the fan-out request")
			}
		})
	}
}
