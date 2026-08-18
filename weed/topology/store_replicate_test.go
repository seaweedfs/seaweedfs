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
	locations []*master_pb.Location
}

func (m *mockMasterServer) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	var vls []*master_pb.LookupVolumeResponse_VolumeIdLocation
	for _, vid := range req.VolumeOrFileIds {
		vls = append(vls, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeOrFileId: vid,
			Locations:      m.locations,
		})
	}
	return &master_pb.LookupVolumeResponse{VolumeIdLocations: vls}, nil
}

// TestReplicatedWriteForwardsFsyncToReplicas verifies that the fsync=true
// request parameter is forwarded to replica volume servers in the fan-out
// request, so a durable write means every replica has flushed to disk.
func TestReplicatedWriteForwardsFsyncToReplicas(t *testing.T) {
	replicaQueries := make(chan url.Values, 4)
	replica := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		replicaQueries <- r.URL.Query()
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"size":1}`))
	}))
	defer replica.Close()
	replicaHost := strings.TrimPrefix(replica.URL, "http://")

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	grpcServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(grpcServer, &mockMasterServer{
		locations: []*master_pb.Location{{Url: replicaHost}},
	})
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	grpcPort := lis.Addr().(*net.TCPAddr).Port
	masterFn := func(_ context.Context) pb.ServerAddress {
		// ServerAddress.ToGrpcAddress treats "host:port" as an http address and
		// adds 10000 to reach the grpc port, so hand it the "port.grpcPort"
		// form to point straight at the mock listener.
		return pb.NewServerAddressWithGrpcPort(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpcPort)
	}
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())

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
