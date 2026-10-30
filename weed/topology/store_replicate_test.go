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
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
		vl := &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeOrFileId: vid,
			Locations:      m.locations,
		}
		if len(m.locations) == 0 {
			vl.Error = fmt.Sprintf("volume id %s not found", vid)
		}
		vls = append(vls, vl)
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
		{Url: "127.0.0.2:8080", ReadOnly: true, ReadOnlyCanDelete: true},
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
	if len(locations) != 1 || !locations[0].ReadOnly || !locations[0].ReadOnlyCanDelete {
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

// newReplicateTestStore builds a single-disk store in dir, draining the notify
// channels so volume mounts never block on a heartbeat reader.
func newReplicateTestStore(t *testing.T, dir string) *storage.Store {
	t.Helper()
	store := storage.NewStore(nil, "127.0.0.1", 8080, 18080, "http://127.0.0.1:8080", "store-id",
		[]string{dir}, []int32{100}, []util.MinFreeSpace{{}}, "",
		storage.NeedleMapInMemory, []types.DiskType{types.HardDriveType}, nil, 3,
		stats.DefaultDiskIOProbeConfig())
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewVolumesChan:
			case <-store.DeletedVolumesChan:
			case <-store.NewEcShardsChan:
			case <-store.DeletedEcShardsChan:
			case <-store.StateUpdateChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		close(done)
		store.Close()
	})
	return store
}

// TestReplicatedWriteOverlapsLocalAndReplicaWrites runs the write path with a
// real local volume and a replica that stalls mid-request: the fan-out starts
// while the local write is still free to run, the local write lands before the
// replica unblocks, and the acknowledged write waits for both legs.
func TestReplicatedWriteOverlapsLocalAndReplicaWrites(t *testing.T) {
	replicaStarted := make(chan url.Values, 1)
	releaseReplica := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseReplica) }) }
	replica := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		replicaStarted <- r.URL.Query()
		<-releaseReplica
		w.WriteHeader(http.StatusCreated)
		if _, err := w.Write([]byte(`{"size":21}`)); err != nil {
			t.Errorf("replica write response: %v", err)
		}
	}))
	defer replica.Close()
	defer release()
	replicaHost := strings.TrimPrefix(replica.URL, "http://")

	masterFn, dialOption := startMockMasterServer(t, &mockMasterServer{
		locations: []*master_pb.Location{{Url: "127.0.0.1:8080"}, {Url: replicaHost}},
	})

	store := newReplicateTestStore(t, t.TempDir())
	volumeId := needle.VolumeId(7)
	operation.InvalidateVolumeIdLocationCache(volumeId.String())
	if err := store.AddVolume(volumeId, "", storage.NeedleMapInMemory, "001", "15d", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Fatal(err)
	}

	data := []byte("overlapped write data")
	n := &needle.Needle{
		Id:       1,
		Data:     data,
		Checksum: needle.NewCRC(data),
		Ttl:      needle.EMPTY_TTL,
	}
	r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8080/7,01637037d6", nil)
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := ReplicatedWrite(context.Background(), masterFn, dialOption, store, volumeId, n, r, "")
		done <- err
	}()

	var q url.Values
	select {
	case q = <-replicaStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("replica never received the fan-out request")
	}
	if got := q.Get("type"); got != "replicate" {
		t.Errorf("replica type = %q, want replicate", got)
	}
	if got := q.Get("ttl"); got != "15d" {
		t.Errorf("replica ttl = %q, want the inherited volume ttl 15d", got)
	}

	select {
	case err := <-done:
		t.Fatalf("write returned before the replica responded: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		readBack := &needle.Needle{Id: n.Id}
		if _, err := store.ReadVolumeNeedle(volumeId, readBack, nil, nil); err == nil && string(readBack.Data) == string(data) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("local write did not land while the replica was still blocked")
		}
		time.Sleep(5 * time.Millisecond)
	}

	release()
	if err := <-done; err != nil {
		t.Fatalf("ReplicatedWrite: %v", err)
	}
}

// TestReplicatedWriteFailsWhenEitherLegFails checks that a failed local write
// or a failed replica fan-out fails the whole request even when the other leg
// succeeded.
func TestReplicatedWriteFailsWhenEitherLegFails(t *testing.T) {
	for _, tc := range []struct {
		name          string
		volumeId      needle.VolumeId
		readonlyLocal bool
		replicaStatus int
	}{
		{name: "local write fails", volumeId: 8, readonlyLocal: true, replicaStatus: http.StatusCreated},
		{name: "replica write fails", volumeId: 9, replicaStatus: http.StatusInternalServerError},
		{name: "replica read only", volumeId: 10, replicaStatus: http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replica := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.replicaStatus)
			}))
			defer replica.Close()
			replicaHost := strings.TrimPrefix(replica.URL, "http://")

			masterFn, dialOption := startMockMasterServer(t, &mockMasterServer{
				locations: []*master_pb.Location{{Url: "127.0.0.1:8080"}, {Url: replicaHost}},
			})

			store := newReplicateTestStore(t, t.TempDir())
			operation.InvalidateVolumeIdLocationCache(tc.volumeId.String())
			if err := store.AddVolume(tc.volumeId, "", storage.NeedleMapInMemory, "001", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
				t.Fatal(err)
			}
			if tc.readonlyLocal {
				if err := store.MarkVolumeReadonly(tc.volumeId, false, false); err != nil {
					t.Fatal(err)
				}
			}

			data := []byte("write data")
			n := &needle.Needle{
				Id:       1,
				Data:     data,
				Checksum: needle.NewCRC(data),
				Ttl:      needle.EMPTY_TTL,
			}
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:8080/%d,01637037d6", tc.volumeId), nil)
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}

			_, err := ReplicatedWrite(context.Background(), masterFn, dialOption, store, tc.volumeId, n, r, "")
			if err == nil {
				t.Fatal("ReplicatedWrite succeeded despite a failed leg")
			}
			if errors.Is(err, ErrorWriteRefused) {
				t.Fatalf("replicated write incorrectly proves no append: %v", err)
			}
		})
	}
}

// TestReplicatedWriteRejectsWriteWithNoTarget verifies that a write this server
// cannot store and has no replica to forward to fails instead of being
// acknowledged with nothing written.
func TestReplicatedWriteRejectsWriteWithNoTarget(t *testing.T) {
	master := &mockMasterServer{}
	masterFn, dialOption := startMockMasterServer(t, master)
	store := &storage.Store{Ip: "127.0.0.1", Port: 8080}
	volumeId := needle.VolumeId(31234)

	for _, tc := range []struct {
		name      string
		query     string
		locations []*master_pb.Location
	}{
		{name: "volume unknown to the master"},
		{name: "master lists only this server", locations: []*master_pb.Location{{Url: "127.0.0.1:8080"}}},
		{name: "replica write for a volume not on this server", query: "?type=replicate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			master.mu.Lock()
			master.locations = tc.locations
			master.mu.Unlock()

			r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8080/31234,01637037d6"+tc.query, nil)
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			n := &needle.Needle{
				Id:   1,
				Data: []byte("test data"),
				Ttl:  needle.EMPTY_TTL,
			}
			_, err := ReplicatedWrite(context.Background(), masterFn, dialOption, store, volumeId, n, r, "")
			if err == nil {
				t.Fatal("ReplicatedWrite acknowledged a write that no volume stored")
			}
			if !strings.Contains(err.Error(), "not found") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
