package command

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// kvFilerServer implements just the KV calls of the filer gRPC API over an
// in-memory map, mirroring the real server's behavior of returning an empty
// response, not an error, for a missing key.
type kvFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu sync.Mutex
	kv map[string][]byte
}

func (s *kvFilerServer) KvGet(_ context.Context, req *filer_pb.KvGetRequest) (*filer_pb.KvGetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &filer_pb.KvGetResponse{Value: s.kv[string(req.Key)]}, nil
}

func (s *kvFilerServer) KvPut(_ context.Context, req *filer_pb.KvPutRequest) (*filer_pb.KvPutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[string(req.Key)] = req.Value
	return &filer_pb.KvPutResponse{}, nil
}

func startKvFiler(t *testing.T) (pb.ServerAddress, grpc.DialOption) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, &kvFilerServer{kv: map[string][]byte{}})
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop() })
	grpcPort := listener.Addr().(*net.TCPAddr).Port
	return pb.NewServerAddressWithGrpcPort("127.0.0.1:8888", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials())
}

// An upgraded backup finds no destination-scoped checkpoint yet and must
// resume from the historical name+directory key; once its own key holds a
// value, the historical key — possibly still advanced by another backup that
// shared it — must no longer influence it.
func TestBackupOffset_HistoricalFallbackThenIndependence(t *testing.T) {
	filerAddr, dial := startKvFiler(t)

	s := &stubSink{name: "s3", dir: "/", destination: "endpoint-a|bucket-a|/"}
	sinkId, legacySinkId := backupCheckpointIds("/", s)

	// pre-upgrade state: only the shared historical checkpoint exists
	if err := setOffset(dial, filerAddr, BackupKeyPrefix, legacySinkId, 111); err != nil {
		t.Fatalf("seed historical offset: %v", err)
	}
	got, err := getOffsetWithFallback(dial, filerAddr, BackupKeyPrefix, sinkId, BackupKeyPrefix, legacySinkId)
	if err != nil || got != 111 {
		t.Fatalf("fallback read = (%d, %v), want (111, nil)", got, err)
	}

	// the backup checkpoints under its own key and prefers it from then on
	if err := setOffset(dial, filerAddr, BackupKeyPrefix, sinkId, 222); err != nil {
		t.Fatalf("write destination-scoped offset: %v", err)
	}
	got, err = getOffsetWithFallback(dial, filerAddr, BackupKeyPrefix, sinkId, BackupKeyPrefix, legacySinkId)
	if err != nil || got != 222 {
		t.Fatalf("destination-scoped read = (%d, %v), want (222, nil)", got, err)
	}

	// another backup advancing the historical key no longer moves this one
	if err := setOffset(dial, filerAddr, BackupKeyPrefix, legacySinkId, 333); err != nil {
		t.Fatalf("advance historical offset: %v", err)
	}
	got, err = getOffsetWithFallback(dial, filerAddr, BackupKeyPrefix, sinkId, BackupKeyPrefix, legacySinkId)
	if err != nil || got != 222 {
		t.Fatalf("read after historical advance = (%d, %v), want (222, nil)", got, err)
	}
}

// The collision scenario from the report, over the real KV wire path: two
// backups whose configurations differ only in endpoint and bucket write
// their checkpoints without disturbing each other's.
func TestBackupOffset_DistinctDestinationsDoNotShare(t *testing.T) {
	filerAddr, dial := startKvFiler(t)

	idA, legacyA := backupCheckpointIds("/", &stubSink{name: "s3", dir: "/", destination: "s3.us-west-004.backblazeb2.com|seaweed-backup-a|/"})
	idB, legacyB := backupCheckpointIds("/", &stubSink{name: "s3", dir: "/", destination: "s3.us-east-005.backblazeb2.com|seaweed-backup-b|/"})

	if err := setOffset(dial, filerAddr, BackupKeyPrefix, idA, 1000); err != nil {
		t.Fatalf("write A: %v", err)
	}
	if err := setOffset(dial, filerAddr, BackupKeyPrefix, idB, 2000); err != nil {
		t.Fatalf("write B: %v", err)
	}

	if got, err := getOffsetWithFallback(dial, filerAddr, BackupKeyPrefix, idA, BackupKeyPrefix, legacyA); err != nil || got != 1000 {
		t.Fatalf("A reads (%d, %v), want (1000, nil)", got, err)
	}
	if got, err := getOffsetWithFallback(dial, filerAddr, BackupKeyPrefix, idB, BackupKeyPrefix, legacyB); err != nil || got != 2000 {
		t.Fatalf("B reads (%d, %v), want (2000, nil)", got, err)
	}
}
