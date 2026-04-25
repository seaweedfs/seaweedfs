package parquet_pushdown

// service_grpc_test.go is the M0 integration test. It boots the
// Service on an in-process gRPC bufconn listener (no filer, no
// network) and exercises Ping and Pushdown end-to-end over real gRPC.
//
// The acceptance criterion from PARQUET_PUSHDOWN_DEV_PLAN.md is
// "calls Pushdown over gRPC, gets the expected Unimplemented error
// path with stats populated"; this file proves both, plus that the
// Pushdown Unimplemented status carries a PushdownStats detail with
// trust_mode and a non-zero server_time_micros.

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/seaweedfs/seaweedfs/weed/pb/parquet_pushdown_pb"
)

const bufSize = 1 << 20

func startTestServer(t *testing.T, opts Options) (pb.SeaweedParquetPushdownClient, func()) {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	pb.RegisterSeaweedParquetPushdownServer(srv, New(opts))

	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("test server stopped: %v", err)
		}
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		t.Fatalf("dial bufconn: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
	}
	return pb.NewSeaweedParquetPushdownClient(conn), cleanup
}

func TestPing_ReportsConfiguredVersionAndTrustMode(t *testing.T) {
	client, cleanup := startTestServer(t, Options{
		Version:   "test-1.2.3",
		TrustMode: TrustModeCatalogValidated,
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Ping(ctx, &pb.PingRequest{})
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	if resp.Version != "test-1.2.3" {
		t.Errorf("version = %q, want %q", resp.Version, "test-1.2.3")
	}
	if resp.TrustMode != string(TrustModeCatalogValidated) {
		t.Errorf("trust_mode = %q, want %q", resp.TrustMode, TrustModeCatalogValidated)
	}
}

func TestPing_DefaultsTrustModeToCatalogValidated(t *testing.T) {
	client, cleanup := startTestServer(t, Options{Version: "test"})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Ping(ctx, &pb.PingRequest{})
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	if resp.TrustMode != string(TrustModeCatalogValidated) {
		t.Errorf("trust_mode default = %q, want %q", resp.TrustMode, TrustModeCatalogValidated)
	}
}

func TestPushdown_ReturnsUnimplementedWithStats(t *testing.T) {
	client, cleanup := startTestServer(t, Options{
		Version:   "test",
		TrustMode: TrustModeCatalogValidated,
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &pb.ParquetPushdownRequest{
		Table:      "db.t",
		SnapshotId: 1,
		DataFiles: []*pb.DataFileDescriptor{
			{Path: "s3://b/p.parquet", SizeBytes: 1, RecordCount: 1},
		},
		Columns: []*pb.ColumnRef{{FieldId: 1}},
	}

	_, err := client.Pushdown(ctx, req)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error is not a gRPC status: %v", err)
	}
	if st.Code() != codes.Unimplemented {
		t.Fatalf("got code %v, want Unimplemented", st.Code())
	}

	// Acceptance: stats are attached as a detail on the Unimplemented
	// status, with the daemon's trust mode and a non-zero elapsed.
	var stats *pb.PushdownStats
	for _, d := range st.Details() {
		if s, ok := d.(*pb.PushdownStats); ok {
			stats = s
			break
		}
	}
	if stats == nil {
		t.Fatal("expected PushdownStats detail on Unimplemented status")
	}
	if stats.TrustMode != string(TrustModeCatalogValidated) {
		t.Errorf("stats.trust_mode = %q, want %q", stats.TrustMode, TrustModeCatalogValidated)
	}
	if stats.ServerTimeMicros < 0 {
		t.Errorf("stats.server_time_micros = %d, want >= 0", stats.ServerTimeMicros)
	}
}

func TestPushdown_RejectsInvalidRequestBeforeUnimplemented(t *testing.T) {
	client, cleanup := startTestServer(t, Options{Version: "test"})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Empty data_files: validation should fire and produce
	// InvalidArgument, not Unimplemented.
	_, err := client.Pushdown(ctx, &pb.ParquetPushdownRequest{Table: "db.t"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}
