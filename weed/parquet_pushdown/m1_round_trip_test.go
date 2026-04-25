package parquet_pushdown

// m1_round_trip_test.go is the M1 round-trip integration test from
// PARQUET_PUSHDOWN_DEV_PLAN.md:
//
//   "Round-trip integration test against a 3-file Iceberg table built
//   in test/parquet_pushdown/fixtures/.
//
//   Footer cache hit-rate test (second call avoids re-parse; verified
//   via PushdownStats.FooterCacheHits)."
//
// It generates three Parquet files in a temp dir using parquet-go,
// boots the SeaweedParquetPushdown service on a bufconn listener
// with LocalLoader pointing at the temp dir, and submits real RPCs.
// It does NOT require a filer; the filer-backed loader is M2+.

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/seaweedfs/seaweedfs/weed/pb/parquet_pushdown_pb"
)

type fixtureRow struct {
	ID    int64
	Name  string
	Value float64
}

func writeParquetFile(t *testing.T, path string, rows []fixtureRow) {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[fixtureRow](&buf, parquet.SchemaOf(fixtureRow{}))
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
}

func makeFixtureTable(t *testing.T) (dir string, files []string, sizes []int64) {
	t.Helper()
	dir = t.TempDir()
	for i, count := range []int{500, 750, 1000} {
		name := filepath.Join(dir, "part-0000"+string(rune('1'+i))+".parquet")
		rows := make([]fixtureRow, count)
		for k := range rows {
			rows[k] = fixtureRow{ID: int64(i*10000 + k), Name: "r", Value: float64(k)}
		}
		writeParquetFile(t, name, rows)
		st, err := os.Stat(name)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		files = append(files, name)
		sizes = append(sizes, st.Size())
	}
	return dir, files, sizes
}

func startServiceForFixtures(t *testing.T) (pb.SeaweedParquetPushdownClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	svc := New(Options{
		Version:   "m1-test",
		TrustMode: TrustModeCatalogValidated,
		Loader:    LocalLoader{},
		CacheSize: 32,
	})
	pb.RegisterSeaweedParquetPushdownServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	return pb.NewSeaweedParquetPushdownClient(conn), func() {
		_ = conn.Close()
		srv.Stop()
	}
}

func descriptorsFor(t *testing.T, files []string, sizes []int64) []*pb.DataFileDescriptor {
	t.Helper()
	out := make([]*pb.DataFileDescriptor, len(files))
	for i, p := range files {
		out[i] = &pb.DataFileDescriptor{Path: p, SizeBytes: sizes[i], RecordCount: 1}
	}
	return out
}

// Round-trip: three files, project two columns, no predicate.
// Expect 3 files × N row groups × 2 columns of FileRanges, each with
// a positive offset / length within the underlying file.
func TestPushdown_M1_RoundTrip(t *testing.T) {
	_, files, sizes := makeFixtureTable(t)
	client, cleanup := startServiceForFixtures(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Pushdown(ctx, &pb.ParquetPushdownRequest{
		Table:      "db.t",
		SnapshotId: 1,
		DataFiles:  descriptorsFor(t, files, sizes),
		Columns:    []*pb.ColumnRef{{Path: "ID"}, {Path: "Value"}},
	})
	if err != nil {
		t.Fatalf("pushdown: %v", err)
	}
	if len(resp.FileRanges) == 0 {
		t.Fatal("expected file ranges, got none")
	}

	// Per file, every range must reference that file and fit inside it.
	byFile := map[string]int64{}
	for i, p := range files {
		byFile[p] = sizes[i]
	}
	for i, r := range resp.FileRanges {
		size, ok := byFile[r.File]
		if !ok {
			t.Fatalf("range %d references unknown file %q", i, r.File)
		}
		if r.Offset <= 0 || r.Length <= 0 {
			t.Errorf("range %d has invalid bounds (offset=%d length=%d)", i, r.Offset, r.Length)
		}
		if r.Offset+r.Length > size {
			t.Errorf("range %d overruns file %q (offset=%d length=%d size=%d)", i, r.File, r.Offset, r.Length, size)
		}
	}

	// First call: every footer is a miss; no hits.
	if got := resp.Stats.FooterCacheHits; got != 0 {
		t.Errorf("first call FooterCacheHits = %d, want 0", got)
	}
	if got := resp.Stats.FooterCacheMisses; got != int64(len(files)) {
		t.Errorf("first call FooterCacheMisses = %d, want %d", got, len(files))
	}
}

// Footer cache hit-rate: a second identical request must hit the
// cache for every file (zero misses).
func TestPushdown_M1_FooterCacheHits(t *testing.T) {
	_, files, sizes := makeFixtureTable(t)
	client, cleanup := startServiceForFixtures(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.ParquetPushdownRequest{
		Table:      "db.t",
		SnapshotId: 1,
		DataFiles:  descriptorsFor(t, files, sizes),
		Columns:    []*pb.ColumnRef{{Path: "ID"}},
	}

	if _, err := client.Pushdown(ctx, req); err != nil {
		t.Fatalf("warm-up: %v", err)
	}

	resp, err := client.Pushdown(ctx, req)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got := resp.Stats.FooterCacheHits; got != int64(len(files)) {
		t.Errorf("FooterCacheHits = %d, want %d", got, len(files))
	}
	if got := resp.Stats.FooterCacheMisses; got != 0 {
		t.Errorf("FooterCacheMisses = %d, want 0", got)
	}
}

// Empty Columns list returns ranges for every column in every row group.
func TestPushdown_M1_NoColumnProjection(t *testing.T) {
	_, files, sizes := makeFixtureTable(t)
	client, cleanup := startServiceForFixtures(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Pushdown(ctx, &pb.ParquetPushdownRequest{
		Table:      "db.t",
		SnapshotId: 1,
		DataFiles:  descriptorsFor(t, files, sizes),
	})
	if err != nil {
		t.Fatalf("pushdown: %v", err)
	}
	// Every fixture has 3 columns; the count must be a multiple of 3.
	if len(resp.FileRanges)%3 != 0 {
		t.Errorf("FileRanges count %d not a multiple of column count 3", len(resp.FileRanges))
	}
}

// An unknown column path is rejected with InvalidArgument before any
// range is emitted.
func TestPushdown_M1_UnknownColumn(t *testing.T) {
	_, files, sizes := makeFixtureTable(t)
	client, cleanup := startServiceForFixtures(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Pushdown(ctx, &pb.ParquetPushdownRequest{
		Table:      "db.t",
		SnapshotId: 1,
		DataFiles:  descriptorsFor(t, files, sizes),
		Columns:    []*pb.ColumnRef{{Path: "definitely_not_a_real_column"}},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// A field-id-only ColumnRef without a Path hint is unsupported in M1
// because schema-based resolution lands in M3.
func TestPushdown_M1_FieldIdOnlyRejected(t *testing.T) {
	_, files, sizes := makeFixtureTable(t)
	client, cleanup := startServiceForFixtures(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Pushdown(ctx, &pb.ParquetPushdownRequest{
		Table:      "db.t",
		SnapshotId: 1,
		DataFiles:  descriptorsFor(t, files, sizes),
		Columns:    []*pb.ColumnRef{{FieldId: 1}}, // no Path
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
