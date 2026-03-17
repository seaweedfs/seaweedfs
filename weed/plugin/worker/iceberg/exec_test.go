package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ---------------------------------------------------------------------------
// Fake filer server for execution tests
// ---------------------------------------------------------------------------

// fakeFilerServer is an in-memory filer that implements the gRPC methods used
// by the iceberg maintenance handler.
type fakeFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu           sync.Mutex
	entries      map[string]map[string]*filer_pb.Entry // dir → name → entry
	beforeUpdate func(*fakeFilerServer, *filer_pb.UpdateEntryRequest) error

	// Counters for assertions
	createCalls int
	updateCalls int
	deleteCalls int
}

func newFakeFilerServer() *fakeFilerServer {
	return &fakeFilerServer{
		entries: make(map[string]map[string]*filer_pb.Entry),
	}
}

func (f *fakeFilerServer) putEntry(dir, name string, entry *filer_pb.Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.entries[dir]; !ok {
		f.entries[dir] = make(map[string]*filer_pb.Entry)
	}
	f.entries[dir][name] = entry
}

func (f *fakeFilerServer) getEntry(dir, name string) *filer_pb.Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	if dirEntries, ok := f.entries[dir]; ok {
		return dirEntries[name]
	}
	return nil
}

func (f *fakeFilerServer) listDir(dir string) []*filer_pb.Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	dirEntries, ok := f.entries[dir]
	if !ok {
		return nil
	}
	result := make([]*filer_pb.Entry, 0, len(dirEntries))
	for _, e := range dirEntries {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

func (f *fakeFilerServer) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	entry := f.getEntry(req.Directory, req.Name)
	if entry == nil {
		return nil, status.Errorf(codes.NotFound, "entry not found: %s/%s", req.Directory, req.Name)
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
}

func (f *fakeFilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	entries := f.listDir(req.Directory)
	if entries == nil {
		return nil // empty directory
	}

	var sent uint32
	for _, entry := range entries {
		if req.Prefix != "" && !strings.HasPrefix(entry.Name, req.Prefix) {
			continue
		}
		if req.StartFromFileName != "" {
			if req.InclusiveStartFrom {
				if entry.Name < req.StartFromFileName {
					continue
				}
			} else {
				if entry.Name <= req.StartFromFileName {
					continue
				}
			}
		}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
		sent++
		if req.Limit > 0 && sent >= req.Limit {
			break
		}
	}
	return nil
}

func (f *fakeFilerServer) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	f.createCalls++
	f.mu.Unlock()

	f.putEntry(req.Directory, req.Entry.Name, req.Entry)
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *fakeFilerServer) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	f.updateCalls++
	beforeUpdate := f.beforeUpdate
	f.beforeUpdate = nil
	f.mu.Unlock()

	if beforeUpdate != nil {
		if err := beforeUpdate(f, req); err != nil {
			return nil, err
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	dirEntries, ok := f.entries[req.Directory]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "entry not found: %s/%s", req.Directory, req.Entry.Name)
	}
	current := dirEntries[req.Entry.Name]
	if current == nil {
		return nil, status.Errorf(codes.NotFound, "entry not found: %s/%s", req.Directory, req.Entry.Name)
	}
	for key, expectedValue := range req.ExpectedExtended {
		actualValue, ok := current.Extended[key]
		if ok {
			if !bytes.Equal(actualValue, expectedValue) {
				return nil, status.Errorf(codes.FailedPrecondition, "extended attribute %q changed", key)
			}
			continue
		}
		if len(expectedValue) > 0 {
			return nil, status.Errorf(codes.FailedPrecondition, "extended attribute %q changed", key)
		}
	}

	dirEntries[req.Entry.Name] = req.Entry
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *fakeFilerServer) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.mu.Lock()
	f.deleteCalls++

	if dirEntries, ok := f.entries[req.Directory]; ok {
		delete(dirEntries, req.Name)
	}
	f.mu.Unlock()
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (f *fakeFilerServer) Ping(_ context.Context, _ *filer_pb.PingRequest) (*filer_pb.PingResponse, error) {
	now := time.Now().UnixNano()
	return &filer_pb.PingResponse{
		StartTimeNs:  now,
		RemoteTimeNs: now,
		StopTimeNs:   now,
	}, nil
}

// startFakeFiler starts a gRPC server and returns a connected client.
func startFakeFiler(t *testing.T) (*fakeFilerServer, filer_pb.SeaweedFilerClient) {
	t.Helper()
	fakeServer, client, _ := startFakeFilerWithAddress(t)
	return fakeServer, client
}

func startFakeFilerWithAddress(t *testing.T) (*fakeFilerServer, filer_pb.SeaweedFilerClient, string) {
	t.Helper()
	fakeServer := newFakeFilerServer()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, fakeServer)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	client := filer_pb.NewSeaweedFilerClient(conn)
	deadline := time.Now().Add(5 * time.Second)
	for {
		pingCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err := client.Ping(pingCtx, &filer_pb.PingRequest{})
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("filer not ready: %v", err)
		}
		code := status.Code(err)
		if code != codes.Unavailable && code != codes.DeadlineExceeded && code != codes.Canceled {
			t.Fatalf("unexpected filer readiness error: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	return fakeServer, client, listener.Addr().String()
}

// ---------------------------------------------------------------------------
// Helpers to populate the fake filer with Iceberg table state
// ---------------------------------------------------------------------------

// tableSetup holds the state needed to set up a test table in the fake filer.
type tableSetup struct {
	BucketName string
	Namespace  string
	TableName  string
	Snapshots  []table.Snapshot
}

func (ts tableSetup) tablePath() string {
	return path.Join(ts.Namespace, ts.TableName)
}

// populateTable creates the directory hierarchy and metadata entries in the
// fake filer for a table, writes manifest files referenced by snapshots,
// and returns the built metadata.
func populateTable(t *testing.T, fs *fakeFilerServer, setup tableSetup) table.Metadata {
	t.Helper()

	meta := buildTestMetadata(t, setup.Snapshots)
	fullMetadataJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}

	// Build internal metadata xattr
	const metadataVersion = 1
	internalMeta := map[string]interface{}{
		"metadataVersion":  metadataVersion,
		"metadataLocation": path.Join("metadata", fmt.Sprintf("v%d.metadata.json", metadataVersion)),
		"metadata": map[string]interface{}{
			"fullMetadata": json.RawMessage(fullMetadataJSON),
		},
	}
	xattr, err := json.Marshal(internalMeta)
	if err != nil {
		t.Fatalf("marshal xattr: %v", err)
	}

	bucketsPath := s3tables.TablesPath // "/buckets"
	bucketPath := path.Join(bucketsPath, setup.BucketName)
	nsPath := path.Join(bucketPath, setup.Namespace)
	tableFilerPath := path.Join(nsPath, setup.TableName)

	// Register bucket entry (marked as table bucket)
	fs.putEntry(bucketsPath, setup.BucketName, &filer_pb.Entry{
		Name:        setup.BucketName,
		IsDirectory: true,
		Extended: map[string][]byte{
			s3tables.ExtendedKeyTableBucket: []byte("true"),
		},
	})

	// Register namespace entry
	fs.putEntry(bucketPath, setup.Namespace, &filer_pb.Entry{
		Name:        setup.Namespace,
		IsDirectory: true,
	})

	// Register table entry with metadata xattr
	fs.putEntry(nsPath, setup.TableName, &filer_pb.Entry{
		Name:        setup.TableName,
		IsDirectory: true,
		Extended: map[string][]byte{
			s3tables.ExtendedKeyMetadata:        xattr,
			s3tables.ExtendedKeyMetadataVersion: metadataVersionXattr(metadataVersion),
		},
	})

	// Create metadata/ and data/ directory placeholders
	metaDir := path.Join(tableFilerPath, "metadata")
	dataDir := path.Join(tableFilerPath, "data")

	// Write manifest files for each snapshot that has a ManifestList
	schema := meta.CurrentSchema()
	spec := meta.PartitionSpec()
	version := meta.Version()

	for _, snap := range setup.Snapshots {
		if snap.ManifestList == "" {
			continue
		}

		// Create a minimal manifest with one dummy entry for this snapshot
		dfBuilder, err := iceberg.NewDataFileBuilder(
			spec,
			iceberg.EntryContentData,
			fmt.Sprintf("data/snap-%d-data.parquet", snap.SnapshotID),
			iceberg.ParquetFile,
			map[int]any{},
			nil, nil,
			10,   // recordCount
			4096, // fileSizeBytes
		)
		if err != nil {
			t.Fatalf("build data file for snap %d: %v", snap.SnapshotID, err)
		}
		snapID := snap.SnapshotID
		entry := iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED,
			&snapID,
			nil, nil,
			dfBuilder.Build(),
		)

		// Write manifest
		manifestFileName := fmt.Sprintf("manifest-%d.avro", snap.SnapshotID)
		manifestPath := path.Join("metadata", manifestFileName)
		var manifestBuf bytes.Buffer
		mf, err := iceberg.WriteManifest(manifestPath, &manifestBuf, version, spec, schema, snap.SnapshotID, []iceberg.ManifestEntry{entry})
		if err != nil {
			t.Fatalf("write manifest for snap %d: %v", snap.SnapshotID, err)
		}

		fs.putEntry(metaDir, manifestFileName, &filer_pb.Entry{
			Name: manifestFileName,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				FileSize: uint64(manifestBuf.Len()),
			},
			Content: manifestBuf.Bytes(),
		})

		// Write manifest list
		manifestListFileName := path.Base(snap.ManifestList)
		var mlBuf bytes.Buffer
		parentSnap := snap.ParentSnapshotID
		seqNum := snap.SequenceNumber
		if err := iceberg.WriteManifestList(version, &mlBuf, snap.SnapshotID, parentSnap, &seqNum, 0, []iceberg.ManifestFile{mf}); err != nil {
			t.Fatalf("write manifest list for snap %d: %v", snap.SnapshotID, err)
		}

		fs.putEntry(metaDir, manifestListFileName, &filer_pb.Entry{
			Name: manifestListFileName,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				FileSize: uint64(mlBuf.Len()),
			},
			Content: mlBuf.Bytes(),
		})

		// Write a dummy data file
		dataFileName := fmt.Sprintf("snap-%d-data.parquet", snap.SnapshotID)
		fs.putEntry(dataDir, dataFileName, &filer_pb.Entry{
			Name: dataFileName,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				FileSize: 4096,
			},
			Content: []byte("fake-parquet-data"),
		})
	}

	return meta
}

func writeCurrentSnapshotManifests(t *testing.T, fs *fakeFilerServer, setup tableSetup, meta table.Metadata, manifestEntries [][]iceberg.ManifestEntry) {
	t.Helper()

	currentSnap := meta.CurrentSnapshot()
	if currentSnap == nil {
		t.Fatal("current snapshot is required")
	}

	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	version := meta.Version()
	schema := meta.CurrentSchema()
	spec := meta.PartitionSpec()

	var manifests []iceberg.ManifestFile
	for i, entries := range manifestEntries {
		manifestName := fmt.Sprintf("detect-manifest-%d.avro", i+1)
		var manifestBuf bytes.Buffer
		mf, err := iceberg.WriteManifest(
			path.Join("metadata", manifestName),
			&manifestBuf,
			version,
			spec,
			schema,
			currentSnap.SnapshotID,
			entries,
		)
		if err != nil {
			t.Fatalf("write manifest %d: %v", i+1, err)
		}
		fs.putEntry(metaDir, manifestName, &filer_pb.Entry{
			Name: manifestName,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				FileSize: uint64(manifestBuf.Len()),
			},
			Content: manifestBuf.Bytes(),
		})
		manifests = append(manifests, mf)
	}

	var manifestListBuf bytes.Buffer
	seqNum := currentSnap.SequenceNumber
	if err := iceberg.WriteManifestList(version, &manifestListBuf, currentSnap.SnapshotID, currentSnap.ParentSnapshotID, &seqNum, 0, manifests); err != nil {
		t.Fatalf("write current manifest list: %v", err)
	}
	fs.putEntry(metaDir, path.Base(currentSnap.ManifestList), &filer_pb.Entry{
		Name: path.Base(currentSnap.ManifestList),
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Unix(),
			FileSize: uint64(manifestListBuf.Len()),
		},
		Content: manifestListBuf.Bytes(),
	})
}

func makeManifestEntries(t *testing.T, specs []testEntrySpec, snapshotID int64) []iceberg.ManifestEntry {
	t.Helper()
	return makeManifestEntriesWithSnapshot(t, specs, snapshotID, iceberg.EntryStatusADDED)
}

// ---------------------------------------------------------------------------
// Recording senders for Execute tests
// ---------------------------------------------------------------------------

type recordingExecutionSender struct {
	mu        sync.Mutex
	progress  []*plugin_pb.JobProgressUpdate
	completed *plugin_pb.JobCompleted
}

func (r *recordingExecutionSender) SendProgress(p *plugin_pb.JobProgressUpdate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progress = append(r.progress, p)
	return nil
}

func (r *recordingExecutionSender) SendCompleted(c *plugin_pb.JobCompleted) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed = c
	return nil
}

// ---------------------------------------------------------------------------
// Execution tests
// ---------------------------------------------------------------------------

func TestExpireSnapshotsExecution(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().Add(-10 * time.Second).UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
			{SnapshotID: 2, TimestampMs: now + 1, ManifestList: "metadata/snap-2.avro"},
			{SnapshotID: 3, TimestampMs: now + 2, ManifestList: "metadata/snap-3.avro"},
		},
	}
	populateTable(t, fs, setup)

	handler := NewHandler(nil)
	config := Config{
		SnapshotRetentionHours: 0, // expire everything eligible
		MaxSnapshotsToKeep:     1, // keep only 1
		MaxCommitRetries:       3,
		Operations:             "expire_snapshots",
	}

	result, _, err := handler.expireSnapshots(context.Background(), client, setup.BucketName, setup.tablePath(), config)
	if err != nil {
		t.Fatalf("expireSnapshots failed: %v", err)
	}

	if !strings.Contains(result, "expired") {
		t.Errorf("expected result to mention expiration, got %q", result)
	}
	t.Logf("expireSnapshots result: %s", result)

	// Verify the metadata was updated (update calls > 0)
	fs.mu.Lock()
	updates := fs.updateCalls
	fs.mu.Unlock()
	if updates == 0 {
		t.Error("expected at least one UpdateEntry call for xattr update")
	}
}

func TestExpireSnapshotsNothingToExpire(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().Add(-10 * time.Second).UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "ns",
		TableName:  "tbl",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
		},
	}
	populateTable(t, fs, setup)

	handler := NewHandler(nil)
	config := Config{
		SnapshotRetentionHours: 24 * 365, // very long retention
		MaxSnapshotsToKeep:     10,
		MaxCommitRetries:       3,
	}

	result, _, err := handler.expireSnapshots(context.Background(), client, setup.BucketName, setup.tablePath(), config)
	if err != nil {
		t.Fatalf("expireSnapshots failed: %v", err)
	}
	if result != "no snapshots expired" {
		t.Errorf("expected 'no snapshots expired', got %q", result)
	}
}

func TestRemoveOrphansExecution(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
		},
	}
	populateTable(t, fs, setup)

	// Add orphan files (old enough to be removed)
	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	dataDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "data")
	oldTime := time.Now().Add(-200 * time.Hour).Unix()

	fs.putEntry(metaDir, "orphan-old.avro", &filer_pb.Entry{
		Name:       "orphan-old.avro",
		Attributes: &filer_pb.FuseAttributes{Mtime: oldTime},
	})
	fs.putEntry(dataDir, "orphan-data.parquet", &filer_pb.Entry{
		Name:       "orphan-data.parquet",
		Attributes: &filer_pb.FuseAttributes{Mtime: oldTime},
	})
	// Add a recent orphan that should NOT be removed (within safety window)
	fs.putEntry(dataDir, "recent-orphan.parquet", &filer_pb.Entry{
		Name:       "recent-orphan.parquet",
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
	})

	handler := NewHandler(nil)
	config := Config{
		OrphanOlderThanHours: 72,
		MaxCommitRetries:     3,
	}

	result, _, err := handler.removeOrphans(context.Background(), client, setup.BucketName, setup.tablePath(), config)
	if err != nil {
		t.Fatalf("removeOrphans failed: %v", err)
	}

	if !strings.Contains(result, "removed 2 orphan") {
		t.Errorf("expected 2 orphans removed, got %q", result)
	}

	// Verify orphan files were deleted
	if fs.getEntry(metaDir, "orphan-old.avro") != nil {
		t.Error("orphan-old.avro should have been deleted")
	}
	if fs.getEntry(dataDir, "orphan-data.parquet") != nil {
		t.Error("orphan-data.parquet should have been deleted")
	}
	// Recent orphan should still exist
	if fs.getEntry(dataDir, "recent-orphan.parquet") == nil {
		t.Error("recent-orphan.parquet should NOT have been deleted (within safety window)")
	}
}

func TestRemoveOrphansPreservesReferencedFiles(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "ns",
		TableName:  "tbl",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
		},
	}
	populateTable(t, fs, setup)

	handler := NewHandler(nil)
	config := Config{
		OrphanOlderThanHours: 0, // no safety window — remove immediately
		MaxCommitRetries:     3,
	}

	result, _, err := handler.removeOrphans(context.Background(), client, setup.BucketName, setup.tablePath(), config)
	if err != nil {
		t.Fatalf("removeOrphans failed: %v", err)
	}

	if !strings.Contains(result, "removed 0 orphan") {
		t.Errorf("expected 0 orphans removed (all files are referenced), got %q", result)
	}

	// Verify referenced files are still present
	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	if fs.getEntry(metaDir, "snap-1.avro") == nil {
		t.Error("snap-1.avro (referenced manifest list) should not have been deleted")
	}
	if fs.getEntry(metaDir, "manifest-1.avro") == nil {
		t.Error("manifest-1.avro (referenced manifest) should not have been deleted")
	}
}

func TestRewriteManifestsExecution(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()

	// Create a table with a single snapshot — we'll add extra small manifests
	// to the manifest list so there's something to rewrite.
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
		},
	}
	meta := populateTable(t, fs, setup)
	schema := meta.CurrentSchema()
	spec := meta.PartitionSpec()
	version := meta.Version()

	// Build 5 small manifests and write them + a manifest list pointing to all of them
	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	var allManifests []iceberg.ManifestFile

	for i := 1; i <= 5; i++ {
		dfBuilder, err := iceberg.NewDataFileBuilder(
			spec,
			iceberg.EntryContentData,
			fmt.Sprintf("data/rewrite-%d.parquet", i),
			iceberg.ParquetFile,
			map[int]any{},
			nil, nil,
			1,
			1024,
		)
		if err != nil {
			t.Fatalf("build data file %d: %v", i, err)
		}
		snapID := int64(1)
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfBuilder.Build())

		manifestName := fmt.Sprintf("small-manifest-%d.avro", i)
		var buf bytes.Buffer
		mf, err := iceberg.WriteManifest(path.Join("metadata", manifestName), &buf, version, spec, schema, 1, []iceberg.ManifestEntry{entry})
		if err != nil {
			t.Fatalf("write small manifest %d: %v", i, err)
		}
		fs.putEntry(metaDir, manifestName, &filer_pb.Entry{
			Name:       manifestName,
			Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
			Content:    buf.Bytes(),
		})
		allManifests = append(allManifests, mf)
	}

	// Overwrite the manifest list with all 5 manifests
	var mlBuf bytes.Buffer
	seqNum := int64(1)
	if err := iceberg.WriteManifestList(version, &mlBuf, 1, nil, &seqNum, 0, allManifests); err != nil {
		t.Fatalf("write manifest list: %v", err)
	}
	fs.putEntry(metaDir, "snap-1.avro", &filer_pb.Entry{
		Name:       "snap-1.avro",
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
		Content:    mlBuf.Bytes(),
	})

	handler := NewHandler(nil)
	config := Config{
		MinInputFiles:    3, // threshold to trigger rewrite (5 >= 3)
		MaxCommitRetries: 3,
	}

	result, _, err := handler.rewriteManifests(context.Background(), client, setup.BucketName, setup.tablePath(), config)
	if err != nil {
		t.Fatalf("rewriteManifests failed: %v", err)
	}

	if !strings.Contains(result, "rewrote 5 manifests into 1") {
		t.Errorf("expected '5 manifests into 1', got %q", result)
	}
	t.Logf("rewriteManifests result: %s", result)

	// Verify a new metadata file and merged manifest were written
	fs.mu.Lock()
	creates := fs.createCalls
	updates := fs.updateCalls
	fs.mu.Unlock()

	if creates < 3 {
		// At minimum: merged manifest, manifest list, new metadata file
		t.Errorf("expected at least 3 CreateEntry calls, got %d", creates)
	}
	if updates == 0 {
		t.Error("expected at least one UpdateEntry call for xattr update")
	}
}

func TestRewriteManifestsBelowThreshold(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "ns",
		TableName:  "tbl",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
		},
	}
	populateTable(t, fs, setup)

	handler := NewHandler(nil)
	config := Config{
		MinInputFiles:         10,
		MinManifestsToRewrite: 10, // threshold higher than actual manifest count (1)
		MaxCommitRetries:      3,
	}

	result, _, err := handler.rewriteManifests(context.Background(), client, setup.BucketName, setup.tablePath(), config)
	if err != nil {
		t.Fatalf("rewriteManifests failed: %v", err)
	}

	if !strings.Contains(result, "below threshold") {
		t.Errorf("expected 'below threshold', got %q", result)
	}
}

func TestFullExecuteFlow(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
			{SnapshotID: 2, TimestampMs: now + 1, ManifestList: "metadata/snap-2.avro"},
			{SnapshotID: 3, TimestampMs: now + 2, ManifestList: "metadata/snap-3.avro"},
		},
	}
	populateTable(t, fs, setup)

	// Add an orphan
	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	fs.putEntry(metaDir, "orphan.avro", &filer_pb.Entry{
		Name:       "orphan.avro",
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Add(-200 * time.Hour).Unix()},
	})

	handler := NewHandler(nil)

	// We need to build the request manually since Execute takes gRPC types
	// but we're connecting directly
	request := &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{
			JobId:   "test-job-1",
			JobType: jobType,
			Parameters: map[string]*plugin_pb.ConfigValue{
				"bucket_name":   {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: setup.BucketName}},
				"namespace":     {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: setup.Namespace}},
				"table_name":    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: setup.TableName}},
				"table_path":    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: setup.tablePath()}},
				"filer_address": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "not-used"}},
			},
		},
		WorkerConfigValues: map[string]*plugin_pb.ConfigValue{
			"snapshot_retention_hours": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
			"max_snapshots_to_keep":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
			"orphan_older_than_hours":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 72}},
			"max_commit_retries":       {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 3}},
			"min_input_files":          {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 100}}, // high threshold to skip rewrite
			"operations":               {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "expire_snapshots,remove_orphans"}},
		},
	}

	// Execute uses grpc.NewClient internally, but we need to pass our existing
	// client. Call operations directly instead of full Execute to avoid the
	// grpc.NewClient call which requires a real address.
	workerConfig := ParseConfig(request.GetWorkerConfigValues())
	ops, err := parseOperations(workerConfig.Operations)
	if err != nil {
		t.Fatalf("parseOperations: %v", err)
	}

	var results []string
	for _, op := range ops {
		var opResult string
		var opErr error
		switch op {
		case "expire_snapshots":
			opResult, _, opErr = handler.expireSnapshots(context.Background(), client, setup.BucketName, setup.tablePath(), workerConfig)
		case "remove_orphans":
			opResult, _, opErr = handler.removeOrphans(context.Background(), client, setup.BucketName, setup.tablePath(), workerConfig)
		case "rewrite_manifests":
			opResult, _, opErr = handler.rewriteManifests(context.Background(), client, setup.BucketName, setup.tablePath(), workerConfig)
		}
		if opErr != nil {
			t.Fatalf("operation %s failed: %v", op, opErr)
		}
		results = append(results, fmt.Sprintf("%s: %s", op, opResult))
	}

	t.Logf("Full execution results: %s", strings.Join(results, "; "))

	// Verify snapshots were expired
	if !strings.Contains(results[0], "expired") {
		t.Errorf("expected snapshot expiration, got %q", results[0])
	}

	// Verify orphan was removed
	if !strings.Contains(results[1], "removed") {
		t.Errorf("expected orphan removal, got %q", results[1])
	}
	if fs.getEntry(metaDir, "orphan.avro") != nil {
		t.Error("orphan.avro should have been deleted")
	}

}

func TestDetectWithFakeFiler(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
			{SnapshotID: 2, TimestampMs: now + 1, ManifestList: "metadata/snap-2.avro"},
			{SnapshotID: 3, TimestampMs: now + 2, ManifestList: "metadata/snap-3.avro"},
		},
	}
	populateTable(t, fs, setup)

	handler := NewHandler(nil)

	config := Config{
		SnapshotRetentionHours: 0, // everything is expired
		MaxSnapshotsToKeep:     2, // 3 > 2, needs maintenance
		MaxCommitRetries:       3,
	}

	tables, err := handler.scanTablesForMaintenance(
		context.Background(),
		client,
		config,
		"", "", "", // no filters
		0, // no limit
	)
	if err != nil {
		t.Fatalf("scanTablesForMaintenance failed: %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("expected 1 table needing maintenance, got %d", len(tables))
	}
	if tables[0].BucketName != setup.BucketName {
		t.Errorf("expected bucket %q, got %q", setup.BucketName, tables[0].BucketName)
	}
	if tables[0].TableName != setup.TableName {
		t.Errorf("expected table %q, got %q", setup.TableName, tables[0].TableName)
	}
}

func TestDetectWithFilters(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	// Create two tables in different buckets
	setup1 := tableSetup{
		BucketName: "bucket-a",
		Namespace:  "ns",
		TableName:  "table1",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
			{SnapshotID: 2, TimestampMs: now + 1, ManifestList: "metadata/snap-2.avro"},
			{SnapshotID: 3, TimestampMs: now + 2, ManifestList: "metadata/snap-3.avro"},
		},
	}
	setup2 := tableSetup{
		BucketName: "bucket-b",
		Namespace:  "ns",
		TableName:  "table2",
		Snapshots: []table.Snapshot{
			{SnapshotID: 4, TimestampMs: now + 3, ManifestList: "metadata/snap-4.avro"},
			{SnapshotID: 5, TimestampMs: now + 4, ManifestList: "metadata/snap-5.avro"},
			{SnapshotID: 6, TimestampMs: now + 5, ManifestList: "metadata/snap-6.avro"},
		},
	}
	populateTable(t, fs, setup1)
	populateTable(t, fs, setup2)

	handler := NewHandler(nil)
	config := Config{
		SnapshotRetentionHours: 0,
		MaxSnapshotsToKeep:     2,
		MaxCommitRetries:       3,
	}

	// Without filter: should find both
	tables, err := handler.scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables without filter, got %d", len(tables))
	}

	// With bucket filter: should find only one
	tables, err = handler.scanTablesForMaintenance(context.Background(), client, config, "bucket-a", "", "", 0)
	if err != nil {
		t.Fatalf("scan with filter failed: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table with bucket filter, got %d", len(tables))
	}
	if tables[0].BucketName != "bucket-a" {
		t.Errorf("expected bucket-a, got %q", tables[0].BucketName)
	}
}

func TestConnectToFilerSkipsUnreachableAddresses(t *testing.T) {
	handler := NewHandler(grpc.WithTransportCredentials(insecure.NewCredentials()))
	_, _, liveAddr := startFakeFilerWithAddress(t)

	deadListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for dead address: %v", err)
	}
	deadAddr := deadListener.Addr().String()
	_ = deadListener.Close()

	addr, conn, err := handler.connectToFiler(context.Background(), []string{deadAddr, liveAddr})
	if err != nil {
		t.Fatalf("connectToFiler failed: %v", err)
	}
	defer conn.Close()

	if addr != liveAddr {
		t.Fatalf("expected live address %q, got %q", liveAddr, addr)
	}
}

func TestConnectToFilerFailsWhenAllAddressesAreUnreachable(t *testing.T) {
	handler := NewHandler(grpc.WithTransportCredentials(insecure.NewCredentials()))

	deadListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for dead address: %v", err)
	}
	deadAddr := deadListener.Addr().String()
	_ = deadListener.Close()

	_, _, err = handler.connectToFiler(context.Background(), []string{deadAddr})
	if err == nil {
		t.Fatal("expected connectToFiler to fail")
	}
}

func TestDetectSchedulesCompactionWithoutSnapshotPressure(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro", SequenceNumber: 1},
		},
	}
	meta := populateTable(t, fs, setup)
	writeCurrentSnapshotManifests(t, fs, setup, meta, [][]iceberg.ManifestEntry{
		makeManifestEntries(t, []testEntrySpec{
			{path: "data/small-1.parquet", size: 1024, partition: map[int]any{}},
			{path: "data/small-2.parquet", size: 1024, partition: map[int]any{}},
			{path: "data/small-3.parquet", size: 1024, partition: map[int]any{}},
		}, 1),
	})

	handler := NewHandler(nil)
	config := Config{
		SnapshotRetentionHours: 24 * 365,
		MaxSnapshotsToKeep:     10,
		TargetFileSizeBytes:    4096,
		MinInputFiles:          2,
		Operations:             "compact",
	}

	tables, err := handler.scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scanTablesForMaintenance failed: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 compaction candidate, got %d", len(tables))
	}
}

func TestDetectSchedulesCompactionWithDeleteManifestPresent(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro", SequenceNumber: 1},
		},
	}
	meta := populateTable(t, fs, setup)

	currentSnap := meta.CurrentSnapshot()
	if currentSnap == nil {
		t.Fatal("current snapshot is required")
	}

	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	version := meta.Version()
	schema := meta.CurrentSchema()
	spec := meta.PartitionSpec()

	dataEntries := makeManifestEntries(t, []testEntrySpec{
		{path: "data/small-1.parquet", size: 1024, partition: map[int]any{}},
		{path: "data/small-2.parquet", size: 1024, partition: map[int]any{}},
		{path: "data/small-3.parquet", size: 1024, partition: map[int]any{}},
	}, currentSnap.SnapshotID)

	var dataManifestBuf bytes.Buffer
	dataManifestName := "detect-manifest-1.avro"
	dataManifest, err := iceberg.WriteManifest(
		path.Join("metadata", dataManifestName),
		&dataManifestBuf,
		version,
		spec,
		schema,
		currentSnap.SnapshotID,
		dataEntries,
	)
	if err != nil {
		t.Fatalf("write data manifest: %v", err)
	}
	fs.putEntry(metaDir, dataManifestName, &filer_pb.Entry{
		Name: dataManifestName,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Unix(),
			FileSize: uint64(dataManifestBuf.Len()),
		},
		Content: dataManifestBuf.Bytes(),
	})

	deleteManifest := iceberg.NewManifestFile(
		version,
		path.Join("metadata", "detect-delete-manifest.avro"),
		0,
		int32(spec.ID()),
		currentSnap.SnapshotID,
	).Content(iceberg.ManifestContentDeletes).
		SequenceNum(currentSnap.SequenceNumber, currentSnap.SequenceNumber).
		DeletedFiles(1).
		DeletedRows(1).
		Build()

	var manifestListBuf bytes.Buffer
	seqNum := currentSnap.SequenceNumber
	if err := iceberg.WriteManifestList(
		version,
		&manifestListBuf,
		currentSnap.SnapshotID,
		currentSnap.ParentSnapshotID,
		&seqNum,
		0,
		[]iceberg.ManifestFile{dataManifest, deleteManifest},
	); err != nil {
		t.Fatalf("write manifest list: %v", err)
	}
	fs.putEntry(metaDir, path.Base(currentSnap.ManifestList), &filer_pb.Entry{
		Name: path.Base(currentSnap.ManifestList),
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Unix(),
			FileSize: uint64(manifestListBuf.Len()),
		},
		Content: manifestListBuf.Bytes(),
	})

	handler := NewHandler(nil)
	config := Config{
		SnapshotRetentionHours: 24 * 365,
		MaxSnapshotsToKeep:     10,
		TargetFileSizeBytes:    4096,
		MinInputFiles:          2,
		Operations:             "compact",
	}

	tables, err := handler.scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scanTablesForMaintenance failed: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 compaction candidate with delete manifest present, got %d", len(tables))
	}
}

func TestDetectSchedulesSnapshotExpiryDespiteCompactionEvaluationError(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro", SequenceNumber: 1},
			{SnapshotID: 2, TimestampMs: now + 1, ManifestList: "metadata/snap-2.avro", SequenceNumber: 2},
		},
	}
	populateTable(t, fs, setup)

	// Corrupt manifest lists so compaction evaluation fails.
	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	for _, snap := range setup.Snapshots {
		manifestListName := path.Base(snap.ManifestList)
		fs.putEntry(metaDir, manifestListName, &filer_pb.Entry{
			Name: manifestListName,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				FileSize: uint64(len("not-a-manifest-list")),
			},
			Content: []byte("not-a-manifest-list"),
		})
	}

	handler := NewHandler(nil)
	config := Config{
		SnapshotRetentionHours: 24 * 365, // very long retention so age doesn't trigger
		MaxSnapshotsToKeep:     1,         // 2 snapshots > 1 triggers expiry
		Operations:             "compact,expire_snapshots",
	}

	tables, err := handler.scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scanTablesForMaintenance failed: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected snapshot expiration candidate despite compaction evaluation error, got %d", len(tables))
	}
}

func TestDetectSchedulesManifestRewriteWithoutSnapshotPressure(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro", SequenceNumber: 1},
		},
	}
	meta := populateTable(t, fs, setup)

	manifestEntries := make([][]iceberg.ManifestEntry, 0, 5)
	for i := 0; i < 5; i++ {
		manifestEntries = append(manifestEntries, makeManifestEntries(t, []testEntrySpec{
			{path: fmt.Sprintf("data/rewrite-%d.parquet", i), size: 1024, partition: map[int]any{}},
		}, 1))
	}
	writeCurrentSnapshotManifests(t, fs, setup, meta, manifestEntries)

	handler := NewHandler(nil)
	config := Config{
		SnapshotRetentionHours: 24 * 365,
		MaxSnapshotsToKeep:     10,
		MinManifestsToRewrite:  5,
		Operations:             "rewrite_manifests",
	}

	tables, err := handler.scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scanTablesForMaintenance failed: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 manifest rewrite candidate, got %d", len(tables))
	}
}

func TestDetectUsesPlanningIndexForRepeatedCompactionScans(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro", SequenceNumber: 1},
		},
	}
	meta := populateTable(t, fs, setup)
	writeCurrentSnapshotManifests(t, fs, setup, meta, [][]iceberg.ManifestEntry{
		makeManifestEntries(t, []testEntrySpec{
			{path: "data/small-1.parquet", size: 1024, partition: map[int]any{}},
			{path: "data/small-2.parquet", size: 1024, partition: map[int]any{}},
			{path: "data/small-3.parquet", size: 1024, partition: map[int]any{}},
		}, 1),
	})

	handler := NewHandler(nil)
	config := Config{
		TargetFileSizeBytes: 4096,
		MinInputFiles:       2,
		Operations:          "compact",
	}

	tables, err := handler.scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scanTablesForMaintenance failed: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 compaction candidate, got %d", len(tables))
	}

	tableDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.Namespace)
	tableEntry := fs.getEntry(tableDir, setup.TableName)
	if tableEntry == nil {
		t.Fatal("table entry not found")
	}

	var envelope struct {
		PlanningIndex *planningIndex `json:"planningIndex,omitempty"`
	}
	if err := json.Unmarshal(tableEntry.Extended[s3tables.ExtendedKeyMetadata], &envelope); err != nil {
		t.Fatalf("parse table metadata xattr: %v", err)
	}
	if envelope.PlanningIndex == nil || envelope.PlanningIndex.Compaction == nil {
		t.Fatal("expected persisted compaction planning index after first scan")
	}

	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	fs.putEntry(metaDir, "snap-1.avro", &filer_pb.Entry{
		Name: "snap-1.avro",
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Unix(),
			FileSize: uint64(len("broken")),
		},
		Content: []byte("broken"),
	})

	tables, err = handler.scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scanTablesForMaintenance with cached planning index failed: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected cached planning index to preserve 1 compaction candidate, got %d", len(tables))
	}
}

func TestDetectInvalidatesPlanningIndexWhenCompactionConfigChanges(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro", SequenceNumber: 1},
		},
	}
	meta := populateTable(t, fs, setup)
	writeCurrentSnapshotManifests(t, fs, setup, meta, [][]iceberg.ManifestEntry{
		makeManifestEntries(t, []testEntrySpec{
			{path: "data/small-1.parquet", size: 1024, partition: map[int]any{}},
			{path: "data/small-2.parquet", size: 1024, partition: map[int]any{}},
		}, 1),
	})

	handler := NewHandler(nil)
	initialConfig := Config{
		TargetFileSizeBytes: 4096,
		MinInputFiles:       3,
		Operations:          "compact",
	}

	tables, err := handler.scanTablesForMaintenance(context.Background(), client, initialConfig, "", "", "", 0)
	if err != nil {
		t.Fatalf("initial scanTablesForMaintenance failed: %v", err)
	}
	if len(tables) != 0 {
		t.Fatalf("expected no compaction candidates with min_input_files=3, got %d", len(tables))
	}

	updatedConfig := Config{
		TargetFileSizeBytes: 4096,
		MinInputFiles:       2,
		Operations:          "compact",
	}

	tables, err = handler.scanTablesForMaintenance(context.Background(), client, updatedConfig, "", "", "", 0)
	if err != nil {
		t.Fatalf("updated scanTablesForMaintenance failed: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected planning index invalidation to yield 1 compaction candidate, got %d", len(tables))
	}
}

func TestDetectPlanningIndexPreservesUnscannedSections(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro", SequenceNumber: 1},
		},
	}
	meta := populateTable(t, fs, setup)
	writeCurrentSnapshotManifests(t, fs, setup, meta, [][]iceberg.ManifestEntry{
		makeManifestEntries(t, []testEntrySpec{
			{path: "data/small-1.parquet", size: 1024, partition: map[int]any{}},
			{path: "data/small-2.parquet", size: 1024, partition: map[int]any{}},
		}, 1),
	})

	handler := NewHandler(nil)
	compactConfig := Config{
		TargetFileSizeBytes: 4096,
		MinInputFiles:       2,
		Operations:          "compact",
	}
	if _, err := handler.scanTablesForMaintenance(context.Background(), client, compactConfig, "", "", "", 0); err != nil {
		t.Fatalf("compact scanTablesForMaintenance failed: %v", err)
	}

	rewriteConfig := Config{
		MinManifestsToRewrite: 5,
		Operations:            "rewrite_manifests",
	}
	if _, err := handler.scanTablesForMaintenance(context.Background(), client, rewriteConfig, "", "", "", 0); err != nil {
		t.Fatalf("rewrite scanTablesForMaintenance failed: %v", err)
	}

	tableDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.Namespace)
	tableEntry := fs.getEntry(tableDir, setup.TableName)
	if tableEntry == nil {
		t.Fatal("table entry not found")
	}

	var envelope struct {
		PlanningIndex *planningIndex `json:"planningIndex,omitempty"`
	}
	if err := json.Unmarshal(tableEntry.Extended[s3tables.ExtendedKeyMetadata], &envelope); err != nil {
		t.Fatalf("parse table metadata xattr: %v", err)
	}
	if envelope.PlanningIndex == nil {
		t.Fatal("expected persisted planning index")
	}
	if envelope.PlanningIndex.Compaction == nil {
		t.Fatal("expected compaction section to be preserved")
	}
	if envelope.PlanningIndex.RewriteManifests == nil {
		t.Fatal("expected rewrite_manifests section to be added")
	}
}

func TestTableNeedsMaintenanceCachesPlanningIndexBuildError(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro", SequenceNumber: 1},
		},
	}
	meta := populateTable(t, fs, setup)
	writeCurrentSnapshotManifests(t, fs, setup, meta, [][]iceberg.ManifestEntry{
		makeManifestEntries(t, []testEntrySpec{
			{path: "data/small-1.parquet", size: 1024, partition: map[int]any{}},
			{path: "data/small-2.parquet", size: 1024, partition: map[int]any{}},
		}, 1),
	})

	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	fs.putEntry(metaDir, "snap-1.avro", &filer_pb.Entry{
		Name: "snap-1.avro",
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Unix(),
			FileSize: uint64(len("broken")),
		},
		Content: []byte("broken"),
	})

	handler := NewHandler(nil)
	config := Config{
		TargetFileSizeBytes:   4096,
		MinInputFiles:         2,
		MinManifestsToRewrite: 2,
		Operations:            "compact,rewrite_manifests",
	}
	ops, err := parseOperations(config.Operations)
	if err != nil {
		t.Fatalf("parseOperations: %v", err)
	}

	needsWork, err := handler.tableNeedsMaintenance(context.Background(), client, setup.BucketName, setup.tablePath(), meta, "v1.metadata.json", nil, config, ops)
	if err == nil {
		t.Fatal("expected planning-index build error")
	}
	if needsWork {
		t.Fatal("expected no maintenance result on planning-index build error")
	}
	if strings.Count(err.Error(), "parse manifest list") != 1 {
		t.Fatalf("expected planning-index build error to be reported once, got %q", err)
	}
}

func TestTableNeedsMaintenanceScopesPlanningIndexBuildErrorsPerOperation(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro", SequenceNumber: 1},
		},
	}
	meta := populateTable(t, fs, setup)
	writeCurrentSnapshotManifests(t, fs, setup, meta, [][]iceberg.ManifestEntry{
		makeManifestEntries(t, []testEntrySpec{
			{path: "data/small-1.parquet", size: 1024, partition: map[int]any{}},
		}, 1),
		makeManifestEntries(t, []testEntrySpec{
			{path: "data/small-2.parquet", size: 1024, partition: map[int]any{}},
		}, 1),
	})

	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	fs.putEntry(metaDir, "detect-manifest-1.avro", &filer_pb.Entry{
		Name: "detect-manifest-1.avro",
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Unix(),
			FileSize: uint64(len("broken")),
		},
		Content: []byte("broken"),
	})

	handler := NewHandler(nil)
	config := Config{
		TargetFileSizeBytes:   4096,
		MinInputFiles:         2,
		MinManifestsToRewrite: 2,
		Operations:            "compact,rewrite_manifests",
	}
	ops, err := parseOperations(config.Operations)
	if err != nil {
		t.Fatalf("parseOperations: %v", err)
	}

	needsWork, err := handler.tableNeedsMaintenance(context.Background(), client, setup.BucketName, setup.tablePath(), meta, "v1.metadata.json", nil, config, ops)
	if err != nil {
		t.Fatalf("expected rewrite_manifests planning to survive compaction planning error, got %v", err)
	}
	if !needsWork {
		t.Fatal("expected rewrite_manifests maintenance despite compaction planning error")
	}
}

func TestPersistPlanningIndexUsesMetadataXattrCASGuard(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro", SequenceNumber: 1},
		},
	}
	populateTable(t, fs, setup)

	tableDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath())
	tableEntry := fs.getEntry(path.Dir(tableDir), path.Base(tableDir))
	if tableEntry == nil {
		t.Fatal("table entry not found")
	}

	observedExpectedExtended := make(chan map[string][]byte, 1)
	fs.beforeUpdate = func(_ *fakeFilerServer, req *filer_pb.UpdateEntryRequest) error {
		cloned := make(map[string][]byte, len(req.ExpectedExtended))
		for key, value := range req.ExpectedExtended {
			cloned[key] = append([]byte(nil), value...)
		}
		observedExpectedExtended <- cloned
		return nil
	}

	err := persistPlanningIndex(context.Background(), client, setup.BucketName, setup.tablePath(), &planningIndex{
		SnapshotID:   1,
		ManifestList: "metadata/snap-1.avro",
		UpdatedAtMs:  time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("persistPlanningIndex: %v", err)
	}

	var expectedExtended map[string][]byte
	select {
	case expectedExtended = <-observedExpectedExtended:
	default:
		t.Fatal("expected persistPlanningIndex to issue an UpdateEntry request")
	}

	if got := expectedExtended[s3tables.ExtendedKeyMetadata]; !bytes.Equal(got, tableEntry.Extended[s3tables.ExtendedKeyMetadata]) {
		t.Fatal("expected metadata xattr to be included in ExpectedExtended")
	}
	if got := expectedExtended[s3tables.ExtendedKeyMetadataVersion]; !bytes.Equal(got, tableEntry.Extended[s3tables.ExtendedKeyMetadataVersion]) {
		t.Fatal("expected metadata version xattr to be preserved in ExpectedExtended")
	}
}

func TestDetectDoesNotScheduleManifestRewriteFromDeleteManifestsOnly(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro", SequenceNumber: 1},
		},
	}
	meta := populateTable(t, fs, setup)

	currentSnap := meta.CurrentSnapshot()
	if currentSnap == nil {
		t.Fatal("current snapshot is required")
	}

	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	version := meta.Version()
	schema := meta.CurrentSchema()
	spec := meta.PartitionSpec()

	dataEntries := makeManifestEntries(t, []testEntrySpec{
		{path: "data/rewrite-0.parquet", size: 1024, partition: map[int]any{}},
	}, currentSnap.SnapshotID)

	var dataManifestBuf bytes.Buffer
	dataManifestName := "detect-rewrite-data.avro"
	dataManifest, err := iceberg.WriteManifest(
		path.Join("metadata", dataManifestName),
		&dataManifestBuf,
		version,
		spec,
		schema,
		currentSnap.SnapshotID,
		dataEntries,
	)
	if err != nil {
		t.Fatalf("write data manifest: %v", err)
	}
	fs.putEntry(metaDir, dataManifestName, &filer_pb.Entry{
		Name: dataManifestName,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Unix(),
			FileSize: uint64(dataManifestBuf.Len()),
		},
		Content: dataManifestBuf.Bytes(),
	})

	manifests := []iceberg.ManifestFile{dataManifest}
	for i := 0; i < 4; i++ {
		deleteManifest := iceberg.NewManifestFile(
			version,
			path.Join("metadata", fmt.Sprintf("detect-delete-%d.avro", i)),
			0,
			int32(spec.ID()),
			currentSnap.SnapshotID,
		).Content(iceberg.ManifestContentDeletes).
			SequenceNum(currentSnap.SequenceNumber, currentSnap.SequenceNumber).
			DeletedFiles(1).
			DeletedRows(1).
			Build()
		manifests = append(manifests, deleteManifest)
	}

	var manifestListBuf bytes.Buffer
	seqNum := currentSnap.SequenceNumber
	if err := iceberg.WriteManifestList(
		version,
		&manifestListBuf,
		currentSnap.SnapshotID,
		currentSnap.ParentSnapshotID,
		&seqNum,
		0,
		manifests,
	); err != nil {
		t.Fatalf("write manifest list: %v", err)
	}
	fs.putEntry(metaDir, path.Base(currentSnap.ManifestList), &filer_pb.Entry{
		Name: path.Base(currentSnap.ManifestList),
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Unix(),
			FileSize: uint64(manifestListBuf.Len()),
		},
		Content: manifestListBuf.Bytes(),
	})

	handler := NewHandler(nil)
	config := Config{
		SnapshotRetentionHours: 24 * 365,
		MaxSnapshotsToKeep:     10,
		MinManifestsToRewrite:  2,
		Operations:             "rewrite_manifests",
	}

	tables, err := handler.scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scanTablesForMaintenance failed: %v", err)
	}
	if len(tables) != 0 {
		t.Fatalf("expected no manifest rewrite candidate when only one data manifest exists, got %d", len(tables))
	}
}

func TestDetectSchedulesOrphanCleanupWithoutSnapshotPressure(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro", SequenceNumber: 1},
		},
	}
	populateTable(t, fs, setup)

	dataDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "data")
	fs.putEntry(dataDir, "stale-orphan.parquet", &filer_pb.Entry{
		Name: "stale-orphan.parquet",
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Add(-200 * time.Hour).Unix(),
			FileSize: 100,
		},
		Content: []byte("orphan"),
	})

	handler := NewHandler(nil)
	config := Config{
		SnapshotRetentionHours: 24 * 365,
		MaxSnapshotsToKeep:     10,
		OrphanOlderThanHours:   72,
		Operations:             "remove_orphans",
	}

	tables, err := handler.scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scanTablesForMaintenance failed: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 orphan cleanup candidate, got %d", len(tables))
	}
}

func TestDetectSchedulesOrphanCleanupWithoutSnapshots(t *testing.T) {
	fs, client := startFakeFiler(t)

	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
	}
	populateTable(t, fs, setup)

	dataDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "data")
	fs.putEntry(dataDir, "stale-orphan.parquet", &filer_pb.Entry{
		Name: "stale-orphan.parquet",
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Add(-200 * time.Hour).Unix(),
			FileSize: 100,
		},
		Content: []byte("orphan"),
	})

	handler := NewHandler(nil)
	config := Config{
		OrphanOlderThanHours: 72,
		Operations:           "remove_orphans",
	}

	tables, err := handler.scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scanTablesForMaintenance failed: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 orphan cleanup candidate without snapshots, got %d", len(tables))
	}
}

func TestStalePlanGuard(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "ns",
		TableName:  "tbl",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
		},
	}
	populateTable(t, fs, setup)

	handler := NewHandler(nil)

	// Call commitWithRetry with a stale plan that expects a different snapshot
	config := Config{MaxCommitRetries: 1}
	staleSnapshotID := int64(999)

	err := handler.commitWithRetry(context.Background(), client, setup.BucketName, setup.tablePath(), "v1.metadata.json", config, func(currentMeta table.Metadata, builder *table.MetadataBuilder) error {
		cs := currentMeta.CurrentSnapshot()
		if cs == nil || cs.SnapshotID != staleSnapshotID {
			return errStalePlan
		}
		return nil
	})

	if err == nil {
		t.Fatal("expected stale plan error")
	}
	if !strings.Contains(err.Error(), "stale plan") {
		t.Errorf("expected stale plan in error, got %q", err)
	}
}

func TestMetadataVersionCAS(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "ns",
		TableName:  "tbl",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
		},
	}
	populateTable(t, fs, setup)

	// The table xattr has metadataVersion=1. Try updating with wrong expected version.
	tableDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath())
	err := updateTableMetadataXattr(context.Background(), client, tableDir, 99, []byte(`{}`), "metadata/v100.metadata.json")
	if err == nil {
		t.Fatal("expected version conflict error")
	}
	if !strings.Contains(err.Error(), "metadata version conflict") {
		t.Errorf("expected version conflict in error, got %q", err)
	}

	// Correct expected version should succeed
	err = updateTableMetadataXattr(context.Background(), client, tableDir, 1, []byte(`{}`), "metadata/v2.metadata.json")
	if err != nil {
		t.Fatalf("expected success with correct version, got: %v", err)
	}

	// Verify version was incremented to 2
	entry := fs.getEntry(path.Dir(tableDir), path.Base(tableDir))
	if entry == nil {
		t.Fatal("table entry not found after update")
	}
	var internalMeta map[string]json.RawMessage
	if err := json.Unmarshal(entry.Extended[s3tables.ExtendedKeyMetadata], &internalMeta); err != nil {
		t.Fatalf("unmarshal xattr: %v", err)
	}
	var version int
	if err := json.Unmarshal(internalMeta["metadataVersion"], &version); err != nil {
		t.Fatalf("unmarshal version: %v", err)
	}
	if version != 2 {
		t.Errorf("expected version 2 after update, got %d", version)
	}
}

func TestMetadataVersionCASDetectsConcurrentUpdate(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "ns",
		TableName:  "tbl",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
		},
	}
	populateTable(t, fs, setup)

	tableDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath())
	fs.beforeUpdate = func(f *fakeFilerServer, req *filer_pb.UpdateEntryRequest) error {
		entry := f.getEntry(path.Dir(tableDir), path.Base(tableDir))
		if entry == nil {
			return fmt.Errorf("table entry not found before concurrent update")
		}

		updatedEntry := cloneEntryForTest(t, entry)
		var internalMeta map[string]json.RawMessage
		if err := json.Unmarshal(updatedEntry.Extended[s3tables.ExtendedKeyMetadata], &internalMeta); err != nil {
			return fmt.Errorf("unmarshal xattr: %w", err)
		}

		versionJSON, err := json.Marshal(2)
		if err != nil {
			return fmt.Errorf("marshal version: %w", err)
		}
		internalMeta["metadataVersion"] = versionJSON

		updatedXattr, err := json.Marshal(internalMeta)
		if err != nil {
			return fmt.Errorf("marshal xattr: %w", err)
		}
		updatedEntry.Extended[s3tables.ExtendedKeyMetadata] = updatedXattr
		updatedEntry.Extended[s3tables.ExtendedKeyMetadataVersion] = metadataVersionXattr(2)
		f.putEntry(path.Dir(tableDir), path.Base(tableDir), updatedEntry)
		return nil
	}

	err := updateTableMetadataXattr(context.Background(), client, tableDir, 1, []byte(`{}`), "metadata/v2.metadata.json")
	if err == nil {
		t.Fatal("expected version conflict error")
	}
	if !strings.Contains(err.Error(), "metadata version conflict") {
		t.Fatalf("expected metadata version conflict, got %q", err)
	}
}

func cloneEntryForTest(t *testing.T, entry *filer_pb.Entry) *filer_pb.Entry {
	t.Helper()

	cloned, ok := proto.Clone(entry).(*filer_pb.Entry)
	if !ok {
		t.Fatal("clone entry: unexpected type")
	}
	return cloned
}

// ---------------------------------------------------------------------------
// Avro manifest content patching for tests
// ---------------------------------------------------------------------------

// patchManifestContentToDeletes performs a binary patch on an Avro manifest
// file to change the "content" metadata value from "data" to "deletes".
// This workaround is needed because iceberg-go's WriteManifest API always
// sets content="data" and provides no way to create delete manifests.
// The function validates the pattern was found (bytes.Equal check) and fails
// fast if not, so breakage from encoding changes is caught immediately.
//
// In Avro OCF encoding, strings are stored as zigzag-encoded length + bytes.
// "content" (7 chars) = \x0e + "content", "data" (4 chars) = \x08 + "data",
// "deletes" (7 chars) = \x0e + "deletes".
func patchManifestContentToDeletes(t *testing.T, manifestBytes []byte) []byte {
	t.Helper()

	// Pattern: zigzag(7)="content" zigzag(4)="data"
	old := append([]byte{0x0e}, []byte("content")...)
	old = append(old, 0x08)
	old = append(old, []byte("data")...)

	// Replacement: zigzag(7)="content" zigzag(7)="deletes"
	new := append([]byte{0x0e}, []byte("content")...)
	new = append(new, 0x0e)
	new = append(new, []byte("deletes")...)

	result := bytes.Replace(manifestBytes, old, new, 1)
	if bytes.Equal(result, manifestBytes) {
		t.Fatal("patchManifestContentToDeletes: pattern not found in manifest bytes")
	}
	return result
}

// ---------------------------------------------------------------------------
// End-to-end compaction tests with deletes
// ---------------------------------------------------------------------------

// writeTestParquetFile creates a Parquet file with id/name columns in the fake filer.
func writeTestParquetFile(t *testing.T, fs *fakeFilerServer, dir, name string, rows []struct {
	ID   int64
	Name string
}) []byte {
	t.Helper()
	type dataRow struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, parquet.SchemaOf(new(dataRow)))
	for _, r := range rows {
		if err := w.Write(&dataRow{r.ID, r.Name}); err != nil {
			t.Fatalf("write parquet row: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close parquet writer: %v", err)
	}
	data := buf.Bytes()
	fs.putEntry(dir, name, &filer_pb.Entry{
		Name:       name,
		Content:    data,
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(len(data))},
	})
	return data
}

// populateTableWithDeleteFiles sets up a table with data files and delete manifest(s)
// for compaction testing. Returns the table metadata.
func populateTableWithDeleteFiles(
	t *testing.T,
	fs *fakeFilerServer,
	setup tableSetup,
	dataFiles []struct {
		Name string
		Rows []struct {
			ID   int64
			Name string
		}
	},
	posDeleteFiles []struct {
		Name string
		Rows []struct {
			FilePath string
			Pos      int64
		}
	},
	eqDeleteFiles []struct {
		Name     string
		FieldIDs []int
		Rows     []struct {
			ID   int64
			Name string
		}
	},
) table.Metadata {
	t.Helper()

	schema := newTestSchema()
	spec := *iceberg.UnpartitionedSpec

	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, "s3://"+setup.BucketName+"/"+setup.tablePath(), nil)
	if err != nil {
		t.Fatalf("create metadata: %v", err)
	}

	bucketsPath := s3tables.TablesPath
	bucketPath := path.Join(bucketsPath, setup.BucketName)
	nsPath := path.Join(bucketPath, setup.Namespace)
	tableFilerPath := path.Join(nsPath, setup.TableName)
	metaDir := path.Join(tableFilerPath, "metadata")
	dataDir := path.Join(tableFilerPath, "data")

	version := meta.Version()

	// Write data files
	var dataManifestEntries []iceberg.ManifestEntry
	for _, df := range dataFiles {
		data := writeTestParquetFile(t, fs, dataDir, df.Name, df.Rows)
		dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, "data/"+df.Name, iceberg.ParquetFile, map[int]any{}, nil, nil, int64(len(df.Rows)), int64(len(data)))
		if err != nil {
			t.Fatalf("build data file %s: %v", df.Name, err)
		}
		snapID := int64(1)
		dataManifestEntries = append(dataManifestEntries, iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfb.Build()))
	}

	// Write data manifest
	var dataManifestBuf bytes.Buffer
	dataManifestName := "data-manifest-1.avro"
	dataMf, err := iceberg.WriteManifest(path.Join("metadata", dataManifestName), &dataManifestBuf, version, spec, schema, 1, dataManifestEntries)
	if err != nil {
		t.Fatalf("write data manifest: %v", err)
	}
	fs.putEntry(metaDir, dataManifestName, &filer_pb.Entry{
		Name: dataManifestName, Content: dataManifestBuf.Bytes(),
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(dataManifestBuf.Len())},
	})

	allManifests := []iceberg.ManifestFile{dataMf}

	// Write position delete files and manifests
	if len(posDeleteFiles) > 0 {
		var posDeleteEntries []iceberg.ManifestEntry
		for _, pdf := range posDeleteFiles {
			type posRow struct {
				FilePath string `parquet:"file_path"`
				Pos      int64  `parquet:"pos"`
			}
			var buf bytes.Buffer
			w := parquet.NewWriter(&buf, parquet.SchemaOf(new(posRow)))
			for _, r := range pdf.Rows {
				if err := w.Write(&posRow{r.FilePath, r.Pos}); err != nil {
					t.Fatalf("write pos delete: %v", err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close pos delete: %v", err)
			}
			fs.putEntry(dataDir, pdf.Name, &filer_pb.Entry{
				Name: pdf.Name, Content: buf.Bytes(),
				Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(buf.Len())},
			})
			dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes, "data/"+pdf.Name, iceberg.ParquetFile, map[int]any{}, nil, nil, int64(len(pdf.Rows)), int64(buf.Len()))
			if err != nil {
				t.Fatalf("build pos delete file: %v", err)
			}
			snapID := int64(1)
			posDeleteEntries = append(posDeleteEntries, iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfb.Build()))
		}

		// WriteManifest always sets content="data", so we patch the Avro
		// metadata to "deletes" and build a ManifestFile with the right content type.
		var posManifestBuf bytes.Buffer
		posManifestName := "pos-delete-manifest-1.avro"
		posManifestPath := path.Join("metadata", posManifestName)
		_, err := iceberg.WriteManifest(posManifestPath, &posManifestBuf, version, spec, schema, 1, posDeleteEntries)
		if err != nil {
			t.Fatalf("write pos delete manifest: %v", err)
		}
		patchedBytes := patchManifestContentToDeletes(t, posManifestBuf.Bytes())
		fs.putEntry(metaDir, posManifestName, &filer_pb.Entry{
			Name: posManifestName, Content: patchedBytes,
			Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(len(patchedBytes))},
		})
		posMf := iceberg.NewManifestFile(version, posManifestPath, int64(len(patchedBytes)), int32(spec.ID()), 1).
			Content(iceberg.ManifestContentDeletes).
			AddedFiles(int32(len(posDeleteEntries))).
			AddedRows(int64(len(posDeleteFiles[0].Rows))).
			Build()
		allManifests = append(allManifests, posMf)
	}

	// Write equality delete files and manifests
	if len(eqDeleteFiles) > 0 {
		var eqDeleteEntries []iceberg.ManifestEntry
		for _, edf := range eqDeleteFiles {
			type eqRow struct {
				ID   int64  `parquet:"id"`
				Name string `parquet:"name"`
			}
			var buf bytes.Buffer
			w := parquet.NewWriter(&buf, parquet.SchemaOf(new(eqRow)))
			for _, r := range edf.Rows {
				if err := w.Write(&eqRow{r.ID, r.Name}); err != nil {
					t.Fatalf("write eq delete: %v", err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close eq delete: %v", err)
			}
			fs.putEntry(dataDir, edf.Name, &filer_pb.Entry{
				Name: edf.Name, Content: buf.Bytes(),
				Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(buf.Len())},
			})
			dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentEqDeletes, "data/"+edf.Name, iceberg.ParquetFile, map[int]any{}, nil, nil, int64(len(edf.Rows)), int64(buf.Len()))
			if err != nil {
				t.Fatalf("build eq delete file: %v", err)
			}
			dfb.EqualityFieldIDs(edf.FieldIDs)
			snapID := int64(1)
			eqDeleteEntries = append(eqDeleteEntries, iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfb.Build()))
		}

		var eqManifestBuf bytes.Buffer
		eqManifestName := "eq-delete-manifest-1.avro"
		eqManifestPath := path.Join("metadata", eqManifestName)
		_, err := iceberg.WriteManifest(eqManifestPath, &eqManifestBuf, version, spec, schema, 1, eqDeleteEntries)
		if err != nil {
			t.Fatalf("write eq delete manifest: %v", err)
		}
		patchedBytes := patchManifestContentToDeletes(t, eqManifestBuf.Bytes())
		fs.putEntry(metaDir, eqManifestName, &filer_pb.Entry{
			Name: eqManifestName, Content: patchedBytes,
			Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(len(patchedBytes))},
		})
		eqMf := iceberg.NewManifestFile(version, eqManifestPath, int64(len(patchedBytes)), int32(spec.ID()), 1).
			Content(iceberg.ManifestContentDeletes).
			AddedFiles(int32(len(eqDeleteEntries))).
			AddedRows(int64(len(eqDeleteFiles[0].Rows))).
			Build()
		allManifests = append(allManifests, eqMf)
	}

	// Write manifest list
	var mlBuf bytes.Buffer
	seqNum := int64(1)
	if err := iceberg.WriteManifestList(version, &mlBuf, 1, nil, &seqNum, 0, allManifests); err != nil {
		t.Fatalf("write manifest list: %v", err)
	}
	fs.putEntry(metaDir, "snap-1.avro", &filer_pb.Entry{
		Name: "snap-1.avro", Content: mlBuf.Bytes(),
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(mlBuf.Len())},
	})

	// Build final metadata with snapshot
	now := time.Now().UnixMilli()
	snap := table.Snapshot{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"}
	meta = buildTestMetadata(t, []table.Snapshot{snap})

	// Register table structure
	fullMetadataJSON, _ := json.Marshal(meta)
	internalMeta := map[string]interface{}{
		"metadataVersion": 1,
		"metadata":        map[string]interface{}{"fullMetadata": json.RawMessage(fullMetadataJSON)},
	}
	xattr, _ := json.Marshal(internalMeta)

	fs.putEntry(bucketsPath, setup.BucketName, &filer_pb.Entry{
		Name: setup.BucketName, IsDirectory: true,
		Extended: map[string][]byte{s3tables.ExtendedKeyTableBucket: []byte("true")},
	})
	fs.putEntry(bucketPath, setup.Namespace, &filer_pb.Entry{Name: setup.Namespace, IsDirectory: true})
	fs.putEntry(nsPath, setup.TableName, &filer_pb.Entry{
		Name: setup.TableName, IsDirectory: true,
		Extended: map[string][]byte{s3tables.ExtendedKeyMetadata: xattr},
	})

	return meta
}

func loadLiveDeleteFilePaths(
	t *testing.T,
	client filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
) (posPaths, eqPaths []string) {
	t.Helper()

	meta, _, err := loadCurrentMetadata(context.Background(), client, bucketName, tablePath)
	if err != nil {
		t.Fatalf("loadCurrentMetadata: %v", err)
	}
	manifests, err := loadCurrentManifests(context.Background(), client, bucketName, tablePath, meta)
	if err != nil {
		t.Fatalf("loadCurrentManifests: %v", err)
	}

	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		manifestData, err := loadFileByIcebergPath(context.Background(), client, bucketName, tablePath, mf.FilePath())
		if err != nil {
			t.Fatalf("load delete manifest: %v", err)
		}
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manifestData), true)
		if err != nil {
			t.Fatalf("read delete manifest: %v", err)
		}
		for _, entry := range entries {
			switch entry.DataFile().ContentType() {
			case iceberg.EntryContentPosDeletes:
				posPaths = append(posPaths, entry.DataFile().FilePath())
			case iceberg.EntryContentEqDeletes:
				eqPaths = append(eqPaths, entry.DataFile().FilePath())
			}
		}
	}

	sort.Strings(posPaths)
	sort.Strings(eqPaths)
	return posPaths, eqPaths
}

func rewriteDeleteManifestsAsMixed(
	t *testing.T,
	fs *fakeFilerServer,
	client filer_pb.SeaweedFilerClient,
	setup tableSetup,
) {
	t.Helper()

	meta, _, err := loadCurrentMetadata(context.Background(), client, setup.BucketName, setup.tablePath())
	if err != nil {
		t.Fatalf("loadCurrentMetadata: %v", err)
	}
	manifests, err := loadCurrentManifests(context.Background(), client, setup.BucketName, setup.tablePath(), meta)
	if err != nil {
		t.Fatalf("loadCurrentManifests: %v", err)
	}

	var dataManifests []iceberg.ManifestFile
	var deleteEntries []iceberg.ManifestEntry
	for _, mf := range manifests {
		if mf.ManifestContent() == iceberg.ManifestContentData {
			dataManifests = append(dataManifests, mf)
			continue
		}
		manifestData, err := loadFileByIcebergPath(context.Background(), client, setup.BucketName, setup.tablePath(), mf.FilePath())
		if err != nil {
			t.Fatalf("load delete manifest: %v", err)
		}
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manifestData), true)
		if err != nil {
			t.Fatalf("read delete manifest: %v", err)
		}
		for _, entry := range entries {
			deleteEntries = append(deleteEntries, entry)
		}
	}

	spec := *iceberg.UnpartitionedSpec
	version := meta.Version()
	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	manifestName := "mixed-delete-manifest-1.avro"
	manifestPath := path.Join("metadata", manifestName)

	var manifestBuf bytes.Buffer
	_, err = iceberg.WriteManifest(manifestPath, &manifestBuf, version, spec, meta.CurrentSchema(), 1, deleteEntries)
	if err != nil {
		t.Fatalf("write mixed delete manifest: %v", err)
	}
	mixedBytes := patchManifestContentToDeletes(t, manifestBuf.Bytes())
	fs.putEntry(metaDir, manifestName, &filer_pb.Entry{
		Name: manifestName, Content: mixedBytes,
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(len(mixedBytes))},
	})

	mixedManifest := iceberg.NewManifestFile(version, manifestPath, int64(len(mixedBytes)), int32(spec.ID()), 1).
		Content(iceberg.ManifestContentDeletes).
		AddedFiles(int32(len(deleteEntries))).
		Build()

	var manifestListBuf bytes.Buffer
	seqNum := int64(1)
	allManifests := append(dataManifests, mixedManifest)
	if err := iceberg.WriteManifestList(version, &manifestListBuf, 1, nil, &seqNum, 0, allManifests); err != nil {
		t.Fatalf("write mixed manifest list: %v", err)
	}
	fs.putEntry(metaDir, "snap-1.avro", &filer_pb.Entry{
		Name: "snap-1.avro", Content: manifestListBuf.Bytes(),
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(manifestListBuf.Len())},
	})
}

func TestCompactDataFilesMetrics(t *testing.T) {
	fs, client := startFakeFiler(t)

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "a"}, {2, "b"}}},
			{"d2.parquet", []struct {
				ID   int64
				Name string
			}{{3, "c"}}},
		},
		nil, nil,
	)

	handler := NewHandler(nil)
	config := Config{
		TargetFileSizeBytes: 256 * 1024 * 1024,
		MinInputFiles:       2,
		MaxCommitRetries:    3,
		ApplyDeletes:        true,
	}

	// Track progress callbacks
	var progressCalls []int
	result, metrics, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, func(binIdx, totalBins int) {
		progressCalls = append(progressCalls, binIdx)
	})
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}

	if !strings.Contains(result, "compacted") {
		t.Errorf("expected compaction result, got %q", result)
	}

	// Verify metrics
	if metrics == nil {
		t.Fatal("expected non-nil metrics")
	}
	if metrics[MetricFilesMerged] != 2 {
		t.Errorf("expected files_merged=2, got %d", metrics[MetricFilesMerged])
	}
	if metrics[MetricFilesWritten] != 1 {
		t.Errorf("expected files_written=1, got %d", metrics[MetricFilesWritten])
	}
	if metrics[MetricBins] != 1 {
		t.Errorf("expected bins=1, got %d", metrics[MetricBins])
	}
	if metrics[MetricDurationMs] < 0 {
		t.Errorf("expected non-negative duration_ms, got %d", metrics[MetricDurationMs])
	}

	// Verify progress callback was invoked
	if len(progressCalls) != 1 {
		t.Errorf("expected 1 progress call, got %d", len(progressCalls))
	}
}

func TestExpireSnapshotsMetrics(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "ns",
		TableName:  "tbl",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
			{SnapshotID: 2, TimestampMs: now + 1, ManifestList: "metadata/snap-2.avro"},
			{SnapshotID: 3, TimestampMs: now + 2, ManifestList: "metadata/snap-3.avro"},
		},
	}
	populateTable(t, fs, setup)

	handler := NewHandler(nil)
	config := Config{
		SnapshotRetentionHours: 0,
		MaxSnapshotsToKeep:     1,
		MaxCommitRetries:       3,
	}

	_, metrics, err := handler.expireSnapshots(context.Background(), client, setup.BucketName, setup.tablePath(), config)
	if err != nil {
		t.Fatalf("expireSnapshots: %v", err)
	}

	if metrics == nil {
		t.Fatal("expected non-nil metrics")
	}
	if metrics[MetricSnapshotsExpired] == 0 {
		t.Error("expected snapshots_expired > 0")
	}
	if metrics[MetricDurationMs] < 0 {
		t.Errorf("expected non-negative duration_ms, got %d", metrics[MetricDurationMs])
	}
}

func TestExecuteCompletionOutputValues(t *testing.T) {
	fs, client := startFakeFiler(t)

	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "ns",
		TableName:  "tbl",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
			{SnapshotID: 2, TimestampMs: now + 1, ManifestList: "metadata/snap-2.avro"},
			{SnapshotID: 3, TimestampMs: now + 2, ManifestList: "metadata/snap-3.avro"},
		},
	}
	populateTable(t, fs, setup)

	handler := NewHandler(nil)
	config := Config{
		SnapshotRetentionHours: 0,
		MaxSnapshotsToKeep:     1,
		MaxCommitRetries:       3,
	}

	_, metrics, err := handler.expireSnapshots(context.Background(), client, setup.BucketName, setup.tablePath(), config)
	if err != nil {
		t.Fatalf("expireSnapshots: %v", err)
	}

	// Verify metrics have the expected keys
	if _, ok := metrics[MetricSnapshotsExpired]; !ok {
		t.Error("expected 'snapshots_expired' key in metrics")
	}
	if _, ok := metrics[MetricFilesDeleted]; !ok {
		t.Error("expected 'files_deleted' key in metrics")
	}
	if _, ok := metrics[MetricDurationMs]; !ok {
		t.Error("expected 'duration_ms' key in metrics")
	}
}

func TestCompactDataFilesWithPositionDeletes(t *testing.T) {
	fs, client := startFakeFiler(t)

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		// 3 small data files (to meet min_input_files=2)
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "alice"}, {2, "bob"}, {3, "charlie"}}},
			{"d2.parquet", []struct {
				ID   int64
				Name string
			}{{4, "dave"}, {5, "eve"}}},
		},
		// Position deletes: delete row 1 (bob) from d1
		[]struct {
			Name string
			Rows []struct {
				FilePath string
				Pos      int64
			}
		}{
			{"pd1.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 1}}},
		},
		nil, // no equality deletes
	)

	handler := NewHandler(nil)
	config := Config{
		TargetFileSizeBytes: 256 * 1024 * 1024,
		MinInputFiles:       2,
		MaxCommitRetries:    3,
		ApplyDeletes:        true,
	}

	result, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}

	if !strings.Contains(result, "compacted") {
		t.Errorf("expected compaction result, got %q", result)
	}
	t.Logf("result: %s", result)

	// Verify: read the merged output and count rows
	// The merged file should have 4 rows (5 total - 1 position delete)
	dataDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "data")
	entries := fs.listDir(dataDir)
	var mergedContent []byte
	for _, e := range entries {
		if strings.HasPrefix(e.Name, "compact-") {
			mergedContent = e.Content
			break
		}
	}
	if mergedContent == nil {
		t.Fatal("no merged file found")
	}

	reader := parquet.NewReader(bytes.NewReader(mergedContent))
	defer reader.Close()
	type row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	var outputRows []row
	for {
		var r row
		if err := reader.Read(&r); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("read: %v", err)
		}
		outputRows = append(outputRows, r)
	}

	if len(outputRows) != 4 {
		t.Errorf("expected 4 rows (5 - 1 pos delete), got %d", len(outputRows))
	}
	for _, r := range outputRows {
		if r.Name == "bob" {
			t.Error("bob should have been deleted by position delete")
		}
	}
}

func TestCompactDataFilesWithEqualityDeletes(t *testing.T) {
	fs, client := startFakeFiler(t)

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "alice"}, {2, "bob"}}},
			{"d2.parquet", []struct {
				ID   int64
				Name string
			}{{3, "charlie"}, {4, "dave"}}},
		},
		nil, // no position deletes
		// Equality deletes: delete rows where name="bob" or name="dave"
		[]struct {
			Name     string
			FieldIDs []int
			Rows     []struct {
				ID   int64
				Name string
			}
		}{
			{"ed1.parquet", []int{2}, []struct {
				ID   int64
				Name string
			}{{0, "bob"}, {0, "dave"}}},
		},
	)

	handler := NewHandler(nil)
	config := Config{
		TargetFileSizeBytes: 256 * 1024 * 1024,
		MinInputFiles:       2,
		MaxCommitRetries:    3,
		ApplyDeletes:        true,
	}

	result, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}

	if !strings.Contains(result, "compacted") {
		t.Errorf("expected compaction result, got %q", result)
	}
	t.Logf("result: %s", result)

	// Verify merged output
	dataDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "data")
	entries := fs.listDir(dataDir)
	var mergedContent []byte
	for _, e := range entries {
		if strings.HasPrefix(e.Name, "compact-") {
			mergedContent = e.Content
			break
		}
	}
	if mergedContent == nil {
		t.Fatal("no merged file found")
	}

	reader := parquet.NewReader(bytes.NewReader(mergedContent))
	defer reader.Close()
	type row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	var outputRows []row
	for {
		var r row
		if err := reader.Read(&r); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("read: %v", err)
		}
		outputRows = append(outputRows, r)
	}

	if len(outputRows) != 2 {
		t.Errorf("expected 2 rows (4 - 2 eq deletes), got %d", len(outputRows))
	}
	for _, r := range outputRows {
		if r.Name == "bob" || r.Name == "dave" {
			t.Errorf("row %q should have been deleted by equality delete", r.Name)
		}
	}
}

func TestCompactDataFilesApplyDeletesDisabled(t *testing.T) {
	fs, client := startFakeFiler(t)

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "a"}, {2, "b"}}},
			{"d2.parquet", []struct {
				ID   int64
				Name string
			}{{3, "c"}}},
		},
		[]struct {
			Name string
			Rows []struct {
				FilePath string
				Pos      int64
			}
		}{
			{"pd1.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 0}}},
		},
		nil,
	)

	handler := NewHandler(nil)
	config := Config{
		TargetFileSizeBytes: 256 * 1024 * 1024,
		MinInputFiles:       2,
		MaxCommitRetries:    3,
		ApplyDeletes:        false, // disabled
	}

	result, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}

	if !strings.Contains(result, "skipped") {
		t.Errorf("expected skip when apply_deletes=false, got %q", result)
	}
}

func TestCompactDataFilesWithMixedDeletes(t *testing.T) {
	fs, client := startFakeFiler(t)

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "alice"}, {2, "bob"}, {3, "charlie"}}},
			{"d2.parquet", []struct {
				ID   int64
				Name string
			}{{4, "dave"}, {5, "eve"}}},
		},
		// Position delete: row 0 (alice) from d1
		[]struct {
			Name string
			Rows []struct {
				FilePath string
				Pos      int64
			}
		}{
			{"pd1.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 0}}},
		},
		// Equality delete: name="eve"
		[]struct {
			Name     string
			FieldIDs []int
			Rows     []struct {
				ID   int64
				Name string
			}
		}{
			{"ed1.parquet", []int{2}, []struct {
				ID   int64
				Name string
			}{{0, "eve"}}},
		},
	)

	handler := NewHandler(nil)
	config := Config{
		TargetFileSizeBytes: 256 * 1024 * 1024,
		MinInputFiles:       2,
		MaxCommitRetries:    3,
		ApplyDeletes:        true,
	}

	result, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}

	if !strings.Contains(result, "compacted") {
		t.Errorf("expected compaction, got %q", result)
	}

	// Verify: 5 total - 1 pos delete (alice) - 1 eq delete (eve) = 3 rows
	dataDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "data")
	entries := fs.listDir(dataDir)
	var mergedContent []byte
	for _, e := range entries {
		if strings.HasPrefix(e.Name, "compact-") {
			mergedContent = e.Content
			break
		}
	}
	if mergedContent == nil {
		t.Fatal("no merged file found")
	}

	reader := parquet.NewReader(bytes.NewReader(mergedContent))
	defer reader.Close()
	type row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	var outputRows []row
	for {
		var r row
		if err := reader.Read(&r); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("read: %v", err)
		}
		outputRows = append(outputRows, r)
	}

	if len(outputRows) != 3 {
		t.Errorf("expected 3 rows (5 - 1 pos - 1 eq), got %d", len(outputRows))
	}
	for _, r := range outputRows {
		if r.Name == "alice" || r.Name == "eve" {
			t.Errorf("%q should have been deleted", r.Name)
		}
	}
}

func TestRewritePositionDeleteFilesExecution(t *testing.T) {
	fs, client := startFakeFiler(t)

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "alice"}, {2, "bob"}, {3, "charlie"}}},
		},
		[]struct {
			Name string
			Rows []struct {
				FilePath string
				Pos      int64
			}
		}{
			{"pd1.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 0}, {"data/d1.parquet", 2}}},
			{"pd2.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 1}}},
		},
		nil,
	)

	handler := NewHandler(nil)
	config := Config{
		DeleteTargetFileSizeBytes:   64 * 1024 * 1024,
		DeleteMinInputFiles:         2,
		DeleteMaxFileGroupSizeBytes: 128 * 1024 * 1024,
		DeleteMaxOutputFiles:        4,
		MaxCommitRetries:            3,
	}

	result, metrics, err := handler.rewritePositionDeleteFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config)
	if err != nil {
		t.Fatalf("rewritePositionDeleteFiles: %v", err)
	}
	if !strings.Contains(result, "rewrote 2 position delete files into 1") {
		t.Fatalf("unexpected result: %q", result)
	}
	if metrics[MetricDeleteFilesRewritten] != 2 {
		t.Fatalf("expected 2 rewritten files, got %d", metrics[MetricDeleteFilesRewritten])
	}
	if metrics[MetricDeleteFilesWritten] != 1 {
		t.Fatalf("expected 1 written file, got %d", metrics[MetricDeleteFilesWritten])
	}

	liveDeletePaths, _ := loadLiveDeleteFilePaths(t, client, setup.BucketName, setup.tablePath())
	if len(liveDeletePaths) != 1 {
		t.Fatalf("expected 1 live rewritten delete file, got %v", liveDeletePaths)
	}
	if !strings.HasPrefix(liveDeletePaths[0], "data/rewrite-delete-") {
		t.Fatalf("expected rewritten delete file path, got %q", liveDeletePaths[0])
	}
}

func TestRewritePositionDeleteFilesDetection(t *testing.T) {
	fs, client := startFakeFiler(t)

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "alice"}, {2, "bob"}}},
		},
		[]struct {
			Name string
			Rows []struct {
				FilePath string
				Pos      int64
			}
		}{
			{"pd1.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 0}}},
			{"pd2.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 1}}},
		},
		nil,
	)

	handler := NewHandler(nil)
	config := Config{
		Operations:                  "rewrite_position_delete_files",
		DeleteTargetFileSizeBytes:   64 * 1024 * 1024,
		DeleteMinInputFiles:         2,
		DeleteMaxFileGroupSizeBytes: 128 * 1024 * 1024,
		DeleteMaxOutputFiles:        4,
	}

	tables, err := handler.scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scanTablesForMaintenance: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table needing delete rewrite, got %d", len(tables))
	}
}

func TestRewritePositionDeleteFilesSkipsSingleFile(t *testing.T) {
	fs, client := startFakeFiler(t)

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "alice"}, {2, "bob"}}},
		},
		[]struct {
			Name string
			Rows []struct {
				FilePath string
				Pos      int64
			}
		}{
			{"pd1.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 0}}},
		},
		nil,
	)

	handler := NewHandler(nil)
	config := Config{
		DeleteTargetFileSizeBytes:   64 * 1024 * 1024,
		DeleteMinInputFiles:         2,
		DeleteMaxFileGroupSizeBytes: 128 * 1024 * 1024,
		DeleteMaxOutputFiles:        4,
		MaxCommitRetries:            3,
	}

	result, _, err := handler.rewritePositionDeleteFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config)
	if err != nil {
		t.Fatalf("rewritePositionDeleteFiles: %v", err)
	}
	if !strings.Contains(result, "no position delete files eligible") {
		t.Fatalf("unexpected result: %q", result)
	}
}

func TestRewritePositionDeleteFilesRespectsMinInputFiles(t *testing.T) {
	fs, client := startFakeFiler(t)

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "alice"}, {2, "bob"}}},
		},
		[]struct {
			Name string
			Rows []struct {
				FilePath string
				Pos      int64
			}
		}{
			{"pd1.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 0}}},
			{"pd2.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 1}}},
		},
		nil,
	)

	handler := NewHandler(nil)
	config := Config{
		DeleteTargetFileSizeBytes:   64 * 1024 * 1024,
		DeleteMinInputFiles:         3,
		DeleteMaxFileGroupSizeBytes: 128 * 1024 * 1024,
		DeleteMaxOutputFiles:        4,
		MaxCommitRetries:            3,
	}

	result, _, err := handler.rewritePositionDeleteFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config)
	if err != nil {
		t.Fatalf("rewritePositionDeleteFiles: %v", err)
	}
	if !strings.Contains(result, "no position delete files eligible") {
		t.Fatalf("unexpected result: %q", result)
	}
}

func TestRewritePositionDeleteFilesPreservesUnsupportedMultiTargetDeletes(t *testing.T) {
	fs, client := startFakeFiler(t)

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "alice"}, {2, "bob"}, {3, "charlie"}}},
			{"d2.parquet", []struct {
				ID   int64
				Name string
			}{{4, "diana"}, {5, "eve"}}},
		},
		[]struct {
			Name string
			Rows []struct {
				FilePath string
				Pos      int64
			}
		}{
			{"pd1.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 0}}},
			{"pd2.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 1}}},
			{"pd3.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 2}, {"data/d2.parquet", 0}}},
		},
		nil,
	)

	handler := NewHandler(nil)
	config := Config{
		DeleteTargetFileSizeBytes:   64 * 1024 * 1024,
		DeleteMinInputFiles:         2,
		DeleteMaxFileGroupSizeBytes: 128 * 1024 * 1024,
		DeleteMaxOutputFiles:        4,
		MaxCommitRetries:            3,
	}

	if _, _, err := handler.rewritePositionDeleteFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config); err != nil {
		t.Fatalf("rewritePositionDeleteFiles: %v", err)
	}

	posPaths, _ := loadLiveDeleteFilePaths(t, client, setup.BucketName, setup.tablePath())
	if len(posPaths) != 2 {
		t.Fatalf("expected rewritten file plus untouched multi-target file, got %v", posPaths)
	}
	if posPaths[0] != "data/pd3.parquet" && posPaths[1] != "data/pd3.parquet" {
		t.Fatalf("expected multi-target delete file to be preserved, got %v", posPaths)
	}
	if !strings.HasPrefix(posPaths[0], "data/rewrite-delete-") && !strings.HasPrefix(posPaths[1], "data/rewrite-delete-") {
		t.Fatalf("expected rewritten delete file to remain live, got %v", posPaths)
	}
}

func TestRewritePositionDeleteFilesRebuildsMixedDeleteManifests(t *testing.T) {
	fs, client := startFakeFiler(t)

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "alice"}, {2, "bob"}, {3, "charlie"}}},
		},
		[]struct {
			Name string
			Rows []struct {
				FilePath string
				Pos      int64
			}
		}{
			{"pd1.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 0}}},
			{"pd2.parquet", []struct {
				FilePath string
				Pos      int64
			}{{"data/d1.parquet", 1}}},
		},
		[]struct {
			Name     string
			FieldIDs []int
			Rows     []struct {
				ID   int64
				Name string
			}
		}{
			{"eq1.parquet", []int{1}, []struct {
				ID   int64
				Name string
			}{{3, "charlie"}}},
		},
	)
	rewriteDeleteManifestsAsMixed(t, fs, client, setup)

	handler := NewHandler(nil)
	config := Config{
		DeleteTargetFileSizeBytes:   64 * 1024 * 1024,
		DeleteMinInputFiles:         2,
		DeleteMaxFileGroupSizeBytes: 128 * 1024 * 1024,
		DeleteMaxOutputFiles:        4,
		MaxCommitRetries:            3,
	}

	if _, _, err := handler.rewritePositionDeleteFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config); err != nil {
		t.Fatalf("rewritePositionDeleteFiles: %v", err)
	}

	posPaths, eqPaths := loadLiveDeleteFilePaths(t, client, setup.BucketName, setup.tablePath())
	if len(posPaths) != 1 || !strings.HasPrefix(posPaths[0], "data/rewrite-delete-") {
		t.Fatalf("expected only the rewritten position delete file to remain live, got %v", posPaths)
	}
	if len(eqPaths) != 1 || eqPaths[0] != "data/eq1.parquet" {
		t.Fatalf("expected equality delete file to be preserved, got %v", eqPaths)
	}
}
