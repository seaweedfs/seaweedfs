package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// Fake filer server for execution tests
// ---------------------------------------------------------------------------

// fakeFilerServer is an in-memory filer that implements the gRPC methods used
// by the iceberg maintenance handler.
type fakeFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu      sync.Mutex
	entries map[string]map[string]*filer_pb.Entry // dir → name → entry

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
	f.mu.Unlock()

	f.putEntry(req.Directory, req.Entry.Name, req.Entry)
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

// startFakeFiler starts a gRPC server and returns a connected client.
func startFakeFiler(t *testing.T) (*fakeFilerServer, filer_pb.SeaweedFilerClient) {
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

	return fakeServer, filer_pb.NewSeaweedFilerClient(conn)
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
	internalMeta := map[string]interface{}{
		"metadataVersion": 1,
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
			s3tables.ExtendedKeyMetadata: xattr,
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
		SnapshotRetentionHours: 0, // expire everything eligible
		MaxSnapshotsToKeep:     1, // keep only 1
		MaxCommitRetries:       3,
		Operations:             "expire_snapshots",
	}

	result, err := handler.expireSnapshots(context.Background(), client, setup.BucketName, setup.tablePath(), config)
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
		SnapshotRetentionHours: 24 * 365, // very long retention
		MaxSnapshotsToKeep:     10,
		MaxCommitRetries:       3,
	}

	result, err := handler.expireSnapshots(context.Background(), client, setup.BucketName, setup.tablePath(), config)
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

	result, err := handler.removeOrphans(context.Background(), client, setup.BucketName, setup.tablePath(), config)
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

	result, err := handler.removeOrphans(context.Background(), client, setup.BucketName, setup.tablePath(), config)
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

	result, err := handler.rewriteManifests(context.Background(), client, setup.BucketName, setup.tablePath(), config)
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
		MinInputFiles:    10, // threshold higher than actual count (1)
		MaxCommitRetries: 3,
	}

	result, err := handler.rewriteManifests(context.Background(), client, setup.BucketName, setup.tablePath(), config)
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
	sender := &recordingExecutionSender{}

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
			opResult, opErr = handler.expireSnapshots(context.Background(), client, setup.BucketName, setup.tablePath(), workerConfig)
		case "remove_orphans":
			opResult, opErr = handler.removeOrphans(context.Background(), client, setup.BucketName, setup.tablePath(), workerConfig)
		case "rewrite_manifests":
			opResult, opErr = handler.rewriteManifests(context.Background(), client, setup.BucketName, setup.tablePath(), workerConfig)
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

	_ = sender // not used in this direct-call approach
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
		SnapshotRetentionHours: 0,  // everything is expired
		MaxSnapshotsToKeep:     2,  // 3 > 2, needs maintenance
		MaxCommitRetries:       3,
	}

	tables, err := handler.scanTablesForMaintenance(
		context.Background(),
		client,
		config,
		"", "", "", // no filters
		0,          // no limit
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
