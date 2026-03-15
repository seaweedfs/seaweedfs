// Package maintenance contains integration tests for the iceberg table
// maintenance plugin worker. Tests start a real weed mini cluster, create
// tables via the S3 Tables API, populate Iceberg metadata via the filer
// gRPC API, and then exercise the iceberg.Handler operations against the
// live filer.
package maintenance

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/command"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	icebergHandler "github.com/seaweedfs/seaweedfs/weed/plugin/worker/iceberg"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// ---------------------------------------------------------------------------
// Cluster lifecycle (mirrors test/s3tables/table-buckets pattern)
// ---------------------------------------------------------------------------

type testCluster struct {
	dataDir        string
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	filerGrpcPort  int
	s3Port         int
	s3Endpoint     string
	isRunning      bool
}

var shared *testCluster

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	testDir, err := os.MkdirTemp("", "seaweed-iceberg-maint-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "SKIP: failed to create temp dir: %v\n", err)
		os.Exit(0)
	}

	cluster, err := startCluster(testDir, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "SKIP: failed to start cluster: %v\n", err)
		os.RemoveAll(testDir)
		os.Exit(0)
	}
	shared = cluster

	code := m.Run()
	shared.stop()
	os.RemoveAll(testDir)
	os.Exit(code)
}

func startCluster(testDir string, extraArgs []string) (*testCluster, error) {
	ports, err := findPorts(10)
	if err != nil {
		return nil, err
	}
	masterPort, masterGrpc := ports[0], ports[1]
	volumePort, volumeGrpc := ports[2], ports[3]
	filerPort, filerGrpc := ports[4], ports[5]
	s3Port, s3Grpc := ports[6], ports[7]
	adminPort, adminGrpc := ports[8], ports[9]

	_ = os.Remove(filepath.Join(testDir, "mini.options"))

	// Empty security.toml disables JWT auth.
	if err := os.WriteFile(filepath.Join(testDir, "security.toml"), []byte("# test\n"), 0644); err != nil {
		return nil, err
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		os.Setenv("AWS_ACCESS_KEY_ID", "admin")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", "admin")
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &testCluster{
		dataDir:       testDir,
		ctx:           ctx,
		cancel:        cancel,
		filerGrpcPort: filerGrpc,
		s3Port:        s3Port,
		s3Endpoint:    fmt.Sprintf("http://127.0.0.1:%d", s3Port),
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		oldDir, _ := os.Getwd()
		oldArgs := os.Args
		defer func() { os.Chdir(oldDir); os.Args = oldArgs }()
		os.Chdir(testDir)

		args := []string{
			"-dir=" + testDir,
			"-master.dir=" + testDir,
			"-master.port=" + strconv.Itoa(masterPort),
			"-master.port.grpc=" + strconv.Itoa(masterGrpc),
			"-volume.port=" + strconv.Itoa(volumePort),
			"-volume.port.grpc=" + strconv.Itoa(volumeGrpc),
			"-volume.port.public=" + strconv.Itoa(volumePort),
			"-volume.publicUrl=127.0.0.1:" + strconv.Itoa(volumePort),
			"-filer.port=" + strconv.Itoa(filerPort),
			"-filer.port.grpc=" + strconv.Itoa(filerGrpc),
			"-s3.port=" + strconv.Itoa(s3Port),
			"-s3.port.grpc=" + strconv.Itoa(s3Grpc),
			"-admin.port=" + strconv.Itoa(adminPort),
			"-admin.port.grpc=" + strconv.Itoa(adminGrpc),
			"-webdav.port=0",
			"-admin.ui=false",
			"-master.volumeSizeLimitMB=32",
			"-ip=127.0.0.1",
			"-master.peers=none",
			"-s3.iam.readOnly=false",
		}
		args = append(args, extraArgs...)
		os.Args = append([]string{"weed"}, args...)
		glog.MaxSize = 1024 * 1024
		for _, cmd := range command.Commands {
			if cmd.Name() == "mini" && cmd.Run != nil {
				cmd.Flag.Parse(os.Args[1:])
				command.MiniClusterCtx = ctx
				cmd.Run(cmd, cmd.Flag.Args())
				command.MiniClusterCtx = nil
				return
			}
		}
	}()

	if err := waitReady(c.s3Endpoint, 30*time.Second); err != nil {
		cancel()
		return nil, err
	}
	c.isRunning = true
	return c, nil
}

func (c *testCluster) stop() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.isRunning {
		time.Sleep(500 * time.Millisecond)
	}
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
}

func (c *testCluster) filerConn(t *testing.T) (*grpc.ClientConn, filer_pb.SeaweedFilerClient) {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", c.filerGrpcPort)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn, filer_pb.NewSeaweedFilerClient(conn)
}

func findPorts(n int) ([]int, error) {
	ls := make([]*net.TCPListener, n)
	ps := make([]int, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for j := 0; j < i; j++ {
				ls[j].Close()
			}
			return nil, err
		}
		ls[i] = l.(*net.TCPListener)
		ps[i] = ls[i].Addr().(*net.TCPAddr).Port
	}
	for _, l := range ls {
		l.Close()
	}
	return ps, nil
}

func waitReady(endpoint string, timeout time.Duration) error {
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(endpoint)
		if err == nil {
			resp.Body.Close()
			time.Sleep(500 * time.Millisecond)
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", endpoint)
}

func randomSuffix() string {
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// ---------------------------------------------------------------------------
// Helpers for populating Iceberg table state via filer gRPC
// ---------------------------------------------------------------------------

func newTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Type: iceberg.PrimitiveTypes.Int64, Name: "id", Required: true},
		iceberg.NestedField{ID: 2, Type: iceberg.PrimitiveTypes.String, Name: "name", Required: false},
	)
}

func buildMetadata(t *testing.T, snapshots []table.Snapshot) table.Metadata {
	t.Helper()
	schema := newTestSchema()
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "s3://test/table", nil)
	require.NoError(t, err)
	if len(snapshots) == 0 {
		return meta
	}

	// Iceberg validates that snapshot timestamps >= metadata lastUpdatedMs.
	// Offset all timestamps so they're safely after the metadata was created.
	baseMs := time.Now().UnixMilli() + 100
	for i := range snapshots {
		snapshots[i].TimestampMs = baseMs + int64(i)
	}

	builder, err := table.MetadataBuilderFromBase(meta, "s3://test/table")
	require.NoError(t, err)
	var lastID int64
	for i := range snapshots {
		s := snapshots[i]
		require.NoError(t, builder.AddSnapshot(&s))
		lastID = s.SnapshotID
	}
	require.NoError(t, builder.SetSnapshotRef(table.MainBranch, lastID, table.BranchRef))
	result, err := builder.Build()
	require.NoError(t, err)
	return result
}

// s3put performs an S3-signed PUT request. When key is empty it creates a
// bucket; otherwise it uploads an object.
func s3put(t *testing.T, s3Endpoint, bucket, key string, body []byte) {
	t.Helper()
	urlPath := "/" + bucket
	if key != "" {
		urlPath += "/" + key
	}
	var reader *bytes.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(http.MethodPut, s3Endpoint+urlPath, reader)
	require.NoError(t, err)
	req.Header.Set("Host", req.URL.Host)
	hash := sha256.Sum256(body)
	err = v4.NewSigner().SignHTTP(context.Background(),
		aws.Credentials{AccessKeyID: "admin", SecretAccessKey: "admin"},
		req, hex.EncodeToString(hash[:]), "s3", "us-east-1", time.Now())
	require.NoError(t, err)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		t.Logf("s3put(%s/%s) → %d: %s", bucket, key, resp.StatusCode, string(b))
	}
}

// createBucketViaS3 creates a regular S3 bucket via the S3 PUT Bucket API.
// populateTableViaFiler creates the Iceberg directory structure, metadata xattr,
// manifests, and data files for a table directly in the filer.
// The table bucket, namespace, and table must already exist (created via S3
// Tables API).
func populateTableViaFiler(
	t *testing.T,
	client filer_pb.SeaweedFilerClient,
	s3Endpoint, bucketName, namespace, tableName string,
	snapshots []table.Snapshot,
) table.Metadata {
	t.Helper()
	ctx := context.Background()
	meta := buildMetadata(t, snapshots)
	fullJSON, err := json.Marshal(meta)
	require.NoError(t, err)

	tablePath := path.Join(namespace, tableName)
	bucketsPath := s3tables.TablesPath
	tableFilerPath := path.Join(bucketsPath, bucketName, tablePath)
	metaDir := path.Join(tableFilerPath, "metadata")
	dataDir := path.Join(tableFilerPath, "data")

	// Build the table metadata xattr
	internalMeta := map[string]interface{}{
		"metadataVersion":  1,
		"metadataLocation": "metadata/v1.metadata.json",
		"metadata": map[string]interface{}{
			"fullMetadata": json.RawMessage(fullJSON),
		},
	}
	xattr, err := json.Marshal(internalMeta)
	require.NoError(t, err)

	// Create the S3 bucket via PUT, then create the directory tree via S3 PUT
	// object with zero-byte content (the filer creates intermediate dirs).
	s3put(t, s3Endpoint, bucketName, "", nil)                                         // create bucket
	s3put(t, s3Endpoint, bucketName, namespace+"/.dir", []byte{})                     // create ns dir
	s3put(t, s3Endpoint, bucketName, tablePath+"/.dir", []byte{})                     // create table dir
	s3put(t, s3Endpoint, bucketName, tablePath+"/metadata/.dir", []byte{})            // create metadata dir
	s3put(t, s3Endpoint, bucketName, tablePath+"/data/.dir", []byte{})                // create data dir

	// Now set the table bucket and table xattrs via filer gRPC.
	// Mark bucket as table bucket.
	bucketEntry := lookupEntry(t, client, bucketsPath, bucketName)
	require.NotNil(t, bucketEntry, "bucket should exist after S3 PUT")
	if bucketEntry.Extended == nil {
		bucketEntry.Extended = make(map[string][]byte)
	}
	bucketEntry.Extended[s3tables.ExtendedKeyTableBucket] = []byte("true")
	_, err = client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory: bucketsPath, Entry: bucketEntry,
	})
	require.NoError(t, err)

	// Set table metadata xattr.
	nsDir := path.Join(bucketsPath, bucketName, namespace)
	tableEntry := lookupEntry(t, client, nsDir, tableName)
	require.NotNil(t, tableEntry, "table dir should exist after S3 PUT")
	if tableEntry.Extended == nil {
		tableEntry.Extended = make(map[string][]byte)
	}
	tableEntry.Extended[s3tables.ExtendedKeyMetadata] = xattr
	_, err = client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory: nsDir, Entry: tableEntry,
	})
	require.NoError(t, err)

	// Write manifest + manifest list + data file for each snapshot
	schema := meta.CurrentSchema()
	spec := meta.PartitionSpec()
	version := meta.Version()

	for _, snap := range snapshots {
		if snap.ManifestList == "" {
			continue
		}
		snapID := snap.SnapshotID
		dataFileName := fmt.Sprintf("snap-%d-data.parquet", snapID)
		dfBuilder, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData,
			path.Join("data", dataFileName), iceberg.ParquetFile,
			map[int]any{}, nil, nil, 10, 4096,
		)
		require.NoError(t, err)

		entry := iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &snapID, nil, nil, dfBuilder.Build(),
		)

		// Manifest
		manifestName := fmt.Sprintf("manifest-%d.avro", snapID)
		var manifestBuf bytes.Buffer
		mf, err := iceberg.WriteManifest(
			path.Join("metadata", manifestName), &manifestBuf,
			version, spec, schema, snapID, []iceberg.ManifestEntry{entry},
		)
		require.NoError(t, err)
		writeFile(t, ctx, client, metaDir, manifestName, manifestBuf.Bytes())

		// Manifest list
		mlName := path.Base(snap.ManifestList)
		var mlBuf bytes.Buffer
		parent := snap.ParentSnapshotID
		seqNum := snap.SequenceNumber
		require.NoError(t, iceberg.WriteManifestList(
			version, &mlBuf, snapID, parent, &seqNum, 0,
			[]iceberg.ManifestFile{mf},
		))
		writeFile(t, ctx, client, metaDir, mlName, mlBuf.Bytes())

		// Data file (dummy content)
		writeFile(t, ctx, client, dataDir, dataFileName, []byte("fake-parquet"))
	}

	// Wait for snapshot timestamps (set slightly in the future by
	// buildMetadata) to move into the past so expiration checks work.
	time.Sleep(200 * time.Millisecond)

	return meta
}

func writeFile(t *testing.T, ctx context.Context, client filer_pb.SeaweedFilerClient, dir, name string, content []byte) {
	t.Helper()
	resp, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory: dir,
		Entry: &filer_pb.Entry{
			Name: name,
			Attributes: &filer_pb.FuseAttributes{
				Mtime: time.Now().Unix(), Crtime: time.Now().Unix(),
				FileMode: uint32(0644), FileSize: uint64(len(content)),
			},
			Content: content,
		},
	})
	require.NoError(t, err, "writeFile(%s, %s): rpc error", dir, name)
	require.Empty(t, resp.Error, "writeFile(%s, %s): resp error", dir, name)
}

func lookupEntry(t *testing.T, client filer_pb.SeaweedFilerClient, dir, name string) *filer_pb.Entry {
	t.Helper()
	resp, err := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir, Name: name,
	})
	if err != nil {
		return nil
	}
	return resp.Entry
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

func TestIcebergMaintenanceIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("ExpireSnapshots", testExpireSnapshots)
	t.Run("RemoveOrphans", testRemoveOrphans)
	t.Run("RewriteManifests", testRewriteManifests)
	t.Run("FullMaintenanceCycle", testFullMaintenanceCycle)
}

func testExpireSnapshots(t *testing.T) {
	_, client := shared.filerConn(t)
	suffix := randomSuffix()
	bucket := "maint-expire-" + suffix
	ns := "ns"
	tbl := "tbl"

	now := time.Now().UnixMilli()
	snapshots := []table.Snapshot{
		{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
		{SnapshotID: 2, TimestampMs: now + 1, ManifestList: "metadata/snap-2.avro"},
		{SnapshotID: 3, TimestampMs: now + 2, ManifestList: "metadata/snap-3.avro"},
	}
	populateTableViaFiler(t, client, shared.s3Endpoint, bucket, ns, tbl, snapshots)

	handler := icebergHandler.NewHandler(nil)
	config := icebergHandler.Config{
		SnapshotRetentionHours: 0, // instant expiry — everything eligible
		MaxSnapshotsToKeep:     1, // keep only the current snapshot
		MaxCommitRetries:       3,
	}

	result, err := handler.ExpireSnapshots(context.Background(), client, bucket, path.Join(ns, tbl), config)
	require.NoError(t, err)
	assert.Contains(t, result, "expired")
	t.Logf("ExpireSnapshots result: %s", result)

	// Verify metadata was updated
	entry := lookupEntry(t, client, path.Join(s3tables.TablesPath, bucket, ns), tbl)
	require.NotNil(t, entry)
	xattr := entry.Extended[s3tables.ExtendedKeyMetadata]
	require.NotEmpty(t, xattr)

	// Parse updated metadata to verify snapshot count
	var internalMeta struct {
		MetadataVersion int `json:"metadataVersion"`
	}
	require.NoError(t, json.Unmarshal(xattr, &internalMeta))
	assert.Greater(t, internalMeta.MetadataVersion, 1, "metadata version should have been incremented")
}

func testRemoveOrphans(t *testing.T) {
	_, client := shared.filerConn(t)
	suffix := randomSuffix()
	bucket := "maint-orphan-" + suffix
	ns := "ns"
	tbl := "tbl"

	now := time.Now().UnixMilli()
	snapshots := []table.Snapshot{
		{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
	}
	populateTableViaFiler(t, client, shared.s3Endpoint, bucket, ns, tbl, snapshots)

	ctx := context.Background()
	tablePath := path.Join(ns, tbl)
	tableFilerPath := path.Join(s3tables.TablesPath, bucket, tablePath)
	dataDir := path.Join(tableFilerPath, "data")
	metaDir := path.Join(tableFilerPath, "metadata")

	// Create orphan files (old enough to be removed)
	oldTime := time.Now().Add(-200 * time.Hour).Unix()
	writeOrphan := func(dir, name string) {
		_, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry: &filer_pb.Entry{
				Name: name,
				Attributes: &filer_pb.FuseAttributes{
					Mtime: oldTime, Crtime: oldTime,
					FileMode: uint32(0644), FileSize: 100,
				},
				Content: []byte("orphan"),
			},
		})
		require.NoError(t, err)
	}
	writeOrphan(dataDir, "orphan-data.parquet")
	writeOrphan(metaDir, "orphan-meta.avro")

	// Create a recent orphan (should NOT be removed)
	writeFile(t, ctx, client, dataDir, "recent-orphan.parquet", []byte("new"))

	handler := icebergHandler.NewHandler(nil)
	config := icebergHandler.Config{
		OrphanOlderThanHours: 72,
		MaxCommitRetries:     3,
	}

	result, err := handler.RemoveOrphans(ctx, client, bucket, tablePath, config)
	require.NoError(t, err)
	assert.Contains(t, result, "removed")
	t.Logf("RemoveOrphans result: %s", result)

	// Verify orphans were removed
	assert.Nil(t, lookupEntry(t, client, dataDir, "orphan-data.parquet"), "old orphan data file should be deleted")
	assert.Nil(t, lookupEntry(t, client, metaDir, "orphan-meta.avro"), "old orphan meta file should be deleted")

	// Verify recent orphan was kept
	assert.NotNil(t, lookupEntry(t, client, dataDir, "recent-orphan.parquet"), "recent orphan should be kept")
}

func testRewriteManifests(t *testing.T) {
	_, client := shared.filerConn(t)
	suffix := randomSuffix()
	bucket := "maint-rewrite-" + suffix
	ns := "ns"
	tbl := "tbl"

	// Each snapshot gets its own manifest list with one manifest. The current
	// snapshot (latest) therefore has only 1 manifest — below the rewrite
	// threshold. This tests the threshold check against a real filer.
	now := time.Now().UnixMilli()
	snapshots := []table.Snapshot{
		{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
	}
	populateTableViaFiler(t, client, shared.s3Endpoint, bucket, ns, tbl, snapshots)

	handler := icebergHandler.NewHandler(nil)
	config := icebergHandler.Config{
		MinManifestsToRewrite: 5, // threshold higher than 1 manifest
		MaxCommitRetries:      3,
	}

	tablePath := path.Join(ns, tbl)
	result, err := handler.RewriteManifests(context.Background(), client, bucket, tablePath, config)
	require.NoError(t, err)
	assert.Contains(t, result, "below threshold")
	t.Logf("RewriteManifests result: %s", result)
}

func testFullMaintenanceCycle(t *testing.T) {
	_, client := shared.filerConn(t)
	suffix := randomSuffix()
	bucket := "maint-full-" + suffix
	ns := "ns"
	tbl := "tbl"

	now := time.Now().UnixMilli()
	snapshots := []table.Snapshot{
		{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
		{SnapshotID: 2, TimestampMs: now + 1, ManifestList: "metadata/snap-2.avro"},
		{SnapshotID: 3, TimestampMs: now + 2, ManifestList: "metadata/snap-3.avro"},
	}
	populateTableViaFiler(t, client, shared.s3Endpoint, bucket, ns, tbl, snapshots)

	ctx := context.Background()
	tablePath := path.Join(ns, tbl)

	// Add an orphan
	dataDir := path.Join(s3tables.TablesPath, bucket, tablePath, "data")
	oldTime := time.Now().Add(-200 * time.Hour).Unix()
	_, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory: dataDir,
		Entry: &filer_pb.Entry{
			Name: "orphan.parquet",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: oldTime, Crtime: oldTime,
				FileMode: uint32(0644), FileSize: 100,
			},
			Content: []byte("orphan"),
		},
	})
	require.NoError(t, err)

	handler := icebergHandler.NewHandler(nil)

	// Step 1: Expire snapshots
	expireConfig := icebergHandler.Config{
		SnapshotRetentionHours: 0, // instant expiry
		MaxSnapshotsToKeep:     1,
		MaxCommitRetries:       3,
	}
	result, err := handler.ExpireSnapshots(ctx, client, bucket, tablePath, expireConfig)
	require.NoError(t, err)
	assert.Contains(t, result, "expired")
	t.Logf("Step 1 (expire): %s", result)

	// Step 2: Remove orphans
	orphanConfig := icebergHandler.Config{
		OrphanOlderThanHours: 72,
		MaxCommitRetries:     3,
	}
	result, err = handler.RemoveOrphans(ctx, client, bucket, tablePath, orphanConfig)
	require.NoError(t, err)
	t.Logf("Step 2 (orphans): %s", result)
	// The orphan and the unreferenced files from expired snapshots should be gone
	assert.Contains(t, result, "removed")

	// Step 3: Verify metadata is consistent
	entry := lookupEntry(t, client, path.Join(s3tables.TablesPath, bucket, ns), tbl)
	require.NotNil(t, entry)
	xattr := entry.Extended[s3tables.ExtendedKeyMetadata]
	require.NotEmpty(t, xattr)

	var internalMeta struct {
		MetadataVersion int `json:"metadataVersion"`
	}
	require.NoError(t, json.Unmarshal(xattr, &internalMeta))
	assert.Greater(t, internalMeta.MetadataVersion, 1, "metadata version should have advanced through the cycle")
	t.Logf("Final metadata version: %d", internalMeta.MetadataVersion)
}

