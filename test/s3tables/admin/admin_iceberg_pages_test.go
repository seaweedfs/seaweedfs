// Package admin contains integration tests for the admin UI's Iceberg
// catalog and data-preview pages. Tests start a real weed mini cluster
// (with the admin UI enabled), create a table bucket, namespace, and table
// via the S3 Tables manager, populate real Parquet data files, manifests,
// and snapshots via S3 and the filer gRPC API, and then assert on the HTML
// the admin server renders.
package admin

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/test/testutil"
	"github.com/seaweedfs/seaweedfs/weed/command"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// ---------------------------------------------------------------------------
// Cluster lifecycle (mirrors test/s3tables/maintenance)
// ---------------------------------------------------------------------------

type testCluster struct {
	dataDir       string
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	filerGrpcPort int
	s3Port        int
	adminPort     int
	s3Endpoint    string
	adminEndpoint string
	isRunning     bool
}

var shared *testCluster

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	testDir, err := os.MkdirTemp("", "seaweed-admin-iceberg-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "SKIP: failed to create temp dir: %v\n", err)
		os.Exit(0)
	}

	cluster, err := startCluster(testDir)
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

func startCluster(testDir string) (*testCluster, error) {
	ports, err := testutil.AllocatePorts(10)
	if err != nil {
		return nil, err
	}
	masterPort, masterGrpc := ports[0], ports[1]
	volumePort, volumeGrpc := ports[2], ports[3]
	filerPort, filerGrpc := ports[4], ports[5]
	s3Port, s3Grpc := ports[6], ports[7]
	adminPort, adminGrpc := ports[8], ports[9]

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
		adminPort:     adminPort,
		s3Endpoint:    fmt.Sprintf("http://127.0.0.1:%d", s3Port),
		adminEndpoint: fmt.Sprintf("http://127.0.0.1:%d", adminPort),
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
			"-master.volumeSizeLimitMB=32",
			"-ip=127.0.0.1",
			"-master.peers=none",
			"-s3.iam.readOnly=false",
		}
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
	if err := waitReady(c.adminEndpoint, 30*time.Second); err != nil {
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

func (c *testCluster) filerConn(t *testing.T) filer_pb.SeaweedFilerClient {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", c.filerGrpcPort)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return filer_pb.NewSeaweedFilerClient(conn)
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
// Catalog and table population
// ---------------------------------------------------------------------------

type previewRow struct {
	ID   int64  `parquet:"id"`
	Name string `parquet:"name"`
}

func buildParquet(t *testing.T, rows []previewRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[previewRow](&buf)
	_, err := writer.Write(rows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func newS3Client(t *testing.T, endpoint string) *s3.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("admin", "admin", "")),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

type catalogFixture struct {
	bucket    string
	namespace string
	table     string
	snap1ID   int64
	snap2ID   int64
	file1Path string // manifest-listed location of the first data file (relative)
	file2Path string // manifest-listed location of the second data file (s3:// URI)
}

// setupCatalog creates a table bucket, namespace, and two tables via the S3
// Tables manager: "events" with two snapshots backed by real Parquet files,
// and "empty" with no Iceberg metadata.
func setupCatalog(t *testing.T, client filer_pb.SeaweedFilerClient) catalogFixture {
	t.Helper()
	ctx := context.Background()

	fx := catalogFixture{
		bucket:    "admin-ui-" + randomSuffix(),
		namespace: "analytics",
		table:     "events",
		snap1ID:   3001,
		snap2ID:   3002,
	}
	fx.file1Path = "data/p1.parquet"
	fx.file2Path = fmt.Sprintf("s3://%s/%s/%s/data/p2.parquet", fx.bucket, fx.namespace, fx.table)

	mgr := s3tables.NewManager()
	mgr.SetAccountID(s3_constants.AccountAdminId)
	mc := s3tables.NewManagerClient(client)
	exec := func(op string, req, resp interface{}) {
		t.Helper()
		require.NoError(t, mgr.Execute(ctx, mc, op, req, resp, s3_constants.AccountAdminId), "s3tables %s", op)
	}

	var bucketResp s3tables.CreateTableBucketResponse
	exec("CreateTableBucket", &s3tables.CreateTableBucketRequest{Name: fx.bucket}, &bucketResp)
	exec("CreateNamespace", &s3tables.CreateNamespaceRequest{TableBucketARN: bucketResp.ARN, Namespace: []string{fx.namespace}}, &s3tables.CreateNamespaceResponse{})
	exec("CreateTable", &s3tables.CreateTableRequest{TableBucketARN: bucketResp.ARN, Namespace: []string{fx.namespace}, Name: fx.table, Format: "ICEBERG"}, &s3tables.CreateTableResponse{})
	exec("CreateTable", &s3tables.CreateTableRequest{TableBucketARN: bucketResp.ARN, Namespace: []string{fx.namespace}, Name: "empty", Format: "ICEBERG"}, &s3tables.CreateTableResponse{})

	// Upload real Parquet data files via S3 so they are chunk-backed entries.
	s3Client := newS3Client(t, shared.s3Endpoint)
	tableKey := path.Join(fx.namespace, fx.table)
	rows1 := []previewRow{{1, "p1-row-1"}, {2, "p1-row-2"}, {3, "p1-row-3"}, {4, "p1-row-4"}, {5, "p1-row-5"}}
	rows2 := []previewRow{{6, "p2-row-6"}, {7, "p2-row-7"}, {8, "p2-row-8"}}
	parquet1 := buildParquet(t, rows1)
	parquet2 := buildParquet(t, rows2)
	putObject(t, s3Client, fx.bucket, path.Join(tableKey, "data/p1.parquet"), parquet1)
	putObject(t, s3Client, fx.bucket, path.Join(tableKey, "data/p2.parquet"), parquet2)

	// Build table metadata with two snapshots. Snapshot 2's manifest list is
	// cumulative (both manifests) as engines write it.
	location := fmt.Sprintf("s3://%s/%s/%s", fx.bucket, fx.namespace, fx.table)
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Type: iceberg.PrimitiveTypes.Int64, Name: "id", Required: true},
		iceberg.NestedField{ID: 2, Type: iceberg.PrimitiveTypes.String, Name: "name", Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, nil)
	require.NoError(t, err)

	baseMs := time.Now().UnixMilli() + 100
	snap1 := table.Snapshot{SnapshotID: fx.snap1ID, SequenceNumber: 1, TimestampMs: baseMs, ManifestList: "metadata/snap-3001.avro"}
	snap2 := table.Snapshot{SnapshotID: fx.snap2ID, ParentSnapshotID: &fx.snap1ID, SequenceNumber: 2, TimestampMs: baseMs + 1, ManifestList: "metadata/snap-3002.avro"}

	builder, err := table.MetadataBuilderFromBase(meta, location)
	require.NoError(t, err)
	require.NoError(t, builder.AddSnapshot(&snap1))
	require.NoError(t, builder.AddSnapshot(&snap2))
	require.NoError(t, builder.SetSnapshotRef(table.MainBranch, fx.snap2ID, table.BranchRef))
	meta, err = builder.Build()
	require.NoError(t, err)

	// Write manifests and manifest lists into the table's metadata directory.
	metaDir := path.Join(s3tables.TablesPath, fx.bucket, fx.namespace, fx.table, "metadata")
	spec := meta.PartitionSpec()
	version := meta.Version()

	mf1 := writeManifest(t, ctx, client, metaDir, "manifest-3001.avro", version, spec, schema, fx.snap1ID, fx.file1Path, int64(len(rows1)), int64(len(parquet1)))
	mf2 := writeManifest(t, ctx, client, metaDir, "manifest-3002.avro", version, spec, schema, fx.snap2ID, fx.file2Path, int64(len(rows2)), int64(len(parquet2)))

	list1 := writeManifestList(t, ctx, client, metaDir, "snap-3001.avro", version, fx.snap1ID, nil, 1, []iceberg.ManifestFile{mf1})
	// Re-read snapshot 1's list so mf1 carries its assigned sequence number,
	// which the cumulative snapshot 2 list requires.
	assigned, err := iceberg.ReadManifestList(bytes.NewReader(list1))
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	writeManifestList(t, ctx, client, metaDir, "snap-3002.avro", version, fx.snap2ID, &fx.snap1ID, 2, []iceberg.ManifestFile{assigned[0], mf2})

	// Point the table's catalog xattr at the new metadata.
	fullJSON, err := json.Marshal(meta)
	require.NoError(t, err)
	writeFile(t, ctx, client, metaDir, "v2.metadata.json", fullJSON)
	updateTableMetadataXattr(t, ctx, client, fx, fullJSON)

	// Snapshot timestamps sit slightly in the future; let them pass.
	time.Sleep(200 * time.Millisecond)
	return fx
}

func putObject(t *testing.T, client *s3.Client, bucket, key string, body []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err)
}

func writeManifest(t *testing.T, ctx context.Context, client filer_pb.SeaweedFilerClient, metaDir, name string, version int, spec iceberg.PartitionSpec, schema *iceberg.Schema, snapID int64, dataFilePath string, recordCount, fileSize int64) iceberg.ManifestFile {
	t.Helper()
	dfBuilder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentData,
		dataFilePath, iceberg.ParquetFile,
		map[int]any{}, nil, nil, recordCount, fileSize,
	)
	require.NoError(t, err)
	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfBuilder.Build())

	var buf bytes.Buffer
	mf, err := iceberg.WriteManifest(path.Join("metadata", name), &buf, version, spec, schema, snapID, []iceberg.ManifestEntry{entry})
	require.NoError(t, err)
	writeFile(t, ctx, client, metaDir, name, buf.Bytes())
	return mf
}

func writeManifestList(t *testing.T, ctx context.Context, client filer_pb.SeaweedFilerClient, metaDir, name string, version int, snapID int64, parent *int64, seqNum int64, files []iceberg.ManifestFile) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(version, &buf, snapID, parent, &seqNum, 0, files))
	writeFile(t, ctx, client, metaDir, name, buf.Bytes())
	return buf.Bytes()
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
	require.NoError(t, err, "writeFile(%s, %s)", dir, name)
	require.Empty(t, resp.Error, "writeFile(%s, %s)", dir, name)
}

func updateTableMetadataXattr(t *testing.T, ctx context.Context, client filer_pb.SeaweedFilerClient, fx catalogFixture, fullJSON []byte) {
	t.Helper()
	nsDir := path.Join(s3tables.TablesPath, fx.bucket, fx.namespace)
	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{Directory: nsDir, Name: fx.table})
	require.NoError(t, err)
	require.NotNil(t, resp.Entry)

	var internalMeta map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(resp.Entry.Extended[s3tables.ExtendedKeyMetadata], &internalMeta))
	metaObj := map[string]json.RawMessage{}
	if raw, ok := internalMeta["metadata"]; ok {
		require.NoError(t, json.Unmarshal(raw, &metaObj))
	}
	metaObj["fullMetadata"] = fullJSON
	metaJSON, err := json.Marshal(metaObj)
	require.NoError(t, err)
	internalMeta["metadata"] = metaJSON
	internalMeta["metadataVersion"] = json.RawMessage("2")
	internalMeta["metadataLocation"] = json.RawMessage(`"metadata/v2.metadata.json"`)
	xattr, err := json.Marshal(internalMeta)
	require.NoError(t, err)

	resp.Entry.Extended[s3tables.ExtendedKeyMetadata] = xattr
	resp.Entry.Extended[s3tables.ExtendedKeyMetadataVersion] = []byte("2")
	_, err = client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{Directory: nsDir, Entry: resp.Entry})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Page fetching
// ---------------------------------------------------------------------------

func fetchPage(t *testing.T, pagePath string) string {
	t.Helper()
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Get(shared.adminEndpoint + pagePath)
	require.NoError(t, err, "GET %s", pagePath)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s: %s", pagePath, string(body))
	return string(body)
}

// fetchPageUntil re-fetches a page until the predicate passes, tolerating the
// admin server's filer-discovery delay right after startup.
func fetchPageUntil(t *testing.T, pagePath string, predicate func(string) bool) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var body string
	for time.Now().Before(deadline) {
		body = fetchPage(t, pagePath)
		if predicate(body) {
			return body
		}
		time.Sleep(500 * time.Millisecond)
	}
	return body
}

func tablePagePath(fx catalogFixture, tableName, suffix string) string {
	return fmt.Sprintf("/object-store/s3tables/buckets/%s/namespaces/%s/tables/%s%s",
		url.PathEscape(fx.bucket), url.PathEscape(fx.namespace), url.PathEscape(tableName), suffix)
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

func TestAdminIcebergPages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client := shared.filerConn(t)
	fx := setupCatalog(t, client)

	t.Run("CatalogBrowsing", func(t *testing.T) {
		buckets := fetchPageUntil(t, "/object-store/s3tables/buckets", func(body string) bool {
			return strings.Contains(body, fx.bucket)
		})
		assert.Contains(t, buckets, fx.bucket)

		namespaces := fetchPage(t, fmt.Sprintf("/object-store/s3tables/buckets/%s/namespaces", url.PathEscape(fx.bucket)))
		assert.Contains(t, namespaces, fx.namespace)

		tables := fetchPage(t, fmt.Sprintf("/object-store/s3tables/buckets/%s/namespaces/%s/tables", url.PathEscape(fx.bucket), url.PathEscape(fx.namespace)))
		assert.Contains(t, tables, fx.table)
		assert.Contains(t, tables, "empty")
	})

	t.Run("TableDetails", func(t *testing.T) {
		body := fetchPage(t, tablePagePath(fx, fx.table, ""))
		assert.Contains(t, body, "Browse Data")
		assert.Contains(t, body, "/data")
		assert.Contains(t, body, ">id<")
		assert.Contains(t, body, ">name<")
		assert.Contains(t, body, strconv.FormatInt(fx.snap1ID, 10))
		assert.Contains(t, body, strconv.FormatInt(fx.snap2ID, 10))
	})

	t.Run("DataPreviewCurrentSnapshot", func(t *testing.T) {
		body := fetchPage(t, tablePagePath(fx, fx.table, "/data"))
		assert.Contains(t, body, "Showing 8 row(s) from 2 data file(s).")
		assert.Contains(t, body, "p1-row-1")
		assert.Contains(t, body, "p2-row-8")
		assert.Contains(t, body, fx.file1Path)
		assert.Contains(t, body, fx.file2Path)
		assert.Contains(t, body, "current")
	})

	t.Run("DataPreviewOldSnapshot", func(t *testing.T) {
		body := fetchPage(t, tablePagePath(fx, fx.table, "/data?snapshot="+strconv.FormatInt(fx.snap1ID, 10)))
		assert.Contains(t, body, "Showing 5 row(s) from 1 data file(s).")
		assert.Contains(t, body, "p1-row-5")
		assert.NotContains(t, body, "p2-row-6")
	})

	t.Run("DataPreviewSingleFile", func(t *testing.T) {
		body := fetchPage(t, tablePagePath(fx, fx.table, "/data?file="+url.QueryEscape(fx.file2Path)))
		assert.Contains(t, body, "Showing 3 row(s) from 1 data file(s).")
		assert.Contains(t, body, "p2-row-6")
		assert.NotContains(t, body, "p1-row-1")
	})

	t.Run("DataPreviewRowLimit", func(t *testing.T) {
		body := fetchPage(t, tablePagePath(fx, fx.table, "/data?limit=3"))
		assert.Contains(t, body, "Showing 3 row(s) from 1 data file(s).")
	})

	t.Run("DataPreviewUnknownFile", func(t *testing.T) {
		body := fetchPage(t, tablePagePath(fx, fx.table, "/data?file="+url.QueryEscape("s3://elsewhere/x.parquet")))
		assert.Contains(t, body, "Requested data file is not part of this snapshot.")
	})

	t.Run("DataPreviewUnknownSnapshot", func(t *testing.T) {
		body := fetchPage(t, tablePagePath(fx, fx.table, "/data?snapshot=42"))
		assert.Contains(t, body, "Snapshot 42 not found.")
	})

	t.Run("DataPreviewTableWithoutMetadata", func(t *testing.T) {
		body := fetchPage(t, tablePagePath(fx, "empty", "/data"))
		assert.Contains(t, body, "Table has no Iceberg metadata.")
	})
}
