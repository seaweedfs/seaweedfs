package s3tables

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"sync"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/command"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
)

var (
	miniClusterMutex sync.Mutex
)

func TestS3TablesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create and start test cluster
	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	// Create S3 Tables client
	client := NewS3TablesClient(cluster.s3Endpoint, testRegion, testAccessKey, testSecretKey)

	// Run test suite
	t.Run("TableBucketLifecycle", func(t *testing.T) {
		testTableBucketLifecycle(t, client)
	})

	t.Run("NamespaceLifecycle", func(t *testing.T) {
		testNamespaceLifecycle(t, client)
	})

	t.Run("TableLifecycle", func(t *testing.T) {
		testTableLifecycle(t, client)
	})

	t.Run("TableBucketPolicy", func(t *testing.T) {
		testTableBucketPolicy(t, client)
	})

	t.Run("Tagging", func(t *testing.T) {
		testTagging(t, client)
	})
}

func testTableBucketLifecycle(t *testing.T, client *S3TablesClient) {
	bucketName := "test-bucket-" + randomString(8)

	// Create table bucket
	createResp, err := client.CreateTableBucket(bucketName, nil)
	require.NoError(t, err, "Failed to create table bucket")
	assert.Contains(t, createResp.ARN, bucketName)
	t.Logf("✓ Created table bucket: %s", createResp.ARN)

	// Get table bucket
	getResp, err := client.GetTableBucket(createResp.ARN)
	require.NoError(t, err, "Failed to get table bucket")
	assert.Equal(t, bucketName, getResp.Name)
	t.Logf("✓ Got table bucket: %s", getResp.Name)

	// List table buckets
	listResp, err := client.ListTableBuckets("", "", 0)
	require.NoError(t, err, "Failed to list table buckets")
	found := false
	for _, b := range listResp.TableBuckets {
		if b.Name == bucketName {
			found = true
			break
		}
	}
	assert.True(t, found, "Created bucket should appear in list")
	t.Logf("✓ Listed table buckets, found %d buckets", len(listResp.TableBuckets))

	// Delete table bucket
	err = client.DeleteTableBucket(createResp.ARN)
	require.NoError(t, err, "Failed to delete table bucket")
	t.Logf("✓ Deleted table bucket: %s", bucketName)

	// Verify bucket is deleted
	_, err = client.GetTableBucket(createResp.ARN)
	assert.Error(t, err, "Bucket should not exist after deletion")
}

func testNamespaceLifecycle(t *testing.T, client *S3TablesClient) {
	bucketName := "test-ns-bucket-" + randomString(8)
	namespaceName := "test_namespace"

	// Create table bucket first
	createBucketResp, err := client.CreateTableBucket(bucketName, nil)
	require.NoError(t, err, "Failed to create table bucket")
	defer client.DeleteTableBucket(createBucketResp.ARN)

	bucketARN := createBucketResp.ARN

	// Create namespace
	createNsResp, err := client.CreateNamespace(bucketARN, []string{namespaceName})
	require.NoError(t, err, "Failed to create namespace")
	assert.Equal(t, []string{namespaceName}, createNsResp.Namespace)
	t.Logf("✓ Created namespace: %s", namespaceName)

	// Get namespace
	getNsResp, err := client.GetNamespace(bucketARN, []string{namespaceName})
	require.NoError(t, err, "Failed to get namespace")
	assert.Equal(t, []string{namespaceName}, getNsResp.Namespace)
	t.Logf("✓ Got namespace: %v", getNsResp.Namespace)

	// List namespaces
	listNsResp, err := client.ListNamespaces(bucketARN, "", "", 0)
	require.NoError(t, err, "Failed to list namespaces")
	found := false
	for _, ns := range listNsResp.Namespaces {
		if len(ns.Namespace) > 0 && ns.Namespace[0] == namespaceName {
			found = true
			break
		}
	}
	assert.True(t, found, "Created namespace should appear in list")
	t.Logf("✓ Listed namespaces, found %d namespaces", len(listNsResp.Namespaces))

	// Delete namespace
	err = client.DeleteNamespace(bucketARN, []string{namespaceName})
	require.NoError(t, err, "Failed to delete namespace")
	t.Logf("✓ Deleted namespace: %s", namespaceName)

	// Verify namespace is deleted
	_, err = client.GetNamespace(bucketARN, []string{namespaceName})
	assert.Error(t, err, "Namespace should not exist after deletion")
}

func testTableLifecycle(t *testing.T, client *S3TablesClient) {
	bucketName := "test-table-bucket-" + randomString(8)
	namespaceName := "test_ns"
	tableName := "test_table"

	// Create table bucket
	createBucketResp, err := client.CreateTableBucket(bucketName, nil)
	require.NoError(t, err, "Failed to create table bucket")
	defer client.DeleteTableBucket(createBucketResp.ARN)

	bucketARN := createBucketResp.ARN

	// Create namespace
	_, err = client.CreateNamespace(bucketARN, []string{namespaceName})
	require.NoError(t, err, "Failed to create namespace")
	defer client.DeleteNamespace(bucketARN, []string{namespaceName})

	// Create table with Iceberg schema
	icebergMetadata := &s3tables.TableMetadata{
		Iceberg: &s3tables.IcebergMetadata{
			Schema: s3tables.IcebergSchema{
				Fields: []s3tables.IcebergSchemaField{
					{Name: "id", Type: "int", Required: true},
					{Name: "name", Type: "string"},
					{Name: "value", Type: "int"},
				},
			},
		},
	}

	createTableResp, err := client.CreateTable(bucketARN, []string{namespaceName}, tableName, "ICEBERG", icebergMetadata, nil)
	require.NoError(t, err, "Failed to create table")
	assert.NotEmpty(t, createTableResp.TableARN)
	assert.NotEmpty(t, createTableResp.VersionToken)
	t.Logf("✓ Created table: %s (version: %s)", createTableResp.TableARN, createTableResp.VersionToken)

	// Get table
	getTableResp, err := client.GetTable(bucketARN, []string{namespaceName}, tableName)
	require.NoError(t, err, "Failed to get table")
	assert.Equal(t, tableName, getTableResp.Name)
	assert.Equal(t, "ICEBERG", getTableResp.Format)
	t.Logf("✓ Got table: %s (format: %s)", getTableResp.Name, getTableResp.Format)

	// List tables
	listTablesResp, err := client.ListTables(bucketARN, []string{namespaceName}, "", "", 0)
	require.NoError(t, err, "Failed to list tables")
	found := false
	for _, tbl := range listTablesResp.Tables {
		if tbl.Name == tableName {
			found = true
			break
		}
	}
	assert.True(t, found, "Created table should appear in list")
	t.Logf("✓ Listed tables, found %d tables", len(listTablesResp.Tables))

	// Delete table
	err = client.DeleteTable(bucketARN, []string{namespaceName}, tableName)
	require.NoError(t, err, "Failed to delete table")
	t.Logf("✓ Deleted table: %s", tableName)

	// Verify table is deleted
	_, err = client.GetTable(bucketARN, []string{namespaceName}, tableName)
	assert.Error(t, err, "Table should not exist after deletion")
}

func testTableBucketPolicy(t *testing.T, client *S3TablesClient) {
	bucketName := "test-policy-bucket-" + randomString(8)

	// Create table bucket
	createBucketResp, err := client.CreateTableBucket(bucketName, nil)
	require.NoError(t, err, "Failed to create table bucket")
	defer client.DeleteTableBucket(createBucketResp.ARN)

	bucketARN := createBucketResp.ARN

	// Put bucket policy
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:*","Resource":"*"}]}`
	err = client.PutTableBucketPolicy(bucketARN, policy)
	require.NoError(t, err, "Failed to put table bucket policy")
	t.Logf("✓ Put table bucket policy")

	// Get bucket policy
	getPolicyResp, err := client.GetTableBucketPolicy(bucketARN)
	require.NoError(t, err, "Failed to get table bucket policy")
	assert.Equal(t, policy, getPolicyResp.ResourcePolicy)
	t.Logf("✓ Got table bucket policy")

	// Delete bucket policy
	err = client.DeleteTableBucketPolicy(bucketARN)
	require.NoError(t, err, "Failed to delete table bucket policy")
	t.Logf("✓ Deleted table bucket policy")

	// Verify policy is deleted
	_, err = client.GetTableBucketPolicy(bucketARN)
	assert.Error(t, err, "Policy should not exist after deletion")
}

func testTagging(t *testing.T, client *S3TablesClient) {
	bucketName := "test-tag-bucket-" + randomString(8)

	// Create table bucket with tags
	initialTags := map[string]string{"Environment": "test"}
	createBucketResp, err := client.CreateTableBucket(bucketName, initialTags)
	require.NoError(t, err, "Failed to create table bucket")
	defer client.DeleteTableBucket(createBucketResp.ARN)

	bucketARN := createBucketResp.ARN

	// List tags
	listTagsResp, err := client.ListTagsForResource(bucketARN)
	require.NoError(t, err, "Failed to list tags")
	assert.Equal(t, "test", listTagsResp.Tags["Environment"])
	t.Logf("✓ Listed tags: %v", listTagsResp.Tags)

	// Add more tags
	newTags := map[string]string{"Department": "Engineering"}
	err = client.TagResource(bucketARN, newTags)
	require.NoError(t, err, "Failed to tag resource")
	t.Logf("✓ Added tags")

	// Verify tags
	listTagsResp, err = client.ListTagsForResource(bucketARN)
	require.NoError(t, err, "Failed to list tags")
	assert.Equal(t, "test", listTagsResp.Tags["Environment"])
	assert.Equal(t, "Engineering", listTagsResp.Tags["Department"])
	t.Logf("✓ Verified tags: %v", listTagsResp.Tags)

	// Remove a tag
	err = client.UntagResource(bucketARN, []string{"Environment"})
	require.NoError(t, err, "Failed to untag resource")
	t.Logf("✓ Removed tag")

	// Verify tag is removed
	listTagsResp, err = client.ListTagsForResource(bucketARN)
	require.NoError(t, err, "Failed to list tags")
	_, hasEnvironment := listTagsResp.Tags["Environment"]
	assert.False(t, hasEnvironment, "Environment tag should be removed")
	assert.Equal(t, "Engineering", listTagsResp.Tags["Department"])
	t.Logf("✓ Verified tag removal")
}

// Helper functions

// findAvailablePort finds an available port by binding to port 0
func findAvailablePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

// startMiniCluster starts a weed mini instance directly without exec
func startMiniCluster(t *testing.T) (*TestCluster, error) {
	// Find available ports
	masterPort, err := findAvailablePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find master port: %v", err)
	}
	masterGrpcPort, err := findAvailablePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find master grpc port: %v", err)
	}
	volumePort, err := findAvailablePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find volume port: %v", err)
	}
	volumeGrpcPort, err := findAvailablePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find volume grpc port: %v", err)
	}
	filerPort, err := findAvailablePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find filer port: %v", err)
	}
	filerGrpcPort, err := findAvailablePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find filer grpc port: %v", err)
	}
	s3Port, err := findAvailablePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find s3 port: %v", err)
	}
	s3GrpcPort, err := findAvailablePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find s3 grpc port: %v", err)
	}
	// Create temporary directory for test data
	testDir := t.TempDir()

	// Ensure no configuration file from previous runs
	configFile := filepath.Join(testDir, "mini.options")
	_ = os.Remove(configFile)

	// Create context with timeout
	ctx, cancel := context.WithCancel(context.Background())

	s3Endpoint := fmt.Sprintf("http://127.0.0.1:%d", s3Port)
	cluster := &TestCluster{
		dataDir:    testDir,
		ctx:        ctx,
		cancel:     cancel,
		masterPort: masterPort,
		volumePort: volumePort,
		filerPort:  filerPort,
		s3Port:     s3Port,
		s3Endpoint: s3Endpoint,
	}

	// Create empty security.toml to disable JWT authentication in tests
	securityToml := filepath.Join(testDir, "security.toml")
	err = os.WriteFile(securityToml, []byte("# Empty security config for testing\n"), 0644)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create security.toml: %v", err)
	}

	// Start weed mini in a goroutine by calling the command directly
	cluster.wg.Add(1)
	go func() {
		defer cluster.wg.Done()

		// Protect global state mutation with a mutex
		miniClusterMutex.Lock()
		defer miniClusterMutex.Unlock()

		// Save current directory and args
		oldDir, _ := os.Getwd()
		oldArgs := os.Args
		defer func() {
			os.Chdir(oldDir)
			os.Args = oldArgs
		}()

		// Change to test directory so mini picks up security.toml
		os.Chdir(testDir)

		// Configure args for mini command
		os.Args = []string{
			"weed",
			"-dir=" + testDir,
			"-master.port=" + strconv.Itoa(masterPort),
			"-master.port.grpc=" + strconv.Itoa(masterGrpcPort),
			"-volume.port=" + strconv.Itoa(volumePort),
			"-volume.port.grpc=" + strconv.Itoa(volumeGrpcPort),
			"-filer.port=" + strconv.Itoa(filerPort),
			"-filer.port.grpc=" + strconv.Itoa(filerGrpcPort),
			"-s3.port=" + strconv.Itoa(s3Port),
			"-s3.port.grpc=" + strconv.Itoa(s3GrpcPort),
			"-webdav.port=0",               // Disable WebDAV
			"-admin.ui=false",              // Disable admin UI
			"-master.volumeSizeLimitMB=32", // Small volumes for testing
			"-ip=127.0.0.1",
			"-master.peers=none",     // Faster startup
			"-s3.iam.readOnly=false", // Enable IAM write operations for tests
		}

		// Suppress most logging during tests
		glog.MaxSize = 1024 * 1024

		// Find and run the mini command
		for _, cmd := range command.Commands {
			if cmd.Name() == "mini" && cmd.Run != nil {
				cmd.Flag.Parse(os.Args[1:])
				args := cmd.Flag.Args()
				command.MiniClusterCtx = ctx
				cmd.Run(cmd, args)
				command.MiniClusterCtx = nil
				return
			}
		}
	}()

	// Wait for S3 service to be ready
	err = waitForS3Ready(cluster.s3Endpoint, 30*time.Second)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("S3 service failed to start: %v", err)
	}

	cluster.isRunning = true

	t.Logf("Test cluster started successfully at %s", cluster.s3Endpoint)
	return cluster, nil
}

// Stop stops the test cluster
func (c *TestCluster) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	// Give services time to shut down gracefully
	if c.isRunning {
		time.Sleep(500 * time.Millisecond)
	}
	// Wait for the mini goroutine to finish
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
		// Goroutine finished
	case <-timer.C:
		// Timeout - goroutine doesn't respond to context cancel
	}

	// Reset the global cmdMini flags to prevent state leakage to other tests
	for _, cmd := range command.Commands {
		if cmd.Name() == "mini" {
			// Reset flags to defaults
			cmd.Flag.VisitAll(func(f *flag.Flag) {
				// Reset to default value
				f.Value.Set(f.DefValue)
			})
			break
		}
	}
}

// waitForS3Ready waits for the S3 service to be ready
func waitForS3Ready(endpoint string, timeout time.Duration) error {
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		resp, err := client.Get(endpoint)
		if err == nil {
			resp.Body.Close()
			// Wait a bit more to ensure service is fully ready
			time.Sleep(500 * time.Millisecond)
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for S3 service at %s", endpoint)
}

// randomString generates a random string for unique naming
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + rand.Int63()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rng.Intn(len(charset))]
	}
	return string(b)
}
