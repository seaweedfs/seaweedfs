package s3tables

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/command"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
)

const (
	testRegion    = "us-west-2"
	testAccessKey = "admin"
	testSecretKey = "admin"
	testAccountID = "111122223333"
)

// TestCluster manages the weed mini instance for integration testing
type TestCluster struct {
	dataDir    string
	ctx        context.Context
	cancel     context.CancelFunc
	isRunning  bool
	startOnce  sync.Once
	wg         sync.WaitGroup
	masterPort int
	volumePort int
	filerPort  int
	s3Port     int
	s3Endpoint string
}

// S3TablesClient is a simple client for S3 Tables API
type S3TablesClient struct {
	endpoint  string
	region    string
	accessKey string
	secretKey string
	client    *http.Client
}

func NewS3TablesClient(endpoint, region, accessKey, secretKey string) *S3TablesClient {
	return &S3TablesClient{
		endpoint:  endpoint,
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

// doRequest sends a request to the S3 Tables API
func (c *S3TablesClient) doRequest(operation string, body interface{}) (*http.Response, error) {
	var bodyBytes []byte
	var err error

	if body != nil {
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
	}

	req, err := http.NewRequest(http.MethodPost, c.endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "S3Tables."+operation)

	// For testing, we use basic auth or skip auth (the test cluster uses anonymous for simplicity)
	// In production, AWS Signature V4 would be used

	return c.client.Do(req)
}

// Table Bucket operations

func (c *S3TablesClient) CreateTableBucket(name string, tags map[string]string) (*s3tables.CreateTableBucketResponse, error) {
	req := &s3tables.CreateTableBucketRequest{
		Name: name,
		Tags: tags,
	}

	resp, err := c.doRequest("CreateTableBucket", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("CreateTableBucket failed: %s - %s", errResp.Type, errResp.Message)
	}

	var result s3tables.CreateTableBucketResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (c *S3TablesClient) GetTableBucket(arn string) (*s3tables.GetTableBucketResponse, error) {
	req := &s3tables.GetTableBucketRequest{
		TableBucketARN: arn,
	}

	resp, err := c.doRequest("GetTableBucket", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("GetTableBucket failed: %s - %s", errResp.Type, errResp.Message)
	}

	var result s3tables.GetTableBucketResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (c *S3TablesClient) ListTableBuckets(prefix string) (*s3tables.ListTableBucketsResponse, error) {
	req := &s3tables.ListTableBucketsRequest{
		Prefix: prefix,
	}

	resp, err := c.doRequest("ListTableBuckets", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("ListTableBuckets failed: %s - %s", errResp.Type, errResp.Message)
	}

	var result s3tables.ListTableBucketsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (c *S3TablesClient) DeleteTableBucket(arn string) error {
	req := &s3tables.DeleteTableBucketRequest{
		TableBucketARN: arn,
	}

	resp, err := c.doRequest("DeleteTableBucket", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("DeleteTableBucket failed: %s - %s", errResp.Type, errResp.Message)
	}

	return nil
}

// Namespace operations

func (c *S3TablesClient) CreateNamespace(bucketARN string, namespace []string) (*s3tables.CreateNamespaceResponse, error) {
	req := &s3tables.CreateNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}

	resp, err := c.doRequest("CreateNamespace", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("CreateNamespace failed: %s - %s", errResp.Type, errResp.Message)
	}

	var result s3tables.CreateNamespaceResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (c *S3TablesClient) GetNamespace(bucketARN, namespace string) (*s3tables.GetNamespaceResponse, error) {
	req := &s3tables.GetNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}

	resp, err := c.doRequest("GetNamespace", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("GetNamespace failed: %s - %s", errResp.Type, errResp.Message)
	}

	var result s3tables.GetNamespaceResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (c *S3TablesClient) ListNamespaces(bucketARN, prefix string) (*s3tables.ListNamespacesResponse, error) {
	req := &s3tables.ListNamespacesRequest{
		TableBucketARN: bucketARN,
		Prefix:         prefix,
	}

	resp, err := c.doRequest("ListNamespaces", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("ListNamespaces failed: %s - %s", errResp.Type, errResp.Message)
	}

	var result s3tables.ListNamespacesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (c *S3TablesClient) DeleteNamespace(bucketARN, namespace string) error {
	req := &s3tables.DeleteNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}

	resp, err := c.doRequest("DeleteNamespace", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("DeleteNamespace failed: %s - %s", errResp.Type, errResp.Message)
	}

	return nil
}

// Table operations

func (c *S3TablesClient) CreateTable(bucketARN, namespace, name, format string, metadata *s3tables.TableMetadata, tags map[string]string) (*s3tables.CreateTableResponse, error) {
	req := &s3tables.CreateTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           name,
		Format:         format,
		Metadata:       metadata,
		Tags:           tags,
	}

	resp, err := c.doRequest("CreateTable", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("CreateTable failed: %s - %s", errResp.Type, errResp.Message)
	}

	var result s3tables.CreateTableResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (c *S3TablesClient) GetTable(bucketARN, namespace, name string) (*s3tables.GetTableResponse, error) {
	req := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           name,
	}

	resp, err := c.doRequest("GetTable", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("GetTable failed: %s - %s", errResp.Type, errResp.Message)
	}

	var result s3tables.GetTableResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (c *S3TablesClient) ListTables(bucketARN, namespace, prefix string) (*s3tables.ListTablesResponse, error) {
	req := &s3tables.ListTablesRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Prefix:         prefix,
	}

	resp, err := c.doRequest("ListTables", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("ListTables failed: %s - %s", errResp.Type, errResp.Message)
	}

	var result s3tables.ListTablesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (c *S3TablesClient) DeleteTable(bucketARN, namespace, name string) error {
	req := &s3tables.DeleteTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           name,
	}

	resp, err := c.doRequest("DeleteTable", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("DeleteTable failed: %s - %s", errResp.Type, errResp.Message)
	}

	return nil
}

// Policy operations

func (c *S3TablesClient) PutTableBucketPolicy(bucketARN, policy string) error {
	req := &s3tables.PutTableBucketPolicyRequest{
		TableBucketARN: bucketARN,
		ResourcePolicy: policy,
	}

	resp, err := c.doRequest("PutTableBucketPolicy", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("PutTableBucketPolicy failed: %s - %s", errResp.Type, errResp.Message)
	}

	return nil
}

func (c *S3TablesClient) GetTableBucketPolicy(bucketARN string) (*s3tables.GetTableBucketPolicyResponse, error) {
	req := &s3tables.GetTableBucketPolicyRequest{
		TableBucketARN: bucketARN,
	}

	resp, err := c.doRequest("GetTableBucketPolicy", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("GetTableBucketPolicy failed: %s - %s", errResp.Type, errResp.Message)
	}

	var result s3tables.GetTableBucketPolicyResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (c *S3TablesClient) DeleteTableBucketPolicy(bucketARN string) error {
	req := &s3tables.DeleteTableBucketPolicyRequest{
		TableBucketARN: bucketARN,
	}

	resp, err := c.doRequest("DeleteTableBucketPolicy", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("DeleteTableBucketPolicy failed: %s - %s", errResp.Type, errResp.Message)
	}

	return nil
}

// Tagging operations

func (c *S3TablesClient) TagResource(resourceARN string, tags map[string]string) error {
	req := &s3tables.TagResourceRequest{
		ResourceARN: resourceARN,
		Tags:        tags,
	}

	resp, err := c.doRequest("TagResource", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("TagResource failed: %s - %s", errResp.Type, errResp.Message)
	}

	return nil
}

func (c *S3TablesClient) ListTagsForResource(resourceARN string) (*s3tables.ListTagsForResourceResponse, error) {
	req := &s3tables.ListTagsForResourceRequest{
		ResourceARN: resourceARN,
	}

	resp, err := c.doRequest("ListTagsForResource", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("ListTagsForResource failed: %s - %s", errResp.Type, errResp.Message)
	}

	var result s3tables.ListTagsForResourceResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (c *S3TablesClient) UntagResource(resourceARN string, tagKeys []string) error {
	req := &s3tables.UntagResourceRequest{
		ResourceARN: resourceARN,
		TagKeys:     tagKeys,
	}

	resp, err := c.doRequest("UntagResource", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp s3tables.S3TablesError
		json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("UntagResource failed: %s - %s", errResp.Type, errResp.Message)
	}

	return nil
}

// Integration tests

// TestS3TablesIntegration tests S3 Tables API operations
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
	listResp, err := client.ListTableBuckets("")
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
	getNsResp, err := client.GetNamespace(bucketARN, namespaceName)
	require.NoError(t, err, "Failed to get namespace")
	assert.Equal(t, []string{namespaceName}, getNsResp.Namespace)
	t.Logf("✓ Got namespace: %v", getNsResp.Namespace)

	// List namespaces
	listNsResp, err := client.ListNamespaces(bucketARN, "")
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
	err = client.DeleteNamespace(bucketARN, namespaceName)
	require.NoError(t, err, "Failed to delete namespace")
	t.Logf("✓ Deleted namespace: %s", namespaceName)

	// Verify namespace is deleted
	_, err = client.GetNamespace(bucketARN, namespaceName)
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
	defer client.DeleteNamespace(bucketARN, namespaceName)

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

	createTableResp, err := client.CreateTable(bucketARN, namespaceName, tableName, "ICEBERG", icebergMetadata, nil)
	require.NoError(t, err, "Failed to create table")
	assert.NotEmpty(t, createTableResp.TableARN)
	assert.NotEmpty(t, createTableResp.VersionToken)
	t.Logf("✓ Created table: %s (version: %s)", createTableResp.TableARN, createTableResp.VersionToken)

	// Get table
	getTableResp, err := client.GetTable(bucketARN, namespaceName, tableName)
	require.NoError(t, err, "Failed to get table")
	assert.Equal(t, tableName, getTableResp.Name)
	assert.Equal(t, "ICEBERG", getTableResp.Format)
	t.Logf("✓ Got table: %s (format: %s)", getTableResp.Name, getTableResp.Format)

	// List tables
	listTablesResp, err := client.ListTables(bucketARN, namespaceName, "")
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
	err = client.DeleteTable(bucketARN, namespaceName, tableName)
	require.NoError(t, err, "Failed to delete table")
	t.Logf("✓ Deleted table: %s", tableName)

	// Verify table is deleted
	_, err = client.GetTable(bucketARN, namespaceName, tableName)
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
	volumePort, err := findAvailablePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find volume port: %v", err)
	}
	filerPort, err := findAvailablePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find filer port: %v", err)
	}
	s3Port, err := findAvailablePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find s3 port: %v", err)
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
			"-volume.port=" + strconv.Itoa(volumePort),
			"-filer.port=" + strconv.Itoa(filerPort),
			"-s3.port=" + strconv.Itoa(s3Port),
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
				cmd.Run(cmd, args)
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
	select {
	case <-done:
		// Goroutine finished
	case <-time.After(2 * time.Second):
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
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rng.Intn(len(charset))]
	}
	return string(b)
}
