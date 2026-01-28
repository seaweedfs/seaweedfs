package s3tables

import (
"bytes"
"encoding/json"
"fmt"
"net/http"

"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

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
