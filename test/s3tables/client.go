package s3tables

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

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
