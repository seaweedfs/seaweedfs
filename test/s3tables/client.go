package s3tables

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

	return c.client.Do(req)
}

func (c *S3TablesClient) doRequestAndDecode(operation string, reqBody interface{}, respBody interface{}) error {
	resp, err := c.doRequest(operation, reqBody)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("%s failed with status %d and could not read error response body: %v", operation, resp.StatusCode, readErr)
		}
		var errResp s3tables.S3TablesError
		if err := json.Unmarshal(bodyBytes, &errResp); err != nil {
			return fmt.Errorf("%s failed with status %d, could not decode error response: %v. Body: %s", operation, resp.StatusCode, err, string(bodyBytes))
		}
		return fmt.Errorf("%s failed: %s - %s", operation, errResp.Type, errResp.Message)
	}

	if respBody != nil {
		if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
			return fmt.Errorf("failed to decode %s response: %w", operation, err)
		}
	}

	return nil
}

// Table Bucket operations

func (c *S3TablesClient) CreateTableBucket(name string, tags map[string]string) (*s3tables.CreateTableBucketResponse, error) {
	req := &s3tables.CreateTableBucketRequest{
		Name: name,
		Tags: tags,
	}
	var result s3tables.CreateTableBucketResponse
	if err := c.doRequestAndDecode("CreateTableBucket", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) GetTableBucket(arn string) (*s3tables.GetTableBucketResponse, error) {
	req := &s3tables.GetTableBucketRequest{
		TableBucketARN: arn,
	}
	var result s3tables.GetTableBucketResponse
	if err := c.doRequestAndDecode("GetTableBucket", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) ListTableBuckets(prefix string) (*s3tables.ListTableBucketsResponse, error) {
	req := &s3tables.ListTableBucketsRequest{
		Prefix: prefix,
	}
	var result s3tables.ListTableBucketsResponse
	if err := c.doRequestAndDecode("ListTableBuckets", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) DeleteTableBucket(arn string) error {
	req := &s3tables.DeleteTableBucketRequest{
		TableBucketARN: arn,
	}
	return c.doRequestAndDecode("DeleteTableBucket", req, nil)
}

// Namespace operations

func (c *S3TablesClient) CreateNamespace(bucketARN string, namespace []string) (*s3tables.CreateNamespaceResponse, error) {
	req := &s3tables.CreateNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}
	var result s3tables.CreateNamespaceResponse
	if err := c.doRequestAndDecode("CreateNamespace", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) GetNamespace(bucketARN string, namespace []string) (*s3tables.GetNamespaceResponse, error) {
	req := &s3tables.GetNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}
	var result s3tables.GetNamespaceResponse
	if err := c.doRequestAndDecode("GetNamespace", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) ListNamespaces(bucketARN, prefix string) (*s3tables.ListNamespacesResponse, error) {
	req := &s3tables.ListNamespacesRequest{
		TableBucketARN: bucketARN,
		Prefix:         prefix,
	}
	var result s3tables.ListNamespacesResponse
	if err := c.doRequestAndDecode("ListNamespaces", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) DeleteNamespace(bucketARN string, namespace []string) error {
	req := &s3tables.DeleteNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}
	return c.doRequestAndDecode("DeleteNamespace", req, nil)
}

// Table operations

func (c *S3TablesClient) CreateTable(bucketARN string, namespace []string, name, format string, metadata *s3tables.TableMetadata, tags map[string]string) (*s3tables.CreateTableResponse, error) {
	req := &s3tables.CreateTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           name,
		Format:         format,
		Metadata:       metadata,
		Tags:           tags,
	}
	var result s3tables.CreateTableResponse
	if err := c.doRequestAndDecode("CreateTable", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) GetTable(bucketARN string, namespace []string, name string) (*s3tables.GetTableResponse, error) {
	req := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           name,
	}
	var result s3tables.GetTableResponse
	if err := c.doRequestAndDecode("GetTable", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) ListTables(bucketARN string, namespace []string, prefix string) (*s3tables.ListTablesResponse, error) {
	req := &s3tables.ListTablesRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Prefix:         prefix,
	}
	var result s3tables.ListTablesResponse
	if err := c.doRequestAndDecode("ListTables", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) DeleteTable(bucketARN string, namespace []string, name string) error {
	req := &s3tables.DeleteTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           name,
	}
	return c.doRequestAndDecode("DeleteTable", req, nil)
}

// Policy operations

func (c *S3TablesClient) PutTableBucketPolicy(bucketARN, policy string) error {
	req := &s3tables.PutTableBucketPolicyRequest{
		TableBucketARN: bucketARN,
		ResourcePolicy: policy,
	}
	return c.doRequestAndDecode("PutTableBucketPolicy", req, nil)
}

func (c *S3TablesClient) GetTableBucketPolicy(bucketARN string) (*s3tables.GetTableBucketPolicyResponse, error) {
	req := &s3tables.GetTableBucketPolicyRequest{
		TableBucketARN: bucketARN,
	}
	var result s3tables.GetTableBucketPolicyResponse
	if err := c.doRequestAndDecode("GetTableBucketPolicy", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) DeleteTableBucketPolicy(bucketARN string) error {
	req := &s3tables.DeleteTableBucketPolicyRequest{
		TableBucketARN: bucketARN,
	}
	return c.doRequestAndDecode("DeleteTableBucketPolicy", req, nil)
}

// Tagging operations

func (c *S3TablesClient) TagResource(resourceARN string, tags map[string]string) error {
	req := &s3tables.TagResourceRequest{
		ResourceARN: resourceARN,
		Tags:        tags,
	}
	return c.doRequestAndDecode("TagResource", req, nil)
}

func (c *S3TablesClient) ListTagsForResource(resourceARN string) (*s3tables.ListTagsForResourceResponse, error) {
	req := &s3tables.ListTagsForResourceRequest{
		ResourceARN: resourceARN,
	}
	var result s3tables.ListTagsForResourceResponse
	if err := c.doRequestAndDecode("ListTagsForResource", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) UntagResource(resourceARN string, tagKeys []string) error {
	req := &s3tables.UntagResourceRequest{
		ResourceARN: resourceARN,
		TagKeys:     tagKeys,
	}
	return c.doRequestAndDecode("UntagResource", req, nil)
}
