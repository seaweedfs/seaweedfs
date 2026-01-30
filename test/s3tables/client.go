package s3tables

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func getFirstNamespace(namespace []string) (string, error) {
	if len(namespace) == 0 {
		return "", fmt.Errorf("namespace must not be empty")
	}
	return namespace[0], nil
}

func (c *S3TablesClient) doRestRequest(method, path string, body interface{}) (*http.Response, error) {
	var bodyBytes []byte
	var err error

	if body != nil {
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
	}

	req, err := http.NewRequest(method, c.endpoint+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	}

	if err := c.signRequest(req, bodyBytes); err != nil {
		return nil, err
	}

	return c.client.Do(req)
}

func (c *S3TablesClient) doTargetRequest(operation string, body interface{}) (*http.Response, error) {
	var bodyBytes []byte
	var err error

	if body != nil {
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
	}

	req, err := http.NewRequest(http.MethodPost, c.endpoint+"/", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.URL.RawPath = "/"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "S3Tables."+operation)

	if err := c.signRequest(req, bodyBytes); err != nil {
		return nil, err
	}

	return c.client.Do(req)
}

func (c *S3TablesClient) doTargetRequestAndDecode(operation string, reqBody interface{}, respBody interface{}) error {
	resp, err := c.doTargetRequest(operation, reqBody)
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

func (c *S3TablesClient) signRequest(req *http.Request, body []byte) error {
	creds := aws.Credentials{
		AccessKeyID:     c.accessKey,
		SecretAccessKey: c.secretKey,
	}
	if req.Host == "" {
		req.Host = req.URL.Host
	}
	req.Header.Set("Host", req.URL.Host)
	payloadHash := sha256.Sum256(body)
	return v4.NewSigner().SignHTTP(context.Background(), creds, req, hex.EncodeToString(payloadHash[:]), "s3tables", c.region, time.Now())
}

func (c *S3TablesClient) doRestRequestAndDecode(operation, method, path string, reqBody interface{}, respBody interface{}) error {
	resp, err := c.doRestRequest(method, path, reqBody)
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
	if err := c.doRestRequestAndDecode("CreateTableBucket", http.MethodPut, "/buckets", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) GetTableBucket(arn string) (*s3tables.GetTableBucketResponse, error) {
	path := "/buckets/" + url.PathEscape(arn)
	var result s3tables.GetTableBucketResponse
	if err := c.doRestRequestAndDecode("GetTableBucket", http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) ListTableBuckets(prefix, continuationToken string, maxBuckets int) (*s3tables.ListTableBucketsResponse, error) {
	query := url.Values{}
	if prefix != "" {
		query.Set("prefix", prefix)
	}
	if continuationToken != "" {
		query.Set("continuationToken", continuationToken)
	}
	if maxBuckets > 0 {
		query.Set("maxBuckets", strconv.Itoa(maxBuckets))
	}
	path := "/buckets"
	if encoded := query.Encode(); encoded != "" {
		path = path + "?" + encoded
	}
	var result s3tables.ListTableBucketsResponse
	if err := c.doRestRequestAndDecode("ListTableBuckets", http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) DeleteTableBucket(arn string) error {
	path := "/buckets/" + url.PathEscape(arn)
	return c.doRestRequestAndDecode("DeleteTableBucket", http.MethodDelete, path, nil, nil)
}

// Namespace operations

func (c *S3TablesClient) CreateNamespace(bucketARN string, namespace []string) (*s3tables.CreateNamespaceResponse, error) {
	if len(namespace) == 0 {
		return nil, fmt.Errorf("CreateNamespace requires namespace")
	}
	req := &s3tables.CreateNamespaceRequest{
		Namespace: namespace,
	}
	path := "/namespaces/" + url.PathEscape(bucketARN)
	var result s3tables.CreateNamespaceResponse
	if err := c.doRestRequestAndDecode("CreateNamespace", http.MethodPut, path, req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) GetNamespace(bucketARN string, namespace []string) (*s3tables.GetNamespaceResponse, error) {
	name, err := getFirstNamespace(namespace)
	if err != nil {
		return nil, fmt.Errorf("GetNamespace requires namespace: %w", err)
	}
	path := "/namespaces/" + url.PathEscape(bucketARN) + "/" + url.PathEscape(name)
	var result s3tables.GetNamespaceResponse
	if err := c.doRestRequestAndDecode("GetNamespace", http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) ListNamespaces(bucketARN, prefix, continuationToken string, maxNamespaces int) (*s3tables.ListNamespacesResponse, error) {
	query := url.Values{}
	if prefix != "" {
		query.Set("prefix", prefix)
	}
	if continuationToken != "" {
		query.Set("continuationToken", continuationToken)
	}
	if maxNamespaces > 0 {
		query.Set("maxNamespaces", strconv.Itoa(maxNamespaces))
	}
	path := "/namespaces/" + url.PathEscape(bucketARN)
	if encoded := query.Encode(); encoded != "" {
		path = path + "?" + encoded
	}
	var result s3tables.ListNamespacesResponse
	if err := c.doRestRequestAndDecode("ListNamespaces", http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) DeleteNamespace(bucketARN string, namespace []string) error {
	name, err := getFirstNamespace(namespace)
	if err != nil {
		return fmt.Errorf("DeleteNamespace requires namespace: %w", err)
	}
	path := "/namespaces/" + url.PathEscape(bucketARN) + "/" + url.PathEscape(name)
	return c.doRestRequestAndDecode("DeleteNamespace", http.MethodDelete, path, nil, nil)
}

// Table operations

func (c *S3TablesClient) CreateTable(bucketARN string, namespace []string, name, format string, metadata *s3tables.TableMetadata, tags map[string]string) (*s3tables.CreateTableResponse, error) {
	nameSpace, err := getFirstNamespace(namespace)
	if err != nil {
		return nil, fmt.Errorf("CreateTable requires namespace: %w", err)
	}
	req := &s3tables.CreateTableRequest{
		Name:     name,
		Format:   format,
		Metadata: metadata,
		Tags:     tags,
	}
	path := "/tables/" + url.PathEscape(bucketARN) + "/" + url.PathEscape(nameSpace)
	var result s3tables.CreateTableResponse
	if err := c.doRestRequestAndDecode("CreateTable", http.MethodPut, path, req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) GetTable(bucketARN string, namespace []string, name string) (*s3tables.GetTableResponse, error) {
	nameSpace, err := getFirstNamespace(namespace)
	if err != nil {
		return nil, fmt.Errorf("GetTable requires namespace: %w", err)
	}
	query := url.Values{}
	query.Set("tableBucketARN", bucketARN)
	query.Set("namespace", nameSpace)
	query.Set("name", name)
	path := "/get-table?" + query.Encode()
	var result s3tables.GetTableResponse
	if err := c.doRestRequestAndDecode("GetTable", http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) ListTables(bucketARN string, namespace []string, prefix, continuationToken string, maxTables int) (*s3tables.ListTablesResponse, error) {
	query := url.Values{}
	if len(namespace) > 0 {
		nameSpace, err := getFirstNamespace(namespace)
		if err != nil {
			return nil, fmt.Errorf("ListTables requires namespace: %w", err)
		}
		query.Set("namespace", nameSpace)
	}
	if prefix != "" {
		query.Set("prefix", prefix)
	}
	if continuationToken != "" {
		query.Set("continuationToken", continuationToken)
	}
	if maxTables > 0 {
		query.Set("maxTables", strconv.Itoa(maxTables))
	}
	path := "/tables/" + url.PathEscape(bucketARN)
	if encoded := query.Encode(); encoded != "" {
		path = path + "?" + encoded
	}
	var result s3tables.ListTablesResponse
	if err := c.doRestRequestAndDecode("ListTables", http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) DeleteTable(bucketARN string, namespace []string, name string) error {
	nameSpace, err := getFirstNamespace(namespace)
	if err != nil {
		return fmt.Errorf("DeleteTable requires namespace: %w", err)
	}
	path := "/tables/" + url.PathEscape(bucketARN) + "/" + url.PathEscape(nameSpace) + "/" + url.PathEscape(name)
	return c.doRestRequestAndDecode("DeleteTable", http.MethodDelete, path, nil, nil)
}

// Policy operations

func (c *S3TablesClient) PutTableBucketPolicy(bucketARN, policy string) error {
	req := &s3tables.PutTableBucketPolicyRequest{
		ResourcePolicy: policy,
	}
	path := "/buckets/" + url.PathEscape(bucketARN) + "/policy"
	return c.doRestRequestAndDecode("PutTableBucketPolicy", http.MethodPut, path, req, nil)
}

func (c *S3TablesClient) GetTableBucketPolicy(bucketARN string) (*s3tables.GetTableBucketPolicyResponse, error) {
	path := "/buckets/" + url.PathEscape(bucketARN) + "/policy"
	var result s3tables.GetTableBucketPolicyResponse
	if err := c.doRestRequestAndDecode("GetTableBucketPolicy", http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) DeleteTableBucketPolicy(bucketARN string) error {
	path := "/buckets/" + url.PathEscape(bucketARN) + "/policy"
	return c.doRestRequestAndDecode("DeleteTableBucketPolicy", http.MethodDelete, path, nil, nil)
}

// Table Policy operations

func (c *S3TablesClient) PutTablePolicy(bucketARN string, namespace []string, name, policy string) error {
	nameSpace, err := getFirstNamespace(namespace)
	if err != nil {
		return fmt.Errorf("PutTablePolicy requires namespace: %w", err)
	}
	req := &s3tables.PutTablePolicyRequest{
		ResourcePolicy: policy,
	}
	path := "/tables/" + url.PathEscape(bucketARN) + "/" + url.PathEscape(nameSpace) + "/" + url.PathEscape(name) + "/policy"
	return c.doRestRequestAndDecode("PutTablePolicy", http.MethodPut, path, req, nil)
}

func (c *S3TablesClient) GetTablePolicy(bucketARN string, namespace []string, name string) (*s3tables.GetTablePolicyResponse, error) {
	nameSpace, err := getFirstNamespace(namespace)
	if err != nil {
		return nil, fmt.Errorf("GetTablePolicy requires namespace: %w", err)
	}
	path := "/tables/" + url.PathEscape(bucketARN) + "/" + url.PathEscape(nameSpace) + "/" + url.PathEscape(name) + "/policy"
	var result s3tables.GetTablePolicyResponse
	if err := c.doRestRequestAndDecode("GetTablePolicy", http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) DeleteTablePolicy(bucketARN string, namespace []string, name string) error {
	nameSpace, err := getFirstNamespace(namespace)
	if err != nil {
		return fmt.Errorf("DeleteTablePolicy requires namespace: %w", err)
	}
	path := "/tables/" + url.PathEscape(bucketARN) + "/" + url.PathEscape(nameSpace) + "/" + url.PathEscape(name) + "/policy"
	return c.doRestRequestAndDecode("DeleteTablePolicy", http.MethodDelete, path, nil, nil)
}

// Tagging operations

func (c *S3TablesClient) TagResource(resourceARN string, tags map[string]string) error {
	req := &s3tables.TagResourceRequest{
		Tags: tags,
	}
	path := "/tag/" + url.PathEscape(resourceARN)
	return c.doRestRequestAndDecode("TagResource", http.MethodPost, path, req, nil)
}

func (c *S3TablesClient) ListTagsForResource(resourceARN string) (*s3tables.ListTagsForResourceResponse, error) {
	path := "/tag/" + url.PathEscape(resourceARN)
	var result s3tables.ListTagsForResourceResponse
	if err := c.doRestRequestAndDecode("ListTagsForResource", http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *S3TablesClient) UntagResource(resourceARN string, tagKeys []string) error {
	if len(tagKeys) == 0 {
		return fmt.Errorf("tagKeys cannot be empty")
	}
	query := url.Values{}
	for _, key := range tagKeys {
		query.Add("tagKeys", key)
	}
	path := "/tag/" + url.PathEscape(resourceARN)
	if encoded := query.Encode(); encoded != "" {
		path = path + "?" + encoded
	}
	return c.doRestRequestAndDecode("UntagResource", http.MethodDelete, path, nil, nil)
}
