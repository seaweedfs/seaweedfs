package polaris

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type polarisSession struct {
	catalogName  string
	bucketName   string
	token        string
	baseLocation string
}

type polarisTableSetup struct {
	namespace string
	table     string
	s3Client  *s3.Client
}

type polarisCatalogClient struct {
	http    *polarisHTTPClient
	catalog string
}

type polarisCredentials struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
}

type createPrincipalResponse struct {
	Credentials polarisCredentials `json:"credentials"`
}

type createCatalogRequest struct {
	Catalog polarisCatalog `json:"catalog"`
}

type polarisCatalog struct {
	Name              string               `json:"name"`
	Type              string               `json:"type"`
	ReadOnly          bool                 `json:"readOnly"`
	Properties        map[string]string    `json:"properties"`
	StorageConfigInfo polarisStorageConfig `json:"storageConfigInfo"`
}

type polarisStorageConfig struct {
	StorageType      string   `json:"storageType"`
	AllowedLocations []string `json:"allowedLocations"`
	Endpoint         string   `json:"endpoint"`
	EndpointInternal string   `json:"endpointInternal"`
	StsEndpoint      string   `json:"stsEndpoint"`
	PathStyleAccess  bool     `json:"pathStyleAccess"`
	RoleArn          string   `json:"roleArn"`
	Region           string   `json:"region"`
}

type createNamespaceRequest struct {
	Namespace []string `json:"namespace"`
}

type createTableRequest struct {
	Name          string            `json:"name"`
	Location      string            `json:"location"`
	Schema        icebergSchema     `json:"schema"`
	PartitionSpec icebergPartition  `json:"partition-spec"`
	SortOrder     icebergSortOrder  `json:"sort-order"`
	Properties    map[string]string `json:"properties"`
}

type icebergSchema struct {
	Type     string               `json:"type"`
	SchemaID int                  `json:"schema-id"`
	Fields   []icebergSchemaField `json:"fields"`
}

type icebergSchemaField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

type icebergPartition struct {
	SpecID int                     `json:"spec-id"`
	Fields []icebergPartitionField `json:"fields"`
}

type icebergPartitionField struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

type icebergSortOrder struct {
	OrderID int                `json:"order-id"`
	Fields  []icebergSortField `json:"fields"`
}

type icebergSortField struct {
	SourceID  int    `json:"source-id"`
	Direction string `json:"direction"`
	NullOrder string `json:"null-order"`
}

type loadTableResponse struct {
	Config             map[string]string   `json:"config"`
	StorageCredentials []storageCredential `json:"storage-credentials"`
}

type storageCredential struct {
	Prefix string            `json:"prefix"`
	Config map[string]string `json:"config"`
}

func bootstrapPolarisTest(t *testing.T, env *TestEnvironment) (context.Context, context.CancelFunc, polarisSession, *polarisTableSetup, func()) {
	t.Helper()

	t.Logf(">>> Starting SeaweedFS with Polaris configuration...")
	env.StartSeaweedFS(t)
	t.Logf(">>> SeaweedFS started.")

	t.Logf(">>> Starting Polaris...")
	env.StartPolaris(t)
	t.Logf(">>> Polaris started.")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	session := newPolarisSession(t, ctx, env)
	setup, cleanup := setupPolarisTable(t, ctx, env, session)

	return ctx, cancel, session, setup, cleanup
}

func TestPolarisIntegration(t *testing.T) {
	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	ctx, cancel, session, setup, cleanup := bootstrapPolarisTest(t, env)
	defer cancel()
	defer cleanup()

	listResp, err := setup.s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}

	found := false
	for _, bucket := range listResp.Buckets {
		if aws.ToString(bucket.Name) == session.bucketName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Bucket %s not found in list", session.bucketName)
	}

	objectKey := fmt.Sprintf("%s/%s/data/hello-%d.txt", setup.namespace, setup.table, time.Now().UnixNano())
	payload := []byte("polaris")

	if _, err := setup.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(session.bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(payload),
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	listObjects, err := setup.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(session.bucketName),
		Prefix: aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}

	found = false
	for _, obj := range listObjects.Contents {
		if aws.ToString(obj.Key) == objectKey {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Object %s not found in list", objectKey)
	}

	getResp, err := setup.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(session.bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	body, err := io.ReadAll(getResp.Body)
	_ = getResp.Body.Close()
	if err != nil {
		t.Fatalf("Read object body failed: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("Unexpected object payload: got %q want %q", string(body), string(payload))
	}

	if _, err := setup.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(session.bucketName),
		Key:    aws.String(objectKey),
	}); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}
}

func TestPolarisTableIntegration(t *testing.T) {
	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	ctx, cancel, session, setup, cleanup := bootstrapPolarisTest(t, env)
	defer cancel()
	defer cleanup()

	objectKey := fmt.Sprintf("%s/%s/data/part-%d.parquet", setup.namespace, setup.table, time.Now().UnixNano())
	createResp, err := setup.s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(session.bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	uploadID := aws.ToString(createResp.UploadId)
	multipartCompleted := false
	defer func() {
		if uploadID == "" || multipartCompleted {
			return
		}
		_, _ = setup.s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(session.bucketName),
			Key:      aws.String(objectKey),
			UploadId: aws.String(uploadID),
		})
	}()

	partSize := 5 * 1024 * 1024
	part1 := bytes.Repeat([]byte("a"), partSize)
	part2 := bytes.Repeat([]byte("b"), 1024*1024)

	part1Resp, err := setup.s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(session.bucketName),
		Key:        aws.String(objectKey),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part1),
	})
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}

	part2Resp, err := setup.s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(session.bucketName),
		Key:        aws.String(objectKey),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader(part2),
	})
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}

	_, err = setup.s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(session.bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: []s3types.CompletedPart{
				{
					ETag:       part1Resp.ETag,
					PartNumber: aws.Int32(1),
				},
				{
					ETag:       part2Resp.ETag,
					PartNumber: aws.Int32(2),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}
	multipartCompleted = true

	headResp, err := setup.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(session.bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("HeadObject after multipart upload failed: %v", err)
	}
	expectedSize := int64(len(part1) + len(part2))
	if headResp.ContentLength == nil || *headResp.ContentLength != expectedSize {
		t.Fatalf("Unexpected content length: got %d want %d", aws.ToInt64(headResp.ContentLength), expectedSize)
	}
}

func newPolarisSession(t *testing.T, ctx context.Context, env *TestEnvironment) polarisSession {
	t.Helper()

	adminCreds := aws.Credentials{
		AccessKeyID:     env.accessKey,
		SecretAccessKey: env.secretKey,
		Source:          "polaris-admin",
	}
	adminS3, err := newS3Client(ctx, env.s3Endpoint(), polarisRegion, adminCreds, true)
	if err != nil {
		t.Fatalf("Create admin S3 client failed: %v", err)
	}

	bucketName := fmt.Sprintf("polaris-bucket-%d", time.Now().UnixNano())
	if _, err := adminS3.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	rootToken, err := fetchPolarisToken(ctx, env.polarisEndpoint(), polarisRootClientID, polarisRootClientSecret)
	if err != nil {
		t.Fatalf("Polaris root token request failed: %v", err)
	}

	managementClient := newPolarisHTTPClient(env.polarisEndpoint(), polarisRealm, rootToken)
	catalogName := fmt.Sprintf("polaris_catalog_%d", time.Now().UnixNano())
	baseLocation := fmt.Sprintf("s3://%s/polaris", bucketName)

	catalogRequest := createCatalogRequest{
		Catalog: polarisCatalog{
			Name:     catalogName,
			Type:     "INTERNAL",
			ReadOnly: false,
			Properties: map[string]string{
				"default-base-location": baseLocation,
			},
			StorageConfigInfo: polarisStorageConfig{
				StorageType:      "S3",
				AllowedLocations: []string{baseLocation},
				Endpoint:         env.s3Endpoint(),
				EndpointInternal: env.s3InternalEndpoint(),
				StsEndpoint:      env.s3InternalEndpoint(),
				PathStyleAccess:  true,
				RoleArn:          polarisRoleArn,
				Region:           polarisRegion,
			},
		},
	}

	if err := managementClient.doJSON(ctx, http.MethodPost, "/api/management/v1/catalogs", catalogRequest, nil); err != nil {
		t.Fatalf("Create catalog failed: %v", err)
	}

	principalName := fmt.Sprintf("polaris_user_%d", time.Now().UnixNano())
	principalRoleName := fmt.Sprintf("polaris_principal_role_%d", time.Now().UnixNano())
	catalogRoleName := fmt.Sprintf("polaris_catalog_role_%d", time.Now().UnixNano())

	var principalResp createPrincipalResponse
	if err := managementClient.doJSON(ctx, http.MethodPost, "/api/management/v1/principals", map[string]interface{}{
		"principal": map[string]interface{}{
			"name":       principalName,
			"properties": map[string]string{},
		},
	}, &principalResp); err != nil {
		t.Fatalf("Create principal failed: %v", err)
	}
	if principalResp.Credentials.ClientID == "" || principalResp.Credentials.ClientSecret == "" {
		t.Fatalf("Missing principal credentials in response")
	}

	if err := managementClient.doJSON(ctx, http.MethodPost, "/api/management/v1/principal-roles", map[string]interface{}{
		"principalRole": map[string]interface{}{
			"name":       principalRoleName,
			"properties": map[string]string{},
		},
	}, nil); err != nil {
		t.Fatalf("Create principal role failed: %v", err)
	}

	if err := managementClient.doJSON(ctx, http.MethodPost, fmt.Sprintf("/api/management/v1/catalogs/%s/catalog-roles", url.PathEscape(catalogName)), map[string]interface{}{
		"catalogRole": map[string]interface{}{
			"name":       catalogRoleName,
			"properties": map[string]string{},
		},
	}, nil); err != nil {
		t.Fatalf("Create catalog role failed: %v", err)
	}

	if err := managementClient.doJSON(ctx, http.MethodPut, fmt.Sprintf("/api/management/v1/principals/%s/principal-roles", url.PathEscape(principalName)), map[string]interface{}{
		"principalRole": map[string]interface{}{
			"name": principalRoleName,
		},
	}, nil); err != nil {
		t.Fatalf("Assign principal role failed: %v", err)
	}

	if err := managementClient.doJSON(ctx, http.MethodPut, fmt.Sprintf("/api/management/v1/principal-roles/%s/catalog-roles/%s", url.PathEscape(principalRoleName), url.PathEscape(catalogName)), map[string]interface{}{
		"catalogRole": map[string]interface{}{
			"name": catalogRoleName,
		},
	}, nil); err != nil {
		t.Fatalf("Assign catalog role failed: %v", err)
	}

	if err := managementClient.doJSON(ctx, http.MethodPut, fmt.Sprintf("/api/management/v1/catalogs/%s/catalog-roles/%s/grants", url.PathEscape(catalogName), url.PathEscape(catalogRoleName)), map[string]interface{}{
		"type":      "catalog",
		"privilege": "CATALOG_MANAGE_CONTENT",
	}, nil); err != nil {
		t.Fatalf("Grant catalog privilege failed: %v", err)
	}

	userToken, err := fetchPolarisToken(ctx, env.polarisEndpoint(), principalResp.Credentials.ClientID, principalResp.Credentials.ClientSecret)
	if err != nil {
		t.Fatalf("Polaris user token request failed: %v", err)
	}

	return polarisSession{
		catalogName:  catalogName,
		bucketName:   bucketName,
		token:        userToken,
		baseLocation: baseLocation,
	}
}

func setupPolarisTable(t *testing.T, ctx context.Context, env *TestEnvironment, session polarisSession) (*polarisTableSetup, func()) {
	t.Helper()

	catalogClient := newPolarisCatalogClient(env.polarisEndpoint(), polarisRealm, session.token, session.catalogName)
	namespace := fmt.Sprintf("polaris_ns_%d", time.Now().UnixNano())
	table := fmt.Sprintf("polaris_table_%d", time.Now().UnixNano())

	if err := catalogClient.CreateNamespace(ctx, namespace); err != nil {
		t.Fatalf("CreateNamespace failed: %v", err)
	}

	location := fmt.Sprintf("%s/%s/%s", session.baseLocation, namespace, table)
	if err := catalogClient.CreateTable(ctx, namespace, table, location); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	loadResp, err := catalogClient.LoadTable(ctx, namespace, table)
	if err != nil {
		t.Fatalf("LoadTable failed: %v", err)
	}

	creds, endpoint, region, pathStyle, err := extractS3Credentials(loadResp, location, env.s3Endpoint(), polarisRegion)
	if err != nil {
		t.Fatalf("Extract vended credentials failed: %v", err)
	}

	s3Client, err := newS3Client(ctx, endpoint, region, creds, pathStyle)
	if err != nil {
		t.Fatalf("Create vended S3 client failed: %v", err)
	}

	cleanup := func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := catalogClient.DeleteTable(cleanupCtx, namespace, table); err != nil {
			t.Logf("DeleteTable failed: %v", err)
		}
		if err := catalogClient.DeleteNamespace(cleanupCtx, namespace); err != nil {
			t.Logf("DeleteNamespace failed: %v", err)
		}
	}

	return &polarisTableSetup{
		namespace: namespace,
		table:     table,
		s3Client:  s3Client,
	}, cleanup
}

func fetchPolarisToken(ctx context.Context, baseURL, clientID, clientSecret string) (string, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", clientID)
	form.Set("client_secret", clientSecret)
	form.Set("scope", "PRINCIPAL_ROLE:ALL")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/catalog/v1/oauth/tokens", strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return "", fmt.Errorf("token request failed with status %d and reading body: %w", resp.StatusCode, readErr)
		}
		return "", fmt.Errorf("token request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("missing access token in response")
	}

	return tokenResp.AccessToken, nil
}

func newPolarisCatalogClient(baseURL, realm, token, catalog string) *polarisCatalogClient {
	return &polarisCatalogClient{
		http:    newPolarisHTTPClient(baseURL, realm, token),
		catalog: catalog,
	}
}

func (c *polarisCatalogClient) CreateNamespace(ctx context.Context, namespace string) error {
	path := fmt.Sprintf("/api/catalog/v1/%s/namespaces", url.PathEscape(c.catalog))
	req := createNamespaceRequest{Namespace: []string{namespace}}
	return c.http.doJSON(ctx, http.MethodPost, path, req, nil)
}

func (c *polarisCatalogClient) DeleteNamespace(ctx context.Context, namespace string) error {
	path := fmt.Sprintf("/api/catalog/v1/%s/namespaces/%s", url.PathEscape(c.catalog), url.PathEscape(namespace))
	return c.http.doJSON(ctx, http.MethodDelete, path, nil, nil)
}

func (c *polarisCatalogClient) CreateTable(ctx context.Context, namespace, table, location string) error {
	path := fmt.Sprintf("/api/catalog/v1/%s/namespaces/%s/tables", url.PathEscape(c.catalog), url.PathEscape(namespace))

	req := createTableRequest{
		Name:     table,
		Location: location,
		Schema: icebergSchema{
			Type:     "struct",
			SchemaID: 0,
			Fields: []icebergSchemaField{
				{
					ID:       1,
					Name:     "id",
					Type:     "long",
					Required: false,
				},
			},
		},
		PartitionSpec: icebergPartition{
			SpecID: 0,
			Fields: []icebergPartitionField{},
		},
		SortOrder: icebergSortOrder{
			OrderID: 0,
			Fields:  []icebergSortField{},
		},
		Properties: map[string]string{
			"format-version": "2",
		},
	}

	return c.http.doJSON(ctx, http.MethodPost, path, req, nil)
}

func (c *polarisCatalogClient) DeleteTable(ctx context.Context, namespace, table string) error {
	path := fmt.Sprintf("/api/catalog/v1/%s/namespaces/%s/tables/%s", url.PathEscape(c.catalog), url.PathEscape(namespace), url.PathEscape(table))
	return c.http.doJSON(ctx, http.MethodDelete, path, nil, nil)
}

func (c *polarisCatalogClient) LoadTable(ctx context.Context, namespace, table string) (*loadTableResponse, error) {
	path := fmt.Sprintf("/api/catalog/v1/%s/namespaces/%s/tables/%s", url.PathEscape(c.catalog), url.PathEscape(namespace), url.PathEscape(table))
	var resp loadTableResponse
	if err := c.http.doJSON(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func extractS3Credentials(load *loadTableResponse, targetPrefix, fallbackEndpoint, fallbackRegion string) (aws.Credentials, string, string, bool, error) {
	configMap, err := selectStorageConfig(load, targetPrefix)
	if err != nil {
		return aws.Credentials{}, "", "", false, err
	}

	lookup := func(keys ...string) string {
		for _, key := range keys {
			if val, ok := configMap[key]; ok && val != "" {
				return val
			}
		}
		return ""
	}

	accessKey := lookup("s3.access-key-id", "aws.access-key-id", "s3.accessKeyId")
	secretKey := lookup("s3.secret-access-key", "aws.secret-access-key", "s3.secretAccessKey")
	sessionToken := lookup("s3.session-token", "aws.session-token")
	if accessKey == "" || secretKey == "" {
		return aws.Credentials{}, "", "", false, fmt.Errorf("missing access key or secret in storage credentials")
	}

	endpoint := lookup("s3.endpoint", "s3.endpoint-url", "aws.endpoint")
	if endpoint == "" {
		endpoint = fallbackEndpoint
	}
	if endpoint != "" && !strings.HasPrefix(endpoint, "http") {
		endpoint = "http://" + endpoint
	}

	region := lookup("s3.region", "aws.region")
	if region == "" {
		region = fallbackRegion
	}

	pathStyle := true
	if value := lookup("s3.path-style-access", "s3.pathStyleAccess", "path-style-access"); value != "" {
		pathStyle = strings.EqualFold(value, "true")
	}

	return aws.Credentials{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
		Source:          "polaris-vended",
	}, endpoint, region, pathStyle, nil
}

func selectStorageConfig(load *loadTableResponse, targetPrefix string) (map[string]string, error) {
	switch len(load.StorageCredentials) {
	case 0:
		if load.Config == nil {
			return nil, fmt.Errorf("polaris returned no storage credentials or config")
		}
		return load.Config, nil
	case 1:
		cred := load.StorageCredentials[0]
		if cred.Config == nil {
			return nil, fmt.Errorf("storage credential for prefix %s returned nil config", cred.Prefix)
		}
		return cred.Config, nil
	default:
		if targetPrefix == "" {
			return nil, fmt.Errorf("multiple storage credentials (%d) returned but no target prefix provided", len(load.StorageCredentials))
		}
		normalizedTarget := normalizePrefix(targetPrefix)
		if normalizedTarget == "" {
			return nil, fmt.Errorf("target prefix %q normalized to empty string", targetPrefix)
		}
		var bestConfig map[string]string
		bestLen := -1
		for _, cred := range load.StorageCredentials {
			if cred.Config == nil {
				continue
			}
			prefix := normalizePrefix(cred.Prefix)
			if prefix == "" {
				if bestLen < 0 {
					bestLen = 0
					bestConfig = cred.Config
				}
				continue
			}
			if normalizedTarget == prefix || strings.HasPrefix(normalizedTarget, prefix+"/") {
				if len(prefix) > bestLen {
					bestLen = len(prefix)
					bestConfig = cred.Config
				}
			}
		}
		if bestConfig != nil {
			return bestConfig, nil
		}
		return nil, fmt.Errorf("none of the %d storage credentials matched prefix %s", len(load.StorageCredentials), targetPrefix)
	}
}

func normalizePrefix(prefix string) string {
	p := strings.TrimSpace(prefix)
	p = strings.TrimSuffix(p, "/")
	return p
}
