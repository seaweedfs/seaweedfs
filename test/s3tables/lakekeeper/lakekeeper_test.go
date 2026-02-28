package lakekeeper

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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/seaweedfs/seaweedfs/test/s3tables/testutil"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

type TestEnvironment struct {
	seaweedDir     string
	weedBinary     string
	dataDir        string
	bindIP         string
	s3Port         int
	s3GrpcPort     int
	masterPort     int
	masterGrpcPort int
	filerPort      int
	filerGrpcPort  int
	volumePort     int
	volumeGrpcPort int
	weedProcess    *exec.Cmd
	weedCancel     context.CancelFunc
	accessKey      string
	secretKey      string
}

func TestLakekeeperIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !testutil.HasDocker() {
		t.Skip("Docker not available, skipping Lakekeeper integration test")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	fmt.Printf(">>> Starting SeaweedFS with Lakekeeper configuration...\n")
	env.StartSeaweedFS(t)
	fmt.Printf(">>> SeaweedFS started.\n")

	// Run python script in docker to test STS and S3 operations
	runLakekeeperRepro(t, env)
}

func TestLakekeeperTableBucketIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !testutil.HasDocker() {
		t.Skip("Docker not available, skipping Lakekeeper table bucket integration test")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	fmt.Printf(">>> Starting SeaweedFS with Lakekeeper configuration...\n")
	env.StartSeaweedFS(t)
	fmt.Printf(">>> SeaweedFS started.\n")

	// Run python script in docker to test STS and S3 Tables operations
	runLakekeeperTableBucketRepro(t, env)
}

func NewTestEnvironment(t *testing.T) *TestEnvironment {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	seaweedDir := wd
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(seaweedDir, "go.mod")); err == nil {
			break
		}
		seaweedDir = filepath.Dir(seaweedDir)
	}

	weedBinary := filepath.Join(seaweedDir, "weed", "weed")
	if _, err := os.Stat(weedBinary); err != nil {
		weedBinary = "weed"
		if _, err := exec.LookPath(weedBinary); err != nil {
			t.Skip("weed binary not found, skipping integration test")
		}
	}

	dataDir, err := os.MkdirTemp("", "seaweed-lakekeeper-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	bindIP := testutil.FindBindIP()

	masterPort, masterGrpcPort := testutil.MustFreePortPair(t, "Master")
	volumePort, volumeGrpcPort := testutil.MustFreePortPair(t, "Volume")
	filerPort, filerGrpcPort := testutil.MustFreePortPair(t, "Filer")
	s3Port, s3GrpcPort := testutil.MustFreePortPair(t, "S3")

	return &TestEnvironment{
		seaweedDir:     seaweedDir,
		weedBinary:     weedBinary,
		dataDir:        dataDir,
		bindIP:         bindIP,
		s3Port:         s3Port,
		s3GrpcPort:     s3GrpcPort,
		masterPort:     masterPort,
		masterGrpcPort: masterGrpcPort,
		filerPort:      filerPort,
		filerGrpcPort:  filerGrpcPort,
		volumePort:     volumePort,
		volumeGrpcPort: volumeGrpcPort,
		accessKey:      "admin",
		secretKey:      "admin",
	}
}

func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

	iamConfigPath := filepath.Join(env.dataDir, "iam.json")
	// Note: signingKey must be base64 encoded for []byte JSON unmarshaling
	iamConfig := fmt.Sprintf(`{
  "identities": [
    {
      "name": "admin",
      "credentials": [
        {
          "accessKey": "%s",
          "secretKey": "%s"
        }
      ],
      "actions": ["Admin", "Read", "List", "Tagging", "Write"]
    }
  ],
  "sts": {
    "tokenDuration": "12h",
    "maxSessionLength": "24h",
    "issuer": "seaweedfs-sts",
    "signingKey": "dGVzdC1zaWduaW5nLWtleS1mb3Itc3RzLWludGVncmF0aW9uLXRlc3Rz"
  },
  "roles": [
    {
      "roleName": "LakekeeperVendedRole",
      "roleArn": "arn:aws:iam::000000000000:role/LakekeeperVendedRole",
      "trustPolicy": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "sts:AssumeRole"
          }
        ]
      },
      "attachedPolicies": ["FullAccess"]
    }
  ],
  "policies": [
    {
      "name": "FullAccess",
      "document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*"
          }
        ]
      }
    }
  ]
}`, env.accessKey, env.secretKey)

	if err := os.WriteFile(iamConfigPath, []byte(iamConfig), 0644); err != nil {
		t.Fatalf("Failed to create IAM config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	env.weedCancel = cancel

	// Start weed mini with both S3 config (standard IAM) and IAM config (advanced IAM/STS)
	cmd := exec.CommandContext(ctx, env.weedBinary, "-v", "4", "mini",
		"-master.port", fmt.Sprintf("%d", env.masterPort),
		"-master.port.grpc", fmt.Sprintf("%d", env.masterGrpcPort),
		"-volume.port", fmt.Sprintf("%d", env.volumePort),
		"-volume.port.grpc", fmt.Sprintf("%d", env.volumeGrpcPort),
		"-filer.port", fmt.Sprintf("%d", env.filerPort),
		"-filer.port.grpc", fmt.Sprintf("%d", env.filerGrpcPort),
		"-s3.port", fmt.Sprintf("%d", env.s3Port),
		"-s3.port.grpc", fmt.Sprintf("%d", env.s3GrpcPort),
		"-s3.config", iamConfigPath,
		"-s3.iam.config", iamConfigPath,
		"-s3.iam.readOnly=false",
		"-ip", env.bindIP,
		"-ip.bind", "0.0.0.0",
		"-dir", env.dataDir,
	)
	cmd.Dir = env.dataDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start SeaweedFS: %v", err)
	}
	env.weedProcess = cmd

	if !testutil.WaitForService(fmt.Sprintf("http://localhost:%d/status", env.s3Port), 30*time.Second) {
		t.Fatalf("S3 API failed to become ready")
	}
}

func (env *TestEnvironment) Cleanup(t *testing.T) {
	t.Helper()
	if env.weedCancel != nil {
		env.weedCancel()
	}
	if env.weedProcess != nil {
		time.Sleep(1 * time.Second)
		_ = env.weedProcess.Wait()
	}
	if env.dataDir != "" {
		_ = os.RemoveAll(env.dataDir)
	}
}

func runLakekeeperRepro(t *testing.T, env *TestEnvironment) {
	t.Helper()

	scriptContent := fmt.Sprintf(`
import boto3
import botocore.config
import botocore
from botocore.exceptions import ClientError
import os
import sys
import time
import logging

# Enable botocore debug logging to see signature calculation
logging.basicConfig(level=logging.DEBUG)
botocore.session.get_session().set_debug_logger()

print("Starting Lakekeeper repro test...")

endpoint_url = "http://host.docker.internal:%d"
access_key = "%s"
secret_key = "%s"
region = "us-east-1"

print(f"Connecting to {endpoint_url}")

try:
    config = botocore.config.Config(
        retries={'max_attempts': 3}
    )
    sts = boto3.client(
        'sts',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=config
    )

    role_arn = "arn:aws:iam::000000000000:role/LakekeeperVendedRole"
    session_name = "lakekeeper-session"

    print(f"Calling AssumeRole on {role_arn} with POST body...")
    
    # Standard boto3 call sends parameters in POST body
    response = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name
    )

    creds = response['Credentials']
    access_key_id = creds['AccessKeyId']
    secret_access_key = creds['SecretAccessKey']
    session_token = creds['SessionToken']

    print(f"Success! Got credentials with prefix: {access_key_id[:4]}")
    
    if not access_key_id.startswith("ASIA"):
        print(f"FAILED: Expected ASIA prefix, got {access_key_id}")
        sys.exit(1)

    print("Verifying S3 operations with vended credentials...")
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token,
        region_name=region,
        config=config
    )

    bucket = "lakekeeper-vended-bucket"
    print(f"Creating bucket {bucket}...")
    s3.create_bucket(Bucket=bucket)

    print("Listing buckets...")
    response = s3.list_buckets()
    buckets = [b['Name'] for b in response['Buckets']]
    print(f"Found buckets: {buckets}")

    if bucket not in buckets:
        print(f"FAILED: Bucket {bucket} not found in list")
        sys.exit(1)

    print("SUCCESS: Lakekeeper flow verified!")
    sys.exit(0)
    
except Exception as e:
    print(f"FAILED: {e}")
    # Print more details if it is a ClientError
    if hasattr(e, 'response'):
        print(f"Response: {e.response}")
    sys.exit(1)
`, env.s3Port, env.accessKey, env.secretKey)

	scriptPath := filepath.Join(env.dataDir, "lakekeeper_repro.py")
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0644); err != nil {
		t.Fatalf("Failed to write python script: %v", err)
	}

	containerName := "seaweed-lakekeeper-client-" + fmt.Sprintf("%d", time.Now().UnixNano())

	// Create a context with timeout for the docker run command
	dockerCtx, dockerCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer dockerCancel()

	cmd := exec.CommandContext(dockerCtx, "docker", "run", "--rm",
		"--name", containerName,
		"--add-host", "host.docker.internal:host-gateway",
		"-v", fmt.Sprintf("%s:/work", env.dataDir),
		"python:3",
		"/bin/bash", "-c", "pip install boto3 && python /work/lakekeeper_repro.py",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		if dockerCtx.Err() == context.DeadlineExceeded {
			t.Fatalf("Lakekeeper repro client timed out after 5 minutes\nOutput:\n%s", string(output))
		}
		t.Fatalf("Lakekeeper repro client failed: %v\nOutput:\n%s", err, string(output))
	}
	t.Logf("Lakekeeper repro client output:\n%s", string(output))
}

func runLakekeeperTableBucketRepro(t *testing.T, env *TestEnvironment) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", env.s3Port)
	region := "us-east-1"
	roleArn := "arn:aws:iam::000000000000:role/LakekeeperVendedRole"
	sessionName := "lakekeeper-session"

	creds, err := assumeRole(ctx, endpoint, region, env.accessKey, env.secretKey, roleArn, sessionName)
	if err != nil {
		t.Fatalf("AssumeRole failed: %v", err)
	}

	client := newS3TablesClient(endpoint, region, creds)
	bucketName := fmt.Sprintf("lakekeeper-table-bucket-%d", time.Now().UnixNano())
	bucketARN, err := client.CreateTableBucket(ctx, bucketName)
	if err != nil {
		t.Fatalf("CreateTableBucket failed: %v", err)
	}

	bucketDeleted := false
	namespaceCreated := false
	tableCreated := false
	namespaceName := fmt.Sprintf("lakekeeper_ns_%d", time.Now().UnixNano())
	tableName := fmt.Sprintf("lakekeeper_table_%d", time.Now().UnixNano())
	defer func() {
		if tableCreated {
			if err := client.DeleteTable(ctx, bucketARN, namespaceName, tableName); err != nil {
				t.Logf("Failed to delete table: %v", err)
			}
		}
		if namespaceCreated {
			if err := client.DeleteNamespace(ctx, bucketARN, namespaceName); err != nil {
				t.Logf("Failed to delete namespace: %v", err)
			}
		}
		if bucketDeleted {
			return
		}
		if err := client.DeleteTableBucket(ctx, bucketARN); err != nil {
			t.Logf("Failed to delete table bucket: %v", err)
		}
	}()

	buckets, err := client.ListTableBuckets(ctx)
	if err != nil {
		t.Fatalf("ListTableBuckets failed: %v", err)
	}
	found := false
	for _, b := range buckets {
		if b.Name == bucketName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Created table bucket %s not found in list", bucketName)
	}

	if _, err := client.GetTableBucket(ctx, bucketARN); err != nil {
		t.Fatalf("GetTableBucket failed: %v", err)
	}

	if err := client.CreateNamespace(ctx, bucketARN, namespaceName); err != nil {
		t.Fatalf("CreateNamespace failed: %v", err)
	}
	namespaceCreated = true

	if err := client.CreateTable(ctx, bucketARN, namespaceName, tableName); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	tableCreated = true

	s3Client, err := newS3Client(ctx, endpoint, region, creds)
	if err != nil {
		t.Fatalf("Create S3 client failed: %v", err)
	}

	objectKey := fmt.Sprintf("%s/%s/data/part-%d.parquet", namespaceName, tableName, time.Now().UnixNano())
	createResp, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
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
		_, _ = s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: aws.String(uploadID),
		})
	}()

	partSize := 5 * 1024 * 1024
	part1 := bytes.Repeat([]byte("a"), partSize)
	part2 := bytes.Repeat([]byte("b"), 1024*1024)

	part1Resp, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part1),
	})
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}

	part2Resp, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader(part2),
	})
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}

	_, err = s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
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

	headResp, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("HeadObject after multipart upload failed: %v", err)
	}
	expectedSize := int64(len(part1) + len(part2))
	if headResp.ContentLength == nil || *headResp.ContentLength != expectedSize {
		t.Fatalf("Unexpected content length: got %d want %d", aws.ToInt64(headResp.ContentLength), expectedSize)
	}

	if err := client.DeleteTable(ctx, bucketARN, namespaceName, tableName); err != nil {
		t.Fatalf("DeleteTable failed: %v", err)
	}
	tableCreated = false

	if err := client.DeleteNamespace(ctx, bucketARN, namespaceName); err != nil {
		t.Fatalf("DeleteNamespace failed: %v", err)
	}
	namespaceCreated = false

	if err := client.DeleteTableBucket(ctx, bucketARN); err != nil {
		t.Fatalf("DeleteTableBucket failed: %v", err)
	}
	bucketDeleted = true

	if _, err := client.GetTableBucket(ctx, bucketARN); err == nil {
		t.Fatalf("expected GetTableBucket to fail after deletion")
	}
}

type s3TablesClient struct {
	endpoint   string
	region     string
	creds      aws.Credentials
	httpClient *http.Client
}

func newS3TablesClient(endpoint, region string, creds aws.Credentials) *s3TablesClient {
	return &s3TablesClient{
		endpoint: endpoint,
		region:   region,
		creds:    creds,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *s3TablesClient) CreateTableBucket(ctx context.Context, name string) (string, error) {
	req := &s3tables.CreateTableBucketRequest{Name: name}
	var resp s3tables.CreateTableBucketResponse
	if err := c.doRequest(ctx, "CreateTableBucket", http.MethodPut, "/buckets", req, &resp); err != nil {
		return "", err
	}
	return resp.ARN, nil
}

func (c *s3TablesClient) GetTableBucket(ctx context.Context, arn string) (*s3tables.GetTableBucketResponse, error) {
	path := "/buckets/" + url.PathEscape(arn)
	var resp s3tables.GetTableBucketResponse
	if err := c.doRequest(ctx, "GetTableBucket", http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *s3TablesClient) ListTableBuckets(ctx context.Context) ([]s3tables.TableBucketSummary, error) {
	var resp s3tables.ListTableBucketsResponse
	if err := c.doRequest(ctx, "ListTableBuckets", http.MethodGet, "/buckets", nil, &resp); err != nil {
		return nil, err
	}
	return resp.TableBuckets, nil
}

func (c *s3TablesClient) DeleteTableBucket(ctx context.Context, arn string) error {
	path := "/buckets/" + url.PathEscape(arn)
	return c.doRequest(ctx, "DeleteTableBucket", http.MethodDelete, path, nil, nil)
}

func (c *s3TablesClient) CreateNamespace(ctx context.Context, bucketARN, namespace string) error {
	req := &s3tables.CreateNamespaceRequest{
		Namespace: []string{namespace},
	}
	path := "/namespaces/" + url.PathEscape(bucketARN)
	return c.doRequest(ctx, "CreateNamespace", http.MethodPut, path, req, nil)
}

func (c *s3TablesClient) DeleteNamespace(ctx context.Context, bucketARN, namespace string) error {
	path := "/namespaces/" + url.PathEscape(bucketARN) + "/" + url.PathEscape(namespace)
	return c.doRequest(ctx, "DeleteNamespace", http.MethodDelete, path, nil, nil)
}

func (c *s3TablesClient) CreateTable(ctx context.Context, bucketARN, namespace, name string) error {
	req := &s3tables.CreateTableRequest{
		Name:   name,
		Format: "ICEBERG",
	}
	path := "/tables/" + url.PathEscape(bucketARN) + "/" + url.PathEscape(namespace)
	return c.doRequest(ctx, "CreateTable", http.MethodPut, path, req, nil)
}

func (c *s3TablesClient) DeleteTable(ctx context.Context, bucketARN, namespace, name string) error {
	path := "/tables/" + url.PathEscape(bucketARN) + "/" + url.PathEscape(namespace) + "/" + url.PathEscape(name)
	return c.doRequest(ctx, "DeleteTable", http.MethodDelete, path, nil, nil)
}

func (c *s3TablesClient) doRequest(ctx context.Context, operation, method, path string, body interface{}, out interface{}) error {
	var bodyBytes []byte
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("%s: marshal request: %w", operation, err)
		}
		bodyBytes = encoded
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("%s: create request: %w", operation, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	}
	req.Host = req.URL.Host
	req.Header.Set("Host", req.URL.Host)

	payloadHash := sha256.Sum256(bodyBytes)
	if err := v4.NewSigner().SignHTTP(ctx, c.creds, req, hex.EncodeToString(payloadHash[:]), "s3tables", c.region, time.Now()); err != nil {
		return fmt.Errorf("%s: sign request: %w", operation, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s: request failed: %w", operation, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("%s failed with status %d and could not read response body: %v", operation, resp.StatusCode, readErr)
		}
		var errResp s3tables.S3TablesError
		if jsonErr := json.Unmarshal(bodyBytes, &errResp); jsonErr == nil && (errResp.Type != "" || errResp.Message != "") {
			return fmt.Errorf("%s failed: %s - %s", operation, errResp.Type, errResp.Message)
		}
		return fmt.Errorf("%s failed with status %d: %s", operation, resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("%s: decode response: %w", operation, err)
	}
	return nil
}

func assumeRole(ctx context.Context, endpoint, region, accessKey, secretKey, roleArn, sessionName string) (aws.Credentials, error) {
	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if service == sts.ServiceID {
			return aws.Endpoint{
				URL:               endpoint,
				SigningRegion:     region,
				HostnameImmutable: true,
			}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithEndpointResolverWithOptions(resolver),
	)
	if err != nil {
		return aws.Credentials{}, err
	}

	client := sts.NewFromConfig(cfg)
	resp, err := client.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String(sessionName),
	})
	if err != nil {
		return aws.Credentials{}, err
	}
	if resp.Credentials == nil {
		return aws.Credentials{}, fmt.Errorf("missing credentials in AssumeRole response")
	}
	return aws.Credentials{
		AccessKeyID:     aws.ToString(resp.Credentials.AccessKeyId),
		SecretAccessKey: aws.ToString(resp.Credentials.SecretAccessKey),
		SessionToken:    aws.ToString(resp.Credentials.SessionToken),
		Source:          "lakekeeper-sts",
	}, nil
}

func newS3Client(ctx context.Context, endpoint, region string, creds aws.Credentials) (*s3.Client, error) {
	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if service == s3.ServiceID {
			return aws.Endpoint{
				URL:               endpoint,
				SigningRegion:     region,
				HostnameImmutable: true,
			}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken)),
		config.WithEndpointResolverWithOptions(resolver),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	}), nil
}
