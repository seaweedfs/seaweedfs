package lakekeeper

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	s3tablesclient "github.com/seaweedfs/seaweedfs/test/s3tables/table-buckets"
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
	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	fmt.Printf(">>> Starting SeaweedFS with Lakekeeper configuration...\n")
	env.StartSeaweedFS(t)
	fmt.Printf(">>> SeaweedFS started.\n")

	// Run STS and S3 operations with vended credentials
	runLakekeeperRepro(t, env)
}

func TestLakekeeperTableBucketIntegration(t *testing.T) {
	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	fmt.Printf(">>> Starting SeaweedFS with Lakekeeper configuration...\n")
	env.StartSeaweedFS(t)
	fmt.Printf(">>> SeaweedFS started.\n")

	// Run S3 Tables operations with vended credentials
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

const (
	lakekeeperRegion  = "us-east-1"
	lakekeeperRoleArn = "arn:aws:iam::000000000000:role/LakekeeperVendedRole"
)

func (env *TestEnvironment) s3Endpoint() string {
	return fmt.Sprintf("http://127.0.0.1:%d", env.s3Port)
}

func assumeLakekeeperRole(t *testing.T, env *TestEnvironment) aws.Credentials {
	t.Helper()
	ctx := context.Background()
	endpoint := env.s3Endpoint()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(lakekeeperRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(env.accessKey, env.secretKey, "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == sts.ServiceID {
				return aws.Endpoint{URL: endpoint, HostnameImmutable: true}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})),
	)
	if err != nil {
		t.Fatalf("Failed to load STS config: %v", err)
	}
	client := sts.NewFromConfig(cfg)
	resp, err := client.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(lakekeeperRoleArn),
		RoleSessionName: aws.String("lakekeeper-session"),
	})
	if err != nil {
		t.Fatalf("AssumeRole failed: %v", err)
	}
	if resp.Credentials == nil {
		t.Fatalf("AssumeRole response missing credentials")
	}
	creds := aws.Credentials{
		AccessKeyID:     aws.ToString(resp.Credentials.AccessKeyId),
		SecretAccessKey: aws.ToString(resp.Credentials.SecretAccessKey),
		SessionToken:    aws.ToString(resp.Credentials.SessionToken),
	}
	if !strings.HasPrefix(creds.AccessKeyID, "ASIA") {
		t.Fatalf("Expected ASIA prefix, got %s", creds.AccessKeyID)
	}
	return creds
}

func newLakekeeperS3Client(t *testing.T, env *TestEnvironment, creds aws.Credentials) *s3.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(lakekeeperRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken)),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{URL: env.s3Endpoint(), HostnameImmutable: true}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})),
	)
	if err != nil {
		t.Fatalf("Failed to load S3 config: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

func newLakekeeperS3TablesClient(t *testing.T, env *TestEnvironment, creds aws.Credentials) *s3tablesclient.S3TablesClient {
	t.Helper()
	return s3tablesclient.NewS3TablesClientWithSession(env.s3Endpoint(), lakekeeperRegion, creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken)
}

func runLakekeeperRepro(t *testing.T, env *TestEnvironment) {
	t.Helper()
	creds := assumeLakekeeperRole(t, env)
	s3Client := newLakekeeperS3Client(t, env, creds)
	ctx := context.Background()

	bucket := "lakekeeper-vended-bucket-" + randomSuffix()
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("Failed to create bucket %s: %v", bucket, err)
	}
	defer func() {
		if _, err := s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Logf("Cleanup: failed to delete bucket %s: %v", bucket, err)
		}
	}()

	listResp, err := s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("Failed to list buckets: %v", err)
	}
	found := false
	for _, b := range listResp.Buckets {
		if aws.ToString(b.Name) == bucket {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Bucket %s not found in list", bucket)
	}
}

func runLakekeeperTableBucketRepro(t *testing.T, env *TestEnvironment) {
	t.Helper()
	creds := assumeLakekeeperRole(t, env)
	client := newLakekeeperS3TablesClient(t, env, creds)
	s3Client := newLakekeeperS3Client(t, env, creds)
	ctx := context.Background()

	bucketName := "lakekeeper-table-bucket-" + randomSuffix()
	createBucketResp, err := client.CreateTableBucket(bucketName, nil)
	if err != nil {
		t.Fatalf("Failed to create table bucket %s: %v", bucketName, err)
	}
	if createBucketResp.ARN == "" {
		t.Fatalf("CreateTableBucket response missing ARN")
	}
	bucketARN := createBucketResp.ARN
	defer func() {
		if err := client.DeleteTableBucket(bucketARN); err != nil {
			t.Logf("Cleanup: failed to delete table bucket %s: %v", bucketName, err)
		}
	}()

	listResp, err := client.ListTableBuckets("", "", 0)
	if err != nil {
		t.Fatalf("Failed to list table buckets: %v", err)
	}
	found := false
	for _, b := range listResp.TableBuckets {
		if b.Name == bucketName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Table bucket %s not found in list", bucketName)
	}

	if _, err := client.GetTableBucket(bucketARN); err != nil {
		t.Fatalf("Failed to get table bucket %s: %v", bucketName, err)
	}

	namespace := "lakekeeper_ns_" + randomSuffix()
	tableName := "lakekeeper_table"
	if _, err := client.CreateNamespace(bucketARN, []string{namespace}); err != nil {
		t.Fatalf("Failed to create namespace %s: %v", namespace, err)
	}
	defer func() {
		if err := client.DeleteNamespace(bucketARN, []string{namespace}); err != nil {
			t.Logf("Cleanup: failed to delete namespace %s: %v", namespace, err)
		}
	}()

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

	if _, err := client.CreateTable(bucketARN, []string{namespace}, tableName, "ICEBERG", icebergMetadata, nil); err != nil {
		t.Fatalf("Failed to create table %s.%s: %v", namespace, tableName, err)
	}
	defer func() {
		if err := client.DeleteTable(bucketARN, []string{namespace}, tableName); err != nil {
			t.Logf("Cleanup: failed to delete table %s.%s: %v", namespace, tableName, err)
		}
	}()

	objectKey := fmt.Sprintf("%s/%s/data/part-%s.parquet", namespace, tableName, randomSuffix())
	multipartUpload(t, s3Client, bucketName, objectKey)
	if _, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucketName), Key: aws.String(objectKey)}); err != nil {
		t.Fatalf("Failed to delete uploaded object %s: %v", objectKey, err)
	}
}

func multipartUpload(t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()
	ctx := context.Background()

	createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}
	uploadID := aws.ToString(createResp.UploadId)
	if uploadID == "" {
		t.Fatalf("CreateMultipartUpload missing upload ID")
	}

	completed := false
	defer func() {
		if completed {
			return
		}
		_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID)})
	}()

	part1 := bytes.Repeat([]byte("a"), 5*1024*1024)
	part2 := []byte("tail")

	uploadPart := func(partNumber int32, data []byte) types.CompletedPart {
		resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int32(partNumber),
			Body:       bytes.NewReader(data),
		})
		if err != nil {
			t.Fatalf("UploadPart %d failed: %v", partNumber, err)
		}
		if resp.ETag == nil {
			t.Fatalf("UploadPart %d missing ETag", partNumber)
		}
		return types.CompletedPart{ETag: resp.ETag, PartNumber: aws.Int32(partNumber)}
	}

	part1Resp := uploadPart(1, part1)
	part2Resp := uploadPart(2, part2)

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{part1Resp, part2Resp},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}
	completed = true
}

func randomSuffix() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
