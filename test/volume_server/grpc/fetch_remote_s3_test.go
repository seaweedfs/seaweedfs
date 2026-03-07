package volume_server_grpc_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// findAvailablePort finds a free TCP port on localhost.
func findAvailablePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
}

// waitForPort waits until a TCP port is listening, up to timeout.
func waitForPort(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("port %d not listening after %v", port, timeout)
}

// startWeedMini starts a weed mini subprocess and returns the S3 endpoint and cleanup func.
func startWeedMini(t *testing.T) (s3Endpoint string, cleanup func()) {
	t.Helper()

	weedBin, err := exec.LookPath("weed")
	if err != nil {
		weedBin = filepath.Join("..", "..", "..", "weed", "weed_binary")
		if _, err := os.Stat(weedBin); os.IsNotExist(err) {
			t.Skip("weed binary not found, skipping S3 remote storage test")
		}
	}

	miniMasterPort, _ := findAvailablePort()
	miniVolumePort, _ := findAvailablePort()
	miniFilerPort, _ := findAvailablePort()
	miniS3Port, _ := findAvailablePort()
	miniDir := t.TempDir()
	os.WriteFile(filepath.Join(miniDir, "security.toml"), []byte("# empty\n"), 0644)

	ctx, cancel := context.WithCancel(context.Background())

	miniCmd := exec.CommandContext(ctx, weedBin, "mini",
		fmt.Sprintf("-dir=%s", miniDir),
		fmt.Sprintf("-master.port=%d", miniMasterPort),
		fmt.Sprintf("-volume.port=%d", miniVolumePort),
		fmt.Sprintf("-filer.port=%d", miniFilerPort),
		fmt.Sprintf("-s3.port=%d", miniS3Port),
	)
	miniCmd.Env = append(os.Environ(), "AWS_ACCESS_KEY_ID=admin", "AWS_SECRET_ACCESS_KEY=admin")
	miniCmd.Dir = miniDir
	logFile, _ := os.CreateTemp("", "weed-mini-*.log")
	miniCmd.Stdout = logFile
	miniCmd.Stderr = logFile
	t.Logf("weed mini logs at %s", logFile.Name())

	if err := miniCmd.Start(); err != nil {
		cancel()
		logFile.Close()
		t.Fatalf("start weed mini: %v", err)
	}

	if err := waitForPort(miniS3Port, 30*time.Second); err != nil {
		cancel()
		miniCmd.Wait()
		logFile.Close()
		t.Fatalf("weed mini S3 not ready: %v", err)
	}
	t.Logf("weed mini S3 ready on port %d", miniS3Port)

	return fmt.Sprintf("http://127.0.0.1:%d", miniS3Port), func() {
		cancel()
		miniCmd.Wait()
		logFile.Close()
	}
}

func newS3Client(endpoint string) *s3.S3 {
	sess, _ := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials("admin", "admin", ""),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})
	return s3.New(sess)
}

// TestFetchAndWriteNeedleFromS3 tests the full FetchAndWriteNeedle flow:
// 1. Start a weed mini instance as S3 backend
// 2. Upload a test object to it via S3 API
// 3. Call FetchAndWriteNeedle on the volume server to fetch from S3
// 4. Verify the response contains a valid e_tag
func TestFetchAndWriteNeedleFromS3(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	s3Endpoint, cleanupMini := startWeedMini(t)
	defer cleanupMini()

	s3Client := newS3Client(s3Endpoint)

	// Create bucket and upload test data
	bucket := "test-remote-fetch"
	s3Client.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bucket)})

	testData := []byte("Hello from S3 remote storage! This is test data for FetchAndWriteNeedle.")
	testKey := "test-object.dat"
	_, err := s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(testKey),
		Body:   bytes.NewReader(testData),
	})
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	t.Logf("uploaded %d bytes to s3://%s/%s", len(testData), bucket, testKey)

	// Start volume server
	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(99)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	grpcCtx, grpcCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer grpcCancel()

	// FetchAndWriteNeedle from S3
	resp, err := grpcClient.FetchAndWriteNeedle(grpcCtx, &volume_server_pb.FetchAndWriteNeedleRequest{
		VolumeId: volumeID,
		NeedleId: 42,
		Cookie:   12345,
		Offset:   0,
		Size:     int64(len(testData)),
		RemoteConf: &remote_pb.RemoteConf{
			Name:             "test-s3",
			Type:             "s3",
			S3AccessKey:      "admin",
			S3SecretKey:      "admin",
			S3Region:         "us-east-1",
			S3Endpoint:       s3Endpoint,
			S3ForcePathStyle: true,
		},
		RemoteLocation: &remote_pb.RemoteStorageLocation{
			Name:   "test-s3",
			Bucket: bucket,
			Path:   "/" + testKey,
		},
	})
	if err != nil {
		t.Fatalf("FetchAndWriteNeedle failed: %v", err)
	}
	if resp.GetETag() == "" {
		t.Fatal("FetchAndWriteNeedle returned empty e_tag")
	}
	t.Logf("FetchAndWriteNeedle success: e_tag=%s", resp.GetETag())
}

// TestFetchAndWriteNeedleFromS3WithPartialRead tests reading a byte range from S3.
func TestFetchAndWriteNeedleFromS3WithPartialRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	s3Endpoint, cleanupMini := startWeedMini(t)
	defer cleanupMini()

	s3Client := newS3Client(s3Endpoint)

	bucket := "partial-read-test"
	s3Client.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bucket)})

	// Upload 1KB of data
	fullData := make([]byte, 1024)
	for i := range fullData {
		fullData[i] = byte(i % 256)
	}
	s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("big.dat"),
		Body: bytes.NewReader(fullData),
	})

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	framework.AllocateVolume(t, grpcClient, 98, "")

	grpcCtx, grpcCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer grpcCancel()

	// Fetch only bytes 100-199 (100 bytes) from the 1KB object
	resp, err := grpcClient.FetchAndWriteNeedle(grpcCtx, &volume_server_pb.FetchAndWriteNeedleRequest{
		VolumeId: 98, NeedleId: 7, Cookie: 999,
		Offset: 100, Size: 100,
		RemoteConf: &remote_pb.RemoteConf{
			Name: "test-s3-partial", Type: "s3",
			S3AccessKey: "admin", S3SecretKey: "admin",
			S3Region: "us-east-1", S3Endpoint: s3Endpoint, S3ForcePathStyle: true,
		},
		RemoteLocation: &remote_pb.RemoteStorageLocation{
			Name: "test-s3-partial", Bucket: bucket, Path: "/big.dat",
		},
	})
	if err != nil {
		t.Fatalf("FetchAndWriteNeedle partial read failed: %v", err)
	}
	if resp.GetETag() == "" {
		t.Fatal("empty e_tag for partial read")
	}
	t.Logf("FetchAndWriteNeedle partial read success: e_tag=%s", resp.GetETag())
}

// TestFetchAndWriteNeedleS3NotFound tests that fetching a non-existent S3 object returns an error.
func TestFetchAndWriteNeedleS3NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	s3Endpoint, cleanupMini := startWeedMini(t)
	defer cleanupMini()

	s3Client := newS3Client(s3Endpoint)

	bucket := "notfound-test"
	s3Client.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bucket)})

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	framework.AllocateVolume(t, grpcClient, 97, "")

	grpcCtx, grpcCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer grpcCancel()

	_, err := grpcClient.FetchAndWriteNeedle(grpcCtx, &volume_server_pb.FetchAndWriteNeedleRequest{
		VolumeId: 97, NeedleId: 1, Cookie: 1,
		Offset: 0, Size: 100,
		RemoteConf: &remote_pb.RemoteConf{
			Name: "test-s3-nf", Type: "s3",
			S3AccessKey: "admin", S3SecretKey: "admin",
			S3Region: "us-east-1", S3Endpoint: s3Endpoint, S3ForcePathStyle: true,
		},
		RemoteLocation: &remote_pb.RemoteStorageLocation{
			Name: "test-s3-nf", Bucket: bucket, Path: "/does-not-exist.dat",
		},
	})
	if err == nil {
		t.Fatal("FetchAndWriteNeedle should fail for non-existent object")
	}
	if !strings.Contains(err.Error(), "read from remote") {
		t.Fatalf("expected 'read from remote' error, got: %v", err)
	}
	t.Logf("correctly got error for non-existent object: %v", err)
}
