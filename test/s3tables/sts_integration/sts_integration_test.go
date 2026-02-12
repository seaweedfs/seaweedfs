package sts_integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/s3tables/testutil"
)

// TestEnvironment mirrors the one in trino_catalog_test.go but simplified
type TestEnvironment struct {
	seaweedDir      string
	weedBinary      string
	dataDir         string
	bindIP          string
	s3Port          int
	s3GrpcPort      int
	masterPort      int
	masterGrpcPort  int
	filerPort       int
	filerGrpcPort   int
	volumePort      int
	volumeGrpcPort  int
	weedProcess     *exec.Cmd
	weedCancel      context.CancelFunc
	dockerAvailable bool
	accessKey       string
	secretKey       string
}

func TestSTSIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping STS integration test")
	}

	fmt.Printf(">>> Starting SeaweedFS...\n")
	env.StartSeaweedFS(t)
	fmt.Printf(">>> SeaweedFS started.\n")

	// Run python script in docker to test STS
	runPythonSTSClient(t, env)
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
	info, err := os.Stat(weedBinary)
	if err != nil || info.IsDir() {
		weedBinary = "weed"
		if _, err := exec.LookPath(weedBinary); err != nil {
			t.Skip("weed binary not found, skipping integration test")
		}
	}

	if !testutil.HasDocker() {
		t.Skip("Docker not available, skipping integration test")
	}

	// Create a unique temporary directory for this test run
	dataDir, err := os.MkdirTemp("", "seaweed-sts-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	// The Cleanup method will remove this directory, so no need for defer here.

	bindIP := testutil.FindBindIP()

	masterPort, masterGrpcPort := testutil.MustFreePortPair(t, "Master")
	volumePort, volumeGrpcPort := testutil.MustFreePortPair(t, "Volume")
	filerPort, filerGrpcPort := testutil.MustFreePortPair(t, "Filer")
	s3Port, s3GrpcPort := testutil.MustFreePortPair(t, "S3") // Changed to use testutil.MustFreePortPair

	return &TestEnvironment{
		seaweedDir:      seaweedDir,
		weedBinary:      weedBinary,
		dataDir:         dataDir,
		bindIP:          bindIP,
		s3Port:          s3Port,
		s3GrpcPort:      s3GrpcPort,
		masterPort:      masterPort,
		masterGrpcPort:  masterGrpcPort,
		filerPort:       filerPort,
		filerGrpcPort:   filerGrpcPort,
		volumePort:      volumePort,
		volumeGrpcPort:  volumeGrpcPort,
		dockerAvailable: testutil.HasDocker(),
		accessKey:       "admin", // Matching default in testutil.WriteIAMConfig
		secretKey:       "admin",
	}
}

func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

	// Create IAM config file
	iamConfigPath, err := testutil.WriteIAMConfig(env.dataDir, env.accessKey, env.secretKey)
	if err != nil {
		t.Fatalf("Failed to create IAM config: %v", err)
	}

	// Create empty security.toml
	securityToml := filepath.Join(env.dataDir, "security.toml")
	if err := os.WriteFile(securityToml, []byte("# Empty security config for testing\n"), 0644); err != nil {
		t.Fatalf("Failed to create security.toml: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	env.weedCancel = cancel

	cmd := exec.CommandContext(ctx, env.weedBinary, "mini",
		"-master.port", fmt.Sprintf("%d", env.masterPort),
		"-master.port.grpc", fmt.Sprintf("%d", env.masterGrpcPort),
		"-volume.port", fmt.Sprintf("%d", env.volumePort),
		"-volume.port.grpc", fmt.Sprintf("%d", env.volumeGrpcPort),
		"-filer.port", fmt.Sprintf("%d", env.filerPort),
		"-filer.port.grpc", fmt.Sprintf("%d", env.filerGrpcPort),
		"-s3.port", fmt.Sprintf("%d", env.s3Port),
		"-s3.port.grpc", fmt.Sprintf("%d", env.s3GrpcPort),
		"-s3.config", iamConfigPath,
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

	// Wait for S3 API to be ready
	if !testutil.WaitForService(fmt.Sprintf("http://localhost:%d/status", env.s3Port), 30*time.Second) {
		t.Fatalf("S3 API failed to become ready")
	}
}

func (env *TestEnvironment) Start(t *testing.T) {
	if !testutil.HasDocker() {
		t.Skip("Docker not available")
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

func runPythonSTSClient(t *testing.T, env *TestEnvironment) {
	t.Helper()

	// Write python script to temp dir
	scriptContent := fmt.Sprintf(`
import boto3
import botocore.config
from botocore.exceptions import ClientError
import os
import sys

print("Starting STS test...")

endpoint_url = "http://host.docker.internal:%d"
access_key = "%s"
secret_key = "%s"
region = "us-east-1"

print(f"Connecting to {endpoint_url} with key {access_key}")

try:
    config = botocore.config.Config(
        retries={'max_attempts': 0}
    )
    sts = boto3.client(
        'sts',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=config
    )

    role_arn = "arn:aws:iam::000000000000:role/test-role"
    session_name = "test-session"

    print(f"Calling AssumeRole on {role_arn}")
    
    # This call typically sends parameters in POST body by default in boto3
    response = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name
    )

    print("Success! Got credentials:")
    print(response['Credentials'])
    
except ClientError as e:
    # Print available keys for debugging if needed
    # print(e.response.keys())
    
    response_meta = e.response.get('ResponseMetadata', {})
    http_code = response_meta.get('HTTPStatusCode')
    
    error_data = e.response.get('Error', {})
    error_code = error_data.get('Code', 'Unknown')
    
    print(f"Got error: {http_code} {error_code}")
    
    # We expect 503 ServiceUnavailable because stsHandlers is nil in weed mini
    # This confirms the request was routed to STS handler logic (UnifiedPostHandler)
    # instead of IAM handler (which would return 403 AccessDenied or 501 NotImplemented)
    if http_code == 503:
        print("SUCCESS: Got expected 503 Service Unavailable (STS not configured)")
        sys.exit(0)
        
    print(f"FAILED: Unexpected error {e}")
    sys.exit(1)
except Exception as e:
    print(f"FAILED: {e}")
    sys.exit(1)
`, env.s3Port, env.accessKey, env.secretKey)

	scriptPath := filepath.Join(env.dataDir, "sts_test.py")
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0644); err != nil {
		t.Fatalf("Failed to write python script: %v", err)
	}

	containerName := "seaweed-sts-client-" + fmt.Sprintf("%d", time.Now().UnixNano())

	cmd := exec.Command("docker", "run", "--rm",
		"--name", containerName,
		"--add-host", "host.docker.internal:host-gateway",
		"-v", fmt.Sprintf("%s:/work", env.dataDir),
		"python:3",
		"/bin/bash", "-c", "pip install boto3 && python /work/sts_test.py",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Python STS client failed: %v\nOutput:\n%s", err, string(output))
	}
	t.Logf("Python STS client output:\n%s", string(output))
}

// Helpers copied from trino_catalog_test.go
