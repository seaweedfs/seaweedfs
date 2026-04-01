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

const testSTSIntegrationSigningKey = "dGVzdC1zaWduaW5nLWtleS1mb3Itc3RzLWludGVncmF0aW9uLXRlc3Rz" // gitleaks:allow - test-signing-key-for-sts-integration-tests

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
		accessKey:       "admin",
		secretKey:       "adminadmin",
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
        { "accessKey": "%s", "secretKey": "%s" }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ],
  "sts": {
    "tokenDuration": "1h",
    "maxSessionLength": "12h",
    "issuer": "seaweedfs-sts",
    "signingKey": "%s"
  },
  "policy": {
    "defaultEffect": "Deny",
    "storeType": "memory"
  },
  "policies": [
    {
      "name": "S3FullAccessPolicy",
      "document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": ["s3:*"],
            "Resource": ["*"]
          }
        ]
      }
    }
  ],
  "roles": [
    {
      "roleName": "TestRole",
      "roleArn": "arn:aws:iam::role/TestRole",
      "attachedPolicies": ["S3FullAccessPolicy"],
      "trustPolicy": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["sts:AssumeRole"]
          }
        ]
      }
    }
  ]
}`, env.accessKey, env.secretKey, testSTSIntegrationSigningKey)
	if err := os.WriteFile(iamConfigPath, []byte(iamConfig), 0644); err != nil {
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
		"-s3.iam.config", iamConfigPath,
		"-s3.iam.readOnly", "false",
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
import json
import sys
import time
import urllib.error
import urllib.request

print("Starting STS inline session policy test...")

primary_endpoint = "http://host.docker.internal:%d"
fallback_endpoint = "http://%s:%d"
access_key = "%s"
secret_key = "%s"
region = "us-east-1"

try:
    def wait_for_endpoint(url, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=2):
                    return True
            except urllib.error.HTTPError:
                return True
            except Exception:
                time.sleep(1)
        return False

    def select_endpoint(urls):
        for url in urls:
            if wait_for_endpoint(url):
                return url
        raise Exception("No reachable S3 endpoint from container")

    endpoint_url = select_endpoint([primary_endpoint, fallback_endpoint])
    print(f"Using endpoint {endpoint_url}")

    config = botocore.config.Config(
        retries={'max_attempts': 0},
        s3={'addressing_style': 'path'}
    )
    admin_s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=config
    )

    bucket = f"sts-inline-policy-{int(time.time() * 1000)}"
    key = "allowed.txt"

    print(f"Creating bucket {bucket} with admin credentials")
    admin_s3.create_bucket(Bucket=bucket)
    admin_s3.put_object(Bucket=bucket, Key=key, Body=b"ok")

    sts = boto3.client(
        'sts',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=config
    )

    role_arn = "arn:aws:iam::role/TestRole"
    session_name = "test-session"
    session_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [f"arn:aws:s3:::{bucket}"]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket}/*"]
            }
        ]
    })

    print(f"Calling AssumeRole on {role_arn} with inline session policy")
    response = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name,
        Policy=session_policy
    )

    creds = response['Credentials']
    vended_s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=creds['AccessKeyId'],
        aws_secret_access_key=creds['SecretAccessKey'],
        aws_session_token=creds['SessionToken'],
        region_name=region,
        config=config
    )

    print("Listing objects (allowed)")
    list_resp = vended_s3.list_objects_v2(Bucket=bucket)
    keys = [obj.get('Key') for obj in list_resp.get('Contents', [])]
    if key not in keys:
        print(f"FAILED: Expected to see {key} in list_objects_v2 results")
        sys.exit(1)

    print("Getting object (allowed)")
    body = vended_s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    if body != b"ok":
        print("FAILED: Unexpected object content")
        sys.exit(1)

    print("Putting object (expected to be denied)")
    try:
        vended_s3.put_object(Bucket=bucket, Key="denied.txt", Body=b"no")
        print("FAILED: PutObject unexpectedly succeeded")
        sys.exit(1)
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code != 'AccessDenied':
            print(f"FAILED: Expected AccessDenied, got {error_code}")
            sys.exit(1)
        print("PutObject correctly denied by inline session policy")

    print("SUCCESS: Inline session policy downscoping verified")
    sys.exit(0)
except Exception as e:
    print(f"FAILED: {e}")
    if hasattr(e, 'response'):
        print(f"Response: {e.response}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
`, env.s3Port, env.bindIP, env.s3Port, env.accessKey, env.secretKey)

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
