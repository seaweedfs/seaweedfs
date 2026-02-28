package lakekeeper

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

	scriptContent := fmt.Sprintf(`
import boto3
import botocore
import botocore.config
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.httpsession import URLLib3Session
from urllib.parse import quote
import json
import sys
import time
import logging

# Enable botocore debug logging to see signature calculation
logging.basicConfig(level=logging.DEBUG)
botocore.session.get_session().set_debug_logger()

print("Starting Lakekeeper table bucket repro test...")

endpoint_url = "http://host.docker.internal:%d"
access_key = "%s"
secret_key = "%s"
region = "us-east-1"

def get_status(resp):
    return getattr(resp, "status_code", getattr(resp, "status", None))

def get_body(resp):
    body = getattr(resp, "content", None)
    if body is None:
        body = getattr(resp, "data", b"")
    if body is None:
        body = b""
    return body

def read_json(resp):
    body = get_body(resp)
    if not body:
        return {}
    return json.loads(body.decode("utf-8"))

def signed_request(method, path, body, access_key_id, secret_access_key, session_token):
    url = endpoint_url + path
    headers = {}
    body_bytes = b""
    if body is not None:
        body_bytes = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/x-amz-json-1.1"
    request = AWSRequest(method=method, url=url, data=body_bytes, headers=headers)
    credentials = Credentials(access_key_id, secret_access_key, session_token)
    SigV4Auth(credentials, "s3tables", region).add_auth(request)
    session = URLLib3Session()
    return session.send(request.prepare())

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
    response = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name
    )

    creds = response['Credentials']
    access_key_id = creds['AccessKeyId']
    secret_access_key = creds['SecretAccessKey']
    session_token = creds['SessionToken']

    print("Verifying S3 Tables operations with vended credentials...")
    bucket = f"lakekeeper-table-bucket-{int(time.time() * 1000)}"

    resp = signed_request("PUT", "/buckets", {"name": bucket}, access_key_id, secret_access_key, session_token)
    status = get_status(resp)
    if status != 200:
        print(f"FAILED: CreateTableBucket returned {status}, body: {get_body(resp)}")
        sys.exit(1)

    data = read_json(resp)
    bucket_arn = data.get("arn")
    if not bucket_arn:
        print(f"FAILED: CreateTableBucket missing arn in response: {data}")
        sys.exit(1)
    print(f"Created table bucket ARN: {bucket_arn}")

    resp = signed_request("GET", "/buckets", None, access_key_id, secret_access_key, session_token)
    status = get_status(resp)
    if status != 200:
        print(f"FAILED: ListTableBuckets returned {status}, body: {get_body(resp)}")
        sys.exit(1)

    data = read_json(resp)
    buckets = [b.get("name") for b in data.get("tableBuckets", [])]
    print(f"Found table buckets: {buckets}")
    if bucket not in buckets:
        print(f"FAILED: Bucket {bucket} not found in list")
        sys.exit(1)

    get_path = "/buckets/" + quote(bucket_arn, safe="")
    resp = signed_request("GET", get_path, None, access_key_id, secret_access_key, session_token)
    status = get_status(resp)
    if status != 200:
        print(f"FAILED: GetTableBucket returned {status}, body: {get_body(resp)}")
        sys.exit(1)

    resp = signed_request("DELETE", get_path, None, access_key_id, secret_access_key, session_token)
    status = get_status(resp)
    if status != 200:
        print(f"FAILED: DeleteTableBucket returned {status}, body: {get_body(resp)}")
        sys.exit(1)

    resp = signed_request("GET", get_path, None, access_key_id, secret_access_key, session_token)
    status = get_status(resp)
    if status == 200:
        print("FAILED: expected GetTableBucket to fail after deletion")
        sys.exit(1)

    print("SUCCESS: Lakekeeper table bucket flow verified!")
    sys.exit(0)

except Exception as e:
    print(f"FAILED: {e}")
    if hasattr(e, 'response'):
        print(f"Response: {e.response}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
`, env.s3Port, env.accessKey, env.secretKey)

	scriptPath := filepath.Join(env.dataDir, "lakekeeper_table_bucket_repro.py")
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0644); err != nil {
		t.Fatalf("Failed to write python script: %v", err)
	}

	containerName := "seaweed-lakekeeper-table-client-" + fmt.Sprintf("%d", time.Now().UnixNano())

	// Create a context with timeout for the docker run command
	dockerCtx, dockerCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer dockerCancel()

	cmd := exec.CommandContext(dockerCtx, "docker", "run", "--rm",
		"--name", containerName,
		"--add-host", "host.docker.internal:host-gateway",
		"-v", fmt.Sprintf("%s:/work", env.dataDir),
		"python:3",
		"/bin/bash", "-c", "pip install boto3 && python /work/lakekeeper_table_bucket_repro.py",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		if dockerCtx.Err() == context.DeadlineExceeded {
			t.Fatalf("Lakekeeper table bucket client timed out after 5 minutes\nOutput:\n%s", string(output))
		}
		t.Fatalf("Lakekeeper table bucket client failed: %v\nOutput:\n%s", err, string(output))
	}
	t.Logf("Lakekeeper table bucket client output:\n%s", string(output))
}
