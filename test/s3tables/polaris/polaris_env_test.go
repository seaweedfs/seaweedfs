package polaris

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

	"github.com/seaweedfs/seaweedfs/test/s3tables/testutil"
)

const (
	polarisImage            = "apache/polaris:latest"
	polarisRealm            = "POLARIS"
	polarisRootClientID     = "root"
	polarisRootClientSecret = "s3cr3t"
	polarisRegion           = "us-east-1"
	polarisRoleArn          = "arn:aws:iam::000000000000:role/LakekeeperVendedRole"
	polarisSigningKey       = "dGVzdC1zaWduaW5nLWtleS1mb3Itc3RzLWludGVncmF0aW9uLXRlc3Rz" // gitleaks:allow - test signing key
)

type TestEnvironment struct {
	seaweedDir       string
	weedBinary       string
	dataDir          string
	bindIP           string
	s3Port           int
	s3GrpcPort       int
	masterPort       int
	masterGrpcPort   int
	filerPort        int
	filerGrpcPort    int
	volumePort       int
	volumeGrpcPort   int
	polarisPort      int
	polarisAdminPort int
	weedProcess      *exec.Cmd
	weedCancel       context.CancelFunc
	polarisContainer string
	accessKey        string
	secretKey        string
}

func NewTestEnvironment(t *testing.T) *TestEnvironment {
	t.Helper()

	if !testutil.HasDocker() {
		t.Fatalf("Docker is required for Polaris integration tests")
	}

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

	dataDir, err := os.MkdirTemp("", "seaweed-polaris-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	bindIP := testutil.FindBindIP()

	masterPort, masterGrpcPort := testutil.MustFreePortPair(t, "Master")
	volumePort, volumeGrpcPort := testutil.MustFreePortPair(t, "Volume")
	filerPort, filerGrpcPort := testutil.MustFreePortPair(t, "Filer")
	s3Port, s3GrpcPort := testutil.MustFreePortPair(t, "S3")
	polarisPort, polarisAdminPort := testutil.MustFreePortPair(t, "Polaris")

	return &TestEnvironment{
		seaweedDir:       seaweedDir,
		weedBinary:       weedBinary,
		dataDir:          dataDir,
		bindIP:           bindIP,
		s3Port:           s3Port,
		s3GrpcPort:       s3GrpcPort,
		masterPort:       masterPort,
		masterGrpcPort:   masterGrpcPort,
		filerPort:        filerPort,
		filerGrpcPort:    filerGrpcPort,
		volumePort:       volumePort,
		volumeGrpcPort:   volumeGrpcPort,
		polarisPort:      polarisPort,
		polarisAdminPort: polarisAdminPort,
		accessKey:        "admin",
		secretKey:        "admin",
	}
}

func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

	iamConfigPath := filepath.Join(env.dataDir, "iam.json")
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
    "signingKey": "%s"
  },
  "roles": [
    {
      "roleName": "LakekeeperVendedRole",
      "roleArn": "%s",
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
}`, env.accessKey, env.secretKey, polarisSigningKey, polarisRoleArn)

	if err := os.WriteFile(iamConfigPath, []byte(iamConfig), 0644); err != nil {
		t.Fatalf("Failed to create IAM config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	env.weedCancel = cancel

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

func (env *TestEnvironment) StartPolaris(t *testing.T) {
	t.Helper()

	containerName := fmt.Sprintf("seaweed-polaris-%d", time.Now().UnixNano())
	cmd := exec.Command("docker", "run", "-d", "--rm",
		"--name", containerName,
		"--add-host", "host.docker.internal:host-gateway",
		"-p", fmt.Sprintf("%d:8181", env.polarisPort),
		"-p", fmt.Sprintf("%d:8182", env.polarisAdminPort),
		"-e", fmt.Sprintf("AWS_REGION=%s", polarisRegion),
		"-e", fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", env.accessKey),
		"-e", fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", env.secretKey),
		"-e", fmt.Sprintf("POLARIS_BOOTSTRAP_CREDENTIALS=%s,%s,%s", polarisRealm, polarisRootClientID, polarisRootClientSecret),
		"-e", fmt.Sprintf("polaris.realm-context.realms=%s", polarisRealm),
		"-e", "quarkus.otel.sdk.disabled=true",
		polarisImage,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to start Polaris: %v\nOutput:\n%s", err, string(output))
	}
	env.polarisContainer = containerName

	if !testutil.WaitForService(fmt.Sprintf("http://localhost:%d/q/health", env.polarisAdminPort), 60*time.Second) {
		logs, _ := exec.Command("docker", "logs", env.polarisContainer).CombinedOutput()
		t.Fatalf("Polaris failed to become ready\nLogs:\n%s", string(logs))
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
	if env.polarisContainer != "" {
		_ = exec.Command("docker", "rm", "-f", env.polarisContainer).Run()
	}
	if env.dataDir != "" {
		_ = os.RemoveAll(env.dataDir)
	}
}

func (env *TestEnvironment) polarisEndpoint() string {
	return fmt.Sprintf("http://127.0.0.1:%d", env.polarisPort)
}

func (env *TestEnvironment) s3Endpoint() string {
	return fmt.Sprintf("http://127.0.0.1:%d", env.s3Port)
}

func (env *TestEnvironment) s3InternalEndpoint() string {
	return fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)
}

type polarisHTTPClient struct {
	baseURL    string
	realm      string
	token      string
	httpClient *http.Client
}

func newPolarisHTTPClient(baseURL, realm, token string) *polarisHTTPClient {
	return &polarisHTTPClient{
		baseURL: baseURL,
		realm:   realm,
		token:   token,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *polarisHTTPClient) doJSON(ctx context.Context, method, path string, body interface{}, out interface{}) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request body: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.realm != "" {
		req.Header.Set("Polaris-Realm", c.realm)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func newS3Client(ctx context.Context, endpoint, region string, creds aws.Credentials, pathStyle bool) (*s3.Client, error) {
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
		o.UsePathStyle = pathStyle
	}), nil
}
