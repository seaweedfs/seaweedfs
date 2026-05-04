package unity_catalog

import (
	"context"
	"fmt"
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

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

const (
	ucImageDefault = "unitycatalog/unitycatalog:main"
	ucContainerCfg = "/home/unitycatalog/etc/conf/server.properties"
	ucStartupGrace = 90 * time.Second
	ucAPIBase      = "/api/2.1/unity-catalog"
	ucWarehouse    = "lakehouse"
	ucWarehouseKey = "warehouse"

	// Role used by the master-role STS-vended variant of the test. The trust
	// policy is wide open so any caller can assume it; in production UC
	// would set this ARN as `aws.masterRoleArn`.
	ucVendedRoleArn  = "arn:aws:iam::000000000000:role/UnityCatalogVendedRole"
	ucVendedRoleName = "UnityCatalogVendedRole"
)

type testEnv struct {
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

	accessKey string
	secretKey string

	ucImage       string
	ucContainerID string
	ucHostPort    int
}

// ucServerOpts customizes the Unity Catalog server.properties and runtime
// environment. Defaults match the upstream playground (static keys, no master
// role).
type ucServerOpts struct {
	// MasterRoleArn populates aws.masterRoleArn. Empty means UC falls back to
	// static aws.accessKey / aws.secretKey for storage operations.
	MasterRoleArn string
	// ExtraEnv adds environment variables to the UC container, useful for
	// AWS_ENDPOINT_URL_STS-style overrides.
	ExtraEnv map[string]string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	seaweedDir := wd
	for i := 0; i < 8; i++ {
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

	dataDir, err := os.MkdirTemp("", "seaweed-uc-test-*")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}

	bindIP := testutil.FindBindIP()
	ports := testutil.MustAllocatePorts(t, 9)
	masterPort, masterGrpcPort := ports[0], ports[1]
	volumePort, volumeGrpcPort := ports[2], ports[3]
	filerPort, filerGrpcPort := ports[4], ports[5]
	s3Port, s3GrpcPort := ports[6], ports[7]
	ucHostPort := ports[8]

	image := os.Getenv("UC_IMAGE")
	if image == "" {
		image = ucImageDefault
	}

	return &testEnv{
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
		ucImage:        image,
		ucHostPort:     ucHostPort,
	}
}

// startSeaweedFS starts a `weed mini` instance. If iamJSON is empty, a minimal
// admin-only IAM config is used. When iamJSON is non-empty, it is passed to
// both -s3.config and -s3.iam.config so the STS handler is enabled (mirroring
// the lakekeeper test).
func (env *testEnv) startSeaweedFS(t *testing.T, iamJSON string) {
	t.Helper()

	enableSTS := iamJSON != ""
	if iamJSON == "" {
		iamJSON = fmt.Sprintf(`{
  "identities": [
    {
      "name": "admin",
      "credentials": [{"accessKey": %q, "secretKey": %q}],
      "actions": ["Admin", "Read", "List", "Tagging", "Write"]
    }
  ]
}`, env.accessKey, env.secretKey)
	}

	iamConfigPath := filepath.Join(env.dataDir, "iam.json")
	if err := os.WriteFile(iamConfigPath, []byte(iamJSON), 0644); err != nil {
		t.Fatalf("write iam config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	env.weedCancel = cancel

	weedArgs := []string{"-v", "4", "mini",
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
	}
	if enableSTS {
		weedArgs = append(weedArgs, "-s3.iam.config", iamConfigPath, "-s3.iam.readOnly=false")
	}

	cmd := exec.CommandContext(ctx, env.weedBinary, weedArgs...)
	cmd.Dir = env.dataDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start weed mini: %v", err)
	}
	env.weedProcess = cmd

	if !testutil.WaitForService(fmt.Sprintf("http://127.0.0.1:%d/status", env.s3Port), testutil.SeaweedMiniStartupTimeout) {
		t.Fatalf("S3 API at 127.0.0.1:%d did not become ready", env.s3Port)
	}
}

// startUnityCatalog launches the Unity Catalog OSS server in Docker against
// the running SeaweedFS instance. It mirrors the upstream playground's
// server.properties layout and bind-mounts only that single file (matching
// docker-compose.yaml from the playground).
func (env *testEnv) startUnityCatalog(t *testing.T, ctx context.Context, opts ucServerOpts) {
	t.Helper()

	s3EndpointForContainer := fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)

	props := strings.Join([]string{
		"server.env=test",
		"server.authorization=disable",
		"server.cookie-timeout=PT1H",
		"server.managed-table.enabled=false",
		fmt.Sprintf("aws.masterRoleArn=%s", opts.MasterRoleArn),
		fmt.Sprintf("aws.accessKey=%s", env.accessKey),
		fmt.Sprintf("aws.secretKey=%s", env.secretKey),
		"aws.region=us-east-1",
		fmt.Sprintf("aws.endpoint=%s", s3EndpointForContainer),
		// UC keys perBucketS3Configs by NormalizedURL.from(bucketPath) and
		// looks up using the storageBase, which is "s3://<bucket>" (scheme +
		// authority only). The playground's "s3://lakehouse/warehouse" never
		// matches because of that asymmetry; the bucket-only form works.
		fmt.Sprintf("s3.bucketPath.0=s3://%s", ucWarehouse),
		"s3.region.0=us-east-1",
		fmt.Sprintf("s3.awsRoleArn.0=%s", opts.MasterRoleArn),
		fmt.Sprintf("s3.accessKey.0=%s", env.accessKey),
		fmt.Sprintf("s3.secretKey.0=%s", env.secretKey),
		fmt.Sprintf("s3.endpoint.0=%s", s3EndpointForContainer),
		"",
	}, "\n")

	confDir := filepath.Join(env.dataDir, "uc-conf")
	if err := os.MkdirAll(confDir, 0755); err != nil {
		t.Fatalf("mkdir uc-conf: %v", err)
	}
	propsPath := filepath.Join(confDir, "server.properties")
	if err := os.WriteFile(propsPath, []byte(props), 0644); err != nil {
		t.Fatalf("write server.properties: %v", err)
	}

	containerName := fmt.Sprintf("seaweed-unity-catalog-%d", time.Now().UnixNano())

	args := []string{
		"run", "-d", "--rm",
		"--name", containerName,
		"--add-host", "host.docker.internal:host-gateway",
		"-p", fmt.Sprintf("%d:8080", env.ucHostPort),
		"-v", fmt.Sprintf("%s:%s:ro", propsPath, ucContainerCfg),
		"-e", "JAVA_OPTS=-Xmx1g",
	}
	for k, v := range opts.ExtraEnv {
		args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
	}
	args = append(args, env.ucImage)

	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run unity-catalog: %v\n%s", err, out)
	}
	env.ucContainerID = strings.TrimSpace(string(out))
	t.Logf("unity-catalog container id: %s", env.ucContainerID)

	probe := fmt.Sprintf("http://127.0.0.1:%d%s/catalogs", env.ucHostPort, ucAPIBase)
	deadline := time.Now().Add(ucStartupGrace)
	var lastErr error
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, probe, nil)
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ctx done while waiting for unity catalog: %v", ctx.Err())
		case <-time.After(time.Second):
		}
	}
	logs, _ := exec.Command("docker", "logs", "--tail", "200", env.ucContainerID).CombinedOutput()
	t.Fatalf("unity catalog did not become ready: %v\nrecent logs:\n%s", lastErr, logs)
}

func (env *testEnv) cleanup(t *testing.T) {
	if env.ucContainerID != "" {
		if logs, err := exec.Command("docker", "logs", "--tail", "200", env.ucContainerID).CombinedOutput(); err == nil {
			t.Logf("unity-catalog tail logs:\n%s", logs)
		}
		_ = exec.Command("docker", "rm", "-f", env.ucContainerID).Run()
	}
	if env.weedCancel != nil {
		env.weedCancel()
	}
	if env.weedProcess != nil {
		time.Sleep(500 * time.Millisecond)
		_ = env.weedProcess.Wait()
	}
	if env.dataDir != "" {
		_ = os.RemoveAll(env.dataDir)
	}
}

// -- S3 client helpers ---------------------------------------------------------

func (env *testEnv) newHostS3Client(t *testing.T, ctx context.Context) *s3.Client {
	t.Helper()
	return env.newHostS3ClientWithCreds(t, ctx, env.accessKey, env.secretKey, "")
}

func (env *testEnv) newHostS3ClientWithCreds(t *testing.T, ctx context.Context, ak, sk, token string) *s3.Client {
	t.Helper()
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", env.s3Port)
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, token)),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

func splitS3URI(t *testing.T, uri string) (bucket, key string) {
	t.Helper()
	const prefix = "s3://"
	if !strings.HasPrefix(uri, prefix) {
		t.Fatalf("not an s3 uri: %s", uri)
	}
	rest := strings.TrimPrefix(uri, prefix)
	idx := strings.Index(rest, "/")
	if idx < 0 {
		t.Fatalf("missing key in s3 uri: %s", uri)
	}
	return rest[:idx], rest[idx+1:]
}

// stsEnabledIAMConfig returns an iam.json that defines an admin user, the
// UnityCatalogVendedRole role with a permissive trust policy, and a FullAccess
// policy attached to it. This is the SeaweedFS-side counterpart of the
// `aws.masterRoleArn` configuration on the Unity Catalog server.
func stsEnabledIAMConfig(accessKey, secretKey string) string {
	return fmt.Sprintf(`{
  "identities": [
    {
      "name": "admin",
      "credentials": [{"accessKey": %q, "secretKey": %q}],
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
      "roleName": %q,
      "roleArn": %q,
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
          {"Effect": "Allow", "Action": "*", "Resource": "*"}
        ]
      }
    }
  ]
}`, accessKey, secretKey, ucVendedRoleName, ucVendedRoleArn)
}
