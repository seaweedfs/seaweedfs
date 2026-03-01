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
	polarisRealm        = "POLARIS"
	polarisClientID     = "root"
	polarisClientSecret = "s3cr3t"
	polarisRegion       = "us-east-1"
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

	polarisPort       int
	polarisHealthPort int
	polarisContainer  string
}

type listTablesResponse struct {
	Identifiers []struct {
		Namespace []string `json:"namespace"`
		Name      string   `json:"name"`
	} `json:"identifiers"`
}

func TestPolarisIntegration(t *testing.T) {
	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	fmt.Printf(">>> Starting SeaweedFS with Polaris configuration...\n")
	env.StartSeaweedFS(t)
	fmt.Printf(">>> SeaweedFS started.\n")

	fmt.Printf(">>> Starting Polaris...\n")
	env.StartPolaris(t)
	fmt.Printf(">>> Polaris started.\n")

	runPolarisRepro(t, env)
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

	dataDir, err := os.MkdirTemp("", "seaweed-polaris-test-*")
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
      "roleName": "PolarisVendedRole",
      "roleArn": "arn:aws:iam::000000000000:role/PolarisVendedRole",
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

	polarisPort, err := testutil.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for Polaris: %v", err)
	}
	env.polarisPort = polarisPort

	healthPort, err := testutil.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for Polaris health: %v", err)
	}
	env.polarisHealthPort = healthPort

	containerName := fmt.Sprintf("seaweedfs-polaris-%d", time.Now().UnixNano())
	env.polarisContainer = containerName

	cmd := exec.Command("docker", "run", "-d", "--rm",
		"--name", containerName,
		"--add-host", "host.docker.internal:host-gateway",
		"-p", fmt.Sprintf("%d:8181", env.polarisPort),
		"-p", fmt.Sprintf("%d:8182", env.polarisHealthPort),
		"-e", "AWS_REGION="+polarisRegion,
		"-e", "AWS_ACCESS_KEY_ID="+env.accessKey,
		"-e", "AWS_SECRET_ACCESS_KEY="+env.secretKey,
		"-e", fmt.Sprintf("POLARIS_BOOTSTRAP_CREDENTIALS=%s,%s,%s", polarisRealm, polarisClientID, polarisClientSecret),
		"-e", "polaris.realm-context.realms="+polarisRealm,
		"-e", "quarkus.otel.sdk.disabled=true",
		"apache/polaris:latest",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to start Polaris container: %v\nOutput: %s", err, string(output))
	}

	healthURL := fmt.Sprintf("http://127.0.0.1:%d/q/health", env.polarisHealthPort)
	if !testutil.WaitForService(healthURL, 2*time.Minute) {
		t.Fatalf("Polaris failed to become ready")
	}
}

func (env *TestEnvironment) Cleanup(t *testing.T) {
	t.Helper()

	if env.polarisContainer != "" {
		_ = exec.Command("docker", "rm", "-f", env.polarisContainer).Run()
	}

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

func (env *TestEnvironment) s3Endpoint() string {
	return fmt.Sprintf("http://127.0.0.1:%d", env.s3Port)
}

func (env *TestEnvironment) s3InternalEndpoint() string {
	return fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)
}

func (env *TestEnvironment) polarisURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", env.polarisPort)
}

func (env *TestEnvironment) polarisCatalogURL() string {
	return env.polarisURL() + "/api/catalog"
}

func (env *TestEnvironment) polarisManagementURL() string {
	return env.polarisURL() + "/api/management/v1"
}

func runPolarisRepro(t *testing.T, env *TestEnvironment) {
	t.Helper()
	ctx := context.Background()
	s3Client := newS3Client(t, env)

	bucketName := "polaris-warehouse-" + randomSuffix()
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)}); err != nil {
		t.Fatalf("Failed to create bucket %s: %v", bucketName, err)
	}

	token := obtainPolarisToken(t, env)
	catalogName := "polaris_catalog_" + randomSuffix()
	warehousePrefix := "warehouse"
	baseLocation := fmt.Sprintf("s3://%s/%s", bucketName, warehousePrefix)

	createPayload := map[string]any{
		"catalog": map[string]any{
			"name":     catalogName,
			"type":     "INTERNAL",
			"readOnly": false,
			"properties": map[string]any{
				"default-base-location": baseLocation,
			},
			"storageConfigInfo": map[string]any{
				"storageType":      "S3",
				"allowedLocations": []string{baseLocation},
				"endpoint":         env.s3Endpoint(),
				"endpointInternal": env.s3InternalEndpoint(),
				"pathStyleAccess":  true,
				"region":           polarisRegion,
			},
		},
	}

	status, body := doPolarisJSON(t, http.MethodPost, env.polarisManagementURL()+"/catalogs", token, createPayload)
	if status != http.StatusCreated && status != http.StatusOK {
		t.Fatalf("Create catalog failed: status %d body: %s", status, string(body))
	}
	defer func() {
		status, body := doPolarisJSON(t, http.MethodDelete, env.polarisManagementURL()+"/catalogs/"+catalogName, token, nil)
		if status != http.StatusOK && status != http.StatusNoContent {
			t.Logf("Cleanup: failed to delete catalog %s: status %d body: %s", catalogName, status, string(body))
		}
	}()

	grantPayload := map[string]any{
		"type":      "catalog",
		"privilege": "CATALOG_MANAGE_CONTENT",
	}
	grantURL := fmt.Sprintf("%s/catalogs/%s/catalog-roles/catalog_admin/grants", env.polarisManagementURL(), catalogName)
	status, body = doPolarisJSON(t, http.MethodPut, grantURL, token, grantPayload)
	if status != http.StatusOK && status != http.StatusNoContent {
		t.Fatalf("Grant catalog privilege failed: status %d body: %s", status, string(body))
	}

	status, body = doPolarisJSON(t, http.MethodGet, env.polarisCatalogURL()+"/v1/config", token, nil)
	if status != http.StatusOK {
		t.Fatalf("Config request failed: status %d body: %s", status, string(body))
	}

	namespace := "polaris_ns_" + randomSuffix()
	tableName := "polaris_table"
	createNamespaceURL := fmt.Sprintf("%s/v1/%s/namespaces", env.polarisCatalogURL(), catalogName)
	status, body = doPolarisJSON(t, http.MethodPost, createNamespaceURL, token, map[string]any{
		"namespace": []string{namespace},
	})
	if status != http.StatusOK && status != http.StatusConflict {
		t.Fatalf("Create namespace failed: status %d body: %s", status, string(body))
	}
	defer func() {
		deleteNamespaceURL := fmt.Sprintf("%s/v1/%s/namespaces/%s", env.polarisCatalogURL(), catalogName, namespace)
		status, body := doPolarisJSON(t, http.MethodDelete, deleteNamespaceURL, token, nil)
		if status != http.StatusOK && status != http.StatusNoContent {
			t.Logf("Cleanup: failed to delete namespace %s: status %d body: %s", namespace, status, string(body))
		}
	}()

	tableLocation := fmt.Sprintf("s3://%s/%s/%s/%s", bucketName, warehousePrefix, namespace, tableName)
	createTableURL := fmt.Sprintf("%s/v1/%s/namespaces/%s/tables", env.polarisCatalogURL(), catalogName, namespace)
	status, body = doPolarisJSON(t, http.MethodPost, createTableURL, token, map[string]any{
		"name":     tableName,
		"location": tableLocation,
		"schema": map[string]any{
			"type": "struct",
			"fields": []map[string]any{
				{"id": 1, "name": "id", "required": false, "type": "int"},
				{"id": 2, "name": "name", "required": false, "type": "string"},
			},
		},
	})
	if status != http.StatusOK {
		t.Fatalf("Create table failed: status %d body: %s", status, string(body))
	}
	defer func() {
		deleteTableURL := fmt.Sprintf("%s/v1/%s/namespaces/%s/tables/%s", env.polarisCatalogURL(), catalogName, namespace, tableName)
		status, body := doPolarisJSON(t, http.MethodDelete, deleteTableURL, token, nil)
		if status != http.StatusOK && status != http.StatusNoContent {
			t.Logf("Cleanup: failed to delete table %s.%s: status %d body: %s", namespace, tableName, status, string(body))
		}
	}()

	getTableURL := fmt.Sprintf("%s/v1/%s/namespaces/%s/tables/%s", env.polarisCatalogURL(), catalogName, namespace, tableName)
	status, body = doPolarisJSON(t, http.MethodGet, getTableURL, token, nil)
	if status != http.StatusOK {
		t.Fatalf("Get table failed: status %d body: %s", status, string(body))
	}

	listTablesURL := fmt.Sprintf("%s/v1/%s/namespaces/%s/tables", env.polarisCatalogURL(), catalogName, namespace)
	status, body = doPolarisJSON(t, http.MethodGet, listTablesURL, token, nil)
	if status != http.StatusOK {
		t.Fatalf("List tables failed: status %d body: %s", status, string(body))
	}
	var listResp listTablesResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		t.Fatalf("Failed to decode list tables response: %v", err)
	}
	found := false
	for _, identifier := range listResp.Identifiers {
		if identifier.Name == tableName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Table %s not found in list", tableName)
	}
}

func newS3Client(t *testing.T, env *TestEnvironment) *s3.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(polarisRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(env.accessKey, env.secretKey, "")),
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

func obtainPolarisToken(t *testing.T, env *TestEnvironment) string {
	t.Helper()

	url := env.polarisCatalogURL() + "/v1/oauth/tokens"
	body := strings.NewReader("grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL")
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		t.Fatalf("Failed to create token request: %v", err)
	}
	req.SetBasicAuth(polarisClientID, polarisClientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Polaris-Realm", polarisRealm)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Token request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read token response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Token request failed: status %d body: %s", resp.StatusCode, string(respBody))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(respBody, &tokenResp); err != nil {
		t.Fatalf("Failed to decode token response: %v", err)
	}
	if tokenResp.AccessToken == "" {
		t.Fatalf("Token response missing access_token")
	}
	return tokenResp.AccessToken
}

func doPolarisJSON(t *testing.T, method, url, token string, payload any) (int, []byte) {
	t.Helper()

	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("Failed to marshal payload: %v", err)
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Polaris-Realm", polarisRealm)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	return resp.StatusCode, respBody
}

func randomSuffix() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
