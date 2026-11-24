package s3

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
)

// fakeIMDS serves an EC2 instance role over IMDSv2: a token, the role's name,
// then its credentials, refused without the token.
func fakeIMDS(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/latest/api/token":
			w.Header().Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")
			w.Write([]byte("tok"))
		case r.Header.Get("X-Aws-Ec2-Metadata-Token") != "tok":
			w.WriteHeader(http.StatusUnauthorized)
		case r.URL.Path == "/latest/meta-data/iam/security-credentials/":
			w.Write([]byte("cell-role"))
		case r.URL.Path == "/latest/meta-data/iam/security-credentials/cell-role":
			w.Write([]byte(`{"Code":"Success","Type":"AWS-HMAC","AccessKeyId":"ROLEKEY","SecretAccessKey":"rolesecret","Token":"roletoken","Expiration":"` +
				time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func accessKeyOf(t *testing.T, conf *remote_pb.RemoteConf) string {
	t.Helper()
	c, err := MakeWithHTTPClient(conf, &http.Client{})
	if err != nil {
		t.Fatalf("MakeWithHTTPClient: %v", err)
	}
	v, err := c.(*s3RemoteStorageClient).conn.(*awss3.S3).Config.Credentials.Get()
	if err != nil {
		t.Fatalf("credentials: %v", err)
	}
	return v.AccessKeyID
}

type stsHostRecorder struct {
	next  http.RoundTripper
	hosts []string
}

func (r *stsHostRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasPrefix(req.URL.Host, "sts.") {
		r.hosts = append(r.hosts, req.URL.Host)
		return nil, errors.New("sts not reachable in tests")
	}
	return r.next.RoundTrip(req)
}

// IRSA in a pod of one region and a bucket in another: the role exchange goes
// to the pod's regional STS endpoint, and S3 is still signed for the bucket's.
func TestWorkloadWebIdentityAsksTheWorkloadRegion(t *testing.T) {
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("header.payload.signature"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(workloadCredentialsEnv, "workload")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", token)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/pod")
	t.Setenv("AWS_STS_REGIONAL_ENDPOINTS", "") // the SDK's own default is the global endpoint
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_DEFAULT_REGION", "")
	rec := &stsHostRecorder{next: http.DefaultTransport}
	http.DefaultTransport = rec
	defer func() { http.DefaultTransport = rec.next }()

	c, err := MakeWithHTTPClient(&remote_pb.RemoteConf{Type: "s3", Name: "r", S3Region: "us-west-2"}, &http.Client{})
	if err != nil {
		t.Fatalf("MakeWithHTTPClient: %v", err)
	}
	conn := c.(*s3RemoteStorageClient).conn.(*awss3.S3)
	if _, err := conn.Config.Credentials.Get(); err == nil {
		t.Fatalf("credentials: want the recorded STS failure")
	}
	if len(rec.hosts) == 0 || rec.hosts[0] != "sts.us-east-1.amazonaws.com" {
		t.Errorf("STS hosts %v, want sts.us-east-1.amazonaws.com", rec.hosts)
	}
	if got := aws.StringValue(conn.Config.Region); got != "us-west-2" {
		t.Errorf("S3 region %q, want the bucket's us-west-2", got)
	}
}

func makeErr(conf *remote_pb.RemoteConf) error {
	_, err := MakeWithHTTPClient(conf, &http.Client{})
	return err
}

// A server holding its own S3 identity in AWS_ACCESS_KEY_ID (weed mini) and a
// remote with no keys: by default the SDK chain takes the environment's keys;
// with WEED_REMOTE_S3_CREDENTIALS=workload it takes the instance role and never
// those keys. Keys in the configuration win either way; half a key pair and an
// unknown value are refused in workload mode; other S3-compatible types and the
// unsigned public-bucket path are untouched when the setting is off.
func TestWorkloadCredentials(t *testing.T) {
	// nothing from the runner's own AWS environment reaches the chain
	for _, k := range []string{"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_PROFILE",
		"AWS_DEFAULT_PROFILE", "AWS_SESSION_TOKEN", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_EC2_METADATA_DISABLED", "AWS_CONFIG_FILE"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	srv := fakeIMDS(t)
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", srv.URL)
	t.Setenv("AWS_ACCESS_KEY_ID", "LOCALSERVERKEY")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "localserversecret")
	ep, region := "https://s3.eu-central-1.amazonaws.com", "eu-central-1"
	noKeys := &remote_pb.RemoteConf{Type: "s3", Name: "r", S3Endpoint: ep, S3Region: region}
	withKeys := &remote_pb.RemoteConf{Type: "s3", Name: "r", S3Endpoint: ep, S3Region: region, S3AccessKey: "CONFKEY", S3SecretKey: "confsecret"}
	halfKeys := &remote_pb.RemoteConf{Type: "s3", Name: "r", S3Endpoint: ep, S3Region: region, S3AccessKey: "CONFKEY"}
	wasabi := &remote_pb.RemoteConf{Type: "wasabi", Name: "w", WasabiEndpoint: "https://s3.wasabisys.com", WasabiRegion: "us-east-1",
		WasabiAccessKey: "WASABIKEY", WasabiSecretKey: "wasabisecret"}

	t.Setenv(workloadCredentialsEnv, "")
	if got := accessKeyOf(t, noKeys); got != "LOCALSERVERKEY" {
		t.Errorf("default chain: %q, want the environment's key (the behaviour the setting exists to avoid)", got)
	}
	t.Setenv(workloadCredentialsEnv, "workload")
	if got := accessKeyOf(t, noKeys); got != "ROLEKEY" {
		t.Errorf("workload: %q, want the instance role's key", got)
	}
	if got := accessKeyOf(t, withKeys); got != "CONFKEY" {
		t.Errorf("workload with keys configured: %q, want the configuration's", got)
	}
	if got := accessKeyOf(t, wasabi); got != "WASABIKEY" {
		t.Errorf("workload, another S3-compatible type: %q, want its own keys (not affected)", got)
	}
	if err := makeErr(halfKeys); err == nil {
		t.Errorf("workload with half a key pair: built, want a refusal (the default chain would sign with the environment's keys)")
	}
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/token") // a token file with no role: skipped, not fatal
	if got := accessKeyOf(t, noKeys); got != "ROLEKEY" {
		t.Errorf("workload, token file without a role: %q, want the instance role's key", got)
	}
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/pod") // web identity named but failing: no fall back to the instance role
	c, err := MakeWithHTTPClient(noKeys, &http.Client{})
	if err != nil {
		t.Fatalf("MakeWithHTTPClient: %v", err)
	}
	if v, err := c.(*s3RemoteStorageClient).conn.(*awss3.S3).Config.Credentials.Get(); err == nil {
		t.Errorf("workload, web identity failing: got %q, want an error (never the instance role)", v.AccessKeyID)
	}
	t.Setenv("AWS_ROLE_ARN", "")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")
	// a selected profile in the shared credentials file is not read in workload mode
	shared := filepath.Join(t.TempDir(), "credentials")
	if err := os.WriteFile(shared, []byte("[irsa]\nrole_arn = arn:aws:iam::123456789012:role/pod\nweb_identity_token_file = /var/run/secrets/token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", shared)
	t.Setenv("AWS_PROFILE", "irsa")
	if got := accessKeyOf(t, noKeys); got != "ROLEKEY" {
		t.Errorf("workload, a profile selected: %q, want the instance role's key", got)
	}
	// nor a broken one, which the SDK would refuse even with credentials given
	if err := os.WriteFile(shared, []byte("[irsa]\nweb_identity_token_file = /var/run/secrets/token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := accessKeyOf(t, noKeys); got != "ROLEKEY" {
		t.Errorf("workload, a broken profile selected: %q, want the instance role's key", got)
	}
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	t.Setenv(workloadCredentialsEnv, "Workload")
	if err := makeErr(noKeys); err == nil {
		t.Errorf("unknown value: built, want a refusal (never a silent fall back to the environment's keys)")
	}

	// the setting off and no credential source named: a keyless remote is unsigned, as before
	t.Setenv(workloadCredentialsEnv, "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "")
	c, err = MakeWithHTTPClient(noKeys, &http.Client{})
	if err != nil {
		t.Fatalf("public bucket: %v", err)
	}
	if c.(*s3RemoteStorageClient).conn.(*awss3.S3).Config.Credentials != credentials.AnonymousCredentials {
		t.Errorf("public bucket, setting off: credentials are not anonymous")
	}
}
