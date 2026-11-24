package s3

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")
	t.Setenv(workloadCredentialsEnv, "Workload")
	if err := makeErr(noKeys); err == nil {
		t.Errorf("unknown value: built, want a refusal (never a silent fall back to the environment's keys)")
	}

	// the setting off and no credential source named: a keyless remote is unsigned, as before
	t.Setenv(workloadCredentialsEnv, "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "")
	c, err := MakeWithHTTPClient(noKeys, &http.Client{})
	if err != nil {
		t.Fatalf("public bucket: %v", err)
	}
	if c.(*s3RemoteStorageClient).conn.(*awss3.S3).Config.Credentials != credentials.AnonymousCredentials {
		t.Errorf("public bucket, setting off: credentials are not anonymous")
	}
}
