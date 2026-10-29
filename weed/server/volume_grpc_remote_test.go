package weed_server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	s3remote "github.com/seaweedfs/seaweedfs/weed/remote_storage/s3"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// stubLookup returns a resolver func that maps the supplied hostnames to
// the supplied IP addresses, and errors for any host that is not in the map.
func stubLookup(t *testing.T, mapping map[string][]net.IP) func(ctx context.Context, host string) ([]net.IPAddr, error) {
	t.Helper()
	return func(_ context.Context, host string) ([]net.IPAddr, error) {
		ips, ok := mapping[host]
		if !ok {
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		}
		out := make([]net.IPAddr, 0, len(ips))
		for _, ip := range ips {
			out = append(out, net.IPAddr{IP: ip})
		}
		return out, nil
	}
}

func TestValidateRemoteEndpoint(t *testing.T) {
	originalLookup := lookupIPAddrFunc
	t.Cleanup(func() { lookupIPAddrFunc = originalLookup })

	lookupIPAddrFunc = stubLookup(t, map[string][]net.IP{
		"s3.us-east-1.amazonaws.com": {net.ParseIP("52.216.10.10")},
		"internal.example.com":       {net.ParseIP("127.0.0.1")},
		"linklocal.example.com":      {net.ParseIP("169.254.10.20")},
		"private.example.com":        {net.ParseIP("10.1.2.3")},
		"private172.example.com":     {net.ParseIP("172.20.0.5")},
		"private192.example.com":     {net.ParseIP("192.168.1.1")},
		"cgnat.example.com":          {net.ParseIP("100.64.0.42")},
	})

	cases := []struct {
		name     string
		endpoint string
		wantErr  bool
		wantSub  string
	}{
		{
			name:     "empty",
			endpoint: "",
			wantErr:  true,
			wantSub:  "empty",
		},
		{
			name:     "loopback literal",
			endpoint: "http://127.0.0.1:8080",
			wantErr:  true,
			wantSub:  "loopback",
		},
		{
			name:     "ipv6 loopback",
			endpoint: "http://[::1]:8080",
			wantErr:  true,
			wantSub:  "loopback",
		},
		{
			name:     "imds ipv4",
			endpoint: "http://169.254.169.254/",
			wantErr:  true,
			wantSub:  "metadata",
		},
		{
			name:     "unspecified ipv4",
			endpoint: "http://0.0.0.0/",
			wantErr:  true,
			wantSub:  "unspecified",
		},
		{
			name:     "link-local ipv6",
			endpoint: "http://[fe80::1]/",
			wantErr:  true,
			wantSub:  "link-local",
		},
		{
			name:     "ftp scheme",
			endpoint: "ftp://example.com/",
			wantErr:  true,
			wantSub:  "http or https",
		},
		{
			name:     "missing scheme",
			endpoint: "example.com/",
			wantErr:  true,
			wantSub:  "http or https",
		},
		{
			name:     "imds hostname",
			endpoint: "http://metadata.google.internal/",
			wantErr:  true,
			wantSub:  "metadata service",
		},
		{
			name:     "imds short hostname",
			endpoint: "http://metadata/",
			wantErr:  true,
			wantSub:  "metadata service",
		},
		{
			name:     "host resolves to loopback",
			endpoint: "https://internal.example.com/",
			wantErr:  true,
			wantSub:  "loopback",
		},
		{
			name:     "host resolves to link-local",
			endpoint: "https://linklocal.example.com/",
			wantErr:  true,
			wantSub:  "link-local",
		},
		{
			name:     "rfc1918 10/8 literal",
			endpoint: "http://10.0.0.1/",
			wantErr:  true,
			wantSub:  "private",
		},
		{
			name:     "rfc1918 172.16/12 literal",
			endpoint: "http://172.16.5.5/",
			wantErr:  true,
			wantSub:  "private",
		},
		{
			name:     "rfc1918 192.168/16 literal",
			endpoint: "http://192.168.0.1/",
			wantErr:  true,
			wantSub:  "private",
		},
		{
			name:     "cgnat literal",
			endpoint: "http://100.64.0.1/",
			wantErr:  true,
			wantSub:  "CGNAT",
		},
		{
			name:     "host resolves to rfc1918 10/8",
			endpoint: "https://private.example.com/",
			wantErr:  true,
			wantSub:  "private",
		},
		{
			name:     "host resolves to rfc1918 172/12",
			endpoint: "https://private172.example.com/",
			wantErr:  true,
			wantSub:  "private",
		},
		{
			name:     "host resolves to rfc1918 192.168/16",
			endpoint: "https://private192.example.com/",
			wantErr:  true,
			wantSub:  "private",
		},
		{
			name:     "host resolves to cgnat",
			endpoint: "https://cgnat.example.com/",
			wantErr:  true,
			wantSub:  "CGNAT",
		},
		{
			name:     "nat64 imds",
			endpoint: "http://[64:ff9b::a9fe:a9fe]/",
			wantErr:  true,
			wantSub:  "metadata",
		},
		{
			name:     "nat64 loopback",
			endpoint: "http://[64:ff9b::7f00:1]/",
			wantErr:  true,
			wantSub:  "loopback",
		},
		{
			name:     "6to4 private",
			endpoint: "http://[2002:a00:1::]/",
			wantErr:  true,
			wantSub:  "private",
		},
		{
			name:     "teredo loopback",
			endpoint: "http://[2001:0:4136:e378:8000:63bf:80ff:fffe]/",
			wantErr:  true,
			wantSub:  "loopback",
		},
		{
			name:     "ipv4-compatible loopback",
			endpoint: "http://[::7f00:1]/",
			wantErr:  true,
			wantSub:  "loopback",
		},
		{
			name:     "nat64 public passes",
			endpoint: "http://[64:ff9b::808:808]/",
			wantErr:  false,
		},
		{
			name:     "6to4 public passes",
			endpoint: "http://[2002:808:808::]/",
			wantErr:  false,
		},
		{
			name:     "teredo public passes",
			endpoint: "http://[2001::f7f7:f7f7]/",
			wantErr:  false,
		},
		{
			name:     "ipv4-compatible public passes",
			endpoint: "http://[::808:808]/",
			wantErr:  false,
		},
		{
			name:     "nat64 non-wellknown-prefix not decoded",
			endpoint: "http://[64:ff9b:1::a9fe:a9fe]/",
			wantErr:  false,
		},
		{
			name:     "public s3",
			endpoint: "https://s3.us-east-1.amazonaws.com/",
			wantErr:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRemoteEndpoint(context.Background(), tc.endpoint)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got nil", tc.endpoint)
				}
				if tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub) {
					t.Fatalf("expected error to contain %q, got %v", tc.wantSub, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.endpoint, err)
			}
		})
	}
}

func TestValidateRemoteEndpointResolverFailure(t *testing.T) {
	originalLookup := lookupIPAddrFunc
	t.Cleanup(func() { lookupIPAddrFunc = originalLookup })

	resolveErr := errors.New("simulated DNS failure")
	lookupIPAddrFunc = func(_ context.Context, _ string) ([]net.IPAddr, error) {
		return nil, resolveErr
	}

	err := validateRemoteEndpoint(context.Background(), "https://does-not-resolve.example.com/")
	if err == nil {
		t.Fatal("expected error when resolver fails")
	}
	if !strings.Contains(err.Error(), "resolve remote endpoint host") {
		t.Fatalf("expected resolver error wrapping, got %v", err)
	}
}

// TestGuardedDialerRebind simulates a DNS rebinding attack: the host first
// resolves to a public address (passing validateRemoteEndpoint) and then
// flips to 127.0.0.1 on the very next lookup (what the AWS SDK would do at
// dial time). The dial path must refuse the loopback answer instead of
// connecting to it.
func TestGuardedDialerRebind(t *testing.T) {
	originalLookup := lookupIPAddrFunc
	t.Cleanup(func() { lookupIPAddrFunc = originalLookup })

	const host = "rebind.example.com"
	endpoint := "https://" + host + "/"

	var calls atomic.Int32
	lookupIPAddrFunc = func(_ context.Context, name string) ([]net.IPAddr, error) {
		if name != host {
			return nil, &net.DNSError{Err: "no such host", Name: name, IsNotFound: true}
		}
		if calls.Add(1) == 1 {
			return []net.IPAddr{{IP: net.ParseIP("52.216.10.10")}}, nil
		}
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
	}

	if err := validateRemoteEndpoint(context.Background(), endpoint); err != nil {
		t.Fatalf("first-pass validation should accept public IP, got %v", err)
	}

	dial := guardedDialer(endpoint)
	conn, err := dial(context.Background(), "tcp", host+":443")
	if conn != nil {
		conn.Close()
		t.Fatalf("guarded dialer must refuse loopback rebind, got conn")
	}
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("guarded dialer should fail with loopback error, got %v", err)
	}
}

// TestRemoteEndpointGuardCoversS3CompatibleSiblings confirms the SSRF guard
// reaches every S3-SDK-backed provider, not just type "s3". It replays the two
// steps FetchAndWriteNeedle performs before building the client: resolve the
// endpoint the type would dial, then validate it against the deny-list. A
// sibling type pointed at an internal address must be rejected.
func TestRemoteEndpointGuardCoversS3CompatibleSiblings(t *testing.T) {
	cases := []struct {
		conf    *remote_pb.RemoteConf
		wantSub string
	}{
		{&remote_pb.RemoteConf{Type: "wasabi", WasabiEndpoint: "http://169.254.169.254/"}, "metadata"},
		{&remote_pb.RemoteConf{Type: "b2", BackblazeEndpoint: "http://127.0.0.1/"}, "loopback"},
		{&remote_pb.RemoteConf{Type: "aliyun", AliyunEndpoint: "http://192.168.0.1/"}, "private"},
		{&remote_pb.RemoteConf{Type: "tencent", TencentEndpoint: "http://100.64.0.1/"}, "CGNAT"},
		{&remote_pb.RemoteConf{Type: "baidu", BaiduEndpoint: "http://169.254.169.254/"}, "metadata"},
		{&remote_pb.RemoteConf{Type: "filebase", FilebaseEndpoint: "http://172.16.0.1/"}, "private"},
		{&remote_pb.RemoteConf{Type: "storj", StorjEndpoint: "http://10.0.0.5/"}, "private"},
		{&remote_pb.RemoteConf{Type: "contabo", ContaboEndpoint: "http://[::1]/"}, "loopback"},
	}
	for _, tc := range cases {
		endpoint, ok := s3remote.S3CompatibleEndpoint(tc.conf)
		if !ok {
			t.Errorf("type %q: not recognized as S3-compatible, guard would be skipped", tc.conf.Type)
			continue
		}
		err := validateRemoteEndpoint(context.Background(), endpoint)
		if err == nil {
			t.Errorf("type %q: expected endpoint %q to be rejected", tc.conf.Type, endpoint)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantSub) {
			t.Errorf("type %q: error %q missing %q", tc.conf.Type, err, tc.wantSub)
		}
	}
}

// TestRemoteEndpointGuardCoversAzure confirms the SSRF guard reaches the azure
// backend, which dials a caller-supplied AzureEndpoint. It replays the two
// steps FetchAndWriteNeedle performs before building the client: resolve the
// endpoint the type would dial via guardedRemoteClient, then validate it. An
// azure conf pointed at an internal address must be rejected.
func TestRemoteEndpointGuardCoversAzure(t *testing.T) {
	cases := []struct {
		name    string
		conf    *remote_pb.RemoteConf
		wantSub string
	}{
		{"imds", &remote_pb.RemoteConf{Type: "azure", AzureEndpoint: "https://169.254.169.254/"}, "metadata"},
		{"loopback", &remote_pb.RemoteConf{Type: "azure", AzureEndpoint: "https://127.0.0.1/"}, "loopback"},
		{"private", &remote_pb.RemoteConf{Type: "azure", AzureEndpoint: "https://10.0.0.5/"}, "private"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			endpoint, _, ok := guardedRemoteClient(tc.conf)
			if !ok {
				t.Fatalf("azure endpoint %q not guarded, the SSRF check would be skipped", tc.conf.AzureEndpoint)
			}
			err := validateRemoteEndpoint(context.Background(), endpoint)
			if err == nil {
				t.Fatalf("expected endpoint %q to be rejected", endpoint)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q missing %q", err, tc.wantSub)
			}
		})
	}
}

// TestValidateReplicaTarget covers the replica upload leg of
// FetchAndWriteNeedle. Replica targets are peer volume servers, so unlike the
// remote endpoint they may sit on a private network; the guard still rejects
// loopback / link-local / unspecified hosts and any target that is not a bare
// host:port, since a scheme, path or query would move the upload to a different
// URL through the format string.
func TestValidateReplicaTarget(t *testing.T) {
	originalLookup := lookupIPAddrFunc
	t.Cleanup(func() { lookupIPAddrFunc = originalLookup })

	lookupIPAddrFunc = stubLookup(t, map[string][]net.IP{
		"peer.example.com":      {net.ParseIP("10.0.0.7")},
		"loop.example.com":      {net.ParseIP("127.0.0.1")},
		"linklocal.example.com": {net.ParseIP("169.254.169.254")},
	})

	cases := []struct {
		name    string
		target  string
		wantErr bool
		wantSub string
	}{
		// A path plus a trailing "?a=" would otherwise swallow ?type=replicate.
		{"embedded path and query", "127.0.0.1:7000/status/x/?a=", true, "bare host:port"},
		{"loopback literal", "127.0.0.1:8080", true, "loopback"},
		{"ipv6 loopback", "[::1]:8080", true, "loopback"},
		{"metadata literal", "169.254.169.254:80", true, "metadata"},
		{"unspecified", "0.0.0.0:8080", true, "unspecified"},
		{"metadata hostname", "metadata:80", true, "metadata"},
		{"scheme rejected", "http://10.0.0.7:8080", true, "bare host:port"},
		{"path rejected", "10.0.0.7:8080/x", true, "bare host:port"},
		{"query rejected", "10.0.0.7:8080?a=b", true, "bare host:port"},
		{"userinfo rejected", "user@10.0.0.7:8080", true, "bare host:port"},
		{"missing port literal", "10.0.0.7", true, "bare host:port"},
		{"missing port hostname", "peer.example.com", true, "bare host:port"},
		{"empty", "", true, "empty"},
		{"resolves to loopback", "loop.example.com:8080", true, "loopback"},
		{"resolves to link-local", "linklocal.example.com:8080", true, "metadata"},
		// Legitimate peer volume servers on private networks must pass.
		{"private peer literal", "10.0.0.7:8080", false, ""},
		{"private 192 peer", "192.168.1.5:8080", false, ""},
		{"private peer hostname", "peer.example.com:8080", false, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateReplicaTarget(context.Background(), tc.target)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got nil", tc.target)
				}
				if tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub) {
					t.Fatalf("expected error to contain %q, got %v", tc.wantSub, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.target, err)
			}
		})
	}
}

// TestGuardedRemoteClientSkipsFixedHostBackends confirms backends that only
// reach a fixed provider host bypass the endpoint guard: azure with no explicit
// endpoint (public cloud, host derived from the account) and unrelated types.
func TestGuardedRemoteClientSkipsFixedHostBackends(t *testing.T) {
	for _, conf := range []*remote_pb.RemoteConf{
		{Type: "azure", AzureAccountName: "acct"},
		{Type: "gcs"},
		nil,
	} {
		if _, _, ok := guardedRemoteClient(conf); ok {
			t.Errorf("conf %+v should not be guarded", conf)
		}
	}
}

// TestGuardedRemoteClientAzureBuildsGuardedClient exercises the whole azure
// path: a public endpoint passes validation and the constructor builds a client
// through the guarded HTTP transport.
func TestGuardedRemoteClientAzureBuildsGuardedClient(t *testing.T) {
	conf := &remote_pb.RemoteConf{
		Type:             "azure",
		AzureAccountName: "testaccount",
		AzureAccountKey:  "aW52YWxpZGtleQ==",
		AzureEndpoint:    "https://testaccount.blob.core.usgovcloudapi.net/",
	}
	endpoint, makeClient, ok := guardedRemoteClient(conf)
	if !ok {
		t.Fatal("azure with an endpoint should be guarded")
	}
	client, err := makeClient(newGuardedHTTPClient(endpoint))
	if err != nil {
		t.Fatalf("build guarded azure client: %v", err)
	}
	if client == nil {
		t.Fatal("expected a client")
	}
}

// TestGuardedRemoteClientGuardsGcsTokenURL confirms the token endpoint named by
// inline gcs credentials is the endpoint the guard validates, so a loopback
// token_uri is refused while the Google default passes.
func TestGuardedRemoteClientGuardsGcsTokenURL(t *testing.T) {
	originalLookup := lookupIPAddrFunc
	t.Cleanup(func() { lookupIPAddrFunc = originalLookup })
	lookupIPAddrFunc = stubLookup(t, map[string][]net.IP{
		"oauth2.googleapis.com": {net.ParseIP("142.250.72.10")},
	})

	endpoint, makeClient, ok := guardedRemoteClient(&remote_pb.RemoteConf{
		Type:                            "gcs",
		GcsGoogleApplicationCredentials: `{"type":"service_account","token_uri":"http://127.0.0.1:9/token"}`,
	})
	if !ok {
		t.Fatal("gcs conf with inline credentials should be guarded")
	}
	if endpoint != "http://127.0.0.1:9/token" {
		t.Errorf("endpoint = %q, want the credential token_uri", endpoint)
	}
	if err := validateRemoteEndpoint(context.Background(), endpoint); err == nil {
		t.Error("expected the loopback token endpoint to be rejected")
	}
	if makeClient == nil {
		t.Error("expected a constructor")
	}

	endpoint, _, ok = guardedRemoteClient(&remote_pb.RemoteConf{
		Type:                            "gcs",
		GcsGoogleApplicationCredentials: `{"type":"service_account"}`,
	})
	if !ok {
		t.Fatal("gcs conf with inline credentials should be guarded")
	}
	if err := validateRemoteEndpoint(context.Background(), endpoint); err != nil {
		t.Errorf("default token endpoint %q should pass: %v", endpoint, err)
	}
}

// TestCheckGcsCredentials confirms only inline credentials that carry their own
// key material are accepted. The federated types name a url, file or executable
// that the SDK reads the token from, none of which the endpoint guard sees.
func TestCheckGcsCredentials(t *testing.T) {
	rejected := []string{
		"/etc/hostname",
		"~/creds.json",
		`{`,
		`{}`,
		`{"type":"external_account","token_url":"http://127.0.0.1:9/v1/token","credential_source":{"url":"http://169.254.169.254/latest/meta-data/"}}`,
		`{"type":"external_account","token_url":"http://127.0.0.1:9/v1/token","credential_source":{"file":"/etc/shadow"}}`,
		`{"type":"external_account","credential_source":{"executable":{"command":"/bin/sh"}}}`,
		`{"type":"external_account_authorized_user","token_url":"http://127.0.0.1:9/v1/token"}`,
		`{"type":"impersonated_service_account","service_account_impersonation_url":"http://127.0.0.1:9/x"}`,
	}
	for _, creds := range rejected {
		if err := checkGcsCredentials(creds); err == nil {
			t.Errorf("expected %q to be rejected", creds)
		}
	}
	accepted := []string{
		"",
		`{"type":"service_account","client_email":"a@b.com","private_key":"k"}`,
		`{"type":"service_account","token_uri":"https://oauth2.googleapis.com/token"}`,
		`{"type":"authorized_user","refresh_token":"r"}`,
	}
	for _, creds := range accepted {
		if err := checkGcsCredentials(creds); err != nil {
			t.Errorf("expected %q to be accepted, got %v", creds, err)
		}
	}
}

// TestGuardedDialerLiteralBlocked confirms that a literal blocked IP target
// is refused without any DNS lookup.
func TestGuardedDialerLiteralBlocked(t *testing.T) {
	originalLookup := lookupIPAddrFunc
	t.Cleanup(func() { lookupIPAddrFunc = originalLookup })
	lookupIPAddrFunc = func(_ context.Context, name string) ([]net.IPAddr, error) {
		t.Fatalf("resolver should not be called for IP literal target, got lookup of %q", name)
		return nil, nil
	}

	dial := guardedDialer("http://10.0.0.5:80")
	conn, err := dial(context.Background(), "tcp", "10.0.0.5:80")
	if conn != nil {
		conn.Close()
		t.Fatalf("guarded dialer must refuse rfc1918 literal, got conn")
	}
	if err == nil || !strings.Contains(err.Error(), "private") {
		t.Fatalf("guarded dialer should fail with private-address error, got %v", err)
	}
}

// TestGuardedReplicaDialerRebind confirms the replica upload's dial-time guard
// refuses a hostname that rebinds to loopback after validateReplicaTarget, yet
// keeps letting private peers through (allowPrivate).
func TestGuardedReplicaDialerRebind(t *testing.T) {
	originalLookup := lookupIPAddrFunc
	t.Cleanup(func() { lookupIPAddrFunc = originalLookup })

	const host = "replica.example.com"
	var calls atomic.Int32
	lookupIPAddrFunc = func(_ context.Context, name string) ([]net.IPAddr, error) {
		if name != host {
			return nil, &net.DNSError{Err: "no such host", Name: name, IsNotFound: true}
		}
		if calls.Add(1) == 1 {
			return []net.IPAddr{{IP: net.ParseIP("52.216.10.10")}}, nil
		}
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
	}

	// Up-front validation sees the public answer and accepts the target.
	if err := validateReplicaTarget(context.Background(), host+":8080"); err != nil {
		t.Fatalf("public replica target should validate, got %v", err)
	}
	// The dial then re-resolves to loopback and must refuse it.
	dial := guardedDialerPolicy(host+":8080", true)
	conn, err := dial(context.Background(), "tcp", host+":8080")
	if conn != nil {
		conn.Close()
		t.Fatalf("guarded replica dialer must refuse loopback rebind, got conn")
	}
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("expected loopback refusal, got %v", err)
	}

	// A private literal peer is allowed through: the dial is attempted (and here
	// fails on the already-cancelled context) rather than blocked as private.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, perr := guardedDialerPolicy("10.0.0.5:80", true)(ctx, "tcp", "10.0.0.5:80"); perr != nil && strings.Contains(perr.Error(), "private") {
		t.Fatalf("private peer must be allowed by the replica dialer, got %v", perr)
	}
}

// TestBuildGuardedRemoteStorageClient confirms the shared builder refuses a
// caller-influenced endpoint that resolves to a blocked address, and a gcs
// credentials path, while allowUntrusted falls back to the plain builder.
func TestBuildGuardedRemoteStorageClient(t *testing.T) {
	loopbackS3 := &remote_pb.RemoteConf{
		Name:        "poc",
		Type:        "s3",
		S3Endpoint:  "http://127.0.0.1:8000",
		S3AccessKey: "k",
		S3SecretKey: "s",
		S3Region:    "us-east-1",
	}
	if _, err := BuildGuardedRemoteStorageClient(context.Background(), loopbackS3, false); err == nil {
		t.Error("expected a loopback s3 endpoint to be rejected")
	} else if !strings.Contains(err.Error(), "reject remote endpoint") {
		t.Errorf("error = %v, want reject remote endpoint", err)
	}
	if _, err := BuildGuardedRemoteStorageClient(context.Background(), loopbackS3, true); err != nil {
		t.Errorf("allowUntrusted should build the client: %v", err)
	}

	gcsPathCreds := &remote_pb.RemoteConf{
		Name:                            "poc",
		Type:                            "gcs",
		GcsGoogleApplicationCredentials: "/etc/hostname",
	}
	if _, err := BuildGuardedRemoteStorageClient(context.Background(), gcsPathCreds, false); err == nil {
		t.Error("expected a non-credentials file path to be rejected")
	} else if !strings.Contains(err.Error(), "reject remote credentials") {
		t.Errorf("error = %v, want reject remote credentials", err)
	} else if strings.Contains(err.Error(), "/etc/hostname") {
		t.Errorf("error must not leak the file path: %v", err)
	}

	// A file path that points to valid GCS credentials should be accepted.
	credsFile := filepath.Join(t.TempDir(), "service-account.json")
	validCreds := `{"type":"service_account","token_uri":"https://oauth2.googleapis.com/token","client_email":"sa@example.iam.gserviceaccount.com","private_key":"-----BEGIN PRIVATE KEY-----\nMIIBVwIBADANBgkqhkiG9w0BAQEFAASCAUEwggE9AgEAAkEAxY\n-----END PRIVATE KEY-----\n","private_key_id":"key1"}`
	if err := os.WriteFile(credsFile, []byte(validCreds), 0600); err != nil {
		t.Fatalf("write creds file: %v", err)
	}
	gcsFileCreds := &remote_pb.RemoteConf{
		Name:                            "good",
		Type:                            "gcs",
		GcsGoogleApplicationCredentials: credsFile,
	}
	if err := checkGcsCredentials(credsFile); err != nil {
		t.Errorf("valid gcs credentials file should pass: %v", err)
	}
	if _, err := BuildGuardedRemoteStorageClient(context.Background(), gcsFileCreds, false); err != nil {
		t.Errorf("valid gcs credentials file should build: %v", err)
	}

	// A nonexistent path must be rejected without leaking the path in the error.
	gcsMissingCreds := &remote_pb.RemoteConf{
		Name:                            "missing",
		Type:                            "gcs",
		GcsGoogleApplicationCredentials: filepath.Join(t.TempDir(), "does-not-exist.json"),
	}
	if _, err := BuildGuardedRemoteStorageClient(context.Background(), gcsMissingCreds, false); err == nil {
		t.Error("expected a nonexistent credentials file to be rejected")
	} else if strings.Contains(err.Error(), "does-not-exist") {
		t.Errorf("error must not leak the file path: %v", err)
	}
}

// TestValidateRemoteConfForLoad confirms the load-time validator (injected into
// the filer's FilerRemoteStorage) rejects a RemoteConf whose endpoint resolves
// to a blocked address, while allowUntrusted skips the check. A conf whose type
// dials a fixed provider host (no caller-supplied endpoint) passes.
func TestValidateRemoteConfForLoad(t *testing.T) {
	loopbackS3 := &remote_pb.RemoteConf{
		Name:       "poc",
		Type:       "s3",
		S3Endpoint: "http://127.0.0.1:8000",
		S3Region:   "us-east-1",
	}
	if err := ValidateRemoteConfForLoad(context.Background(), loopbackS3, false); err == nil {
		t.Error("expected a loopback s3 endpoint to be rejected at load")
	} else if !strings.Contains(err.Error(), "reject remote endpoint") {
		t.Errorf("error = %v, want reject remote endpoint", err)
	}
	// allowUntrusted mirrors the volume server opt-out.
	if err := ValidateRemoteConfForLoad(context.Background(), loopbackS3, true); err != nil {
		t.Errorf("allowUntrusted should accept the conf: %v", err)
	}
	// A non-S3-compatible type with no caller-supplied endpoint dials a fixed
	// provider host, so there is nothing caller-influenced to deny.
	fixedHost := &remote_pb.RemoteConf{Name: "fixed", Type: "gcs"}
	if err := ValidateRemoteConfForLoad(context.Background(), fixedHost, false); err != nil {
		t.Errorf("fixed-host provider should pass: %v", err)
	}
	// nil conf is a no-op.
	if err := ValidateRemoteConfForLoad(context.Background(), nil, false); err != nil {
		t.Errorf("nil conf should be a no-op: %v", err)
	}
	// A hostname endpoint is not resolved at load time (DNS is left to the
	// build-time guard at dial), so it must pass even if it would resolve to a
	// blocked address. This prevents transient DNS failures from disabling
	// working mounts during /etc/remote reload.
	hostnameS3 := &remote_pb.RemoteConf{
		Name:       "host",
		Type:       "s3",
		S3Endpoint: "http://internal.example.com",
		S3Region:   "us-east-1",
	}
	if err := ValidateRemoteConfForLoad(context.Background(), hostnameS3, false); err != nil {
		t.Errorf("hostname endpoint should pass at load (DNS deferred to dial): %v", err)
	}
	// A standard AWS S3 config with no custom endpoint (empty S3Endpoint) has
	// no caller-supplied endpoint to guard — the AWS SDK derives the regional
	// endpoint. Both the load-time validator and the build-time guard must
	// accept it so standard AWS S3 mounts keep working.
	standardS3 := &remote_pb.RemoteConf{
		Name:     "aws",
		Type:     "s3",
		S3Region: "us-east-1",
	}
	if err := ValidateRemoteConfForLoad(context.Background(), standardS3, false); err != nil {
		t.Errorf("standard AWS S3 (empty endpoint) should pass: %v", err)
	}
	if _, err := BuildGuardedRemoteStorageClient(context.Background(), standardS3, false); err != nil {
		t.Errorf("standard AWS S3 (empty endpoint) should build: %v", err)
	}
	// A non-s3 S3-compatible type with an empty endpoint is a misconfiguration
	// (the AWS SDK would derive an AWS endpoint). The guard must reject it
	// rather than fall through to the unguarded cache.
	aliyunNoEndpoint := &remote_pb.RemoteConf{
		Name:         "aliyun",
		Type:         "aliyun",
		AliyunRegion: "cn-hangzhou",
	}
	if err := ValidateRemoteConfForLoad(context.Background(), aliyunNoEndpoint, false); err == nil {
		t.Error("aliyun with empty endpoint should be rejected at load")
	}
	if _, err := BuildGuardedRemoteStorageClient(context.Background(), aliyunNoEndpoint, false); err == nil {
		t.Error("aliyun with empty endpoint should be rejected by the guard")
	}
}

// TestGuardedPeerHTTPClientCarriesClusterTLS confirms the replica-upload
// client borrows the shared global client's TLS configuration — an https
// volume server expects the https.client cert and CA — while the remote
// storage client stays on a plain transport.
func TestGuardedPeerHTTPClientCarriesClusterTLS(t *testing.T) {
	globalTransport := util_http.GetGlobalHttpClient().GetClientTransport()
	original := globalTransport.TLSClientConfig
	globalTransport.TLSClientConfig = &tls.Config{ServerName: "volume-peer.test"}
	t.Cleanup(func() { globalTransport.TLSClientConfig = original })

	peer := newGuardedPeerHTTPClient("10.0.0.1:8080")
	if peer.Transport.(*http.Transport).TLSClientConfig != globalTransport.TLSClientConfig {
		t.Error("peer upload client should share the global TLS config")
	}
	if peer.CheckRedirect == nil {
		t.Error("peer upload client should share the global redirect policy")
	}
	if remote := newGuardedHTTPClient("10.0.0.1:8080").Transport.(*http.Transport); remote.TLSClientConfig != nil {
		t.Error("remote storage client should not carry cluster TLS config")
	}
}

// peerIPv4 returns a non-loopback IPv4 of this host — the only kind of
// address the peer dialer connects to — or skips the test when none exists.
func peerIPv4(t *testing.T) net.IP {
	t.Helper()

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Skipf("list interface addresses: %v", err)
	}
	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		if ip := ipnet.IP.To4(); ip != nil && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
			return ip
		}
	}
	t.Skip("no non-loopback IPv4 for the peer dialer to reach")
	return nil
}

// peerTLSServer runs handler over TLS on a non-loopback IPv4 under a
// self-signed cert carrying that IP, and returns its host:port plus a pool
// trusting the cert.
func peerTLSServer(t *testing.T, handler http.Handler) (string, *x509.CertPool) {
	t.Helper()

	peerIP := peerIPv4(t)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "volume-peer"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{peerIP},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)

	listener, err := net.Listen("tcp", net.JoinHostPort(peerIP.String(), "0"))
	if err != nil {
		t.Skipf("bind non-loopback %s: %v", peerIP, err)
	}
	server := &httptest.Server{
		Listener: listener,
		Config:   &http.Server{Handler: handler},
		TLS:      &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}},
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	return listener.Addr().String(), pool
}

// usePeerCA points the shared global client's transport at pool for the test.
func usePeerCA(t *testing.T, pool *x509.CertPool) {
	t.Helper()
	transport := util_http.GetGlobalHttpClient().GetClientTransport()
	original := transport.TLSClientConfig
	transport.TLSClientConfig = &tls.Config{RootCAs: pool}
	t.Cleanup(func() { transport.TLSClientConfig = original })
}

// TestGuardedPeerHTTPClientUploadsOverTLS sends a real replicate upload
// through the guarded peer client to a TLS server — the global client's CA
// config and the https scheme must carry it, so a hardcoded http:// URL
// could never reach the handler.
func TestGuardedPeerHTTPClientUploadsOverTLS(t *testing.T) {
	var sawReplicate atomic.Bool
	addr, pool := peerTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			t.Error("peer upload arrived over plaintext")
		}
		sawReplicate.Store(r.URL.Query().Get("type") == "replicate")
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	usePeerCA(t, pool)

	uploader := operation.NewUploaderWithHttpClient(newGuardedPeerHTTPClient(addr))
	data := []byte("needle-data")
	result, err := uploader.UploadData(t.Context(), data, &operation.UploadOption{
		UploadUrl:     "https://" + addr + "/1,01637037d6?type=replicate",
		IsReplication: true,
	})
	if err != nil {
		t.Fatalf("upload to TLS peer: %v", err)
	}
	if !sawReplicate.Load() {
		t.Error("peer never saw a ?type=replicate upload")
	}
	if result == nil || result.Size != uint32(len(data)) {
		t.Errorf("unexpected upload result %+v", result)
	}
}

// TestGuardedPeerHTTPClientRejectsHTTPSDowngrade pins the downgrade guard the
// global client carries: an HTTPS peer answering with a redirect to a
// reachable http server must fail the upload rather than resend the body and
// auth header in plaintext. The plaintext target sits on an address the peer
// dialer accepts, so only the redirect policy can stop the resend.
func TestGuardedPeerHTTPClientRejectsHTTPSDowngrade(t *testing.T) {
	var sawPlaintext atomic.Bool
	plaintext := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawPlaintext.Store(true)
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	listener, err := net.Listen("tcp", net.JoinHostPort(peerIPv4(t).String(), "0"))
	if err != nil {
		t.Skipf("bind non-loopback plaintext target: %v", err)
	}
	plaintext.Listener = listener
	plaintext.Start()
	defer plaintext.Close()

	var sawUpload atomic.Bool
	addr, pool := peerTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawUpload.Store(true)
		io.Copy(io.Discard, r.Body)
		http.Redirect(w, r, "http://"+listener.Addr().String()+"/latest/meta-data", http.StatusTemporaryRedirect)
	}))
	usePeerCA(t, pool)

	uploader := operation.NewUploaderWithHttpClient(newGuardedPeerHTTPClient(addr))
	if _, err := uploader.UploadData(t.Context(), []byte("needle-data"), &operation.UploadOption{
		UploadUrl:     "https://" + addr + "/1,01637037d6?type=replicate",
		IsReplication: true,
		MaxAttempts:   1,
	}); err == nil {
		t.Error("HTTPS→HTTP redirect must fail the upload, not resend the body in plaintext")
	}
	if sawPlaintext.Load() {
		t.Error("redirect resend reached the plaintext server")
	}
	if !sawUpload.Load() {
		t.Error("HTTPS peer never saw the original upload")
	}
}

// guardedCacheTest empties the process-wide remote client cache before and
// after a test and stubs DNS with a lookup counter.
func guardedCacheTest(t *testing.T, ips map[string][]net.IP) *atomic.Int64 {
	t.Helper()
	remote_storage.ResetRemoteStorageClients()
	t.Cleanup(remote_storage.ResetRemoteStorageClients)
	original := lookupIPAddrFunc
	t.Cleanup(func() { lookupIPAddrFunc = original })
	var lookups atomic.Int64
	stub := stubLookup(t, ips)
	lookupIPAddrFunc = func(ctx context.Context, host string) ([]net.IPAddr, error) {
		lookups.Add(1)
		return stub(ctx, host)
	}
	return &lookups
}

// The same configuration gets the same client; a rotated key a new one; a loopback endpoint under a cached name is
// still refused (it is another configuration: a miss, checked when it is built).
func TestBuildGuardedRemoteStorageClientReusesOneClientPerConf(t *testing.T) {
	guardedCacheTest(t, nil)
	conf := &remote_pb.RemoteConf{
		Name:        "reuse",
		Type:        "s3",
		S3Endpoint:  "https://93.184.216.34", // a public address literal: validated without DNS
		S3AccessKey: "k",
		S3SecretKey: "s",
		S3Region:    "us-east-1",
	}
	first, err := BuildGuardedRemoteStorageClient(context.Background(), conf, false)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for i := 0; i < 5; i++ {
		again, err := BuildGuardedRemoteStorageClient(context.Background(), proto.Clone(conf).(*remote_pb.RemoteConf), false)
		if err != nil {
			t.Fatalf("build again: %v", err)
		}
		if again != first {
			t.Fatalf("call %d built a new client for the same configuration", i)
		}
	}
	rotated := proto.Clone(conf).(*remote_pb.RemoteConf)
	rotated.S3SecretKey = "s2"
	if next, err := BuildGuardedRemoteStorageClient(context.Background(), rotated, false); err != nil || next == first {
		t.Errorf("a changed configuration (a rotated key) must get a new client (err %v)", err)
	}
	loop := proto.Clone(conf).(*remote_pb.RemoteConf)
	loop.S3Endpoint = "http://127.0.0.1:8000"
	if _, err := BuildGuardedRemoteStorageClient(context.Background(), loop, false); err == nil ||
		!strings.Contains(err.Error(), "reject remote endpoint") {
		t.Errorf("a loopback endpoint under a cached name must still be rejected, got %v", err)
	}
}

// The endpoint's DNS is checked when a client is built (a cache miss), not on every call; the guarded dialer still
// checks every new connection.
func TestBuildGuardedRemoteStorageClientResolvesOnlyOnAMiss(t *testing.T) {
	lookups := guardedCacheTest(t, map[string][]net.IP{"s3.example.test": {net.ParseIP("52.216.10.10")}})
	conf := &remote_pb.RemoteConf{Name: "dns", Type: "s3", S3Endpoint: "https://s3.example.test",
		S3AccessKey: "k", S3SecretKey: "s", S3Region: "us-east-1"}
	for i := 0; i < 10; i++ {
		if _, err := BuildGuardedRemoteStorageClient(context.Background(), conf, false); err != nil {
			t.Fatalf("build: %v", err)
		}
	}
	if n := lookups.Load(); n != 1 {
		t.Fatalf("%d DNS lookups for 10 calls with one configuration, want 1", n)
	}
}

// The shared transport keeps a burst's connections for reuse and caps them per host (WEED_REMOTE_MAX_CONNS_PER_HOST).
func TestGuardedHTTPClientBoundsConnectionsPerHost(t *testing.T) {
	tr := newGuardedHTTPClient("https://93.184.216.34").Transport.(*http.Transport)
	if tr.MaxIdleConnsPerHost < 32 || tr.MaxConnsPerHost != 64 {
		t.Fatalf("MaxIdleConnsPerHost %d (want >= 32), MaxConnsPerHost %d (want 64)", tr.MaxIdleConnsPerHost, tr.MaxConnsPerHost)
	}
	t.Setenv("WEED_REMOTE_MAX_CONNS_PER_HOST", "8")
	if got := newGuardedHTTPClient("https://93.184.216.34").Transport.(*http.Transport).MaxConnsPerHost; got != 8 {
		t.Fatalf("MaxConnsPerHost %d with WEED_REMOTE_MAX_CONNS_PER_HOST=8", got)
	}
}

// A gcs credentials file is part of the key by its content: the same file shares one client, a changed file builds anew.
func TestBuildGuardedRemoteStorageClientGcsFileCredentialsByContent(t *testing.T) {
	guardedCacheTest(t, map[string][]net.IP{"oauth2.googleapis.com": {net.ParseIP("142.250.0.95")}})
	credsFile := filepath.Join(t.TempDir(), "service-account.json")
	write := func(email string) {
		creds := `{"type":"service_account","token_uri":"https://oauth2.googleapis.com/token","client_email":"` + email + `","private_key":"-----BEGIN PRIVATE KEY-----\nMIIBVwIBADANBgkqhkiG9w0BAQEFAASCAUEwggE9AgEAAkEAxY\n-----END PRIVATE KEY-----\n","private_key_id":"key1"}`
		if err := os.WriteFile(credsFile, []byte(creds), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("sa@example.iam.gserviceaccount.com")
	conf := &remote_pb.RemoteConf{Name: "gcsfile", Type: "gcs", GcsGoogleApplicationCredentials: credsFile}
	first, err := BuildGuardedRemoteStorageClient(context.Background(), conf, false)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	again, _ := BuildGuardedRemoteStorageClient(context.Background(), conf, false)
	if again != first {
		t.Fatal("the same credentials file must share one client")
	}
	write("other@example.iam.gserviceaccount.com")
	if next, err := BuildGuardedRemoteStorageClient(context.Background(), conf, false); err != nil || next == first {
		t.Fatalf("a changed credentials file must build a new client (err %v)", err)
	}
}

// An s3-compatible remote without keys (the SDK's default chain) is keyed by the shared credentials file and the AWS_*
// variables: unchanged, one client; changed, a new one.
func TestBuildGuardedRemoteStorageClientS3DefaultChainByCredentials(t *testing.T) {
	guardedCacheTest(t, nil)
	shared := filepath.Join(t.TempDir(), "credentials")
	if err := os.WriteFile(shared, []byte("[default]\naws_access_key_id = a\naws_secret_access_key = b\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", shared)
	conf := &remote_pb.RemoteConf{Name: "chain", Type: "s3", S3Endpoint: "https://93.184.216.34", S3Region: "us-east-1"}
	first, err := BuildGuardedRemoteStorageClient(context.Background(), conf, false)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if again, _ := BuildGuardedRemoteStorageClient(context.Background(), conf, false); again != first {
		t.Fatal("unchanged credentials must share one client")
	}
	if err := os.WriteFile(shared, []byte("[default]\naws_access_key_id = c\naws_secret_access_key = d\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if next, err := BuildGuardedRemoteStorageClient(context.Background(), conf, false); err != nil || next == first {
		t.Fatalf("a changed shared credentials file must build a new client (err %v)", err)
	}
	awsConfig := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(awsConfig, []byte("[default]\nregion = us-east-1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", awsConfig)
	withConfig, err := BuildGuardedRemoteStorageClient(context.Background(), conf, false)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := os.WriteFile(awsConfig, []byte("[default]\nregion = us-west-2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if next, _ := BuildGuardedRemoteStorageClient(context.Background(), conf, false); next == withConfig {
		t.Fatal("a changed shared config file must build a new client")
	}
	t.Setenv("AWS_PROFILE", "other")
	prev, _ := BuildGuardedRemoteStorageClient(context.Background(), conf, false)
	t.Setenv("AWS_PROFILE", "third")
	if next, _ := BuildGuardedRemoteStorageClient(context.Background(), conf, false); next == prev {
		t.Fatal("a changed AWS_PROFILE must build a new client")
	}
}

// An s3-compatible remote whose keys come from its type's environment fallback is keyed by them too: a changed variable
// builds a new client even though nothing in the configuration changed.
func TestBuildGuardedRemoteStorageClientS3EnvironmentCredentials(t *testing.T) {
	guardedCacheTest(t, nil)
	t.Setenv("ALICLOUD_ACCESS_KEY_ID", "a")
	t.Setenv("ALICLOUD_ACCESS_KEY_SECRET", "b")
	conf := &remote_pb.RemoteConf{Name: "env", Type: "aliyun", AliyunEndpoint: "https://93.184.216.34"}
	first, err := BuildGuardedRemoteStorageClient(context.Background(), conf, false)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if again, _ := BuildGuardedRemoteStorageClient(context.Background(), conf, false); again != first {
		t.Fatal("unchanged environment credentials must share one client")
	}
	t.Setenv("ALICLOUD_ACCESS_KEY_SECRET", "c")
	if next, err := BuildGuardedRemoteStorageClient(context.Background(), conf, false); err != nil || next == first {
		t.Fatalf("a changed environment credential must build a new client (err %v)", err)
	}
}
