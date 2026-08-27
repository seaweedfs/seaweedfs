package weed_server

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	s3remote "github.com/seaweedfs/seaweedfs/weed/remote_storage/s3"
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

// TestGcsCredentialsArePath confirms a caller-supplied gcs credentials value is
// only accepted as inline JSON. A filesystem path would otherwise be read from
// disk by the SDK when handling the request.
func TestGcsCredentialsArePath(t *testing.T) {
	paths := []string{
		"/etc/hostname",
		"/etc/shadow",
		"/nope/nothere",
		"~/creds.json",
		"relative/creds.json",
	}
	for _, p := range paths {
		if !gcsCredentialsArePath(p) {
			t.Errorf("expected %q to be treated as a path", p)
		}
	}
	inlineOrEmpty := []string{
		"",
		`{"type":"service_account"}`,
		`{}`,
	}
	for _, c := range inlineOrEmpty {
		if gcsCredentialsArePath(c) {
			t.Errorf("expected %q to be accepted (inline or empty)", c)
		}
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
		t.Error("expected a gcs credentials path to be rejected")
	} else if !strings.Contains(err.Error(), "reject remote credentials") {
		t.Errorf("error = %v, want reject remote credentials", err)
	}
}
