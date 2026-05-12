package weed_server

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
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
