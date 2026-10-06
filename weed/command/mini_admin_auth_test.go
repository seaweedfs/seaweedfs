package command

import (
	"net"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// weed mini must resolve admin credentials from security.toml [admin] /
// WEED_ADMIN_* env vars the same way the standalone `weed admin` command does.
// This exercises the production fallback so the flag-name -> viper-key mapping
// stays correct, in particular the read-only keys where the mini flag
// (admin.readOnlyUser) and viper key (admin.readonly.user) differ.
// TestApplyMiniAdminCredentialFallbackFromEnv verifies those environment
// fallbacks without starting the full mini cluster.
func TestApplyMiniAdminCredentialFallbackFromEnv(t *testing.T) {
	adminUser, adminPassword, readOnlyUser, readOnlyPassword := "admin", "", "", ""
	options := &AdminOptions{
		adminUser:        &adminUser,
		adminPassword:    &adminPassword,
		readOnlyUser:     &readOnlyUser,
		readOnlyPassword: &readOnlyPassword,
	}

	t.Setenv("WEED_ADMIN_USER", "env-admin")
	t.Setenv("WEED_ADMIN_PASSWORD", "env-secret")
	t.Setenv("WEED_ADMIN_READONLY_USER", "env-ro")
	t.Setenv("WEED_ADMIN_READONLY_PASSWORD", "env-ro-secret")

	applyMiniAdminCredentialFallback(options)

	checks := []struct {
		name string
		got  string
		want string
	}{
		{"adminUser", *options.adminUser, "env-admin"},
		{"adminPassword", *options.adminPassword, "env-secret"},
		{"readOnlyUser", *options.readOnlyUser, "env-ro"},
		{"readOnlyPassword", *options.readOnlyPassword, "env-ro-secret"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}
}

// TestMiniAdminBindIP covers the authentication-dependent HTTP bind policy.
func TestMiniAdminBindIP(t *testing.T) {
	tests := []struct {
		name               string
		requestedIP        string
		passwordConfigured bool
		mtlsConfigured     bool
		allowInsecure      bool
		want               string
	}{
		{
			name:        "unauthenticated wildcard binds to loopback",
			requestedIP: "0.0.0.0",
			want:        "127.0.0.1",
		},
		{
			name:        "unauthenticated IPv6 wildcard binds to loopback",
			requestedIP: "::",
			want:        "127.0.0.1",
		},
		{
			name:        "existing loopback bind is preserved",
			requestedIP: "127.0.0.1",
			want:        "127.0.0.1",
		},
		{
			name:               "password permits requested bind",
			requestedIP:        "0.0.0.0",
			passwordConfigured: true,
			want:               "0.0.0.0",
		},
		{
			name:           "mTLS permits requested bind",
			requestedIP:    "0.0.0.0",
			mtlsConfigured: true,
			want:           "0.0.0.0",
		},
		{
			name:          "explicit insecure opt-out permits requested bind",
			requestedIP:   "0.0.0.0",
			allowInsecure: true,
			want:          "0.0.0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := miniAdminBindIP(
				tt.requestedIP,
				tt.passwordConfigured,
				tt.mtlsConfigured,
				tt.allowInsecure,
			)
			if got != tt.want {
				t.Fatalf("miniAdminBindIP() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestMiniAdminWorkerBindDefaultsToLoopback protects the worker control-plane default.
func TestMiniAdminWorkerBindDefaultsToLoopback(t *testing.T) {
	if miniAdminWorkerBindIP == nil {
		t.Fatal("mini Admin worker bind flag is not initialized")
	}
	if got, want := *miniAdminWorkerBindIP, "127.0.0.1"; got != want {
		t.Fatalf("default mini Admin worker bind IP = %q, want %q", got, want)
	}
}

// TestListenMiniAdminWorkerUsesRequestedAddress verifies the production
// listener is actually restricted to the configured loopback address.
func TestListenMiniAdminWorkerUsesRequestedAddress(t *testing.T) {
	listener, err := listenMiniAdminWorker("127.0.0.1", 0)
	if err != nil {
		t.Fatalf("listen for mini Admin worker: %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener address type = %T, want *net.TCPAddr", listener.Addr())
	}
	if !address.IP.IsLoopback() {
		t.Fatalf("listener IP = %s, want loopback", address.IP)
	}
}

// TestMiniAdminWorkerAddressUsesFinalGrpcPort verifies that local workers do
// not have to discover or infer a custom Admin worker gRPC port.
func TestMiniAdminWorkerAddressUsesFinalGrpcPort(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		want     string
		wantGrpc string
	}{
		{
			name:     "IPv4",
			ip:       "127.0.0.1",
			want:     "127.0.0.1:23646.34567",
			wantGrpc: "127.0.0.1:34567",
		},
		{
			name:     "IPv6",
			ip:       "::1",
			want:     "::1:23646.34567",
			wantGrpc: "[::1]:34567",
		},
		{
			name:     "IPv4 wildcard dials loopback",
			ip:       "0.0.0.0",
			want:     "127.0.0.1:23646.34567",
			wantGrpc: "127.0.0.1:34567",
		},
		{
			name:     "IPv6 wildcard dials loopback",
			ip:       "::",
			want:     "::1:23646.34567",
			wantGrpc: "[::1]:34567",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			address := miniAdminWorkerAddress(tt.ip, 23646, 34567)
			if address != tt.want {
				t.Fatalf("miniAdminWorkerAddress() = %q, want %q", address, tt.want)
			}
			if got := pb.ServerToGrpcAddress(address); got != tt.wantGrpc {
				t.Fatalf("pb.ServerToGrpcAddress() = %q, want %q", got, tt.wantGrpc)
			}
		})
	}
}

// TestMiniAdminWorkerDialIP verifies wildcard listener addresses are converted
// into valid same-family destinations without rewriting specific addresses.
func TestMiniAdminWorkerDialIP(t *testing.T) {
	tests := []struct {
		bindIP string
		want   string
	}{
		{bindIP: "0.0.0.0", want: "127.0.0.1"},
		{bindIP: "::", want: "::1"},
		{bindIP: "[::]", want: "::1"},
		{bindIP: "192.0.2.10", want: "192.0.2.10"},
		{bindIP: "2001:db8::10", want: "2001:db8::10"},
		{bindIP: "worker.internal", want: "worker.internal"},
	}

	for _, tt := range tests {
		t.Run(tt.bindIP, func(t *testing.T) {
			if got := miniAdminWorkerDialIP(tt.bindIP); got != tt.want {
				t.Fatalf("miniAdminWorkerDialIP(%q) = %q, want %q", tt.bindIP, got, tt.want)
			}
		})
	}
}

// TestMiniAdminAdvertisedIP verifies that the welcome message uses the
// selected loopback address but replaces wildcard binds with a reachable host.
func TestMiniAdminAdvertisedIP(t *testing.T) {
	oldMiniIP := miniIp
	oldAdminIP := miniAdminOptions.ip
	t.Cleanup(func() {
		miniIp = oldMiniIP
		miniAdminOptions.ip = oldAdminIP
	})

	detectedIP := "192.0.2.10"
	miniIp = &detectedIP

	tests := []struct {
		name     string
		adminIP  string
		expected string
	}{
		{
			name:     "selected loopback",
			adminIP:  "127.0.0.1",
			expected: "127.0.0.1",
		},
		{
			name:     "IPv4 wildcard",
			adminIP:  "0.0.0.0",
			expected: detectedIP,
		},
		{
			name:     "IPv6 wildcard",
			adminIP:  "::",
			expected: detectedIP,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adminIP := tt.adminIP
			miniAdminOptions.ip = &adminIP
			if got := miniAdminAdvertisedIP(); got != tt.expected {
				t.Fatalf("miniAdminAdvertisedIP() = %q, want %q", got, tt.expected)
			}
		})
	}
}
