package command

import "testing"

// weed mini must resolve admin credentials from security.toml [admin] /
// WEED_ADMIN_* env vars the same way the standalone `weed admin` command does.
// This exercises the production fallback so the flag-name -> viper-key mapping
// stays correct, in particular the read-only keys where the mini flag
// (admin.readOnlyUser) and viper key (admin.readonly.user) differ.
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

func TestMiniAdminWorkerBindDefaultsToLoopback(t *testing.T) {
	if miniAdminWorkerBindIP == nil {
		t.Fatal("mini Admin worker bind flag is not initialized")
	}
	if got, want := *miniAdminWorkerBindIP, "127.0.0.1"; got != want {
		t.Fatalf("default mini Admin worker bind IP = %q, want %q", got, want)
	}
}
