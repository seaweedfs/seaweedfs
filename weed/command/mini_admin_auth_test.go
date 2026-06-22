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
