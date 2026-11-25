package command

import "testing"

func TestMiniWebDavBindIP(t *testing.T) {
	allow := *miniWebDavAllowInsecureBind
	t.Cleanup(func() { *miniWebDavAllowInsecureBind = allow })

	tests := []struct {
		name          string
		requestedIP   string
		password      string
		allowInsecure bool
		want          string
	}{
		{name: "unauthenticated wildcard binds to loopback", requestedIP: "0.0.0.0", want: "127.0.0.1"},
		{name: "unauthenticated host address binds to loopback", requestedIP: "10.0.0.5", want: "127.0.0.1"},
		{name: "existing loopback bind is preserved", requestedIP: "127.0.0.1", want: "127.0.0.1"},
		{name: "bracketed IPv6 loopback bind is preserved", requestedIP: "[::1]", want: "[::1]"},
		{name: "bracketed IPv6 wildcard binds to loopback", requestedIP: "[::]", want: "127.0.0.1"},
		{name: "basic auth permits requested bind", requestedIP: "0.0.0.0", password: "secret", want: "0.0.0.0"},
		{name: "explicit insecure opt-out permits requested bind", requestedIP: "0.0.0.0", allowInsecure: true, want: "0.0.0.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("WEED_WEBDAV_BASIC_AUTH_PASSWORD", tt.password)
			*miniWebDavAllowInsecureBind = tt.allowInsecure
			if got := miniWebDavBindIP(tt.requestedIP); got != tt.want {
				t.Fatalf("miniWebDavBindIP(%q) = %q, want %q", tt.requestedIP, got, tt.want)
			}
		})
	}
}

// mini is a single-host sandbox, so its unauthenticated Admin and WebDAV
// listen on -ip.bind like every other component unless an operator opts out.
func TestMiniAllowsInsecureBindByDefault(t *testing.T) {
	for name, flag := range map[string]string{"admin": "admin.allowInsecureBind", "webdav": "webdav.allowInsecureBind"} {
		f := cmdMini.Flag.Lookup(flag)
		if f == nil {
			t.Fatalf("%s: flag -%s is not defined", name, flag)
		}
		if f.DefValue != "true" {
			t.Fatalf("%s: -%s defaults to %s, want true", name, flag, f.DefValue)
		}
	}
}
