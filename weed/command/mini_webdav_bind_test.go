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
