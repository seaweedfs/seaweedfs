package mount

import (
	"strings"
	"testing"
)

func TestResolvePeerAdvertiseAddr(t *testing.T) {
	tests := []struct {
		name      string
		listen    string
		advertise string
		wantErr   bool
		// addr substring check — DetectedHostAddress is system-dependent so
		// we don't assert an exact value for the wildcard path.
		wantSuffix string
	}{
		{"explicit advertise wins", ":18080", "10.1.1.9:20000", false, ":20000"},
		{"bind host used verbatim", "10.0.0.5:18080", "", false, "10.0.0.5:18080"},
		{"wildcard ipv4 bind", "0.0.0.0:18080", "", false, ":18080"},
		{"empty host bind", ":18080", "", false, ":18080"},
		{"ipv6 wildcard", "[::]:18080", "", false, ":18080"},
		{"unparseable listen errors", "garbage", "", true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolvePeerAdvertiseAddr(tt.listen, tt.advertise)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got addr=%q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.HasSuffix(got, tt.wantSuffix) {
				t.Errorf("addr=%q, expected suffix %q", got, tt.wantSuffix)
			}
		})
	}
}

func TestIsWildcardHost(t *testing.T) {
	for _, h := range []string{"", "0.0.0.0", "::", "[::]"} {
		if !isWildcardHost(h) {
			t.Errorf("%q should be wildcard", h)
		}
	}
	for _, h := range []string{"10.0.0.5", "host.example", "localhost"} {
		if isWildcardHost(h) {
			t.Errorf("%q should NOT be wildcard", h)
		}
	}
}
