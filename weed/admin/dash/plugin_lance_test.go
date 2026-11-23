package dash

import "testing"

func TestLanceNamespaceURL(t *testing.T) {
	for _, tc := range []struct {
		address string
		port    uint32
		want    string
	}{
		{"10.0.0.5:18333", 9101, "http://10.0.0.5:9101"},
		{"s3-0.s3.default:18333", 9200, "http://s3-0.s3.default:9200"},
		{"[fd00::5]:18333", 9101, "http://[fd00::5]:9101"},
		{"10.0.0.5:18333", 0, ""},
	} {
		if got := lanceNamespaceURL(tc.address, tc.port); got != tc.want {
			t.Errorf("lanceNamespaceURL(%q, %d) = %q, want %q", tc.address, tc.port, got, tc.want)
		}
	}
}
