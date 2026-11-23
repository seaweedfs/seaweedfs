package command

import "testing"

func TestCheckSyncDigests(t *testing.T) {
	cases := []struct {
		a, b   string
		refuse bool
	}{
		{"md5", "md5", false},
		{"sha256", "sha256", false},
		{"sha256", "md5", true},
		{"md5", "sha256", true},
		{"", "sha256", false},
		{"md5", "", false},
	}
	for _, c := range cases {
		err := checkSyncDigests("a:8888", c.a, "b:8888", c.b)
		if (err != nil) != c.refuse {
			t.Errorf("checkSyncDigests(%q, %q) = %v, want refused %v", c.a, c.b, err, c.refuse)
		}
	}
}
