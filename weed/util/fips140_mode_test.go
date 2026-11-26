package util

import (
	"strings"
	"testing"
)

// setFIPS140Godebug makes this test see the fips140 GODEBUG value godebug
// ("off", "on" or "only") without running the test binary under it.
func setFIPS140Godebug(t *testing.T, godebug string) {
	t.Helper()
	setFIPS140Enforced(t, godebug == "only")
	saved := fips140Enabled
	fips140Enabled = func() bool { return godebug != "off" }
	t.Cleanup(func() { fips140Enabled = saved })
}

func TestCheckFipsMode(t *testing.T) {
	cases := []struct {
		mode    string
		godebug string
		wantErr string
	}{
		{"", "off", ""},
		{"", "on", ""},
		{"", "only", ""},
		{"off", "off", ""},
		{"off", "on", `"off", which needs GODEBUG=fips140=off; this process runs with fips140=on`},
		{"off", "only", `"off", which needs GODEBUG=fips140=off; this process runs with fips140=only`},
		{"on", "on", ""},
		{"on", "off", `"on", which needs GODEBUG=fips140=on; this process runs with fips140=off`},
		{"on", "only", `"on", which needs GODEBUG=fips140=on; this process runs with fips140=only`},
		{"strict", "only", ""},
		{"strict", "off", `"strict", which needs GODEBUG=fips140=only; this process runs with fips140=off`},
		{"strict", "on", `"strict", which needs GODEBUG=fips140=only; this process runs with fips140=on`},
		{"only", "only", `"only"; want off, on or strict`},
		{"On", "on", `"On"; want off, on or strict`},
	}
	for _, tc := range cases {
		setFIPS140Godebug(t, tc.godebug)
		err := CheckFipsMode("security.toml: fips.mode", tc.mode)
		if tc.wantErr == "" {
			if err != nil {
				t.Errorf("mode %q, fips140=%s: got %v, want no error", tc.mode, tc.godebug, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("mode %q, fips140=%s: got no error, want %q", tc.mode, tc.godebug, tc.wantErr)
			continue
		}
		if want := "security.toml: fips.mode is " + tc.wantErr; err.Error() != want {
			t.Errorf("mode %q, fips140=%s: got %q, want %q", tc.mode, tc.godebug, err, want)
		}
	}
}

func TestFipsModeSourceNamesTheEnvironmentWhenSet(t *testing.T) {
	t.Setenv(FipsModeEnv, "")
	if got := fipsModeSource("/etc/seaweedfs/security.toml"); got != "/etc/seaweedfs/security.toml: fips.mode" {
		t.Errorf("without %s: got %q", FipsModeEnv, got)
	}
	t.Setenv(FipsModeEnv, "strict")
	if got := fipsModeSource("/etc/seaweedfs/security.toml"); got != FipsModeEnv {
		t.Errorf("with %s: got %q", FipsModeEnv, got)
	}
}

func TestFipsModeIsReadFromTheEnvironment(t *testing.T) {
	t.Setenv(FipsModeEnv, "strict")
	setFIPS140Godebug(t, "on")
	err := CheckFipsMode(fipsModeSource("security.toml"), GetViper().GetString(FipsModeSetting))
	if err == nil || !strings.HasPrefix(err.Error(), FipsModeEnv+` is "strict"`) {
		t.Fatalf("got %v, want the %s value refused", err, FipsModeEnv)
	}
}
