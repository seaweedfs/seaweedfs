package shell

import (
	_ "embed"
	"os"
	"path/filepath"
	"testing"
)

//go:embed volume.list.txt
var topoData string

//go:embed volume.list2.txt
var topoData2 string

//go:embed volume.ecshards.txt
var topoDataEc string

var (
	testTopology1  = parseOutput(topoData)
	testTopology2  = parseOutput(topoData2)
	testTopologyEc = parseOutput(topoDataEc)
)

func TestExpandHomeDir(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("no user home dir available: %v", err)
	}

	cases := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"~", home},
		{"~/", home},
		{"~/foo/bar.meta", filepath.Join(home, "foo/bar.meta")},
		{"/abs/path", "/abs/path"},
		{"relative/path", "relative/path"},
		{"./local", "./local"},
		{"~user/foo", "~user/foo"},
		{"~~weird", "~~weird"},
	}
	for _, tc := range cases {
		if got := expandHomeDir(tc.in); got != tc.want {
			t.Errorf("expandHomeDir(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
