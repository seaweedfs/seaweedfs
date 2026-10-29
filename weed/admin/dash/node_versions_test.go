package dash

import (
	"testing"
)

func TestShortVersion(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"8000GB 4.44-enterprise 192465e6", "4.44-enterprise 192465e6"},
		{"30GB 4.51", "4.51"},
		{"4.51 abc123", "4.51 abc123"},
		{"4.51", "4.51"},
		{"", "unknown"},
		{"  ", "unknown"},
	}
	for _, tc := range tests {
		if got := ShortVersion(tc.in); got != tc.want {
			t.Errorf("ShortVersion(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestBuildClusterVersionSummary(t *testing.T) {
	vs := func(version string, n int) (out []VolumeServer) {
		for i := 0; i < n; i++ {
			out = append(out, VolumeServer{Version: version})
		}
		return
	}
	filers := func(version string, n int) (out []FilerNode) {
		for i := 0; i < n; i++ {
			out = append(out, FilerNode{Version: version})
		}
		return
	}

	t.Run("uniform across types", func(t *testing.T) {
		s := buildClusterVersionSummary(
			vs("8000GB 4.44 aaa", 3),
			filers("30GB 4.44 aaa", 2), // different size-limit prefix, same binary
			nil, nil,
		)
		if !s.Uniform || s.Version != "4.44 aaa" {
			t.Fatalf("expected uniform 4.44 aaa, got %+v", s)
		}
		if len(s.NodeTypes) != 2 {
			t.Fatalf("expected 2 node types, got %+v", s.NodeTypes)
		}
	})

	t.Run("mixed volume server versions", func(t *testing.T) {
		servers := append(vs("8000GB 4.44 aaa", 24), vs("8000GB 4.39 bbb", 31)...)
		s := buildClusterVersionSummary(servers, nil, nil, nil)
		if s.Uniform {
			t.Fatalf("expected non-uniform summary, got %+v", s)
		}
		if len(s.NodeTypes) != 1 {
			t.Fatalf("expected 1 node type, got %+v", s.NodeTypes)
		}
		nt := s.NodeTypes[0]
		if nt.Type != "Volume Servers" || nt.Total != 55 {
			t.Fatalf("unexpected node type summary: %+v", nt)
		}
		// sorted by count desc: 31x 4.39 first, then 24x 4.44
		if len(nt.Versions) != 2 || nt.Versions[0].Version != "4.39 bbb" || nt.Versions[0].Count != 31 {
			t.Fatalf("unexpected versions: %+v", nt.Versions)
		}
		if nt.Spread() != "31× 4.39 bbb, 24× 4.44 aaa" {
			t.Fatalf("unexpected spread: %q", nt.Spread())
		}
	})

	t.Run("unreported versions count as unknown", func(t *testing.T) {
		s := buildClusterVersionSummary(vs("", 2), nil, nil, nil)
		// all empty -> a single "unknown" group, uniform but not a real version
		if !s.Uniform || s.Version != "unknown" {
			t.Fatalf("expected uniform unknown, got %+v", s)
		}
		if s.NodeTypes[0].Versions[0].Version != "unknown" || s.NodeTypes[0].Versions[0].Count != 2 {
			t.Fatalf("expected unknown group, got %+v", s.NodeTypes[0].Versions)
		}
	})

	t.Run("unknown mixed with real versions is not uniform", func(t *testing.T) {
		servers := append(vs("8000GB 4.44 aaa", 1), vs("", 1)...)
		s := buildClusterVersionSummary(servers, nil, nil, nil)
		if s.Uniform {
			t.Fatalf("expected non-uniform summary, got %+v", s)
		}
	})

	t.Run("no nodes is not uniform", func(t *testing.T) {
		s := buildClusterVersionSummary(nil, nil, nil, nil)
		if s.Uniform {
			t.Fatal("empty cluster should not report a uniform version")
		}
	})
}
