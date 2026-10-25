package weed_server

import "testing"

func TestWrappedFsConfine(t *testing.T) {
	w := wrappedFs{subFolder: "/confined"}
	tests := []struct {
		name string
		want string
	}{
		{"/a/b", "/confined/a/b"},
		{"a/b", "/confined/a/b"},
		{"/", "/confined/"},
		{"/../etc/passwd", "/confined/etc/passwd"},
		{"/a/../../etc", "/confined/etc"},
		{"/a/./b", "/confined/a/b"},
		{"/a//b", "/confined/a/b"},
	}
	for _, tt := range tests {
		if got := w.confine(tt.name); got != tt.want {
			t.Errorf("confine(%q) = %q, want %q (must stay under subFolder)", tt.name, got, tt.want)
		}
	}
}
