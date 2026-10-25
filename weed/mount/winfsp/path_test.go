package winfsp

import (
	"strings"
	"testing"
)

func TestSplitPath(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"/", nil},
		{"", nil},
		{"//", nil},
		{".", nil},
		{"/foo", []string{"foo"}},
		{"foo", []string{"foo"}},
		{"/foo/", []string{"foo"}},
		{"/foo/bar", []string{"foo", "bar"}},
		{"/foo//bar", []string{"foo", "bar"}},
		{"/foo/./bar", []string{"foo", "bar"}},
		{`\foo\bar`, []string{"foo", "bar"}},
		{`/foo\bar`, []string{"foo", "bar"}},
		{"/foo bar/baz qux", []string{"foo bar", "baz qux"}},
		{"/eñe/日本", []string{"eñe", "日本"}},
		// ".." is a name like any other here; the filer resolves it, and
		// treating it structurally would let a path escape the mount root.
		{"/foo/../bar", []string{"foo", "..", "bar"}},
	}
	for _, c := range cases {
		got := splitPath(c.in)
		if len(got) != len(c.want) {
			t.Errorf("splitPath(%q) = %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("splitPath(%q) = %v, want %v", c.in, got, c.want)
				break
			}
		}
	}
}

func TestSplitParent(t *testing.T) {
	cases := []struct {
		in         string
		wantParent string
		wantName   string
		wantOK     bool
	}{
		{"/", "", "", false},
		{"", "", "", false},
		{"/foo", "", "foo", true},
		{"/foo/bar", "foo", "bar", true},
		{"/foo/bar/baz", "foo/bar", "baz", true},
		{`\foo\bar`, "foo", "bar", true},
		{"/foo/bar/", "foo", "bar", true},
	}
	for _, c := range cases {
		parent, name, ok := splitParent(c.in)
		if ok != c.wantOK || name != c.wantName || strings.Join(parent, "/") != c.wantParent {
			t.Errorf("splitParent(%q) = (%q, %q, %v), want (%q, %q, %v)",
				c.in, strings.Join(parent, "/"), name, ok, c.wantParent, c.wantName, c.wantOK)
		}
	}
}

// The root has no parent, so operations that need one must be rejected rather
// than silently acting on the root itself.
func TestSplitParentRejectsRoot(t *testing.T) {
	for _, root := range []string{"/", "", "//", `\`} {
		if _, _, ok := splitParent(root); ok {
			t.Errorf("splitParent(%q) accepted the root", root)
		}
	}
}
