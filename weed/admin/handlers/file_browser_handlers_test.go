package handlers

import (
	"testing"
)

func TestValidateAndCleanFilePath_AllowsControlChars(t *testing.T) {
	h := &FileBrowserHandlers{}

	// S3 object keys may legally contain any UTF-8 bytes, including control
	// characters like \n, \r, and \x00. The admin UI must be able to browse
	// and manage such entries rather than silently stripping or rejecting them.
	cases := []struct {
		in   string
		want string
	}{
		{"/buckets/profilebuilder/3testGB.zip\n ", "/buckets/profilebuilder/3testGB.zip\n "},
		{"/foo\rbar", "/foo\rbar"},
		{"/foo\x00bar", "/foo\x00bar"},
		{"/normal/path.txt", "/normal/path.txt"},
		// Missing leading slash should be added back.
		{"relative/path.txt", "/relative/path.txt"},
		// Duplicate slashes should be collapsed by path.Clean.
		{"/a//b", "/a/b"},
	}
	for _, tc := range cases {
		got, err := h.validateAndCleanFilePath(tc.in)
		if err != nil {
			t.Errorf("validateAndCleanFilePath(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("validateAndCleanFilePath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestValidateAndCleanFilePath_RejectsEmpty(t *testing.T) {
	h := &FileBrowserHandlers{}
	if _, err := h.validateAndCleanFilePath(""); err == nil {
		t.Errorf("expected empty path rejection")
	}
}

