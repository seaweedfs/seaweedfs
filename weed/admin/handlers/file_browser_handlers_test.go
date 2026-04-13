package handlers

import (
	"strings"
	"testing"
)

func TestValidateAndCleanFilePath_AllowsControlChars(t *testing.T) {
	h := &FileBrowserHandlers{}

	// S3 object keys may legally contain any UTF-8 bytes, including control
	// characters like \n, \r, and \x00. The admin UI must be able to browse
	// and manage such entries rather than refusing them.
	cases := []string{
		"/buckets/profilebuilder/3testGB.zip\n ",
		"/foo\rbar",
		"/foo\x00bar",
		"/normal/path.txt",
	}
	for _, in := range cases {
		got, err := h.validateAndCleanFilePath(in)
		if err != nil {
			t.Errorf("validateAndCleanFilePath(%q) unexpected error: %v", in, err)
			continue
		}
		if !strings.HasPrefix(got, "/") {
			t.Errorf("validateAndCleanFilePath(%q) = %q, want leading /", in, got)
		}
	}
}

func TestValidateAndCleanFilePath_RejectsEmpty(t *testing.T) {
	h := &FileBrowserHandlers{}
	if _, err := h.validateAndCleanFilePath(""); err == nil {
		t.Errorf("expected empty path rejection")
	}
}

func TestFilerFileURL_EscapesControlChars(t *testing.T) {
	got := filerFileURL("http://127.0.0.1:8888", "/buckets/profilebuilder/3testGB.zip\n ")
	want := "http://127.0.0.1:8888/buckets/profilebuilder/3testGB.zip%0A%20"
	if got != want {
		t.Errorf("filerFileURL = %q, want %q", got, want)
	}
	// A path with no special characters should round-trip unchanged.
	if got := filerFileURL("http://h:1", "/a/b.txt"); got != "http://h:1/a/b.txt" {
		t.Errorf("filerFileURL unchanged path = %q", got)
	}
}
