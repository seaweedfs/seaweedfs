package util

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// TestSanitizeUTF8Name_ValidPassThrough asserts the fast path returns the
// input unchanged (no allocation, no byte alteration).
func TestSanitizeUTF8Name_ValidPassThrough(t *testing.T) {
	for _, s := range []string{
		"",
		"plain.txt",
		"日本語.txt",
		"🦑 squid",
	} {
		if got := SanitizeUTF8Name(s); got != s {
			t.Fatalf("SanitizeUTF8Name(%q) = %q, want unchanged", s, got)
		}
	}
}

// TestSanitizeUTF8Name_InvalidBytes asserts invalid bytes are replaced with a
// single '_' (URL-safe, single-byte) and the output is valid UTF-8. The
// replacement char is load-bearing — downstream code places these strings in
// HTTP URLs, where '?' would be parsed as the query delimiter.
func TestSanitizeUTF8Name_InvalidBytes(t *testing.T) {
	out := SanitizeUTF8Name("foo\x80bar")
	if !utf8.ValidString(out) {
		t.Fatalf("result is not valid UTF-8: %q", out)
	}
	if out != "foo_bar" {
		t.Fatalf("SanitizeUTF8Name = %q, want %q", out, "foo_bar")
	}
	if strings.ContainsRune(out, '?') {
		t.Fatalf("replacement must be URL-safe, got %q", out)
	}
}

// TestFullPathSanitized_WholePath ensures Sanitized() scrubs invalid bytes in
// every component, not just the last — that's the difference from Name() and
// the reason call sites that need to pass a full path to a proto field must
// use Sanitized(), not (dir, _) := DirAndName().
func TestFullPathSanitized_WholePath(t *testing.T) {
	// Invalid byte sits in the middle component.
	fp := FullPath("/home/bad\x80dir/file.txt")
	got := fp.Sanitized()
	want := "/home/bad_dir/file.txt"
	if got != want {
		t.Fatalf("Sanitized() = %q, want %q", got, want)
	}

	// Bytes in every component — all get replaced, structure preserved.
	fp = FullPath("/a\xffb/c\xffd/e\xfff")
	got = fp.Sanitized()
	want = "/a_b/c_d/e_f"
	if got != want {
		t.Fatalf("Sanitized() = %q, want %q", got, want)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("Sanitized() returned non-UTF-8: %q", got)
	}
}

// TestFullPathDirAndName_OnlyNameSanitized documents a (deliberate) sharp
// edge: DirAndName() sanitizes only the trailing name, not dir. Callers who
// need a sanitized full path must use Sanitized(); using dir from DirAndName
// will still carry invalid bytes in parent components. This test pins the
// existing behavior so it is not accidentally "fixed" in a way that changes
// the (dir, name) semantics that everything else depends on.
func TestFullPathDirAndName_OnlyNameSanitized(t *testing.T) {
	fp := FullPath("/home/bad\x80dir/child\xffname")
	dir, name := fp.DirAndName()
	if !utf8.ValidString(name) {
		t.Fatalf("name must be sanitized: %q", name)
	}
	// dir still contains the invalid byte — this is by design, because dir is
	// used positionally (e.g. as a parent key) and changing its bytes would
	// change identity. Sanitized() is the method to use for proto fields.
	if utf8.ValidString(dir) {
		t.Fatalf("regression: dir should remain raw (%q); callers needing a clean path must use Sanitized()", dir)
	}
}

// TestFullPathBackslashNotSeparator ensures a literal backslash in a filer
// path component is treated as a regular character, not as a path separator.
// Filer paths always use "/" as the separator; using filepath.Split (which
// treats "\" as a separator on Windows) corrupts paths that contain literal
// backslashes (#11243). path.Split is OS-independent and only splits on "/".
func TestFullPathBackslashNotSeparator(t *testing.T) {
	tests := []struct {
		fullPath string
		wantDir  string
		wantName string
	}{
		// Backslash in the filename — must stay in the name, not split the path.
		{"/test/special\\reverseslash4.jpg", "/test", "special\\reverseslash4.jpg"},
		// Backslash in a directory component — the dir keeps it, name is after last "/".
		{"/a\\b/c.jpg", "/a\\b", "c.jpg"},
		// Multiple backslashes in the filename.
		{"/dir/a\\b\\c.txt", "/dir", "a\\b\\c.txt"},
		// Backslash at the start of the filename.
		{"/dir/\\file.txt", "/dir", "\\file.txt"},
	}
	for _, tc := range tests {
		t.Run(tc.fullPath, func(t *testing.T) {
			fp := FullPath(tc.fullPath)
			dir, name := fp.DirAndName()
			if dir != tc.wantDir {
				t.Errorf("DirAndName dir: got %q want %q", dir, tc.wantDir)
			}
			if name != tc.wantName {
				t.Errorf("DirAndName name: got %q want %q", name, tc.wantName)
			}
			if got := fp.Name(); got != tc.wantName {
				t.Errorf("Name: got %q want %q", got, tc.wantName)
			}
		})
	}
}

// TestFullPathSpecialCharactersInName ensures that special characters that are
// valid in filenames (#, ?, %) are preserved by DirAndName and Name. These
// characters must be percent-encoded by the client when constructing the HTTP
// request URL, but once decoded by the server they are regular path bytes.
// The "/" in the path is always a separator; special chars in a directory
// component stay in the dir, and the last component is the name.
func TestFullPathSpecialCharactersInName(t *testing.T) {
	tests := []struct {
		fullPath string
		wantDir  string
		wantName string
	}{
		// "#" in a directory component — dir keeps it, name is after last "/".
		{"/test/special#/endhashfolder.jpg", "/test/special#", "endhashfolder.jpg"},
		// "?" in a directory component.
		{"/test/special?/endqfolder.jpg", "/test/special?", "endqfolder.jpg"},
		// "%" in a directory component.
		{"/test/special%/endpctfolder.jpg", "/test/special%", "endpctfolder.jpg"},
		// "&" in a directory component.
		{"/test/special&/endampfolder.jpg", "/test/special&", "endampfolder.jpg"},
		// "=" in a directory component.
		{"/test/special=/endeqfolder.jpg", "/test/special=", "endeqfolder.jpg"},
		// Special chars in the filename itself (no "/" after them).
		{"/test/hash#file.jpg", "/test", "hash#file.jpg"},
		{"/test/q?file.jpg", "/test", "q?file.jpg"},
		{"/test/pct%file.jpg", "/test", "pct%file.jpg"},
	}
	for _, tc := range tests {
		t.Run(tc.fullPath, func(t *testing.T) {
			fp := FullPath(tc.fullPath)
			dir, name := fp.DirAndName()
			if dir != tc.wantDir {
				t.Errorf("DirAndName dir: got %q want %q", dir, tc.wantDir)
			}
			if name != tc.wantName {
				t.Errorf("DirAndName name: got %q want %q", name, tc.wantName)
			}
			if got := fp.Name(); got != tc.wantName {
				t.Errorf("Name: got %q want %q", got, tc.wantName)
			}
		})
	}
}

// TestJoinPreservesBackslash ensures Join does not convert backslashes to
// forward slashes — they are regular filename characters in filer paths.
func TestJoinPreservesBackslash(t *testing.T) {
	got := Join("/parent", "child\\name.txt")
	want := "/parent/child\\name.txt"
	if got != want {
		t.Fatalf("Join: got %q want %q", got, want)
	}
}
