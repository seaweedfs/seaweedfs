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
