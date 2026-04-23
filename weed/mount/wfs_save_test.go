package mount

import (
	"strings"
	"syscall"
	"testing"
	"unicode/utf8"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// TestSanitizeFuseName_InvalidBytesReplaced reproduces the filename from
// seaweedfs#9139: GNOME Trash "partial" files carry raw binary bytes
// (\x10\x98=\\\x8a\x7f) that are not valid UTF-8. The sanitizer must return a
// UTF-8-valid string so the subsequent proto marshal cannot fail.
func TestSanitizeFuseName_InvalidBytesReplaced(t *testing.T) {
	raw := "\x10\x98=\\\x8a\x7f.trashinfo.9a51454f.partial"
	out := sanitizeFuseName(raw)
	if !utf8.ValidString(out) {
		t.Fatalf("sanitizeFuseName returned non-UTF-8: %q", out)
	}
	if strings.ContainsRune(out, 0xFFFD) {
		t.Fatalf("expected '?' replacement to match util.FullPath.DirAndName, got U+FFFD")
	}
	// Valid bytes (\x10, \x3D '=', \x5C '\\', \x7F) must be preserved; only
	// \x98 and \x8A — the standalone continuation bytes — get replaced.
	if !strings.HasSuffix(out, ".trashinfo.9a51454f.partial") {
		t.Fatalf("trailing valid bytes were dropped: %q", out)
	}
}

func TestSanitizeFuseName_PassThroughValidUTF8(t *testing.T) {
	// An already-valid UTF-8 string must be returned unchanged — no heap
	// allocation, no alteration of byte content. Preserving identity matters
	// because the overwhelming hot path is valid input.
	for _, s := range []string{
		"plain.txt",
		"日本語.txt",
		"🦑 squid",
		"",
		strings.Repeat("a", 255),
	} {
		if got := sanitizeFuseName(s); got != s {
			t.Fatalf("sanitizeFuseName(%q) = %q, want unchanged", s, got)
		}
	}
}

// TestCheckName_SanitizesBeforeLengthCheck verifies the caller contract: the
// returned name is always safe to put into a proto string field, and the
// length guard still fires on over-long inputs.
func TestCheckName_SanitizesBeforeLengthCheck(t *testing.T) {
	bad := "foo\x80bar"
	got, s := checkName(bad)
	if s != fuse.OK {
		t.Fatalf("checkName(%q) status = %v, want OK", bad, s)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("checkName did not sanitize: %q", got)
	}

	tooLong := strings.Repeat("x", 300)
	if _, s := checkName(tooLong); s != fuse.Status(syscall.ENAMETOOLONG) {
		t.Fatalf("checkName length guard lost: status=%v", s)
	}
}
