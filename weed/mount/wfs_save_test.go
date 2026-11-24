package mount

import (
	"strings"
	"syscall"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

func TestCheckNameRefusesInvalidUTF8(t *testing.T) {
	for _, bad := range []string{
		"report-\xd6\xd0.txt",
		"a\xff.txt",
		"\x10\x98=\\\x8a\x7f.trashinfo.9a51454f.partial",
	} {
		got, s := checkName(bad)
		if s != fuse.Status(syscall.EILSEQ) {
			t.Fatalf("checkName(%q) status = %v, want EILSEQ", bad, s)
		}
		if got != bad {
			t.Fatalf("checkName(%q) rewrote the name to %q", bad, got)
		}
	}
}

func TestCheckNamePassesValidNames(t *testing.T) {
	for _, name := range []string{
		"plain.txt",
		"a_.txt",
		"日本語.txt",
		"🦑 squid",
		strings.Repeat("a", 255),
	} {
		if got, s := checkName(name); s != fuse.OK || got != name {
			t.Fatalf("checkName(%q) = %q, %v, want unchanged and OK", name, got, s)
		}
	}
	if _, s := checkName(strings.Repeat("x", 300)); s != fuse.Status(syscall.ENAMETOOLONG) {
		t.Fatalf("checkName length guard lost: status=%v", s)
	}
}

// A name that is not valid UTF-8 never names an entry: making one fails with
// EILSEQ, and looking one up or removing it answers ENOENT, all before any
// path resolution or filer request.
func TestInvalidUTF8NamesNeverReachTheFiler(t *testing.T) {
	wfs := &WFS{option: &Option{}}
	bad := "a\xff.txt"
	header := fuse.InHeader{NodeId: 1}
	eilseq := fuse.Status(syscall.EILSEQ)

	cases := []struct {
		op   string
		got  fuse.Status
		want fuse.Status
	}{
		{"lookup", wfs.Lookup(nil, &header, bad, &fuse.EntryOut{}), fuse.ENOENT},
		{"unlink", wfs.Unlink(nil, &header, bad), fuse.ENOENT},
		{"rmdir", wfs.Rmdir(nil, &header, bad), fuse.ENOENT},
		{"rename source", wfs.Rename(nil, &fuse.RenameIn{InHeader: header, Newdir: 1}, bad, "b.txt"), fuse.ENOENT},
		{"rename target", wfs.Rename(nil, &fuse.RenameIn{InHeader: header, Newdir: 1}, "b.txt", bad), eilseq},
		{"create", wfs.Create(nil, &fuse.CreateIn{InHeader: header}, bad, &fuse.CreateOut{}), eilseq},
		{"mknod", wfs.Mknod(nil, &fuse.MknodIn{InHeader: header}, bad, &fuse.EntryOut{}), eilseq},
		{"mkdir", wfs.Mkdir(nil, &fuse.MkdirIn{InHeader: header}, bad, &fuse.EntryOut{}), eilseq},
		{"symlink", wfs.Symlink(nil, &header, "target", bad, &fuse.EntryOut{}), eilseq},
		{"link", wfs.Link(nil, &fuse.LinkIn{InHeader: header, Oldnodeid: 2}, bad, &fuse.EntryOut{}), eilseq},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s %q: status %v, want %v", c.op, bad, c.got, c.want)
		}
	}
}
