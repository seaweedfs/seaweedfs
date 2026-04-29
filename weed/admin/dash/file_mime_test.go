package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestResolveEntryMimePrefersStoredMime(t *testing.T) {
	entry := &filer_pb.Entry{
		Name: "report.txt",
		Attributes: &filer_pb.FuseAttributes{
			Mime: " application/pdf; charset=binary ",
		},
	}

	if got := ResolveEntryMime(entry); got != "application/pdf" {
		t.Fatalf("ResolveEntryMime() = %q, want %q", got, "application/pdf")
	}
}

func TestResolveEntryMimePrefersStoredMimeMalformedParameter(t *testing.T) {
	entry := &filer_pb.Entry{
		Name: "report.txt",
		Attributes: &filer_pb.FuseAttributes{
			Mime: "APPLICATION/PDF; bad",
		},
	}

	if got := ResolveEntryMime(entry); got != "application/pdf" {
		t.Fatalf("ResolveEntryMime() = %q, want %q", got, "application/pdf")
	}
}

func TestResolveEntryMimeFallsBackToFilename(t *testing.T) {
	entry := &filer_pb.Entry{Name: "archive.zip"}

	if got := ResolveEntryMime(entry); got != "application/zip" {
		t.Fatalf("ResolveEntryMime() = %q, want %q", got, "application/zip")
	}
}

func TestResolveEntryMimeReturnsDirectoryMime(t *testing.T) {
	entry := &filer_pb.Entry{
		Name:        "folder",
		IsDirectory: true,
		Attributes: &filer_pb.FuseAttributes{
			Mime: "application/json",
		},
	}

	if got := ResolveEntryMime(entry); got != "inode/directory" {
		t.Fatalf("ResolveEntryMime() = %q, want %q", got, "inode/directory")
	}
}
