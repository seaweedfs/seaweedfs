package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// TestDirectoryListedAsCommonPrefix tests that regular directories (without MIME)
// are correctly listed as CommonPrefixes when delimiter="/" is used, not as objects.
// Note: The fix applies to any non-empty delimiter ('/', '_', ':'), but this test focuses on '/'.
func TestDirectoryListedAsCommonPrefix(t *testing.T) {
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}

	// Regular directory (no MIME) - matching actual user metadata
	regularDir := &filer_pb.Entry{
		Name:        "f2f1237f-0e69-4e0b-8f01-d4fa299787e1.vhd",
		IsDirectory: true,
		Attributes: &filer_pb.FuseAttributes{
			Mime: "", // Empty MIME - IsDirectoryKeyObject() returns false
		},
	}

	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/xoa-bucket/xo-vm-backups/data": {regularDir},
		},
	}

	cursor := &ListingCursor{maxKeys: 1000}
	var seenDirs []string
	var seenFiles []string

	_, err := s3a.doListFilerEntries(
		client,
		"/buckets/xoa-bucket/xo-vm-backups/data",
		"", // prefix
		cursor,
		"",  // marker
		"/", // delimiter="/" - should yield directory for CommonPrefix processing
		false,
		"xoa-bucket",
		func(dir string, entry *filer_pb.Entry) {
			if entry.IsDirectory {
				seenDirs = append(seenDirs, entry.Name)
			} else {
				seenFiles = append(seenFiles, entry.Name)
			}
		},
	)

	assert.NoError(t, err)

	// The directory should be passed to the callback for delimiter="/"
	assert.Contains(t, seenDirs, "f2f1237f-0e69-4e0b-8f01-d4fa299787e1.vhd",
		"Directory should be passed to callback for CommonPrefix processing with delimiter=/")
	assert.Empty(t, seenFiles, "No files should be seen, only the directory")
}

// TestEmptyDirectorySurfacedAsMarker reproduces the hadoop-aws / Spark case where a
// real but empty directory (created via mount, mkdir or the filer API, so it has no
// MIME) must be visible to S3 clients that detect directories by listing under the
// "<dir>/" prefix. Such a directory is surfaced as a directory marker, identical to
// one created via PutObject with a trailing "/".
func TestEmptyDirectorySurfacedAsMarker(t *testing.T) {
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{BucketsPath: "/buckets"},
	}

	emptyDir := &filer_pb.Entry{
		Name:        "logs",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{Mime: ""},
	}
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":      {emptyDir},
			"/buckets/test/logs": {}, // empty directory has no children
		},
	}

	// Mirrors the getFileStatus probe: prefix "logs/" with delimiter "/".
	cursor := &ListingCursor{maxKeys: 1000, prefixEndsOnDelimiter: true}
	var seen []*filer_pb.Entry
	_, err := s3a.doListFilerEntries(client, "/buckets/test", "logs", cursor, "", "/", false, "test",
		func(dir string, entry *filer_pb.Entry) {
			seen = append(seen, entry)
		})

	assert.NoError(t, err)
	if assert.Len(t, seen, 1, "empty directory must be surfaced under the logs/ prefix") {
		assert.True(t, seen[0].IsDirectoryKeyObject(),
			"empty directory must be marked as a directory key object so it lists as logs/")
		assert.Equal(t, s3_constants.FolderMimeType, seen[0].Attributes.Mime)
	}
}

// TestNonEmptyDirectoryGetsNoPhantomMarker ensures the empty-directory fix does not add
// a spurious marker for directories that already have children; the children represent
// the directory.
func TestNonEmptyDirectoryGetsNoPhantomMarker(t *testing.T) {
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{BucketsPath: "/buckets"},
	}

	dir := &filer_pb.Entry{
		Name:        "data",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{Mime: ""},
	}
	child := &filer_pb.Entry{
		Name:        "a.txt",
		IsDirectory: false,
		Attributes:  &filer_pb.FuseAttributes{},
	}
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":      {dir},
			"/buckets/test/data": {child},
		},
	}

	// Recursive listing under the "data/" prefix.
	cursor := &ListingCursor{maxKeys: 1000, prefixEndsOnDelimiter: true}
	var seen []string
	_, err := s3a.doListFilerEntries(client, "/buckets/test", "data", cursor, "", "", false, "test",
		func(dir string, entry *filer_pb.Entry) {
			seen = append(seen, entry.Name)
		})

	assert.NoError(t, err)
	assert.Equal(t, []string{"a.txt"}, seen, "non-empty directory must be represented by its child only")
}

// TestEmptyDirectoryHiddenInFlatListing ensures the marker is only surfaced for an
// explicit "<dir>/" probe, not in a plain (no prefix, no delimiter) listing. An empty
// directory left behind by deleted objects (e.g. after lifecycle expiration) must not
// appear as a phantom key, matching AWS S3.
func TestEmptyDirectoryHiddenInFlatListing(t *testing.T) {
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{BucketsPath: "/buckets"},
	}

	emptyDir := &filer_pb.Entry{
		Name:        "expire1",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{Mime: ""},
	}
	keepDir := &filer_pb.Entry{
		Name:        "keep2",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{Mime: ""},
	}
	keepObj := &filer_pb.Entry{
		Name:        "foo",
		IsDirectory: false,
		Attributes:  &filer_pb.FuseAttributes{},
	}
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":         {emptyDir, keepDir},
			"/buckets/test/expire1": {},
			"/buckets/test/keep2":   {keepObj},
		},
	}

	// Plain flat listing: no prefix, no delimiter.
	cursor := &ListingCursor{maxKeys: 1000}
	var seen []string
	_, err := s3a.doListFilerEntries(client, "/buckets/test", "", cursor, "", "", false, "test",
		func(dir string, entry *filer_pb.Entry) {
			seen = append(seen, entry.Name)
		})

	assert.NoError(t, err)
	assert.Equal(t, []string{"foo"}, seen, "flat listing must hide the empty directory and return only real objects")
}
