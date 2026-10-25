package s3api

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		context.Background(),
		client,
		listDirectoryRequest{
			dir:       "/buckets/xoa-bucket/xo-vm-backups/data",
			delimiter: "/", // should yield directory for CommonPrefix processing
			bucket:    "xoa-bucket",
		},
		cursor,
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

// TestEmptyDirectoryNotListedUnderOwnPrefix covers the explicit "<dir>/" probe
// on a real but empty directory (created via mount, mkdir or the filer API, so
// it has no MIME): it is listed under its own prefix in neither delimited nor
// recursive mode, matching S3 where an empty prefix has no key to return.
func TestEmptyDirectoryNotListedUnderOwnPrefix(t *testing.T) {
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

	for _, delimiter := range []string{"/", ""} {
		cursor := &ListingCursor{maxKeys: 1000, prefixEndsOnDelimiter: true}
		var seen []*filer_pb.Entry
		_, err := s3a.doListFilerEntries(context.Background(), client, listDirectoryRequest{dir: "/buckets/test", prefix: "logs", delimiter: delimiter, bucket: "test"}, cursor,
			func(dir string, entry *filer_pb.Entry) {
				seen = append(seen, entry)
			})
		assert.NoError(t, err)
		assert.Empty(t, seen, "an empty directory reports nothing under its own prefix (delimiter %q)", delimiter)
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
	_, err := s3a.doListFilerEntries(context.Background(), client, listDirectoryRequest{dir: "/buckets/test", prefix: "data", bucket: "test"}, cursor,
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
	_, err := s3a.doListFilerEntries(context.Background(), client, listDirectoryRequest{dir: "/buckets/test", bucket: "test"}, cursor,
		func(dir string, entry *filer_pb.Entry) {
			seen = append(seen, entry.Name)
		})

	assert.NoError(t, err)
	assert.Equal(t, []string{"foo"}, seen, "flat listing must hide the empty directory and return only real objects")
}

// A trailing-slash delimited listing of an empty directory enumerates its
// children, so it returns nothing: the directory is neither a readable Contents
// key nor a CommonPrefix of its own.
func TestEmptyDirectoryHiddenInDelimitedSelfListing(t *testing.T) {
	filer := &fakeBucketDeleteFiler{entriesByDir: map[string][]*filer_pb.Entry{
		"/buckets/test": {
			{Name: "locks", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: ""}},
		},
		"/buckets/test/locks": {},
	}}
	s3a := newFailoverTestServer(t, startFakeFiler(t, filer))
	s3a.option.BucketsPath = "/buckets"
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("test", &BucketConfig{Name: "test"})

	response, err := s3a.listFilerEntries(context.Background(), listObjectsRequest{
		bucket:    "test",
		prefix:    "locks/",
		delimiter: "/",
		maxKeys:   1000,
	})
	require.NoError(t, err)
	assert.Empty(t, response.Contents, "an empty directory is not a readable key")
	assert.Empty(t, response.CommonPrefixes, "an empty directory has no prefixes to fold into")
}
