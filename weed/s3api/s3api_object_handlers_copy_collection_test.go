package s3api

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestCopyDestinationPathResolvesBucketCollection guards against server-side
// copies routing data into the default collection.
//
// The filer maps a write to a bucket's collection only when the AssignVolume
// Path sits under its buckets folder (/buckets/<bucket>/...). UploadPartCopy
// and SSE-C CopyObject used to assign destination volumes against r.URL.Path,
// the S3 request URI (e.g. /bucket/key), which never has that prefix; the
// copied bytes therefore landed in the default collection instead of the
// destination bucket's. The handlers must assign against the real filer path
// of the destination.
func TestCopyDestinationPathResolvesBucketCollection(t *testing.T) {
	const bucket = "docker-registry"
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	f := &filer.Filer{DirBucketsPath: "/buckets"}

	// The S3 request URI is not a filer path: the filer cannot derive the
	// destination bucket from it. This is the shape that caused the leak.
	if got := f.DetectBucket(util.FullPath("/" + bucket + "/blobs/data")); got != "" {
		t.Fatalf("S3 request URI unexpectedly mapped to collection %q; the test no longer reproduces the bug", got)
	}

	// UploadPartCopy assigns against the destination part path under .uploads.
	partPath := s3a.genUploadsFolder(bucket) + "/uploadid/" + fmt.Sprintf("%04d_%s.part", 1, "copy")
	if got := f.DetectBucket(util.FullPath(partPath)); got != bucket {
		t.Fatalf("UploadPartCopy dst path %q resolved to collection %q, want %q", partPath, got, bucket)
	}

	// CopyObject (including the SSE-C paths) assigns against the destination
	// object path.
	objPath := fmt.Sprintf("%s/%s", s3a.bucketDir(bucket), "blobs/data")
	if got := f.DetectBucket(util.FullPath(objPath)); got != bucket {
		t.Fatalf("CopyObject dst path %q resolved to collection %q, want %q", objPath, got, bucket)
	}
}
