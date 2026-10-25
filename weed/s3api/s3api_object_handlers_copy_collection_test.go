package s3api

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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

	// CopyObject (including the SSE-C paths) and UploadPartCopy both assign
	// against the destination object path.
	objPath := fmt.Sprintf("%s/%s", s3a.bucketDir(bucket), "blobs/data")
	if got := f.DetectBucket(util.FullPath(objPath)); got != bucket {
		t.Fatalf("CopyObject dst path %q resolved to collection %q, want %q", objPath, got, bucket)
	}

	// The part entry itself still lives under .uploads, which maps to the same
	// collection, so the entry write is unaffected either way.
	uploadDir, partName := s3a.copyPartLocation(bucket, "uploadid", 1)
	partPath := uploadDir + "/" + partName
	if got := f.DetectBucket(util.FullPath(partPath)); got != bucket {
		t.Fatalf("UploadPartCopy part path %q resolved to collection %q, want %q", partPath, got, bucket)
	}
}

// TestMultipartStorageRuleFollowsDestinationObject guards the filer.conf storage
// rule a multipart part's chunks are placed by.
//
// Parts stage under /buckets/<bucket>/.uploads/..., so a rule scoped to a key
// prefix ("place /buckets/b/data/ on a 30d TTL volume") matches the object but
// not the part. Assigning against the part path therefore scattered a large
// object's bytes onto TTL-less volumes while its entry carried the rule's TTL.
// The gateway assigns against the destination object instead, the way the
// x-seaweedfs-destination header made the filer resolve it before the S3 write
// path moved off the filer proxy.
func TestMultipartStorageRuleFollowsDestinationObject(t *testing.T) {
	const bucket, object = "b", "/data/big.bin"
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	fc := filer.NewFilerConf()
	fc.AddLocationConf(&filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/b/data/", Ttl: "30d"})

	partPath := s3a.genPartUploadPath(bucket, "uploadid", 1)
	if got := fc.MatchStorageRule(partPath).GetTtl(); got != "" {
		t.Fatalf("part path %q matched ttl %q; the test no longer reproduces the gap", partPath, got)
	}

	// What PutObjectPart and UploadPartCopy now assign against.
	dstPath := s3a.toFilerPath(bucket, object)
	if got := fc.MatchStorageRule(dstPath).GetTtl(); got != "30d" {
		t.Fatalf("destination path %q resolved ttl %q, want 30d", dstPath, got)
	}
}
