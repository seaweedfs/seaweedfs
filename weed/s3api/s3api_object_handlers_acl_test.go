package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestObjectAclUpdateDirectory guards against a PutObjectAcl scope bypass: a nested object
// key must resolve to its own parent directory, not the bucket root, otherwise updating the
// ACL of allowed/protected.txt would rewrite a different protected.txt at the bucket root.
func TestObjectAclUpdateDirectory(t *testing.T) {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	const bucket = "target-bucket"
	bucketDir := "/buckets/target-bucket"

	versioned := func(id string) *filer_pb.Entry {
		return &filer_pb.Entry{Extended: map[string][]byte{s3_constants.ExtVersionIdKey: []byte(id)}}
	}

	tests := []struct {
		name                 string
		object               string
		versioningConfigured bool
		versionId            string
		entry                *filer_pb.Entry
		want                 string
	}{
		{"non-versioned nested key", "allowed/protected.txt", false, "", &filer_pb.Entry{}, bucketDir + "/allowed"},
		{"non-versioned root key", "protected.txt", false, "", &filer_pb.Entry{}, bucketDir},
		{"non-versioned deep key", "a/b/c/protected.txt", false, "", &filer_pb.Entry{}, bucketDir + "/a/b/c"},
		{"null version nested key", "allowed/protected.txt", true, "", versioned("null"), bucketDir + "/allowed"},
		{"empty version nested key", "allowed/protected.txt", true, "", &filer_pb.Entry{}, bucketDir + "/allowed"},
		{"specific version nested key", "allowed/protected.txt", true, "v1", versioned("v1"), bucketDir + "/allowed/protected.txt" + s3_constants.VersionsFolder},
		{"latest versioned nested key", "allowed/protected.txt", true, "", versioned("v1"), bucketDir + "/allowed/protected.txt" + s3_constants.VersionsFolder},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s3a.objectAclUpdateDirectory(bucket, tt.object, tt.versioningConfigured, tt.versionId, tt.entry)
			if got != tt.want {
				t.Errorf("objectAclUpdateDirectory(%q) = %q, want %q", tt.object, got, tt.want)
			}
		})
	}
}
