package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// A flagged owner reads back as recently unreachable; an unflagged one does not.
func TestOwnerRecentlyUnreachable(t *testing.T) {
	s3a := &S3ApiServer{}
	owner := pb.ServerAddress("127.0.0.1:8888.18888")

	if s3a.ownerRecentlyUnreachable(owner) {
		t.Fatal("unmarked owner should not be flagged")
	}
	s3a.markOwnerUnreachable(owner)
	if !s3a.ownerRecentlyUnreachable(owner) {
		t.Fatal("marked owner should be flagged within the TTL")
	}
	if s3a.ownerRecentlyUnreachable(pb.ServerAddress("127.0.0.1:9999.19999")) {
		t.Fatal("a different owner should not be flagged")
	}
}
