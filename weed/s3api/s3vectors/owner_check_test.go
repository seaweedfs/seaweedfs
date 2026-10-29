package s3vectors

import (
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// The zero-config deployment authenticates every request as "admin"; the
// principal the vector checks compute must match the owner the catalog
// records at creation.
func TestAdminNamedIdentityReachesOwnBucket(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
	if w := postJSONAs(t, "admin", s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "admin", s.handleGetVectorBucket, &GetVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("get denied after admin-named create: %d %s", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "admin", s.handleCreateIndex, &CreateIndexRequest{
		VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "euclidean",
	}); w.Code != http.StatusOK {
		t.Fatalf("create index denied after admin-named create: %d %s", w.Code, w.Body.String())
	}
}
