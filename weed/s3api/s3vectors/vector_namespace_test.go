package s3vectors

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// An identity the vector policy denies DeleteIndex must not delete the index
// through the S3 Tables API instead, while the gateway's own index lifecycle
// keeps working.
func TestCatalogCannotBypassVectorPolicy(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
	if w := postJSONAs(t, "", s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	for _, name := range []string{"idx", "spare"} {
		if w := postJSONAs(t, "", s.handleCreateIndex, &CreateIndexRequest{VectorBucketName: "vec-bucket", IndexName: name,
			DataType: "float32", Dimension: 3, DistanceMetric: "euclidean"}); w.Code != http.StatusOK {
			t.Fatalf("create index %s: %d %s", name, w.Code, w.Body.String())
		}
	}
	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3vectors:DeleteIndex","Resource":"%s"}]}`,
		s.indexArn("vec-bucket", "idx"))
	if w := postJSONAs(t, "", s.handlePutVectorBucketPolicy, &PutVectorBucketPolicyRequest{VectorBucketName: "vec-bucket", Policy: policy}); w.Code != http.StatusOK {
		t.Fatalf("put policy: %d %s", w.Code, w.Body.String())
	}

	bob := &s3api.Identity{Name: "bob", Actions: []s3api.Action{"s3tables:*"}}
	ctx := s3_constants.SetIdentityInContext(s3_constants.SetIdentityNameInContext(context.Background(), bob.Name), bob)
	deleteReq := &DeleteIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx"}
	if w := postJSONAs(t, "bob", s.handleDeleteIndex, deleteReq); w.Code != http.StatusForbidden {
		t.Fatalf("gateway DeleteIndex under deny: %d %s, want 403", w.Code, w.Body.String())
	}
	err := s.tables.Execute(ctx, s.filerClient, "DeleteTable", &s3tables.DeleteTableRequest{
		TableBucketARN: s.s3tablesArn("vec-bucket"), Namespace: []string{indexNamespace}, Name: "idx"}, nil, bob.Name)
	if catalogErrCode(err) != s3tables.ErrCodeAccessDenied {
		t.Fatalf("catalog DeleteTable of a vector index: %v, want AccessDenied", err)
	}
	if _, err := s.indexRecord(context.Background(), "vec-bucket", "idx"); err != nil {
		t.Fatalf("index gone after a refused catalog delete: %v", err)
	}

	if w := postJSONAs(t, "", s.handleDeleteIndex, &DeleteIndexRequest{VectorBucketName: "vec-bucket", IndexName: "spare"}); w.Code != http.StatusOK {
		t.Fatalf("gateway DeleteIndex: %d %s", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "", s.handleDeleteVectorBucketPolicy, &DeleteVectorBucketPolicyRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("delete policy: %d %s", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "", s.handleDeleteIndex, deleteReq); w.Code != http.StatusOK {
		t.Fatalf("gateway DeleteIndex: %d %s", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "", s.handleDeleteVectorBucket, &DeleteVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("gateway DeleteVectorBucket: %d %s", w.Code, w.Body.String())
	}
}
