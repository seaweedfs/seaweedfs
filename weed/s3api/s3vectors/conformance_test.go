package s3vectors

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// headerAuth names the caller per request so a policy test can act as an
// identity the policy can deny; an owner bypasses policy evaluation.
type headerAuth struct{}

func (headerAuth) AuthenticateRequest(r *http.Request) (string, interface{}, s3err.ErrorCode) {
	return r.Header.Get("X-Test-Identity"), nil, s3err.ErrNone
}
func (headerAuth) DefaultAllow() bool { return true }

// The conformance round-trip runs the whole stack a client sees: HTTP ->
// dispatch -> policy evaluation -> catalog -> engine stub.
func TestVectorBucketPolicyEndToEnd(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), headerAuth{}, newFakeVectors(0))
	router := mux.NewRouter()
	s.RegisterRoutes(router)
	ts := httptest.NewServer(router)
	defer ts.Close()

	post := func(identity, op string, payload interface{}) (int, string) {
		t.Helper()
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal %s: %v", op, err)
		}
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("build %s: %v", op, err)
		}
		req.Header.Set("X-Amz-Target", "AmazonS3Vectors."+op)
		if identity != "" {
			req.Header.Set("X-Test-Identity", identity)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post %s: %v", op, err)
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(out)
	}
	want := func(identity, op string, payload interface{}, code int) {
		t.Helper()
		if got, body := post(identity, op, payload); got != code {
			t.Fatalf("%s as %q = %d %s, want %d", op, identity, got, body, code)
		}
	}

	want("", "CreateVectorBucket", &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}, http.StatusOK)
	want("", "CreateIndex", &CreateIndexRequest{
		VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3,
	}, http.StatusOK)
	put := &PutVectorsRequest{
		VectorBucketName: "vec-bucket", IndexName: "idx",
		Vectors: []*PutInputVector{{Key: "a", Data: &VectorData{Float32: []float32{1, 2, 3}}}},
	}
	get := &GetVectorsRequest{VectorBucketName: "vec-bucket", IndexName: "idx", Keys: []string{"a"}}
	bucketArn := s.vectorBucketArn("vec-bucket")
	putPolicy := func(doc string) {
		t.Helper()
		want("", "PutVectorBucketPolicy", &PutVectorBucketPolicyRequest{
			VectorBucketName: "vec-bucket", Policy: doc,
		}, http.StatusOK)
	}

	// A non-owner has no grant without a policy; an Allow on the bucket names
	// every index under it.
	want("writer", "PutVectors", put, http.StatusForbidden)
	putPolicy(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:*","Resource":"` + bucketArn + `/*"}]}`)
	want("writer", "PutVectors", put, http.StatusOK)
	want("reader", "GetVectors", get, http.StatusOK)

	// Deny vetoes an Allow on the same action; the read grant survives.
	putPolicy(`{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":"s3vectors:Get*","Resource":"` + bucketArn + `/*"},` +
		`{"Effect":"Deny","Principal":"*","Action":"s3vectors:PutVectors","Resource":"` + bucketArn + `/*"}]}`)
	want("writer", "PutVectors", put, http.StatusForbidden)
	want("reader", "GetVectors", get, http.StatusOK)

	// Removing the policy restores owner-only access: the writer loses its
	// grant while the owner keeps full control.
	want("", "DeleteVectorBucketPolicy", &DeleteVectorBucketPolicyRequest{VectorBucketName: "vec-bucket"}, http.StatusOK)
	want("writer", "PutVectors", put, http.StatusForbidden)
	want("", "PutVectors", put, http.StatusOK)
}
