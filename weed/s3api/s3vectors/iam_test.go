package s3vectors

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

type iamAuthenticator struct {
	fakeAuthenticator
	allowed, denied bool
}

func (a *iamAuthenticator) EvaluateIAMPolicy(*http.Request, string, string, *s3tables.PolicyContext) (bool, bool) {
	return a.allowed, a.denied
}

func TestIAMGrantRespectsVectorResourceDenies(t *testing.T) {
	auth := &iamAuthenticator{allowed: true}
	server := &Server{authenticator: auth}
	request := httptest.NewRequest("POST", "/QueryVectors", nil)
	const action = "s3vectors:QueryVectors"
	const resource = "arn:aws:s3vectors:us-east-1:000000000000:bucket/embeddings/index/docs"
	ctx := &s3tables.PolicyContext{IdentityActions: []string{}}
	if !server.checkPermission(request, action, "reader", "owner", "", resource, ctx) {
		t.Fatal("IAM grant denied")
	}
	deny := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3vectors:*","Resource":"*"}]}`
	if server.checkPermission(request, action, "reader", "owner", deny, resource, ctx) {
		t.Fatal("resource deny was overridden by IAM")
	}
	auth.denied = true
	if server.checkPermission(request, action, "owner", "owner", "", resource, ctx) {
		t.Fatal("identity deny was overridden by ownership")
	}
}

type requestTagAuthenticator struct{ fakeAuthenticator }

func (*requestTagAuthenticator) EvaluateIAMPolicy(_ *http.Request, _, _ string, ctx *s3tables.PolicyContext) (bool, bool) {
	return true, ctx.RequestTags["classification"] == "forbidden"
}

func TestIAMCreateBucketReceivesRequestTags(t *testing.T) {
	fs := s3tablestest.Start(t)
	server := NewServer(s3tables.NewManagerClient(fs.Client), &requestTagAuthenticator{}, nil)
	response := postJSONAs(t, "", server.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "blocked", Tags: map[string]string{"classification": "forbidden"}})
	if response.Code != http.StatusForbidden {
		t.Fatalf("creation tag deny bypassed: %d %s", response.Code, response.Body)
	}
}
