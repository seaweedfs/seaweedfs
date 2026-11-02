package iceberg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

type openAuthenticator struct{}

func (openAuthenticator) AuthenticateRequest(*http.Request) (string, interface{}, s3err.ErrorCode) {
	return s3tables.DefaultAccountID, nil, s3err.ErrNone
}

func (openAuthenticator) DefaultAllow() bool { return true }

// An index in a vector bucket is a catalog table the Iceberg catalog can load,
// but a caller holding every s3tables grant gets neither write credentials to
// it nor a commit into it.
func TestVectorIndexIsReadOnlyOverIceberg(t *testing.T) {
	filer := s3tablestest.Start(t)
	vendor := &stubVendor{credentials: &VendedCredentials{AccessKeyID: "ASIAEXAMPLE"}}
	s := NewServer(s3tables.NewManagerClient(filer.Client), openAuthenticator{})
	s.SetCredentialVendor(vendor)
	s.s3Endpoint = "http://127.0.0.1:8333"
	router := mux.NewRouter().SkipClean(true)
	s.RegisterRoutes(router)

	admin := s3tables.NewManager()
	admin.SetTrusted(true)
	client := s3tables.NewManagerClient(filer.Client)
	arn, err := s3tables.BuildBucketARN(s3tables.DefaultRegion, s3tables.DefaultAccountID, "vec")
	if err != nil {
		t.Fatal(err)
	}
	metadata, _ := json.Marshal(map[string]any{"dimension": 4, "distanceMetric": "cosine"})
	for _, op := range []struct {
		name string
		req  interface{}
	}{
		{"CreateTableBucket", &s3tables.CreateTableBucketRequest{Name: "vec", Format: s3tables.FormatLance}},
		{"CreateNamespace", &s3tables.CreateNamespaceRequest{TableBucketARN: arn, Namespace: []string{s3tables.VectorIndexNamespace}}},
		{"CreateTable", &s3tables.CreateTableRequest{TableBucketARN: arn, Namespace: []string{s3tables.VectorIndexNamespace},
			Name: "idx", Format: s3tables.FormatLance, Metadata: &s3tables.TableMetadata{FullMetadata: metadata}}},
	} {
		if err := admin.Execute(t.Context(), client, op.name, op.req, nil, ""); err != nil {
			t.Fatalf("%s: %v", op.name, err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/vec/namespaces/vector/tables/idx", nil)
	req.Header.Set(accessDelegationHeader, "vended-credentials")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("load = %d (%s)", rec.Code, rec.Body.String())
	}
	if vendor.gotPrefix != "vector/idx" || !vendor.gotReadOnly {
		t.Errorf("vended prefix %q read-only %v, want vector/idx read-only", vendor.gotPrefix, vendor.gotReadOnly)
	}

	reqErr := s.authorizeCommit(t.Context(), arn, []string{s3tables.VectorIndexNamespace}, "idx", s3tables.DefaultAccountID)
	if reqErr == nil || reqErr.status != http.StatusForbidden {
		t.Errorf("commit authorization = %+v, want 403", reqErr)
	}
}

// Stage-create registers nothing, so the catalog's own location and vector
// namespace checks never see it. It must not hand out write credentials to an
// index, whether the staged table names the index's location from another
// namespace or sits in the vector namespace itself.
func TestStageCreateCannotReachVectorIndex(t *testing.T) {
	filer := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(filer.Client), openAuthenticator{})
	s.s3Endpoint = "http://127.0.0.1:8333"
	router := mux.NewRouter().SkipClean(true)
	s.RegisterRoutes(router)

	admin := s3tables.NewManager()
	admin.SetTrusted(true)
	client := s3tables.NewManagerClient(filer.Client)
	arn, err := s3tables.BuildBucketARN(s3tables.DefaultRegion, s3tables.DefaultAccountID, "vec")
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []struct {
		name string
		req  interface{}
	}{
		{"CreateTableBucket", &s3tables.CreateTableBucketRequest{Name: "vec", Format: s3tables.FormatLance}},
		{"CreateNamespace", &s3tables.CreateNamespaceRequest{TableBucketARN: arn, Namespace: []string{s3tables.VectorIndexNamespace}}},
		{"CreateNamespace", &s3tables.CreateNamespaceRequest{TableBucketARN: arn, Namespace: []string{"other"}}},
		{"CreateTable", &s3tables.CreateTableRequest{TableBucketARN: arn, Namespace: []string{s3tables.VectorIndexNamespace},
			Name: "idx", Format: s3tables.FormatLance}},
	} {
		if err := admin.Execute(t.Context(), client, op.name, op.req, nil, ""); err != nil {
			t.Fatalf("%s: %v", op.name, err)
		}
	}

	for name, tc := range map[string]struct {
		namespace, body string
		status          int
	}{
		"index location from another namespace": {"other", `{"name":"x","stage-create":true,"location":"s3://vec/vector/idx"}`, http.StatusBadRequest},
		"vector namespace":                      {s3tables.VectorIndexNamespace, `{"name":"y","stage-create":true}`, http.StatusForbidden},
	} {
		t.Run(name, func(t *testing.T) {
			vendor := &stubVendor{credentials: &VendedCredentials{AccessKeyID: "ASIAEXAMPLE"}}
			s.SetCredentialVendor(vendor)
			req := httptest.NewRequest(http.MethodPost, "/v1/vec/namespaces/"+tc.namespace+"/tables", strings.NewReader(tc.body))
			req.Header.Set(accessDelegationHeader, "vended-credentials")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Errorf("stage-create = %d (%s), want %d", rec.Code, rec.Body.String(), tc.status)
			}
			if vendor.gotPrefix != "" && !vendor.gotReadOnly {
				t.Errorf("vended write credentials to %q", vendor.gotPrefix)
			}
		})
	}
}
