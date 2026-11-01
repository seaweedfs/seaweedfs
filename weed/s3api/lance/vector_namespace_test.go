package lance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

// seedVectorIndex creates bucket/vector/idx through the trusted path, the way
// the S3 Vectors gateway does before it asks the engine to build the index.
func (h *testHarness) seedVectorIndex(t *testing.T, bucket, format string) {
	t.Helper()
	h.createBucket(t, bucket, format)
	client := s3tables.NewManagerClient(h.filer.Client)
	for _, op := range []struct {
		name string
		req  interface{}
	}{
		{"CreateNamespace", &s3tables.CreateNamespaceRequest{TableBucketARN: h.bucketARN(t, bucket), Namespace: []string{s3tables.VectorIndexNamespace}}},
		{"CreateNamespace", &s3tables.CreateNamespaceRequest{TableBucketARN: h.bucketARN(t, bucket), Namespace: []string{"other"}}},
		{"CreateTable", &s3tables.CreateTableRequest{TableBucketARN: h.bucketARN(t, bucket), Namespace: []string{s3tables.VectorIndexNamespace},
			Name: "idx", Format: s3tables.FormatLance}},
	} {
		if err := h.admin.Execute(t.Context(), client, op.name, op.req, nil, ""); err != nil {
			t.Fatalf("%s: %v", op.name, err)
		}
	}
}

// Through the Lance namespace the vector namespace of a vector bucket is
// read-only too, while the engine's declare of an index the gateway already
// created keeps working.
func TestVectorNamespaceIsReadOnlyOverLance(t *testing.T) {
	h := newTestHarness(t)
	h.seedVectorIndex(t, "vec", s3tables.FormatLance)
	idx := "/v1/table/vec$vector$idx"

	h.mustDo(t, http.MethodPost, idx+"/declare", `{"id":["vec","vector","idx"]}`, http.StatusOK)
	h.mustDo(t, http.MethodPost, idx+"/describe", `{}`, http.StatusOK)

	for _, tc := range []struct{ name, target, body string }{
		{"declare a new index", "/v1/table/vec$vector$fresh/declare", `{}`},
		{"register a new index", "/v1/table/vec$vector$fresh/register", `{"location":"s3://vec/vector/fresh"}`},
		{"register over the index", idx + "/register", `{"location":"s3://vec/vector/idx","mode":"overwrite"}`},
		{"repoint the index", idx + "/register", `{"location":"s3://vec/vector/elsewhere","mode":"overwrite"}`},
		{"deregister", idx + "/deregister", `{}`},
		{"drop", idx + "/drop", `{}`},
		{"rename out", idx + "/rename", `{"new_id":["vec","other","moved"]}`},
		{"drop the namespace", "/v1/namespace/vec$vector/drop", `{}`},
		{"overwrite the namespace", "/v1/namespace/vec$vector/create", `{"mode":"overwrite"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h.mustDo(t, http.MethodPost, tc.target, tc.body, http.StatusForbidden)
		})
	}

	if h.filer.Get("/buckets/vec/vector/idx", deregisteredMarker) != nil {
		t.Fatal("a refused deregister left its marker")
	}
	h.mustDo(t, http.MethodPost, idx+"/exists", `{}`, http.StatusOK)

	h.createBucket(t, "fresh", s3tables.FormatLance)
	h.mustDo(t, http.MethodPost, "/v1/namespace/fresh$vector/create", `{}`, http.StatusForbidden)
}

// A bucket from before formats existed is not a vector bucket here, so its
// namespace named vector keeps the ordinary Lance lifecycle.
func TestVectorNamespaceOfUndeclaredBucketIsWritableOverLance(t *testing.T) {
	h := newTestHarness(t)
	h.seedVectorIndex(t, "old", "")
	idx := "/v1/table/old$vector$idx"

	h.mustDo(t, http.MethodPost, idx+"/deregister", `{}`, http.StatusOK)
	h.mustDo(t, http.MethodPost, idx+"/register", `{"location":"s3://old/vector/idx"}`, http.StatusOK)
	h.mustDo(t, http.MethodPost, idx+"/drop", `{}`, http.StatusOK)
}

// recordingVendor vends a credential whose key says whether it may write and
// whose secret is the prefix it covers.
type recordingVendor struct{}

func (recordingVendor) VendTableCredentials(_ context.Context, _, _, prefix string, readOnly bool) (*VendedCredentials, error) {
	if readOnly {
		return &VendedCredentials{AccessKeyID: "read-only", SecretAccessKey: prefix}, nil
	}
	return &VendedCredentials{AccessKeyID: "read-write", SecretAccessKey: prefix}, nil
}

func (h *testHarness) vendedKey(t *testing.T, target, token string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(`{"vend_credentials":true}`))
	if token != "" {
		req.Header.Set(clusterTokenHeader, token)
	}
	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST %s = %d (%s)", target, rec.Code, rec.Body.String())
	}
	return decode[DescribeTableResponse](t, rec).StorageOptions["aws_access_key_id"]
}

// A caller holding every s3tables grant still gets read-only credentials to a
// vector bucket's index data; only the cluster's own components can write it.
func TestVectorNamespaceVendsWriteCredentialsOnlyToTrustedCallers(t *testing.T) {
	h := newTestHarness(t)
	key := security.SigningKey("cluster-key")
	h.server.SetSigningKey(func() security.SigningKey { return key })
	h.server.SetCredentialVendor(recordingVendor{})
	h.seedVectorIndex(t, "vec", s3tables.FormatLance)
	h.mustDo(t, http.MethodPost, "/v1/table/vec$other$tbl/declare", `{}`, http.StatusOK)
	h.seedVectorIndex(t, "old", "")

	h.vendedKey(t, "/v1/table/vec$vector$idx/declare", "")
	if h.filer.Get("/buckets/vec/vector/idx", reservedMarker) != nil {
		t.Error("an untrusted declare wrote into the index")
	}

	trusted := string(security.GenJwtForFilerAdmin(clusterTokenKey(key), 60))
	forged := string(security.GenJwtForFilerAdmin(key, 60))
	for _, tc := range []struct {
		name, target, token, want string
	}{
		{"describe the index", "/v1/table/vec$vector$idx/describe", "", "read-only"},
		{"declare the index", "/v1/table/vec$vector$idx/declare", "", "read-only"},
		{"describe with an admin token", "/v1/table/vec$vector$idx/describe", forged, "read-only"},
		{"trusted describe", "/v1/table/vec$vector$idx/describe", trusted, "read-write"},
		{"trusted declare", "/v1/table/vec$vector$idx/declare", trusted, "read-write"},
		{"another namespace", "/v1/table/vec$other$tbl/describe", "", "read-write"},
		{"bucket from before formats", "/v1/table/old$vector$idx/describe", "", "read-write"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := h.vendedKey(t, tc.target, tc.token); got != tc.want {
				t.Errorf("vended %q, want %q", got, tc.want)
			}
		})
	}
}

// Declaring a name that already exists answers for that table, so naming the
// index's location on another table gets no credentials to the index.
func TestDeclareOfAnExistingTableKeepsItsLocation(t *testing.T) {
	h := newTestHarness(t)
	h.server.SetCredentialVendor(recordingVendor{})
	h.seedVectorIndex(t, "vec", s3tables.FormatLance)
	h.mustDo(t, http.MethodPost, "/v1/table/vec$other$tbl/declare", `{}`, http.StatusOK)

	rec := h.mustDo(t, http.MethodPost, "/v1/table/vec$other$tbl/declare",
		`{"location":"s3://vec/vector/idx","vend_credentials":true}`, http.StatusOK)
	resp := decode[DeclareTableResponse](t, rec)
	if resp.Location != "s3://vec/other/tbl" {
		t.Errorf("location = %q, want the table's own s3://vec/other/tbl", resp.Location)
	}
	if got := resp.StorageOptions["aws_secret_access_key"]; got != "other/tbl" {
		t.Errorf("credentials cover %q, want other/tbl", got)
	}
	if h.filer.Get("/buckets/vec/vector/idx", reservedMarker) != nil {
		t.Error("the declare wrote into the index")
	}
}
