package iceberg

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func TestWantsVendedCredentials(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   bool
	}{
		{name: "no header", want: false},
		{name: "vended credentials", values: []string{"vended-credentials"}, want: true},
		{name: "mechanism list", values: []string{"remote-signing,vended-credentials"}, want: true},
		{name: "spaced and mixed case", values: []string{"Remote-Signing, Vended-Credentials"}, want: true},
		{name: "repeated header", values: []string{"remote-signing", "vended-credentials"}, want: true},
		{name: "remote signing only", values: []string{"remote-signing"}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/v1/namespaces/ns/tables/t", nil)
			for _, v := range tc.values {
				r.Header.Add(accessDelegationHeader, v)
			}
			if got := wantsVendedCredentials(r); got != tc.want {
				t.Fatalf("wantsVendedCredentials(%v) = %v, want %v", tc.values, got, tc.want)
			}
		})
	}
}

// A client that asked for vended credentials replaces its own storage
// credentials with whatever the catalog returns. With no vending configured
// there are none, so an endpoint by itself would leave the client signing
// nothing and every data file read and write would come back 403.
func TestBuildFileIOConfigWithholdsEndpointFromCredentialVendingClients(t *testing.T) {
	s := &Server{s3Endpoint: "http://seaweed.example:8333"}

	r := httptest.NewRequest(http.MethodGet, "/v1/namespaces/ns/tables/t", nil)
	r.Header.Set(accessDelegationHeader, "vended-credentials")
	if got := s.buildFileIOConfig(r, "s3://warehouse/ns/t"); len(got) != 0 {
		t.Fatalf("buildFileIOConfig() = %v, want empty for a vended-credentials request", got)
	}

	plain := httptest.NewRequest(http.MethodGet, "/v1/namespaces/ns/tables/t", nil)
	if got := s.buildFileIOConfig(plain, "s3://warehouse/ns/t"); got["s3.endpoint"] != s.s3Endpoint {
		t.Fatalf("s3.endpoint = %q, want %q", got["s3.endpoint"], s.s3Endpoint)
	}
}

// The body of a load response depends on the delegation header, so a cache in
// front of the catalog must not key on the URL alone.
func TestWriteLoadResultVariesOnTheDelegationHeader(t *testing.T) {
	rec := httptest.NewRecorder()
	writeLoadResult(rec, http.StatusOK, LoadTableResult{})
	if got := rec.Header().Get("Vary"); got != accessDelegationHeader {
		t.Fatalf("Vary = %q, want %q", got, accessDelegationHeader)
	}
}

func TestLoadTableResultOmitsConfigForCredentialVendingClients(t *testing.T) {
	s := &Server{s3Endpoint: "http://seaweed.example:8333"}
	r := httptest.NewRequest(http.MethodGet, "/v1/namespaces/ns/tables/t", nil)
	r.Header.Set(accessDelegationHeader, "vended-credentials")

	getResp := s3tables.GetTableResponse{MetadataLocation: "s3://bkt/ns/t/metadata/v1.metadata.json"}
	result, err := s.buildLoadTableResult(r, getResp, "bkt", []string{"ns"}, "t")
	if err != nil {
		t.Fatalf("buildLoadTableResult() error = %v", err)
	}
	if len(result.Config) != 0 {
		t.Fatalf("LoadTableResult.Config = %v, want empty", result.Config)
	}
}
