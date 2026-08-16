package iceberg

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

type stubVendor struct {
	credentials *VendedCredentials
	err         error
	gotBucket   string
	gotPrefix   string
	gotcaller   string
}

func (v *stubVendor) VendTableCredentials(_ context.Context, principal, bucket, prefix string) (*VendedCredentials, error) {
	v.gotcaller, v.gotBucket, v.gotPrefix = principal, bucket, prefix
	return v.credentials, v.err
}

func vendingServer(vendor CredentialVendor) *Server {
	s := &Server{s3Endpoint: "http://s3.example:8333"}
	s.SetCredentialVendor(vendor)
	return s
}

func TestBuildFileIOConfigVendingVendsScopedCredentials(t *testing.T) {
	expiry := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	vendor := &stubVendor{credentials: &VendedCredentials{
		AccessKeyID:     "ASIAEXAMPLE",
		SecretAccessKey: "secret",
		SessionToken:    "token",
		Expiration:      expiry,
	}}
	s := vendingServer(vendor)

	r := httptest.NewRequest("GET", "/v1/namespaces/ns/tables/t", nil)
	r.Header.Set(accessDelegationHeader, "vended-credentials")

	config, storageCredentials := s.buildFileIOConfig(r, "s3://warehouse/ns/t")

	if vendor.gotBucket != "warehouse" || vendor.gotPrefix != "ns/t" {
		t.Errorf("vendor called with bucket=%q prefix=%q, want warehouse and ns/t", vendor.gotBucket, vendor.gotPrefix)
	}
	if config["s3.access-key-id"] != "ASIAEXAMPLE" || config["s3.secret-access-key"] != "secret" || config["s3.session-token"] != "token" {
		t.Errorf("config missing vended credentials: %v", config)
	}
	if config["s3.endpoint"] != "http://s3.example:8333" {
		t.Errorf("config missing the endpoint the credentials are for: %v", config)
	}
	if len(storageCredentials) != 1 {
		t.Fatalf("storage-credentials = %d entries, want 1", len(storageCredentials))
	}
	if storageCredentials[0].Prefix != "s3://warehouse/ns/t" {
		t.Errorf("storage credential prefix = %q", storageCredentials[0].Prefix)
	}
	if storageCredentials[0].Config["s3.session-token"] != "token" {
		t.Errorf("storage credential is missing the session token: %v", storageCredentials[0].Config)
	}
}

// Without a vendor the response must stay silent about the endpoint too: a
// client that asked for vended credentials would otherwise drop its own and
// start sending unsigned requests.
func TestBuildFileIOConfigVendingWithoutVendorSaysNothing(t *testing.T) {
	s := &Server{s3Endpoint: "http://s3.example:8333"}
	r := httptest.NewRequest("GET", "/v1/namespaces/ns/tables/t", nil)
	r.Header.Set(accessDelegationHeader, "vended-credentials")

	config, storageCredentials := s.buildFileIOConfig(r, "s3://warehouse/ns/t")
	if len(config) != 0 || storageCredentials != nil {
		t.Errorf("config = %v, storage-credentials = %v, want both empty", config, storageCredentials)
	}
}

func TestBuildFileIOConfigVendingFallsBackWhenVendingFails(t *testing.T) {
	s := vendingServer(&stubVendor{err: errors.New("no role configured")})
	r := httptest.NewRequest("GET", "/v1/namespaces/ns/tables/t", nil)
	r.Header.Set(accessDelegationHeader, "vended-credentials")

	config, storageCredentials := s.buildFileIOConfig(r, "s3://warehouse/ns/t")
	if len(config) != 0 || storageCredentials != nil {
		t.Errorf("config = %v, storage-credentials = %v, want both empty", config, storageCredentials)
	}
}

// A client that did not ask for delegation keeps the plain endpoint config and
// must never be handed credentials.
func TestBuildFileIOConfigVendingWithoutDelegationHeader(t *testing.T) {
	vendor := &stubVendor{credentials: &VendedCredentials{AccessKeyID: "ASIAEXAMPLE"}}
	s := vendingServer(vendor)

	r := httptest.NewRequest("GET", "/v1/namespaces/ns/tables/t", nil)
	config, storageCredentials := s.buildFileIOConfig(r, "s3://warehouse/ns/t")

	if _, vended := config["s3.access-key-id"]; vended {
		t.Errorf("credentials vended without the delegation header: %v", config)
	}
	if config["s3.endpoint"] != "http://s3.example:8333" {
		t.Errorf("config = %v, want the endpoint", config)
	}
	if storageCredentials != nil {
		t.Errorf("storage-credentials = %v, want none", storageCredentials)
	}
	if vendor.gotBucket != "" {
		t.Errorf("vendor was called for a request that did not ask for delegation")
	}
}
