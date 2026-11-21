package s3api

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const certificateIdentityConfig = `{
  "identities": [
    {"name": "anonymous", "actions": ["Read:public"]},
    {"name": "ingest-writer", "certificates": [{"uriSan": "spiffe://example.org/ns/ingest/sa/writer"}], "actions": ["Write:ingest"]},
    {"name": "by-cn", "certificates": [{"subjectCn": "reporter", "dnsSan": "reporter.example.internal"}], "actions": ["Read:reports"]},
    {"name": "retired", "disabled": true, "certificates": [{"subjectCn": "retired"}], "actions": ["Read:reports"]},
    {"name": "twin-a", "certificates": [{"subjectCn": "twin"}], "actions": ["Read:reports"]},
    {"name": "twin-b", "certificates": [{"subjectCn": "twin"}], "actions": ["Read:reports"]},
    {"name": "empty-match", "certificates": [{}], "actions": ["Admin"]},
    {"name": "signer", "credentials": [{"accessKey": "AKIDSIGNER", "secretKey": "secret"}], "actions": ["Admin"]}
  ]
}`

func clientCertificate(cn string, uri string, dnsNames ...string) *x509.Certificate {
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: cn}, DNSNames: dnsNames}
	if uri != "" {
		u, _ := url.Parse(uri)
		cert.URIs = []*url.URL{u}
	}
	return cert
}

func requestWithClientCertificate(cert *x509.Certificate, verified bool) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "https://localhost:8333/ingest/object", nil)
	r.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	if verified {
		r.TLS.VerifiedChains = [][]*x509.Certificate{{cert}}
	}
	return r
}

func TestCertificateIdentity(t *testing.T) {
	resetMemoryStore()
	defer resetMemoryStore()
	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, nil, "memory")
	_, err := iam.loadS3ApiConfigurationFromBytes([]byte(certificateIdentityConfig), true)
	require.NoError(t, err)

	tests := []struct {
		name     string
		r        *http.Request
		identity string
		errCode  s3err.ErrorCode
	}{
		{"uri SAN", requestWithClientCertificate(clientCertificate("x", "spiffe://example.org/ns/ingest/sa/writer"), true), "ingest-writer", s3err.ErrNone},
		{"every field of a match", requestWithClientCertificate(clientCertificate("reporter", "", "other.example", "REPORTER.example.internal"), true), "by-cn", s3err.ErrNone},
		{"one field of a match differs", requestWithClientCertificate(clientCertificate("reporter", "", "other.example"), true), "anonymous", s3err.ErrNone},
		{"certificate the gateway did not verify", requestWithClientCertificate(clientCertificate("x", "spiffe://example.org/ns/ingest/sa/writer"), false), "anonymous", s3err.ErrNone},
		{"plain http", httptest.NewRequest(http.MethodGet, "http://localhost:8333/ingest/object", nil), "anonymous", s3err.ErrNone},
		{"disabled identity", requestWithClientCertificate(clientCertificate("retired", ""), true), "", s3err.ErrAccessDenied},
		{"two identities", requestWithClientCertificate(clientCertificate("twin", ""), true), "", s3err.ErrAccessDenied},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			identity, errCode := iam.AuthenticateRequest(tc.r)
			assert.Equal(t, tc.errCode, errCode)
			if tc.identity == "" {
				assert.Nil(t, identity)
				return
			}
			require.NotNil(t, identity)
			assert.Equal(t, tc.identity, identity.Name)
			assert.Equal(t, tc.identity == "anonymous", iam.isAnonymousRequest(tc.r))
		})
	}

	signed := requestWithClientCertificate(clientCertificate("x", "spiffe://example.org/ns/ingest/sa/writer"), true)
	signed.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDSIGNER/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00")
	identity, errCode := iam.AuthenticateRequest(signed)
	assert.NotEqual(t, s3err.ErrNone, errCode, "a bad signature is not rescued by the certificate")
	assert.Nil(t, identity)
	assert.False(t, iam.isAnonymousRequest(signed))
}

func TestCertificateIdentityReadIsNotDeferredAsAnonymous(t *testing.T) {
	resetMemoryStore()
	defer resetMemoryStore()
	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, nil, "memory")
	_, err := iam.loadS3ApiConfigurationFromBytes([]byte(certificateIdentityConfig), true)
	require.NoError(t, err)
	s3a := &S3ApiServer{iam: iam}

	for _, tc := range []struct {
		cert     *x509.Certificate
		deferred bool
	}{
		{clientCertificate("x", "spiffe://example.org/ns/ingest/sa/writer"), false},
		{clientCertificate("retired", ""), false},
		{clientCertificate("nobody", ""), true},
	} {
		r := mux.SetURLVars(requestWithClientCertificate(tc.cert, true), map[string]string{"bucket": "ingest", "object": "object"})
		_, deferred := s3a.deferAnonymousObjectRead(r, s3_constants.ACTION_READ, "ingest", "object")
		assert.Equal(t, tc.deferred, deferred, tc.cert.Subject.CommonName)
	}
}
