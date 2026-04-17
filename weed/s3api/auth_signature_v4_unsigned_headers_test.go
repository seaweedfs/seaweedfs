package s3api

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// TestVerifySignedHeadersCoverage_Unit exercises the helper directly.
func TestVerifySignedHeadersCoverage_Unit(t *testing.T) {
	tests := []struct {
		name          string
		headers       map[string]string
		signedHeaders []string
		isPresigned   bool
		want          s3err.ErrorCode
	}{
		{
			name:          "no x-amz headers is fine",
			headers:       map[string]string{"Content-Type": "text/plain"},
			signedHeaders: []string{"host"},
			want:          s3err.ErrNone,
		},
		{
			name: "signed x-amz header is accepted",
			headers: map[string]string{
				"X-Amz-Date":    "20250101T000000Z",
				"X-Amz-Tagging": "a=b",
			},
			signedHeaders: []string{"host", "x-amz-date", "x-amz-tagging"},
			want:          s3err.ErrNone,
		},
		{
			name: "unsigned x-amz-tagging is rejected (header-based)",
			headers: map[string]string{
				"X-Amz-Date":    "20250101T000000Z",
				"X-Amz-Tagging": "secret=pwn",
			},
			signedHeaders: []string{"host", "x-amz-date"},
			want:          s3err.ErrSignatureDoesNotMatch,
		},
		{
			name: "unsigned x-amz-meta-* is rejected",
			headers: map[string]string{
				"X-Amz-Meta-Owner": "attacker",
			},
			signedHeaders: []string{"host"},
			want:          s3err.ErrSignatureDoesNotMatch,
		},
		{
			name: "unsigned x-amz-acl is rejected",
			headers: map[string]string{
				"X-Amz-Acl": "public-read",
			},
			signedHeaders: []string{"host"},
			want:          s3err.ErrSignatureDoesNotMatch,
		},
		{
			name: "unsigned x-amz-storage-class is rejected",
			headers: map[string]string{
				"X-Amz-Storage-Class": "GLACIER",
			},
			signedHeaders: []string{"host"},
			want:          s3err.ErrSignatureDoesNotMatch,
		},
		{
			name: "unsigned x-amz-server-side-encryption is rejected",
			headers: map[string]string{
				"X-Amz-Server-Side-Encryption": "AES256",
			},
			signedHeaders: []string{"host"},
			want:          s3err.ErrSignatureDoesNotMatch,
		},
		{
			name: "unsigned x-amz-object-lock-retain-until-date is rejected",
			headers: map[string]string{
				"X-Amz-Object-Lock-Retain-Until-Date": "2099-01-01T00:00:00Z",
			},
			signedHeaders: []string{"host"},
			want:          s3err.ErrSignatureDoesNotMatch,
		},
		{
			name: "unsigned x-amz-security-token is rejected (even presigned)",
			headers: map[string]string{
				"X-Amz-Security-Token": "attacker-token",
			},
			signedHeaders: []string{"host"},
			isPresigned:   true,
			want:          s3err.ErrSignatureDoesNotMatch,
		},
		{
			name: "x-amz-content-sha256 is always exempt (presigned)",
			headers: map[string]string{
				"X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD",
			},
			signedHeaders: []string{"host"},
			isPresigned:   true,
			want:          s3err.ErrNone,
		},
		{
			name: "x-amz-content-sha256 is always exempt (header-based)",
			headers: map[string]string{
				"X-Amz-Content-Sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			},
			signedHeaders: []string{"host"},
			isPresigned:   false,
			want:          s3err.ErrNone,
		},
		{
			name: "presigned exempts sigv4 query params echoed as headers",
			headers: map[string]string{
				"X-Amz-Algorithm":     "AWS4-HMAC-SHA256",
				"X-Amz-Credential":    "AKIA/20250101/us-east-1/s3/aws4_request",
				"X-Amz-Date":          "20250101T000000Z",
				"X-Amz-Expires":       "3600",
				"X-Amz-SignedHeaders": "host",
				"X-Amz-Signature":     "deadbeef",
			},
			signedHeaders: []string{"host"},
			isPresigned:   true,
			want:          s3err.ErrNone,
		},
		{
			name: "header-based does NOT exempt x-amz-date when unsigned",
			headers: map[string]string{
				"X-Amz-Date": "20250101T000000Z",
			},
			signedHeaders: []string{"host"},
			isPresigned:   false,
			want:          s3err.ErrSignatureDoesNotMatch,
		},
		{
			name: "signed headers match is case-insensitive",
			headers: map[string]string{
				"X-Amz-Tagging": "a=b",
			},
			signedHeaders: []string{"HOST", "X-Amz-Tagging"},
			want:          s3err.ErrNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPut, "http://example.com/bucket/key", nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			got := verifySignedHeadersCoverage(req, tt.signedHeaders, tt.isPresigned)
			if got != tt.want {
				t.Fatalf("verifySignedHeadersCoverage = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestPresignedPutRejectsUnsignedTagging exercises the full verification path
// through reqSignatureV4Verify for a presigned URL that signs only `host` but
// whose caller attached an x-amz-tagging header after-the-fact.
func TestPresignedPutRejectsUnsignedTagging(t *testing.T) {
	iam := newTestIAM()

	req, err := newTestRequest(http.MethodPut, "http://127.0.0.1:9000/bucket/key", 0, nil)
	if err != nil {
		t.Fatalf("newTestRequest: %v", err)
	}
	if err := preSignV4(iam, req, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", 600); err != nil {
		t.Fatalf("preSignV4: %v", err)
	}
	// Attacker appends an unsigned header after the URL was signed.
	req.Header.Set("X-Amz-Tagging", "classification=public")

	_, errCode := iam.reqSignatureV4Verify(req)
	if errCode != s3err.ErrSignatureDoesNotMatch {
		t.Fatalf("expected ErrSignatureDoesNotMatch for unsigned x-amz-tagging, got %v", errCode)
	}
}

// TestPresignedPutAcceptsSignedTagging confirms that a presigned URL whose
// SignedHeaders list covers x-amz-tagging still validates successfully.
func TestPresignedPutAcceptsSignedTagging(t *testing.T) {
	iam := newTestIAM()

	req, err := newTestRequest(http.MethodPut, "http://127.0.0.1:9000/bucket/key", 0, nil)
	if err != nil {
		t.Fatalf("newTestRequest: %v", err)
	}
	req.Header.Set("X-Amz-Tagging", "classification=public")
	if err := preSignV4WithHeaders(iam, req, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", 600, []string{"host", "x-amz-tagging"}); err != nil {
		t.Fatalf("preSignV4WithHeaders: %v", err)
	}

	_, errCode := iam.reqSignatureV4Verify(req)
	if errCode != s3err.ErrNone {
		t.Fatalf("expected ErrNone for signed x-amz-tagging, got %v", errCode)
	}
}

// preSignV4WithHeaders is a test helper that builds a presigned URL whose
// SignedHeaders list covers the specified headers (which must already be set
// on the request), then computes the signature over those headers and
// attaches X-Amz-Signature.
func preSignV4WithHeaders(_ *IdentityAccessManagement, req *http.Request, accessKey, secretKey string, expires int64, signedHeaders []string) error {
	now := time.Now().UTC()
	dateStr := now.Format(iso8601Format)

	scope := fmt.Sprintf("%s/%s/%s/%s", now.Format(yyyymmdd), "us-east-1", "s3", "aws4_request")
	credential := fmt.Sprintf("%s/%s", accessKey, scope)

	normalized := make([]string, 0, len(signedHeaders))
	for _, h := range signedHeaders {
		normalized = append(normalized, strings.ToLower(h))
	}
	sort.Strings(normalized)

	query := req.URL.Query()
	query.Set("X-Amz-Algorithm", signV4Algorithm)
	query.Set("X-Amz-Credential", credential)
	query.Set("X-Amz-Date", dateStr)
	query.Set("X-Amz-Expires", fmt.Sprintf("%d", expires))
	query.Set("X-Amz-SignedHeaders", strings.Join(normalized, ";"))
	req.URL.RawQuery = query.Encode()

	hashedPayload := query.Get("X-Amz-Content-Sha256")
	if hashedPayload == "" {
		hashedPayload = unsignedPayload
	}

	extracted := make(http.Header)
	for _, h := range normalized {
		if h == "host" {
			extracted[h] = []string{req.Host}
			continue
		}
		if values, ok := req.Header[http.CanonicalHeaderKey(h)]; ok {
			extracted[h] = values
		}
	}

	canonicalRequest := getCanonicalRequest(extracted, hashedPayload, req.URL.RawQuery, req.URL.Path, req.Method)
	stringToSign := getStringToSign(canonicalRequest, now, scope)
	signingKey := getSigningKey(secretKey, now.Format(yyyymmdd), "us-east-1", "s3")
	signature := getSignature(signingKey, stringToSign)

	query.Set("X-Amz-Signature", signature)
	req.URL.RawQuery = query.Encode()
	return nil
}

// TestPresignedPutRejectsUnsignedMetadataHeaders checks a spread of
// security-relevant headers that the PUT handler persists.
func TestPresignedPutRejectsUnsignedMetadataHeaders(t *testing.T) {
	dangerous := []struct {
		name   string
		header string
		value  string
	}{
		{"acl", "X-Amz-Acl", "public-read"},
		{"user-metadata", "X-Amz-Meta-Owner", "attacker"},
		{"storage-class", "X-Amz-Storage-Class", "GLACIER"},
		{"server-side-encryption", "X-Amz-Server-Side-Encryption", "AES256"},
		{"sse-kms-key-id", "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", "kms-key"},
		{"object-lock-mode", "X-Amz-Object-Lock-Mode", "GOVERNANCE"},
		{"object-lock-retain-until", "X-Amz-Object-Lock-Retain-Until-Date", "2099-01-01T00:00:00Z"},
		{"website-redirect-location", "X-Amz-Website-Redirect-Location", "https://attacker.example/"},
		{"grant-full-control", "X-Amz-Grant-Full-Control", "id=attacker"},
	}

	for _, c := range dangerous {
		t.Run(c.name, func(t *testing.T) {
			iam := newTestIAM()

			req, err := newTestRequest(http.MethodPut, "http://127.0.0.1:9000/bucket/key", 0, nil)
			if err != nil {
				t.Fatalf("newTestRequest: %v", err)
			}
			if err := preSignV4(iam, req, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", 600); err != nil {
				t.Fatalf("preSignV4: %v", err)
			}
			req.Header.Set(c.header, c.value)

			_, errCode := iam.reqSignatureV4Verify(req)
			if errCode != s3err.ErrSignatureDoesNotMatch {
				t.Fatalf("expected rejection for unsigned %s, got %v", c.header, errCode)
			}
		})
	}
}
