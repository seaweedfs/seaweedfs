package s3api

import (
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func setupTestIAMForV2Auth() *IdentityAccessManagement {
	iam := &IdentityAccessManagement{
		identities:     []*Identity{},
		accessKeyIdent: make(map[string]*Identity),
	}

	testCred := &Credential{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}

	testIdentity := &Identity{
		Name:        "testUser",
		Account:     &AccountAdmin,
		Credentials: []*Credential{testCred},
		Actions: []Action{
			s3_constants.ACTION_ADMIN,
		},
	}

	iam.identities = append(iam.identities, testIdentity)
	iam.accessKeyIdent[testCred.AccessKey] = testIdentity

	return iam
}

func TestValidateV2AuthHeader(t *testing.T) {
	tests := []struct {
		name              string
		authHeader        string
		expectedAccessKey string
		expectedError     s3err.ErrorCode
	}{
		{
			name:              "valid auth header with space",
			authHeader:        "AWS AKIAIOSFODNN7EXAMPLE:frJIUN8DYpKDtOLCwo//yllqDzg=",
			expectedAccessKey: "AKIAIOSFODNN7EXAMPLE",
			expectedError:     s3err.ErrNone,
		},
		{
			name:          "empty auth header",
			authHeader:    "",
			expectedError: s3err.ErrAuthHeaderEmpty,
		},
		{
			name:          "wrong algorithm prefix",
			authHeader:    "HMAC AKIAIOSFODNN7EXAMPLE:signature",
			expectedError: s3err.ErrSignatureVersionNotSupported,
		},
		{
			name:          "missing colon separator",
			authHeader:    "AWS AKIAIOSFODNN7EXAMPLE",
			expectedError: s3err.ErrMissingFields,
		},
		{
			name:          "empty access key",
			authHeader:    "AWS :signature",
			expectedError: s3err.ErrInvalidAccessKeyID,
		},
		{
			name:          "empty signature",
			authHeader:    "AWS AKIAIOSFODNN7EXAMPLE:",
			expectedError: s3err.ErrMissingFields,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accessKey, errCode := validateV2AuthHeader(tt.authHeader)

			if errCode != tt.expectedError {
				t.Errorf("validateV2AuthHeader() error = %v, want %v", errCode, tt.expectedError)
			}

			if errCode == s3err.ErrNone && accessKey != tt.expectedAccessKey {
				t.Errorf("validateV2AuthHeader() accessKey = %q, want %q", accessKey, tt.expectedAccessKey)
			}
		})
	}
}

func TestSignatureV2Format(t *testing.T) {
	cred := &Credential{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}

	headers := http.Header{}
	headers.Set("Date", "Mon, 09 Sep 2011 23:36:00 GMT")

	signature := signatureV2(cred, "GET", "/bucket/object", "", headers)

	// Verify format: "AWS <AccessKey>:<Signature>" with space after AWS
	expectedPrefix := "AWS " + cred.AccessKey + ":"
	if len(signature) < len(expectedPrefix) {
		t.Fatalf("Signature too short: %s", signature)
	}

	actualPrefix := signature[:len(expectedPrefix)]
	if actualPrefix != expectedPrefix {
		t.Errorf("Signature prefix = %q, want %q", actualPrefix, expectedPrefix)
	}
}

func TestDoesSignV2Match(t *testing.T) {
	iam := setupTestIAMForV2Auth()
	cred := &Credential{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}

	tests := []struct {
		name          string
		method        string
		path          string
		query         string
		headers       map[string]string
		authOverride  string
		expectedError s3err.ErrorCode
		expectIdent   bool
	}{
		{
			name:          "valid GET request",
			method:        "GET",
			path:          "/bucket/object",
			query:         "",
			headers:       map[string]string{"Date": "Mon, 09 Sep 2011 23:36:00 GMT"},
			expectedError: s3err.ErrNone,
			expectIdent:   true,
		},
		{
			name:   "valid PUT request with content headers",
			method: "PUT",
			path:   "/bucket/object",
			query:  "",
			headers: map[string]string{
				"Date":         "Mon, 09 Sep 2011 23:36:00 GMT",
				"Content-Type": "text/plain",
				"Content-Md5":  "c8fdb181845a4ca6b8fec737b3581d76",
			},
			expectedError: s3err.ErrNone,
			expectIdent:   true,
		},
		{
			name:   "request with query parameters",
			method: "GET",
			path:   "/bucket/object",
			query:  "acl&versionId=123",
			headers: map[string]string{
				"Date": "Mon, 09 Sep 2011 23:36:00 GMT",
			},
			expectedError: s3err.ErrNone,
			expectIdent:   true,
		},
		{
			name:   "request with x-amz headers",
			method: "PUT",
			path:   "/bucket/object",
			query:  "",
			headers: map[string]string{
				"Date":                "Mon, 09 Sep 2011 23:36:00 GMT",
				"x-amz-storage-class": "REDUCED_REDUNDANCY",
				"x-amz-meta-custom":   "value",
			},
			expectedError: s3err.ErrNone,
			expectIdent:   true,
		},
		{
			name:          "invalid signature",
			method:        "GET",
			path:          "/bucket/object",
			query:         "",
			headers:       map[string]string{"Date": "Mon, 09 Sep 2011 23:36:00 GMT"},
			authOverride:  "AWS AKIAIOSFODNN7EXAMPLE:invalidSignature123456==",
			expectedError: s3err.ErrSignatureDoesNotMatch,
			expectIdent:   false,
		},
		{
			name:          "non-existent access key",
			method:        "GET",
			path:          "/bucket/object",
			query:         "",
			headers:       map[string]string{"Date": "Mon, 09 Sep 2011 23:36:00 GMT"},
			authOverride:  "AWS NONEXISTENTKEY:signature==",
			expectedError: s3err.ErrInvalidAccessKeyID,
			expectIdent:   false,
		},
		{
			name:          "empty authorization header",
			method:        "GET",
			path:          "/bucket/object",
			query:         "",
			headers:       map[string]string{"Date": "Mon, 09 Sep 2011 23:36:00 GMT"},
			authOverride:  "",
			expectedError: s3err.ErrAuthHeaderEmpty,
			expectIdent:   false,
		},
		{
			name:          "malformed auth - missing signature",
			method:        "GET",
			path:          "/bucket/object",
			query:         "",
			headers:       map[string]string{"Date": "Mon, 09 Sep 2011 23:36:00 GMT"},
			authOverride:  "AWS AKIAIOSFODNN7EXAMPLE",
			expectedError: s3err.ErrMissingFields,
			expectIdent:   false,
		},
		{
			name:          "malformed auth - wrong prefix",
			method:        "GET",
			path:          "/bucket/object",
			query:         "",
			headers:       map[string]string{"Date": "Mon, 09 Sep 2011 23:36:00 GMT"},
			authOverride:  "HMAC AKIAIOSFODNN7EXAMPLE:sig",
			expectedError: s3err.ErrSignatureVersionNotSupported,
			expectIdent:   false,
		},
		{
			name:          "malformed auth - no space after AWS",
			method:        "GET",
			path:          "/bucket/object",
			query:         "",
			headers:       map[string]string{"Date": "Mon, 09 Sep 2011 23:36:00 GMT"},
			authOverride:  "AWSAKIAIOSFODNN7EXAMPLE:signature==",
			expectedError: s3err.ErrInvalidAccessKeyID,
			expectIdent:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := "http://example.com" + tt.path
			if tt.query != "" {
				url += "?" + tt.query
			}
			req, err := http.NewRequest(tt.method, url, nil)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}

			var authHeader string
			if tt.authOverride != "" {
				authHeader = tt.authOverride
			} else {
				authHeader = signatureV2(cred, req.Method, req.URL.Path, req.URL.Query().Encode(), req.Header)
			}
			if tt.name != "empty authorization header" {
				req.Header.Set("Authorization", authHeader)
			}

			identity, errCode := iam.doesSignV2Match(req)

			if errCode != tt.expectedError {
				t.Errorf("doesSignV2Match() error = %v, want %v", errCode, tt.expectedError)
			}

			if tt.expectIdent && identity == nil {
				t.Error("Expected non-nil identity")
			}

			if !tt.expectIdent && identity != nil {
				t.Error("Expected nil identity")
			}

			if identity != nil && identity.Name != "testUser" {
				t.Errorf("Identity name = %q, want %q", identity.Name, "testUser")
			}
		})
	}
}
