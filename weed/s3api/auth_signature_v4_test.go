package s3api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func TestExtractV4AuthInfoFromHeader_S3Tables(t *testing.T) {
	now := time.Now().UTC()
	dateStr := now.Format(iso8601Format)

	tests := []struct {
		name           string
		service        string
		body           string
		expectAutoHash bool
	}{
		{
			name:           "s3 service should not auto-hash",
			service:        "s3",
			body:           "hello",
			expectAutoHash: false,
		},
		{
			name:           "s3tables service should auto-hash",
			service:        "s3tables",
			body:           "hello",
			expectAutoHash: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := bytes.NewReader([]byte(tt.body))
			req, _ := http.NewRequest(http.MethodPost, "http://localhost/", body)

			authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/%s/us-east-1/%s/aws4_request, SignedHeaders=host, Signature=dummy",
				now.Format(yyyymmdd), tt.service)
			req.Header.Set("Authorization", authHeader)
			req.Header.Set("x-amz-date", dateStr)

			authInfo, errCode := extractV4AuthInfoFromHeader(req)
			if errCode != s3err.ErrNone {
				t.Fatalf("extractV4AuthInfoFromHeader failed: %v", errCode)
			}

			if tt.expectAutoHash {
				expectedHash := sha256.Sum256([]byte(tt.body))
				expectedHashStr := hex.EncodeToString(expectedHash[:])
				if authInfo.HashedPayload != expectedHashStr {
					t.Errorf("Expected auto-hashed payload %s, got %s", expectedHashStr, authInfo.HashedPayload)
				}
			} else {
				if authInfo.HashedPayload != emptySHA256 {
					t.Errorf("Expected non-auto-hashed payload %s (emptySHA256), got %s", emptySHA256, authInfo.HashedPayload)
				}
			}
		})
	}
}

func TestBuildPathWithForwardedPrefix(t *testing.T) {
	tests := []struct {
		name            string
		forwardedPrefix string
		urlPath         string
		expected        string
	}{
		{
			name:            "empty prefix returns urlPath",
			forwardedPrefix: "",
			urlPath:         "/bucket/obj",
			expected:        "/bucket/obj",
		},
		{
			name:            "prefix without trailing slash",
			forwardedPrefix: "/storage",
			urlPath:         "/bucket/obj",
			expected:        "/storage/bucket/obj",
		},
		{
			name:            "prefix with trailing slash",
			forwardedPrefix: "/storage/",
			urlPath:         "/bucket/obj",
			expected:        "/storage/bucket/obj",
		},
		{
			name:            "prefix without leading slash",
			forwardedPrefix: "storage",
			urlPath:         "/bucket/obj",
			expected:        "/storage/bucket/obj",
		},
		{
			name:            "prefix without leading slash and with trailing slash",
			forwardedPrefix: "storage/",
			urlPath:         "/bucket/obj",
			expected:        "/storage/bucket/obj",
		},
		{
			name:            "preserve double slashes in key",
			forwardedPrefix: "/storage",
			urlPath:         "/bucket//obj",
			expected:        "/storage/bucket//obj",
		},
		{
			name:            "preserve trailing slash in urlPath",
			forwardedPrefix: "/storage",
			urlPath:         "/bucket/folder/",
			expected:        "/storage/bucket/folder/",
		},
		{
			name:            "preserve trailing slash with prefix having trailing slash",
			forwardedPrefix: "/storage/",
			urlPath:         "/bucket/folder/",
			expected:        "/storage/bucket/folder/",
		},
		{
			name:            "root path",
			forwardedPrefix: "/storage",
			urlPath:         "/",
			expected:        "/storage/",
		},
		{
			name:            "complex key with multiple slashes",
			forwardedPrefix: "/api/v1",
			urlPath:         "/bucket/path//with///slashes",
			expected:        "/api/v1/bucket/path//with///slashes",
		},
		{
			name:            "urlPath without leading slash",
			forwardedPrefix: "/storage",
			urlPath:         "bucket/obj",
			expected:        "/storage/bucket/obj",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildPathWithForwardedPrefix(tt.forwardedPrefix, tt.urlPath)
			if result != tt.expected {
				t.Errorf("buildPathWithForwardedPrefix(%q, %q) = %q, want %q",
					tt.forwardedPrefix, tt.urlPath, result, tt.expected)
			}
		})
	}
}

// TestExtractHostHeader tests the extractHostHeader function with various scenarios
func TestExtractHostHeader(t *testing.T) {
	tests := []struct {
		name           string
		hostHeader     string
		forwardedHost  string
		forwardedPort  string
		forwardedProto string
		externalHost   string
		expected       string
	}{
		{
			name:           "basic host without forwarding",
			hostHeader:     "example.com",
			forwardedHost:  "",
			forwardedPort:  "",
			forwardedProto: "",
			expected:       "example.com",
		},
		{
			name:           "host with port without forwarding",
			hostHeader:     "example.com:8080",
			forwardedHost:  "",
			forwardedPort:  "",
			forwardedProto: "",
			expected:       "example.com:8080",
		},
		{
			name:           "X-Forwarded-Host without port",
			hostHeader:     "backend:8333",
			forwardedHost:  "example.com",
			forwardedPort:  "",
			forwardedProto: "",
			expected:       "example.com",
		},
		{
			name:           "X-Forwarded-Host with X-Forwarded-Port (HTTP non-standard)",
			hostHeader:     "backend:8333",
			forwardedHost:  "example.com",
			forwardedPort:  "8080",
			forwardedProto: "http",
			expected:       "example.com:8080",
		},
		{
			name:           "X-Forwarded-Host with X-Forwarded-Port (HTTPS non-standard)",
			hostHeader:     "backend:8333",
			forwardedHost:  "example.com",
			forwardedPort:  "8443",
			forwardedProto: "https",
			expected:       "example.com:8443",
		},
		{
			name:           "X-Forwarded-Host with X-Forwarded-Port (HTTP standard port 80)",
			hostHeader:     "backend:8333",
			forwardedHost:  "example.com",
			forwardedPort:  "80",
			forwardedProto: "http",
			expected:       "example.com",
		},
		{
			name:           "X-Forwarded-Host with X-Forwarded-Port (HTTPS standard port 443)",
			hostHeader:     "backend:8333",
			forwardedHost:  "example.com",
			forwardedPort:  "443",
			forwardedProto: "https",
			expected:       "example.com",
		},
		// Issue #6649: X-Forwarded-Host already contains port (Traefik/HAProxy style)
		{
			name:           "X-Forwarded-Host with port already included (should not add port again)",
			hostHeader:     "backend:8333",
			forwardedHost:  "127.0.0.1:8433",
			forwardedPort:  "8433",
			forwardedProto: "https",
			expected:       "127.0.0.1:8433",
		},
		{
			name:           "X-Forwarded-Host with standard port already included (HTTPS 443)",
			hostHeader:     "backend:8333",
			forwardedHost:  "example.com:443",
			forwardedPort:  "443",
			forwardedProto: "https",
			expected:       "example.com",
		},
		{
			name:           "X-Forwarded-Host with port, no X-Forwarded-Port header",
			hostHeader:     "backend:8333",
			forwardedHost:  "example.com:9000",
			forwardedPort:  "",
			forwardedProto: "http",
			expected:       "example.com:9000",
		},
		// IPv6 test cases
		{
			name:           "IPv6 address with brackets and port in X-Forwarded-Host",
			hostHeader:     "backend:8333",
			forwardedHost:  "[::1]:8080",
			forwardedPort:  "8080",
			forwardedProto: "http",
			expected:       "[::1]:8080",
		},
		{
			name:           "IPv6 address without brackets, should add brackets with port",
			hostHeader:     "backend:8333",
			forwardedHost:  "::1",
			forwardedPort:  "8080",
			forwardedProto: "http",
			expected:       "[::1]:8080",
		},
		{
			name:           "IPv6 address without brackets and standard port, should strip default port",
			hostHeader:     "backend:8333",
			forwardedHost:  "::1",
			forwardedPort:  "80",
			forwardedProto: "http",
			expected:       "::1",
		},
		{
			name:           "IPv6 address without brackets and standard HTTPS port, should strip default port",
			hostHeader:     "backend:8333",
			forwardedHost:  "2001:db8::1",
			forwardedPort:  "443",
			forwardedProto: "https",
			expected:       "2001:db8::1",
		},
		{
			name:           "IPv6 address with brackets but no port, should add port",
			hostHeader:     "backend:8333",
			forwardedHost:  "[2001:db8::1]",
			forwardedPort:  "8080",
			forwardedProto: "http",
			expected:       "[2001:db8::1]:8080",
		},
		{
			name:           "IPv6 full address with brackets and default port (should strip default port)",
			hostHeader:     "backend:8333",
			forwardedHost:  "[2001:db8:85a3::8a2e:370:7334]:443",
			forwardedPort:  "443",
			forwardedProto: "https",
			expected:       "2001:db8:85a3::8a2e:370:7334",
		},
		{
			name:           "IPv4-mapped IPv6 address without brackets, should add brackets with port",
			hostHeader:     "backend:8333",
			forwardedHost:  "::ffff:127.0.0.1",
			forwardedPort:  "8080",
			forwardedProto: "http",
			expected:       "[::ffff:127.0.0.1]:8080",
		},
		{
			name:       "Simple port 442",
			hostHeader: "bucket.domain.com:442",
			expected:   "bucket.domain.com:442",
		},
		{
			name:          "Port 442 with X-Forwarded-Host",
			hostHeader:    "backend:8333",
			forwardedHost: "bucket.domain.com:442",
			expected:      "bucket.domain.com:442",
		},
		{
			name:          "Port 442 with X-Forwarded-Port",
			hostHeader:    "backend:8333",
			forwardedHost: "bucket.domain.com",
			forwardedPort: "442",
			expected:      "bucket.domain.com:442",
		},
		{
			name:           "HTTPS with port 442 (should NOT strip)",
			hostHeader:     "bucket.domain.com:442",
			forwardedProto: "https",
			expected:       "bucket.domain.com:442",
		},
		{
			name:          "X-Forwarded-Host with multiple hosts (including port)",
			forwardedHost: "bucket.domain.com:442, internal.proxy",
			expected:      "bucket.domain.com:442",
		},
		{
			name:       "IPv6 with port",
			hostHeader: "[2001:db8::1]:442",
			expected:   "[2001:db8::1]:442",
		},
		{
			name:           "X-Forwarded-Host with port 442, but X-Forwarded-Port is 80 (should PREFER 442)",
			forwardedHost:  "bucket.domain.com:442",
			forwardedPort:  "80",
			forwardedProto: "http",
			expected:       "bucket.domain.com:442",
		},
		// externalHost override tests
		{
			name:         "externalHost overrides everything",
			hostHeader:   "backend:8333",
			externalHost: "api.example.com:9000",
			expected:     "api.example.com:9000",
		},
		{
			name:           "externalHost overrides X-Forwarded-Host",
			hostHeader:     "backend:8333",
			forwardedHost:  "proxy.example.com",
			forwardedPort:  "443",
			forwardedProto: "https",
			externalHost:   "api.example.com",
			expected:       "api.example.com",
		},
		{
			name:         "externalHost with IPv6",
			hostHeader:   "backend:8333",
			externalHost: "[::1]:9000",
			expected:     "[::1]:9000",
		},
		// Bug fix: X-Forwarded-Port should not override more specific ports in other headers
		{
			name:           "User reported case: X-Forwarded-Port misreports 443 but Host has 30007",
			hostHeader:     "storage-stgops.mt.mtnet:30007",
			forwardedHost:  "storage-stgops.mt.mtnet",
			forwardedPort:  "443",
			forwardedProto: "https",
			expected:       "storage-stgops.mt.mtnet:30007",
		},
		{
			name:           "X-Forwarded-Host already contains correct port, ignore misaligned X-Forwarded-Port",
			hostHeader:     "backend:8333",
			forwardedHost:  "storage-stgops.mt.mtnet:30007",
			forwardedPort:  "443",
			forwardedProto: "https",
			expected:       "storage-stgops.mt.mtnet:30007",
		},
		{
			name:           "X-Forwarded-Host has no port, match r.Host hostname and take its port",
			hostHeader:     "example.com:8080",
			forwardedHost:  "example.com",
			forwardedPort:  "80",
			forwardedProto: "http",
			expected:       "example.com:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock request
			req, err := http.NewRequest("GET", "http://"+tt.hostHeader+"/bucket/object", nil)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			// Set headers
			req.Host = tt.hostHeader
			if tt.forwardedHost != "" {
				req.Header.Set("X-Forwarded-Host", tt.forwardedHost)
			}
			if tt.forwardedPort != "" {
				req.Header.Set("X-Forwarded-Port", tt.forwardedPort)
			}
			if tt.forwardedProto != "" {
				req.Header.Set("X-Forwarded-Proto", tt.forwardedProto)
			}

			// Test the function
			result := extractHostHeader(req, tt.externalHost)
			if result != tt.expected {
				t.Errorf("extractHostHeader() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestExtractSignedHeadersCase(t *testing.T) {
	tests := []struct {
		name        string
		host        string
		signedHeads []string
		expected    string
	}{
		{
			name:        "lowercase host",
			host:        "bucket.domain.com:442",
			signedHeads: []string{"host"},
			expected:    "host:bucket.domain.com:442\n",
		},
		{
			name:        "uppercase Host",
			host:        "bucket.domain.com:442",
			signedHeads: []string{"Host"},
			expected:    "host:bucket.domain.com:442\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := http.NewRequest("GET", "http://"+tt.host+"/", nil)
			r.Host = tt.host
			extracted, errCode := extractSignedHeaders(tt.signedHeads, r, "")
			if errCode != s3err.ErrNone {
				t.Fatalf("extractSignedHeaders failed: %v", errCode)
			}
			actual := getCanonicalHeaders(extracted)
			if actual != tt.expected {
				t.Errorf("%s: expected %q, got %q", tt.name, tt.expected, actual)
			}
		})
	}
}
