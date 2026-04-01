package s3api

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReverseProxySignatureVerification is an integration test that exercises
// the full HTTP stack: real AWS SDK v4 signer -> real httputil.ReverseProxy ->
// real net/http server running IAM signature verification.
//
// This catches issues that unit tests miss: header normalization by net/http,
// proxy header injection, and real-world Host header handling.
func TestReverseProxySignatureVerification(t *testing.T) {
	const (
		accessKey = "AKIAIOSFODNN7EXAMPLE"
		secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)

	configJSON := `{
  "identities": [
    {
      "name": "test_user",
      "credentials": [
        {
          "accessKey": "` + accessKey + `",
          "secretKey": "` + secretKey + `"
        }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}`

	tests := []struct {
		name              string
		externalUrl       string // s3.externalUrl config for the backend
		clientScheme      string // scheme the client uses for signing
		clientHost        string // host the client signs against
		proxyForwardsHost bool   // whether proxy sets X-Forwarded-Host
		expectSuccess     bool
	}{
		{
			name:              "non-standard port, externalUrl matches proxy address",
			externalUrl:       "", // filled dynamically with proxy address
			clientScheme:      "http",
			clientHost:        "", // filled dynamically
			proxyForwardsHost: true,
			expectSuccess:     true,
		},
		{
			name:              "externalUrl with non-standard port, client signs against external host",
			externalUrl:       "http://api.example.com:9000",
			clientScheme:      "http",
			clientHost:        "api.example.com:9000",
			proxyForwardsHost: true,
			expectSuccess:     true,
		},
		{
			name:              "externalUrl with HTTPS default port stripped, client signs without port",
			externalUrl:       "https://api.example.com:443",
			clientScheme:      "https",
			clientHost:        "api.example.com",
			proxyForwardsHost: true,
			expectSuccess:     true,
		},
		{
			name:              "externalUrl with HTTP default port stripped, client signs without port",
			externalUrl:       "http://api.example.com:80",
			clientScheme:      "http",
			clientHost:        "api.example.com",
			proxyForwardsHost: true,
			expectSuccess:     true,
		},
		{
			name:              "proxy forwards X-Forwarded-Host correctly, no externalUrl needed",
			externalUrl:       "",
			clientScheme:      "http",
			clientHost:        "api.example.com:9000",
			proxyForwardsHost: true,
			expectSuccess:     true,
		},
		{
			name:              "proxy without X-Forwarded-Host, no externalUrl: host mismatch",
			externalUrl:       "",
			clientScheme:      "http",
			clientHost:        "api.example.com:9000",
			proxyForwardsHost: false,
			expectSuccess:     false,
		},
		{
			name:              "proxy without X-Forwarded-Host, externalUrl saves the day",
			externalUrl:       "http://api.example.com:9000",
			clientScheme:      "http",
			clientHost:        "api.example.com:9000",
			proxyForwardsHost: false,
			expectSuccess:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// --- Write config to temp file ---
			tmpFile := t.TempDir() + "/s3.json"
			require.NoError(t, os.WriteFile(tmpFile, []byte(configJSON), 0644))

			// --- Set up backend ---
			var iam *IdentityAccessManagement
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, errCode := iam.authRequest(r, "Read")
				if errCode != 0 {
					w.WriteHeader(http.StatusForbidden)
					fmt.Fprintf(w, "error: %d", int(errCode))
					return
				}
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, "OK")
			}))
			defer backend.Close()

			// --- Set up reverse proxy ---
			backendURL, _ := url.Parse(backend.URL)
			proxy := httputil.NewSingleHostReverseProxy(backendURL)

			forwardsHost := tt.proxyForwardsHost
			originalDirector := proxy.Director
			proxy.Director = func(req *http.Request) {
				originalHost := req.Host
				originalScheme := req.URL.Scheme
				if originalScheme == "" {
					originalScheme = "http"
				}
				originalDirector(req)
				// Simulate real proxy behavior: rewrite Host to backend address
				// (nginx proxy_pass and Kong both do this by default)
				req.Host = backendURL.Host
				if forwardsHost {
					req.Header.Set("X-Forwarded-Host", originalHost)
					req.Header.Set("X-Forwarded-Proto", originalScheme)
				}
			}

			proxyServer := httptest.NewServer(proxy)
			defer proxyServer.Close()

			// --- Configure IAM ---
			externalUrl := tt.externalUrl
			clientHost := tt.clientHost
			clientScheme := tt.clientScheme
			if externalUrl == "" && clientHost == "" {
				// Dynamic: use the proxy's actual address
				proxyURL, _ := url.Parse(proxyServer.URL)
				externalUrl = proxyServer.URL
				clientHost = proxyURL.Host
				clientScheme = proxyURL.Scheme
			}

			option := &S3ApiServerOption{
				Config:      tmpFile,
				ExternalUrl: externalUrl,
			}
			iam = NewIdentityAccessManagementWithStore(option, nil, "memory")
			require.True(t, iam.isEnabled())

			// --- Sign the request using real AWS SDK v4 signer ---
			clientURL := fmt.Sprintf("%s://%s/test-bucket/test-object", clientScheme, clientHost)
			req, err := http.NewRequest(http.MethodGet, clientURL, nil)
			require.NoError(t, err)
			req.Host = clientHost

			signer := v4.NewSigner()
			payloadHash := fmt.Sprintf("%x", sha256.Sum256([]byte{}))
			err = signer.SignHTTP(
				context.Background(),
				aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey},
				req, payloadHash, "s3", "us-east-1", time.Now(),
			)
			require.NoError(t, err)

			// --- Send the signed request through the proxy ---
			// Rewrite destination to the proxy, but keep signed headers and Host intact
			proxyURL, _ := url.Parse(proxyServer.URL)
			req.URL.Scheme = proxyURL.Scheme
			req.URL.Host = proxyURL.Host

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			if tt.expectSuccess {
				assert.Equal(t, http.StatusOK, resp.StatusCode,
					"Expected signature verification to succeed through reverse proxy")
			} else {
				assert.Equal(t, http.StatusForbidden, resp.StatusCode,
					"Expected signature verification to fail (host mismatch)")
			}
		})
	}
}
