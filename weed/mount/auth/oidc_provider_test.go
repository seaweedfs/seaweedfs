package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"github.com/seaweedfs/seaweedfs/weed/testutil/mockoidc"
)

func createSecretFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "client-secret")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("failed to write secret file: %v", err)
	}
	return path
}

func TestDiscoverySuccess(t *testing.T) {
	server, err := mockoidc.NewServer(mockoidc.Config{
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		ExpiresIn:    3600,
	})
	if err != nil {
		t.Fatalf("failed to create mock server: %v", err)
	}
	defer server.Close()
	secretFile := createSecretFile(t, "test-secret")
	ctx := context.Background()
	provider, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        server.URL,
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}
}

func TestDiscoveryInvalidURL(t *testing.T) {
	secretFile := createSecretFile(t, "test-secret")
	ctx := context.Background()
	_, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        "http://localhost:99999",
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
	})
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestDiscoveryMalformedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}))
	defer server.Close()
	secretFile := createSecretFile(t, "test-secret")
	ctx := context.Background()
	_, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        server.URL,
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
	})
	if err == nil {
		t.Fatal("expected error for malformed response")
	}
}

func TestDiscoveryTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
	}))
	defer server.Close()
	secretFile := createSecretFile(t, "test-secret")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        server.URL,
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
		HTTPClient:       &http.Client{Timeout: 100 * time.Millisecond},
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestTokenSuccess(t *testing.T) {
	server, err := mockoidc.NewServer(mockoidc.Config{
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		ExpiresIn:    3600,
	})
	if err != nil {
		t.Fatalf("failed to create mock server: %v", err)
	}
	defer server.Close()
	secretFile := createSecretFile(t, "test-secret")
	ctx := context.Background()
	provider, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        server.URL,
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	token, err := provider.GetToken(ctx)
	if err != nil {
		t.Fatalf("failed to get token: %v", err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}
}

func TestTokenInvalidCredentials(t *testing.T) {
	server, err := mockoidc.NewServer(mockoidc.Config{
		ClientID:     "test-client",
		ClientSecret: "test-secret",
	})
	if err != nil {
		t.Fatalf("failed to create mock server: %v", err)
	}
	defer server.Close()
	secretFile := createSecretFile(t, "wrong-secret")
	ctx := context.Background()
	provider, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        server.URL,
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	_, err = provider.GetToken(ctx)
	if err == nil {
		t.Fatal("expected error for invalid credentials")
	}
}

func TestTokenRefresh(t *testing.T) {
	server, err := mockoidc.NewServer(mockoidc.Config{
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		ExpiresIn:    2,
	})
	if err != nil {
		t.Fatalf("failed to create mock server: %v", err)
	}
	defer server.Close()
	secretFile := createSecretFile(t, "test-secret")
	ctx := context.Background()
	provider, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        server.URL,
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	token1, err := provider.GetToken(ctx)
	if err != nil {
		t.Fatalf("failed to get first token: %v", err)
	}
	time.Sleep(3 * time.Second)
	token2, err := provider.GetToken(ctx)
	if err != nil {
		t.Fatalf("failed to get second token after expiry: %v", err)
	}
	if token2 == "" {
		t.Fatal("expected non-empty second token")
	}
	if token1 == token2 {
		t.Fatal("expected different token after expiry, but got the same token")
	}
}

func TestConcurrentGetToken(t *testing.T) {
	server, err := mockoidc.NewServer(mockoidc.Config{
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		ExpiresIn:    3600,
	})
	if err != nil {
		t.Fatalf("failed to create mock server: %v", err)
	}
	defer server.Close()
	secretFile := createSecretFile(t, "test-secret")
	ctx := context.Background()
	provider, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        server.URL,
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	var wg sync.WaitGroup
	errors := make(chan error, 10)
	tokens := make(chan string, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := provider.GetToken(ctx)
			if err != nil {
				errors <- err
				return
			}
			tokens <- token
		}()
	}
	wg.Wait()
	close(errors)
	close(tokens)
	for err := range errors {
		t.Errorf("concurrent GetToken failed: %v", err)
	}
	var tokenList []string
	for token := range tokens {
		if token == "" {
			t.Error("got empty token")
		}
		tokenList = append(tokenList, token)
	}
	if len(tokenList) != 10 {
		t.Errorf("expected 10 tokens, got %d", len(tokenList))
	}
}

func TestSecretFileValid(t *testing.T) {
	server, err := mockoidc.NewServer(mockoidc.Config{
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		ExpiresIn:    3600,
	})
	if err != nil {
		t.Fatalf("failed to create mock server: %v", err)
	}
	defer server.Close()
	secretFile := createSecretFile(t, "  test-secret  \n")
	ctx := context.Background()
	provider, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        server.URL,
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	token, err := provider.GetToken(ctx)
	if err != nil {
		t.Fatalf("failed to get token: %v", err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}
}

func TestSecretFileNotExists(t *testing.T) {
	ctx := context.Background()
	_, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        "http://localhost:8080",
		ClientID:         "test-client",
		ClientSecretFile: "/nonexistent/path/secret",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent secret file")
	}
}

func TestSecretFileEmpty(t *testing.T) {
	secretFile := createSecretFile(t, "")
	ctx := context.Background()
	_, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        "http://localhost:8080",
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
	})
	if err == nil {
		t.Fatal("expected error for empty secret file")
	}
}

func TestSecretFileWhitespaceOnly(t *testing.T) {
	secretFile := createSecretFile(t, "   \n\t  ")
	ctx := context.Background()
	_, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        "http://localhost:8080",
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
	})
	if err == nil {
		t.Fatal("expected error for whitespace-only secret file")
	}
}

func TestMissingIssuerURL(t *testing.T) {
	secretFile := createSecretFile(t, "test-secret")
	ctx := context.Background()
	_, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
	})
	if err == nil {
		t.Fatal("expected error for missing issuer URL")
	}
}

func TestMissingClientID(t *testing.T) {
	secretFile := createSecretFile(t, "test-secret")
	ctx := context.Background()
	_, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        "http://localhost:8080",
		ClientSecretFile: secretFile,
	})
	if err == nil {
		t.Fatal("expected error for missing client ID")
	}
}

func TestMissingSecretFile(t *testing.T) {
	ctx := context.Background()
	_, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL: "http://localhost:8080",
		ClientID:  "test-client",
	})
	if err == nil {
		t.Fatal("expected error for missing secret file")
	}
}

func TestDiscovery404(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	secretFile := createSecretFile(t, "test-secret")
	ctx := context.Background()
	_, err := NewOIDCTokenProvider(ctx, OIDCConfig{
		IssuerURL:        server.URL,
		ClientID:         "test-client",
		ClientSecretFile: secretFile,
	})
	if err == nil {
		t.Fatal("expected error for 404 response")
	}
}
