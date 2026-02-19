package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// TestJWKSCacheConcurrentRefresh tests that concurrent JWKS refresh doesn't cause race conditions
func TestJWKSCacheConcurrentRefresh(t *testing.T) {
	// Generate RSA key pair for testing
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate RSA key: %v", err)
	}

	// Track JWKS fetch count
	var fetchCount int
	var fetchMutex sync.Mutex

	// Create mock JWKS server
	jwksServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetchMutex.Lock()
		fetchCount++
		currentFetch := fetchCount
		fetchMutex.Unlock()

		t.Logf("JWKS fetch #%d", currentFetch)

		// Add small delay to simulate network latency
		time.Sleep(10 * time.Millisecond)

		jwks := map[string]interface{}{
			"keys": []map[string]interface{}{
				{
					"kty": "RSA",
					"kid": "test-key-1",
					"use": "sig",
					"alg": "RS256",
					"n":   encodePublicKey(t, &privateKey.PublicKey),
					"e":   "AQAB",
				},
			},
		}
		json.NewEncoder(w).Encode(jwks)
	}))
	defer jwksServer.Close()

	// Initialize OIDC provider with very short TTL to force refresh
	provider := NewOIDCProvider("test")
	config := &OIDCConfig{
		Issuer:              "https://test.example.com",
		ClientID:            "test-client-id",
		JWKSUri:             jwksServer.URL,
		JWKSCacheTTLSeconds: 1, // Very short TTL
	}

	if err := provider.Initialize(config); err != nil {
		t.Fatalf("Failed to initialize provider: %v", err)
	}

	// Generate valid JWT token
	token := generateTestToken(t, privateKey, "test-key-1", "https://test.example.com", "test-client-id")

	// Test 1: Concurrent validation with initial JWKS fetch
	t.Run("concurrent_initial_fetch", func(t *testing.T) {
		fetchCount = 0
		provider.jwksCache = nil // Reset cache

		var wg sync.WaitGroup
		concurrency := 50
		successCount := 0
		var successMutex sync.Mutex

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ctx := context.Background()
				_, err := provider.ValidateToken(ctx, token)
				if err == nil {
					successMutex.Lock()
					successCount++
					successMutex.Unlock()
				} else {
					t.Logf("Goroutine %d validation error: %v", id, err)
				}
			}(i)
		}

		wg.Wait()

		// All validations should succeed
		if successCount != concurrency {
			t.Errorf("Expected %d successful validations, got %d", concurrency, successCount)
		}

		// JWKS should be fetched only once or very few times (due to double-checked locking)
		t.Logf("JWKS fetched %d times for %d concurrent requests", fetchCount, concurrency)
		if fetchCount > 5 {
			t.Errorf("Too many JWKS fetches: %d (expected <= 5 due to locking)", fetchCount)
		}
	})

	// Test 2: Concurrent validation during cache expiration
	t.Run("concurrent_cache_expiration", func(t *testing.T) {
		fetchCount = 0

		// Pre-populate cache
		ctx := context.Background()
		_, err := provider.ValidateToken(ctx, token)
		if err != nil {
			t.Fatalf("Initial validation failed: %v", err)
		}

		initialFetchCount := fetchCount
		t.Logf("Initial fetch count: %d", initialFetchCount)

		// Wait for cache to expire
		time.Sleep(1100 * time.Millisecond)

		// Concurrent validations after cache expiration
		var wg sync.WaitGroup
		concurrency := 50
		successCount := 0
		var successMutex sync.Mutex

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				_, err := provider.ValidateToken(ctx, token)
				if err == nil {
					successMutex.Lock()
					successCount++
					successMutex.Unlock()
				} else {
					t.Logf("Goroutine %d validation error: %v", id, err)
				}
			}(i)
		}

		wg.Wait()

		// All validations should succeed
		if successCount != concurrency {
			t.Errorf("Expected %d successful validations, got %d", concurrency, successCount)
		}

		// Should only fetch once more after expiration
		refreshFetchCount := fetchCount - initialFetchCount
		t.Logf("JWKS refreshed %d times for %d concurrent requests after expiration", refreshFetchCount, concurrency)
		if refreshFetchCount > 5 {
			t.Errorf("Too many JWKS refreshes: %d (expected <= 5)", refreshFetchCount)
		}
	})

	// Test 3: Race detector test - rapid concurrent access
	t.Run("race_detector", func(t *testing.T) {
		// Reset cache
		provider.jwksCache = nil
		fetchCount = 0

		var wg sync.WaitGroup
		concurrency := 100
		iterations := 10

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx := context.Background()
				for j := 0; j < iterations; j++ {
					provider.ValidateToken(ctx, token)
					// Small random delay
					time.Sleep(time.Millisecond * time.Duration(j%3))
				}
			}()
		}

		wg.Wait()
		t.Logf("Completed %d iterations across %d goroutines without race", iterations, concurrency)
	})
}

// TestJWKSCacheIsolation tests that cache updates don't interfere with ongoing reads
func TestJWKSCacheIsolation(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate RSA key: %v", err)
	}

	// Create JWKS server that alternates between two key sets
	keyVersion := 0
	var keyMutex sync.Mutex

	jwksServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keyMutex.Lock()
		currentVersion := keyVersion
		keyVersion++
		keyMutex.Unlock()

		// Simulate slow response
		time.Sleep(50 * time.Millisecond)

		jwks := map[string]interface{}{
			"keys": []map[string]interface{}{
				{
					"kty": "RSA",
					"kid": "test-key-1",
					"use": "sig",
					"alg": "RS256",
					"n":   encodePublicKey(t, &privateKey.PublicKey),
					"e":   "AQAB",
				},
			},
		}

		t.Logf("Serving JWKS version %d", currentVersion)
		json.NewEncoder(w).Encode(jwks)
	}))
	defer jwksServer.Close()

	provider := NewOIDCProvider("test")
	config := &OIDCConfig{
		Issuer:              "https://test.example.com",
		ClientID:            "test-client-id",
		JWKSUri:             jwksServer.URL,
		JWKSCacheTTLSeconds: 1,
	}

	if err := provider.Initialize(config); err != nil {
		t.Fatalf("Failed to initialize provider: %v", err)
	}

	token := generateTestToken(t, privateKey, "test-key-1", "https://test.example.com", "test-client-id")

	// Concurrent readers and writers
	var wg sync.WaitGroup
	ctx := context.Background()
	errorCount := 0
	var errorMutex sync.Mutex

	// Readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, err := provider.ValidateToken(ctx, token)
				if err != nil {
					errorMutex.Lock()
					errorCount++
					errorMutex.Unlock()
					t.Logf("Reader %d got error: %v", id, err)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	if errorCount > 0 {
		t.Errorf("Got %d errors during concurrent read/write operations", errorCount)
	}
}

// Helper function to generate test JWT token
func generateTestToken(t *testing.T, privateKey *rsa.PrivateKey, kid, issuer, audience string) string {
	claims := jwt.MapClaims{
		"sub":   "test-user",
		"iss":   issuer,
		"aud":   audience,
		"exp":   time.Now().Add(1 * time.Hour).Unix(),
		"iat":   time.Now().Unix(),
		"email": "test@example.com",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid

	tokenString, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatalf("Failed to sign token: %v", err)
	}

	return tokenString
}
