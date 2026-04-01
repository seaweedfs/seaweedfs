package webhook

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func init() {
	util_http.InitGlobalHttpClient()
}

func TestHttpClientSendMessage(t *testing.T) {
	var receivedPayload map[string]interface{}
	var receivedHeaders http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &receivedPayload); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &config{
		endpoint:        server.URL,
		authBearerToken: "test-token",
	}

	client, err := newHTTPClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create HTTP client: %v", err)
	}

	message := &filer_pb.EventNotification{
		OldEntry: nil,
		NewEntry: &filer_pb.Entry{
			Name:        "test.txt",
			IsDirectory: false,
		},
	}

	err = client.sendMessage(newWebhookMessage("/test/path", message))
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	if receivedPayload["key"] != "/test/path" {
		t.Errorf("Expected key '/test/path', got %v", receivedPayload["key"])
	}

	if receivedPayload["event_type"] != "create" {
		t.Errorf("Expected event_type 'create', got %v", receivedPayload["event_type"])
	}

	if receivedPayload["message"] == nil {
		t.Error("Expected message to be present")
	}

	if receivedHeaders.Get("Content-Type") != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got %s", receivedHeaders.Get("Content-Type"))
	}

	expectedAuth := "Bearer test-token"
	if receivedHeaders.Get("Authorization") != expectedAuth {
		t.Errorf("Expected Authorization '%s', got %s", expectedAuth, receivedHeaders.Get("Authorization"))
	}
}

func TestHttpClientSendMessageWithoutToken(t *testing.T) {
	var receivedHeaders http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &config{
		endpoint:        server.URL,
		authBearerToken: "",
	}

	client, err := newHTTPClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create HTTP client: %v", err)
	}

	message := &filer_pb.EventNotification{}

	err = client.sendMessage(newWebhookMessage("/test/path", message))
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	if receivedHeaders.Get("Authorization") != "" {
		t.Errorf("Expected no Authorization header, got %s", receivedHeaders.Get("Authorization"))
	}
}

func TestHttpClientSendMessageServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := &config{
		endpoint:        server.URL,
		authBearerToken: "test-token",
	}

	client, err := newHTTPClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create HTTP client: %v", err)
	}

	message := &filer_pb.EventNotification{}

	err = client.sendMessage(newWebhookMessage("/test/path", message))
	if err == nil {
		t.Error("Expected error for server error response")
	}
}

func TestHttpClientSendMessageNetworkError(t *testing.T) {
	cfg := &config{
		endpoint:        "http://localhost:99999",
		authBearerToken: "",
	}

	client, err := newHTTPClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create HTTP client: %v", err)
	}

	message := &filer_pb.EventNotification{}

	err = client.sendMessage(newWebhookMessage("/test/path", message))
	if err == nil {
		t.Error("Expected error for network failure")
	}
}

// TestHttpClientFollowsRedirectAsPost verifies that redirects are followed with POST method preserved
func TestHttpClientFollowsRedirectAsPost(t *testing.T) {
	redirectCalled := false
	finalCalled := false
	var finalMethod string
	var finalBody map[string]interface{}

	// Create final destination server
	finalServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		finalCalled = true
		finalMethod = r.Method
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &finalBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer finalServer.Close()

	// Create redirect server
	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectCalled = true
		// Return 301 redirect to final server
		http.Redirect(w, r, finalServer.URL, http.StatusMovedPermanently)
	}))
	defer redirectServer.Close()

	cfg := &config{
		endpoint:        redirectServer.URL,
		authBearerToken: "test-token",
		timeoutSeconds:  5,
	}

	client, err := newHTTPClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create HTTP client: %v", err)
	}

	message := &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{
			Name: "test.txt",
		},
	}

	// Send message - should follow redirect and recreate POST request
	err = client.sendMessage(newWebhookMessage("/test/path", message))
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	if !redirectCalled {
		t.Error("Expected redirect server to be called")
	}

	if !finalCalled {
		t.Error("Expected final server to be called after redirect")
	}

	if finalMethod != "POST" {
		t.Errorf("Expected POST method at final destination, got %s", finalMethod)
	}

	if finalBody["key"] != "/test/path" {
		t.Errorf("Expected key '/test/path' at final destination, got %v", finalBody["key"])
	}

	// Verify the final URL is cached
	client.endpointMu.RLock()
	cachedURL := client.finalURL
	client.endpointMu.RUnlock()

	if cachedURL != finalServer.URL {
		t.Errorf("Expected cached URL %s, got %s", finalServer.URL, cachedURL)
	}
}

// TestHttpClientUsesCachedRedirect verifies that subsequent requests use the cached redirect destination
func TestHttpClientUsesCachedRedirect(t *testing.T) {
	redirectCount := 0
	finalCount := 0

	// Create final destination server
	finalServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		finalCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer finalServer.Close()

	// Create redirect server
	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectCount++
		http.Redirect(w, r, finalServer.URL, http.StatusMovedPermanently)
	}))
	defer redirectServer.Close()

	cfg := &config{
		endpoint:        redirectServer.URL,
		authBearerToken: "test-token",
		timeoutSeconds:  5,
	}

	client, err := newHTTPClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create HTTP client: %v", err)
	}

	message := &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{
			Name: "test.txt",
		},
	}

	// First request - should hit redirect server
	err = client.sendMessage(newWebhookMessage("/test/path1", message))
	if err != nil {
		t.Fatalf("Failed to send first message: %v", err)
	}

	if redirectCount != 1 {
		t.Errorf("Expected 1 redirect call, got %d", redirectCount)
	}
	if finalCount != 1 {
		t.Errorf("Expected 1 final call, got %d", finalCount)
	}

	// Second request - should use cached URL and skip redirect server
	err = client.sendMessage(newWebhookMessage("/test/path2", message))
	if err != nil {
		t.Fatalf("Failed to send second message: %v", err)
	}

	if redirectCount != 1 {
		t.Errorf("Expected redirect server to be called only once (cached), got %d calls", redirectCount)
	}
	if finalCount != 2 {
		t.Errorf("Expected 2 final calls, got %d", finalCount)
	}
}

// TestHttpClientPreservesPostMethod verifies POST method is preserved and not converted to GET
func TestHttpClientPreservesPostMethod(t *testing.T) {
	var receivedMethod string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &config{
		endpoint:        server.URL,
		authBearerToken: "test-token",
		timeoutSeconds:  5,
	}

	client, err := newHTTPClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create HTTP client: %v", err)
	}

	message := &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{
			Name: "test.txt",
		},
	}

	err = client.sendMessage(newWebhookMessage("/test/path", message))
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	if receivedMethod != "POST" {
		t.Errorf("Expected POST method, got %s", receivedMethod)
	}
}

// TestHttpClientInvalidatesCacheOnError verifies that cache is invalidated when cached URL fails
func TestHttpClientInvalidatesCacheOnError(t *testing.T) {
	finalServerDown := false // Start with server UP
	originalCallCount := 0
	finalCallCount := 0

	// Create final destination server that can be toggled
	finalServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		finalCallCount++
		if finalServerDown {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer finalServer.Close()

	// Create redirect server
	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originalCallCount++
		http.Redirect(w, r, finalServer.URL, http.StatusMovedPermanently)
	}))
	defer redirectServer.Close()

	cfg := &config{
		endpoint:        redirectServer.URL,
		authBearerToken: "test-token",
		timeoutSeconds:  5,
	}

	client, err := newHTTPClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create HTTP client: %v", err)
	}

	message := &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{
			Name: "test.txt",
		},
	}

	// First request - should follow redirect and cache the final URL
	err = client.sendMessage(newWebhookMessage("/test/path1", message))
	if err != nil {
		t.Fatalf("Failed to send first message: %v", err)
	}

	if originalCallCount != 1 {
		t.Errorf("Expected 1 original call, got %d", originalCallCount)
	}
	if finalCallCount != 1 {
		t.Errorf("Expected 1 final call, got %d", finalCallCount)
	}

	// Verify cache was set
	client.endpointMu.RLock()
	cachedURL := client.finalURL
	client.endpointMu.RUnlock()
	if cachedURL != finalServer.URL {
		t.Errorf("Expected cached URL %s, got %s", finalServer.URL, cachedURL)
	}

	// Second request with cached URL working - should use cache
	err = client.sendMessage(newWebhookMessage("/test/path2", message))
	if err != nil {
		t.Fatalf("Failed to send second message: %v", err)
	}

	if originalCallCount != 1 {
		t.Errorf("Expected still 1 original call (using cache), got %d", originalCallCount)
	}
	if finalCallCount != 2 {
		t.Errorf("Expected 2 final calls, got %d", finalCallCount)
	}

	// Third request - bring final server DOWN, should invalidate cache and retry with original
	// Flow: cached URL (fail, depth=0) -> clear cache -> retry original (depth=1) -> redirect -> final (fail, depth=2)
	finalServerDown = true
	err = client.sendMessage(newWebhookMessage("/test/path3", message))
	if err == nil {
		t.Error("Expected error when cached URL fails and retry also fails")
	}

	// originalCallCount: 1 (initial) + 1 (retry after cache invalidation) = 2
	if originalCallCount != 2 {
		t.Errorf("Expected 2 original calls, got %d", originalCallCount)
	}
	// finalCallCount: 2 (previous) + 1 (cached fail) + 1 (retry after redirect) = 4
	if finalCallCount != 4 {
		t.Errorf("Expected 4 final calls, got %d", finalCallCount)
	}

	// Verify final URL is still set (to the failed destination from the redirect)
	client.endpointMu.RLock()
	finalURLAfterError := client.finalURL
	client.endpointMu.RUnlock()
	if finalURLAfterError != finalServer.URL {
		t.Errorf("Expected finalURL to be %s after error, got %s", finalServer.URL, finalURLAfterError)
	}

	// Fourth request - bring final server back UP
	// Since cache still has the final URL, it should use it directly
	finalServerDown = false
	err = client.sendMessage(newWebhookMessage("/test/path4", message))
	if err != nil {
		t.Fatalf("Failed to send fourth message after recovery: %v", err)
	}

	// Should have used the cached URL directly (no new original call)
	// originalCallCount: still 2
	if originalCallCount != 2 {
		t.Errorf("Expected 2 original calls (using cache), got %d", originalCallCount)
	}
	// finalCallCount: 4 + 1 = 5
	if finalCallCount != 5 {
		t.Errorf("Expected 5 final calls, got %d", finalCallCount)
	}

	// Verify cache was re-established
	client.endpointMu.RLock()
	reestablishedCache := client.finalURL
	client.endpointMu.RUnlock()
	if reestablishedCache != finalServer.URL {
		t.Errorf("Expected cache to be re-established to %s, got %s", finalServer.URL, reestablishedCache)
	}
}

// TestHttpClientInvalidatesCacheOnNetworkError verifies cache invalidation on network errors
func TestHttpClientInvalidatesCacheOnNetworkError(t *testing.T) {
	originalCallCount := 0
	var finalServer *httptest.Server
	// Create redirect server
	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originalCallCount++
		if finalServer != nil {
			http.Redirect(w, r, finalServer.URL, http.StatusMovedPermanently)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer redirectServer.Close()

	// Create final destination server
	finalServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	cfg := &config{
		endpoint:        redirectServer.URL,
		authBearerToken: "test-token",
		timeoutSeconds:  5,
	}

	client, err := newHTTPClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create HTTP client: %v", err)
	}

	message := &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{
			Name: "test.txt",
		},
	}

	// First request - establish cache
	err = client.sendMessage(newWebhookMessage("/test/path1", message))
	if err != nil {
		t.Fatalf("Failed to send first message: %v", err)
	}

	if originalCallCount != 1 {
		t.Errorf("Expected 1 original call, got %d", originalCallCount)
	}

	// Close final server to simulate network error
	cachedURL := finalServer.URL
	finalServer.Close()
	finalServer = nil

	// Second request - cached URL is down, should invalidate and retry with original
	err = client.sendMessage(newWebhookMessage("/test/path2", message))
	if err == nil {
		t.Error("Expected error when network fails")
	}

	if originalCallCount != 2 {
		t.Errorf("Expected 2 original calls (retry after cache invalidation), got %d", originalCallCount)
	}

	// Verify cache was cleared
	client.endpointMu.RLock()
	clearedCache := client.finalURL
	client.endpointMu.RUnlock()
	if clearedCache == cachedURL {
		t.Errorf("Expected cache to be invalidated, but still has %s", clearedCache)
	}
}
