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
