package webhook

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: &config{
				endpoint:          "https://example.com/webhook",
				authBearerToken:   "test-token",
				timeoutSeconds:    30,
				maxRetries:        3,
				backoffSeconds:    5,
				maxBackoffSeconds: 30,
				nWorkers:          5,
				bufferSize:        10000,
			},
			wantErr: false,
		},
		{
			name: "empty endpoint",
			config: &config{
				endpoint:          "",
				timeoutSeconds:    30,
				maxRetries:        3,
				backoffSeconds:    5,
				maxBackoffSeconds: 30,
				nWorkers:          5,
				bufferSize:        10000,
			},
			wantErr: true,
			errMsg:  "endpoint is required",
		},
		{
			name: "invalid URL",
			config: &config{
				endpoint:          "://invalid-url",
				timeoutSeconds:    30,
				maxRetries:        3,
				backoffSeconds:    5,
				maxBackoffSeconds: 30,
				nWorkers:          5,
				bufferSize:        10000,
			},
			wantErr: true,
			errMsg:  "invalid webhook endpoint",
		},
		{
			name: "timeout too large",
			config: &config{
				endpoint:          "https://example.com/webhook",
				timeoutSeconds:    301,
				maxRetries:        3,
				backoffSeconds:    5,
				maxBackoffSeconds: 30,
				nWorkers:          5,
				bufferSize:        10000,
			},
			wantErr: true,
			errMsg:  "timeout must be between",
		},
		{
			name: "too many retries",
			config: &config{
				endpoint:          "https://example.com/webhook",
				timeoutSeconds:    30,
				maxRetries:        11,
				backoffSeconds:    5,
				maxBackoffSeconds: 30,
				nWorkers:          5,
				bufferSize:        10000,
			},
			wantErr: true,
			errMsg:  "max retries must be between",
		},
		{
			name: "too many workers",
			config: &config{
				endpoint:          "https://example.com/webhook",
				timeoutSeconds:    30,
				maxRetries:        3,
				backoffSeconds:    5,
				maxBackoffSeconds: 30,
				nWorkers:          101,
				bufferSize:        10000,
			},
			wantErr: true,
			errMsg:  "workers must be between",
		},
		{
			name: "buffer too large",
			config: &config{
				endpoint:          "https://example.com/webhook",
				timeoutSeconds:    30,
				maxRetries:        3,
				backoffSeconds:    5,
				maxBackoffSeconds: 30,
				nWorkers:          5,
				bufferSize:        1000001,
			},
			wantErr: true,
			errMsg:  "buffer size must be between",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.errMsg != "" {
				if err.Error() == "" || !contains(err.Error(), tt.errMsg) {
					t.Errorf("validate() error message = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			}
		})
	}
}

func TestWebhookMessageSerialization(t *testing.T) {
	msg := &filer_pb.EventNotification{
		OldEntry: nil,
		NewEntry: &filer_pb.Entry{
			Name:        "test.txt",
			IsDirectory: false,
		},
	}

	webhookMsg := newWebhookMessage("/test/path", msg)

	wmMsg, err := webhookMsg.toWaterMillMessage()
	if err != nil {
		t.Fatalf("Failed to convert to windmill message: %v", err)
	}

	var deserializedMsg webhookMessage
	err = json.Unmarshal(wmMsg.Payload, &deserializedMsg)
	if err != nil {
		t.Fatalf("Failed to unmarshal message: %v", err)
	}

	if deserializedMsg.Key != "/test/path" {
		t.Errorf("Expected key '/test/path', got %v", deserializedMsg.Key)
	}

	var eventNotification filer_pb.EventNotification
	err = json.Unmarshal(deserializedMsg.MessageData, &eventNotification)
	if err != nil {
		t.Fatalf("Failed to unmarshal event notification from message data: %v", err)
	}

	if eventNotification.NewEntry.Name != "test.txt" {
		t.Errorf("Expected file name 'test.txt', got %v", eventNotification.NewEntry.Name)
	}
}

func TestQueueInitialize(t *testing.T) {
	cfg := &config{
		endpoint:          "https://example.com/webhook",
		authBearerToken:   "test-token",
		timeoutSeconds:    10,
		maxRetries:        3,
		backoffSeconds:    3,
		maxBackoffSeconds: 60,
		nWorkers:          20,
		bufferSize:        100,
	}

	q := &Queue{}
	err := q.initialize(cfg)
	if err != nil {
		t.Errorf("Initialize() error = %v", err)
	}

	if q.router == nil {
		t.Error("Expected router to be initialized")
	}
	if q.publisher == nil {
		t.Error("Expected publisher to be initialized")
	}
	if q.client == nil {
		t.Error("Expected client to be initialized")
	}
	if q.config == nil {
		t.Error("Expected config to be initialized")
	}
}

func TestQueueSendMessage(t *testing.T) {
	cfg := &config{
		endpoint:          "https://example.com/webhook",
		authBearerToken:   "test-token",
		timeoutSeconds:    10,
		maxRetries:        3,
		backoffSeconds:    3,
		maxBackoffSeconds: 60,
		nWorkers:          1,
		bufferSize:        100,
	}

	q := &Queue{}
	err := q.initialize(cfg)
	if err != nil {
		t.Fatalf("Failed to initialize queue: %v", err)
	}

	msg := &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{
			Name: "test.txt",
		},
	}

	err = q.SendMessage("/test/path", msg)
	if err != nil {
		t.Errorf("SendMessage() error = %v", err)
	}
}

func TestQueueHandleWebhook(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &config{
		endpoint:          server.URL,
		authBearerToken:   "test-token",
		timeoutSeconds:    1,
		maxRetries:        0,
		backoffSeconds:    1,
		maxBackoffSeconds: 1,
		nWorkers:          1,
		bufferSize:        10,
	}

	client, _ := newHTTPClient(cfg)
	q := &Queue{
		client: client,
	}

	message := newWebhookMessage("/test/path", &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{
			Name: "test.txt",
		},
	})

	wmMsg, err := message.toWaterMillMessage()
	if err != nil {
		t.Fatalf("Failed to create windmill message: %v", err)
	}

	err = q.handleWebhook(wmMsg)
	if err != nil {
		t.Errorf("handleWebhook() error = %v", err)
	}
}

func TestQueueEndToEnd(t *testing.T) {
	var receivedMessages []string
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		mu.Lock()
		receivedMessages = append(receivedMessages, payload["key"].(string))
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &config{
		endpoint:          server.URL,
		authBearerToken:   "test-token",
		timeoutSeconds:    1,
		maxRetries:        0,
		backoffSeconds:    1,
		maxBackoffSeconds: 1,
		nWorkers:          1,
		bufferSize:        10,
	}

	q := &Queue{}
	err := q.initialize(cfg)
	if err != nil {
		t.Fatalf("Failed to initialize queue: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 2; i++ {
		msg := &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name: fmt.Sprintf("file%d.txt", i),
			},
		}

		err = q.SendMessage(fmt.Sprintf("/test/path/%d", i), msg)
		if err != nil {
			t.Errorf("Failed to send message %d: %v", i, err)
		}
	}

	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(receivedMessages) == 0 {
		t.Error("Expected to receive some messages, got none")
	}

	for i, key := range receivedMessages {
		if !contains(key, "/test/path/") {
			t.Errorf("Message %d: expected key to contain '/test/path/', got %v", i, key)
		}
	}
}

func TestQueueRetryMechanism(t *testing.T) {
	attemptCount := int32(0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attemptCount, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := &config{
		endpoint:          server.URL,
		authBearerToken:   "test-token",
		timeoutSeconds:    1,
		maxRetries:        1,
		backoffSeconds:    1,
		maxBackoffSeconds: 1,
		nWorkers:          1,
		bufferSize:        10,
	}

	q := &Queue{}
	err := q.initialize(cfg)
	if err != nil {
		t.Fatalf("Failed to initialize queue: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	msg := &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{Name: "test.txt"},
	}

	err = q.SendMessage("/test/retry", msg)
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	finalCount := atomic.LoadInt32(&attemptCount)
	if finalCount < 1 {
		t.Errorf("Expected at least 1 attempt, got %d", finalCount)
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
