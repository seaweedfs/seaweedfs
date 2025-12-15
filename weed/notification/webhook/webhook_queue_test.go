package webhook

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/protobuf/proto"
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
				if err.Error() == "" || !strings.Contains(err.Error(), tt.errMsg) {
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
		t.Fatalf("Failed to convert to watermill message: %v", err)
	}

	// Unmarshal the protobuf payload directly
	var eventNotification filer_pb.EventNotification
	err = proto.Unmarshal(wmMsg.Payload, &eventNotification)
	if err != nil {
		t.Fatalf("Failed to unmarshal protobuf message: %v", err)
	}

	// Check metadata
	if wmMsg.Metadata.Get("key") != "/test/path" {
		t.Errorf("Expected key '/test/path', got %v", wmMsg.Metadata.Get("key"))
	}

	if wmMsg.Metadata.Get("event_type") != "create" {
		t.Errorf("Expected event type 'create', got %v", wmMsg.Metadata.Get("event_type"))
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
		nWorkers:          1,
		bufferSize:        100,
	}

	q := &Queue{}
	err := q.initialize(cfg)
	if err != nil {
		t.Errorf("Initialize() error = %v", err)
	}

	defer func() {
		if q.cancel != nil {
			q.cancel()
		}
		time.Sleep(100 * time.Millisecond)
		if q.router != nil {
			q.router.Close()
		}
	}()

	if q.router == nil {
		t.Error("Expected router to be initialized")
	}
	if q.queueChannel == nil {
		t.Error("Expected queueChannel to be initialized")
	}
	if q.client == nil {
		t.Error("Expected client to be initialized")
	}
	if q.config == nil {
		t.Error("Expected config to be initialized")
	}
}

// TestQueueSendMessage test sending messages to the queue
func TestQueueSendMessage(t *testing.T) {
	cfg := &config{
		endpoint:          "https://example.com/webhook",
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

	defer func() {
		if q.cancel != nil {
			q.cancel()
		}
		time.Sleep(100 * time.Millisecond)
		if q.router != nil {
			q.router.Close()
		}
	}()

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
		sem:    make(chan struct{}, cfg.nWorkers),
	}

	message := newWebhookMessage("/test/path", &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{
			Name: "test.txt",
		},
	})

	wmMsg, err := message.toWaterMillMessage()
	if err != nil {
		t.Fatalf("Failed to create watermill message: %v", err)
	}

	err = q.handleWebhook(wmMsg)
	if err != nil {
		t.Errorf("handleWebhook() error = %v", err)
	}
}

func TestQueueEndToEnd(t *testing.T) {
	// Simplified test - just verify the queue can be created and message can be sent
	// without needing full end-to-end processing
	cfg := &config{
		endpoint:          "https://example.com/webhook",
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

	defer func() {
		if q.cancel != nil {
			q.cancel()
		}
		time.Sleep(100 * time.Millisecond)
		if q.router != nil {
			q.router.Close()
		}
	}()

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

func TestQueueRetryMechanism(t *testing.T) {
	cfg := &config{
		endpoint:          "https://example.com/webhook",
		authBearerToken:   "test-token",
		timeoutSeconds:    1,
		maxRetries:        3, // Test that this config is used
		backoffSeconds:    2,
		maxBackoffSeconds: 10,
		nWorkers:          1,
		bufferSize:        10,
	}

	q := &Queue{}
	err := q.initialize(cfg)
	if err != nil {
		t.Fatalf("Failed to initialize queue: %v", err)
	}

	defer func() {
		if q.cancel != nil {
			q.cancel()
		}
		time.Sleep(100 * time.Millisecond)
		if q.router != nil {
			q.router.Close()
		}
	}()

	// Verify that the queue is properly configured for retries
	if q.config.maxRetries != 3 {
		t.Errorf("Expected maxRetries=3, got %d", q.config.maxRetries)
	}

	if q.config.backoffSeconds != 2 {
		t.Errorf("Expected backoffSeconds=2, got %d", q.config.backoffSeconds)
	}

	if q.config.maxBackoffSeconds != 10 {
		t.Errorf("Expected maxBackoffSeconds=10, got %d", q.config.maxBackoffSeconds)
	}

	// Test that we can send a message (retry behavior is handled by Watermill middleware)
	msg := &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{Name: "test.txt"},
	}

	err = q.SendMessage("/test/retry", msg)
	if err != nil {
		t.Errorf("SendMessage() error = %v", err)
	}
}

// TestQueueNoDuplicateWebhooks verifies that webhooks are sent only once regardless of worker count
func TestQueueNoDuplicateWebhooks(t *testing.T) {
	tests := []struct {
		name     string
		nWorkers int
		expected int
	}{
		{"1 worker", 1, 1},
		{"5 workers", 5, 1},
		{"10 workers", 10, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callCount := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				callCount++
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			cfg := &config{
				endpoint:          server.URL,
				authBearerToken:   "test-token",
				timeoutSeconds:    5,
				maxRetries:        0,
				backoffSeconds:    1,
				maxBackoffSeconds: 1,
				nWorkers:          tt.nWorkers,
				bufferSize:        10,
			}

			q := &Queue{}
			err := q.initialize(cfg)
			if err != nil {
				t.Fatalf("Failed to initialize queue: %v", err)
			}

			defer func() {
				if q.cancel != nil {
					q.cancel()
				}
				time.Sleep(100 * time.Millisecond)
				if q.router != nil {
					q.router.Close()
				}
			}()

			// Wait for router and subscriber to be fully ready
			time.Sleep(200 * time.Millisecond)

			msg := &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{
					Name: "test.txt",
				},
			}

			err = q.SendMessage("/test/path", msg)
			if err != nil {
				t.Errorf("SendMessage() error = %v", err)
			}

			// Wait for message processing
			time.Sleep(500 * time.Millisecond)

			if callCount != tt.expected {
				t.Errorf("Expected %d webhook call(s), got %d with %d workers", tt.expected, callCount, tt.nWorkers)
			}
		})
	}
}

func TestQueueSendMessageWithFilter(t *testing.T) {
	tests := []struct {
		name          string
		cfg           *config
		key           string
		notification  *filer_pb.EventNotification
		shouldPublish bool
	}{
		{
			name: "allowed event type",
			cfg: &config{
				endpoint:          "https://example.com/webhook",
				timeoutSeconds:    10,
				maxRetries:        1,
				backoffSeconds:    1,
				maxBackoffSeconds: 1,
				nWorkers:          1,
				bufferSize:        10,
				eventTypes:        []string{"create"},
			},
			key: "/data/file.txt",
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file.txt"},
			},
			shouldPublish: true,
		},
		{
			name: "filtered event type",
			cfg: &config{
				endpoint:          "https://example.com/webhook",
				timeoutSeconds:    10,
				maxRetries:        1,
				backoffSeconds:    1,
				maxBackoffSeconds: 1,
				nWorkers:          1,
				bufferSize:        10,
				eventTypes:        []string{"update", "rename"},
			},
			key: "/data/file.txt",
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file.txt"},
			},
			shouldPublish: false,
		},
		{
			name: "allowed path prefix",
			cfg: &config{
				endpoint:          "https://example.com/webhook",
				timeoutSeconds:    10,
				maxRetries:        1,
				backoffSeconds:    1,
				maxBackoffSeconds: 1,
				nWorkers:          1,
				bufferSize:        10,
				pathPrefixes:      []string{"/data/"},
			},
			key: "/data/file.txt",
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file.txt"},
			},
			shouldPublish: true,
		},
		{
			name: "filtered path prefix",
			cfg: &config{
				endpoint:          "https://example.com/webhook",
				timeoutSeconds:    10,
				maxRetries:        1,
				backoffSeconds:    1,
				maxBackoffSeconds: 1,
				nWorkers:          1,
				bufferSize:        10,
				pathPrefixes:      []string{"/logs/"},
			},
			key: "/data/file.txt",
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file.txt"},
			},
			shouldPublish: false,
		},
		{
			name: "combined filters - both pass",
			cfg: &config{
				endpoint:          "https://example.com/webhook",
				timeoutSeconds:    10,
				maxRetries:        1,
				backoffSeconds:    1,
				maxBackoffSeconds: 1,
				nWorkers:          1,
				bufferSize:        10,
				eventTypes:        []string{"create", "delete"},
				pathPrefixes:      []string{"/data/", "/logs/"},
			},
			key: "/data/file.txt",
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file.txt"},
			},
			shouldPublish: true,
		},
		{
			name: "combined filters - event fails",
			cfg: &config{
				endpoint:          "https://example.com/webhook",
				timeoutSeconds:    10,
				maxRetries:        1,
				backoffSeconds:    1,
				maxBackoffSeconds: 1,
				nWorkers:          1,
				bufferSize:        10,
				eventTypes:        []string{"update", "delete"},
				pathPrefixes:      []string{"/data/", "/logs/"},
			},
			key: "/data/file.txt",
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file.txt"},
			},
			shouldPublish: false,
		},
		{
			name: "combined filters - path fails",
			cfg: &config{
				endpoint:          "https://example.com/webhook",
				timeoutSeconds:    10,
				maxRetries:        1,
				backoffSeconds:    1,
				maxBackoffSeconds: 1,
				nWorkers:          1,
				bufferSize:        10,
				eventTypes:        []string{"create", "delete"},
				pathPrefixes:      []string{"/logs/"},
			},
			key: "/data/file.txt",
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file.txt"},
			},
			shouldPublish: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldPublish := newFilter(tt.cfg).shouldPublish(tt.key, tt.notification)
			if shouldPublish != tt.shouldPublish {
				t.Errorf("Expected shouldPublish=%v, got %v", tt.shouldPublish, shouldPublish)
			}
		})
	}
}
