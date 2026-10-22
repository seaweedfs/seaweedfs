package webhook

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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

func newQueueForTest(t *testing.T, cfg *config) *Queue {
	t.Helper()
	tgt, err := newTarget("", cfg)
	if err != nil {
		t.Fatalf("newTarget() error = %v", err)
	}
	q := &Queue{}
	if err := q.initialize([]*target{tgt}); err != nil {
		t.Fatalf("initialize() error = %v", err)
	}
	return q
}

func closeQueueForTest(q *Queue) {
	if q.cancel != nil {
		q.cancel()
	}
	time.Sleep(100 * time.Millisecond)
	for _, r := range q.routers {
		r.Close()
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

	q := newQueueForTest(t, cfg)
	defer closeQueueForTest(q)

	if len(q.routers) != 1 {
		t.Error("Expected router to be initialized")
	}
	if q.queueChannel == nil {
		t.Error("Expected queueChannel to be initialized")
	}
	if q.targets[0].client == nil {
		t.Error("Expected client to be initialized")
	}
	if q.targets[0].config == nil {
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

	q := newQueueForTest(t, cfg)
	defer closeQueueForTest(q)

	msg := &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{
			Name: "test.txt",
		},
	}

	if err := q.SendMessage("/test/path", msg); err != nil {
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
	tgt := &target{
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

	if err := tgt.handleWebhook(wmMsg); err != nil {
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

	q := newQueueForTest(t, cfg)
	defer closeQueueForTest(q)

	msg := &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{
			Name: "test.txt",
		},
	}

	if err := q.SendMessage("/test/path", msg); err != nil {
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

	q := newQueueForTest(t, cfg)
	defer closeQueueForTest(q)

	// Verify that the queue is properly configured for retries
	if q.targets[0].config.maxRetries != 3 {
		t.Errorf("Expected maxRetries=3, got %d", q.targets[0].config.maxRetries)
	}

	if q.targets[0].config.backoffSeconds != 2 {
		t.Errorf("Expected backoffSeconds=2, got %d", q.targets[0].config.backoffSeconds)
	}

	if q.targets[0].config.maxBackoffSeconds != 10 {
		t.Errorf("Expected maxBackoffSeconds=10, got %d", q.targets[0].config.maxBackoffSeconds)
	}

	// Test that we can send a message (retry behavior is handled by Watermill middleware)
	msg := &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{Name: "test.txt"},
	}

	if err := q.SendMessage("/test/retry", msg); err != nil {
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

			q := newQueueForTest(t, cfg)
			defer closeQueueForTest(q)

			// Wait for router and subscriber to be fully ready
			time.Sleep(200 * time.Millisecond)

			msg := &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{
					Name: "test.txt",
				},
			}

			if err := q.SendMessage("/test/path", msg); err != nil {
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

type mapConfig struct {
	values map[string]interface{}
}

func (m *mapConfig) GetString(key string) string {
	v, _ := m.values[key].(string)
	return v
}

func (m *mapConfig) GetBool(key string) bool {
	v, _ := m.values[key].(bool)
	return v
}

func (m *mapConfig) GetInt(key string) int {
	v, _ := m.values[key].(int)
	return v
}

func (m *mapConfig) GetStringSlice(key string) []string {
	v, _ := m.values[key].([]string)
	return v
}

func (m *mapConfig) SetDefault(key string, value interface{}) {}

func (m *mapConfig) GetStringMap(key string) map[string]interface{} {
	v, _ := m.values[key].(map[string]interface{})
	return v
}

func TestNamedTargets(t *testing.T) {
	var hitsA, hitsB atomic.Int32
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsA.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer serverA.Close()
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsB.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer serverB.Close()

	cfg := &mapConfig{values: map[string]interface{}{
		"notification.webhook": map[string]interface{}{
			"file_ingress": map[string]interface{}{
				"enabled":       true,
				"endpoint":      serverA.URL,
				"path_prefixes": []string{"/data/"},
			},
			"metadata_extractor": map[string]interface{}{
				"enabled":       true,
				"endpoint":      serverB.URL,
				"path_prefixes": []string{"/repository/"},
			},
			"disabled_one": map[string]interface{}{
				"enabled":  false,
				"endpoint": "https://example.com/never",
			},
		},
		"notification.webhook.file_ingress.enabled":             true,
		"notification.webhook.file_ingress.endpoint":            serverA.URL,
		"notification.webhook.file_ingress.path_prefixes":       []string{"/data/"},
		"notification.webhook.metadata_extractor.enabled":       true,
		"notification.webhook.metadata_extractor.endpoint":      serverB.URL,
		"notification.webhook.metadata_extractor.path_prefixes": []string{"/repository/"},
	}}

	q := &Queue{}
	if !q.HasEnabledSubconfig(cfg, "notification.webhook.") {
		t.Fatal("expected named subconfig to be detected as enabled")
	}

	targets, err := buildTargets(cfg, "notification.webhook.")
	if err != nil {
		t.Fatalf("buildTargets() error = %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(targets))
	}
	if targets[0].name != "file_ingress" || targets[1].name != "metadata_extractor" {
		t.Fatalf("unexpected target names: %s, %s", targets[0].name, targets[1].name)
	}

	if err := q.initialize(targets); err != nil {
		t.Fatalf("initialize() error = %v", err)
	}
	defer closeQueueForTest(q)

	time.Sleep(200 * time.Millisecond)

	msg := &filer_pb.EventNotification{
		NewEntry: &filer_pb.Entry{Name: "f.txt"},
	}
	for _, key := range []string{"/data/f.txt", "/repository/f.txt", "/elsewhere/f.txt"} {
		if err := q.SendMessage(key, msg); err != nil {
			t.Errorf("SendMessage(%q) error = %v", key, err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && (hitsA.Load() == 0 || hitsB.Load() == 0) {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	if got := hitsA.Load(); got != 1 {
		t.Errorf("expected file_ingress to receive 1 event, got %d", got)
	}
	if got := hitsB.Load(); got != 1 {
		t.Errorf("expected metadata_extractor to receive 1 event, got %d", got)
	}
}

func TestQueueBurstKeepsPendingEvents(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &config{
		endpoint:          server.URL,
		timeoutSeconds:    5,
		maxRetries:        0,
		backoffSeconds:    1,
		maxBackoffSeconds: 1,
		nWorkers:          1,
		bufferSize:        2,
	}
	q := newQueueForTest(t, cfg)
	defer closeQueueForTest(q)

	const total = 6
	msg := &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "f.txt"}}
	for i := 0; i < total; i++ {
		if err := q.SendMessage("/test/path", msg); err != nil {
			t.Fatalf("SendMessage() error = %v", err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && hits.Load() < total {
		time.Sleep(50 * time.Millisecond)
	}
	if got := hits.Load(); got != total {
		t.Errorf("expected %d deliveries, got %d", total, got)
	}
}

// A saturated target still applies backpressure to the sender, but it must
// not starve the targets after it: the event reaches a healthy endpoint
// while the slow one is still out of pending slots.
func TestSaturatedTargetDoesNotStarveOthers(t *testing.T) {
	var hitsA, hitsB atomic.Int32
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsA.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer serverA.Close()
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsB.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer serverB.Close()

	newTestTarget := func(name, endpoint string) *target {
		tgt, err := newTarget(name, &config{
			endpoint:          endpoint,
			timeoutSeconds:    5,
			maxRetries:        0,
			backoffSeconds:    1,
			maxBackoffSeconds: 1,
			nWorkers:          1,
			bufferSize:        2,
		})
		if err != nil {
			t.Fatalf("newTarget() error = %v", err)
		}
		return tgt
	}
	slow := newTestTarget("slow", serverA.URL)
	fast := newTestTarget("fast", serverB.URL)

	q := &Queue{}
	if err := q.initialize([]*target{slow, fast}); err != nil {
		t.Fatalf("initialize() error = %v", err)
	}
	defer closeQueueForTest(q)

	for i := 0; i < cap(slow.pending); i++ {
		slow.pending <- struct{}{}
	}

	done := make(chan error, 1)
	go func() {
		done <- q.SendMessage("/test/path", &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "f.txt"}})
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && hitsB.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := hitsB.Load(); got != 1 {
		t.Fatalf("expected fast target to receive the event while slow is saturated, got %d", got)
	}
	select {
	case err := <-done:
		t.Fatalf("SendMessage() returned %v while the saturated target had no free slot", err)
	default:
	}

	<-slow.pending
	if err := <-done; err != nil {
		t.Fatalf("SendMessage() error = %v", err)
	}

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && hitsA.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := hitsA.Load(); got != 1 {
		t.Errorf("expected the saturated target to deliver once a slot freed, got %d", got)
	}
}

// A disabled flat section keeps no target even when an endpoint is set —
// the scaffold ships exactly that combination, and a named target must not
// reactivate it.
func TestBuildTargetsDisabledFlat(t *testing.T) {
	cfg := &mapConfig{values: map[string]interface{}{
		"notification.webhook": map[string]interface{}{
			"enabled":  false,
			"endpoint": "https://your-server.com/webhook",
			"named": map[string]interface{}{
				"enabled":  true,
				"endpoint": "https://example.com/hook",
			},
		},
		"notification.webhook.enabled":        false,
		"notification.webhook.endpoint":       "https://your-server.com/webhook",
		"notification.webhook.named.enabled":  true,
		"notification.webhook.named.endpoint": "https://example.com/hook",
	}}

	targets, err := buildTargets(cfg, "notification.webhook.")
	if err != nil {
		t.Fatalf("buildTargets() error = %v", err)
	}
	if len(targets) != 1 || targets[0].name != "named" {
		t.Fatalf("expected only the named target, got %v", targets)
	}
}
