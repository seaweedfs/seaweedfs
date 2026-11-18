package webhook

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestParseManagedConfig(t *testing.T) {
	c, err := ParseManagedConfig(nil)
	if err != nil || len(c.Targets) != 0 {
		t.Fatalf("empty content: got %+v, %v", c, err)
	}

	c, err = ParseManagedConfig([]byte(`{"targets":[{"name":"ingest","enabled":true,"endpoint":"https://example.com/hook","event_types":["create"],"path_prefixes":["/buckets/a"],"max_retries":5}]}`))
	if err != nil {
		t.Fatalf("ParseManagedConfig: %v", err)
	}
	cfg := c.Targets[0].config()
	if cfg.maxRetries != 5 || cfg.timeoutSeconds != 10 || cfg.nWorkers != 5 || cfg.bufferSize != 10_000 {
		t.Fatalf("unexpected settings %+v", cfg)
	}

	for name, body := range map[string]string{
		"bad name":      `{"targets":[{"name":"a b","endpoint":"https://example.com"}]}`,
		"duplicate":     `{"targets":[{"name":"a","endpoint":"https://example.com"},{"name":"a","endpoint":"https://example.com"}]}`,
		"no scheme":     `{"targets":[{"name":"a","endpoint":"example.com/hook"}]}`,
		"ftp endpoint":  `{"targets":[{"name":"a","endpoint":"ftp://example.com/hook"}]}`,
		"event type":    `{"targets":[{"name":"a","endpoint":"https://example.com","event_types":["chmod"]}]}`,
		"relative path": `{"targets":[{"name":"a","endpoint":"https://example.com","path_prefixes":["buckets"]}]}`,
		"timeout range": `{"targets":[{"name":"a","endpoint":"https://example.com","timeout_seconds":301}]}`,
		"neg timeout":   `{"targets":[{"name":"a","endpoint":"https://example.com","timeout_seconds":-1}]}`,
		"neg workers":   `{"targets":[{"name":"a","endpoint":"https://example.com","workers":-1}]}`,
		"not json":      `targets`,
	} {
		if _, err := ParseManagedConfig([]byte(body)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestManagedQueueDeliversAndDrainsOnClose(t *testing.T) {
	var hits atomic.Int32
	var auth atomic.Value
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		<-release
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()

	q, err := NewManagedQueue(&ManagedConfig{Targets: []ManagedTarget{
		{Name: "on", Enabled: true, Endpoint: server.URL, BearerToken: "secret", PathPrefixes: []string{"/data/"}},
		{Name: "off", Endpoint: "https://example.com/never"},
	}})
	if err != nil {
		t.Fatalf("NewManagedQueue: %v", err)
	}
	if len(q.targets) != 1 {
		t.Fatalf("expected only the enabled target, got %d", len(q.targets))
	}

	msg := &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "f.txt"}}
	for _, key := range []string{"/data/f.txt", "/other/f.txt"} {
		if err := q.SendMessage(key, msg); err != nil {
			t.Fatalf("SendMessage(%q): %v", key, err)
		}
	}

	closed := make(chan struct{})
	go func() {
		q.Close(10 * time.Second)
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned before the in-flight delivery finished")
	case <-time.After(300 * time.Millisecond):
	}
	unblock()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the delivery finished")
	}

	if got := hits.Load(); got != 1 {
		t.Fatalf("expected 1 delivery, got %d", got)
	}
	if got, _ := auth.Load().(string); !strings.HasSuffix(got, "secret") {
		t.Fatalf("expected the bearer token, got %q", got)
	}
}

func TestManagedQueueNoEnabledTargets(t *testing.T) {
	q, err := NewManagedQueue(&ManagedConfig{Targets: []ManagedTarget{{Name: "off", Endpoint: "https://example.com"}}})
	if err != nil || q != nil {
		t.Fatalf("expected no queue, got %v, %v", q, err)
	}
}

func TestManagedQueueCloseWaitsForBlockedSender(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		hits.Add(1)
	}))
	defer server.Close()
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()

	q, err := NewManagedQueue(&ManagedConfig{Targets: []ManagedTarget{
		{Name: "slow", Enabled: true, Endpoint: server.URL, Workers: 1, BufferSize: 100},
	}})
	if err != nil {
		t.Fatalf("NewManagedQueue: %v", err)
	}
	msg := &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "f.txt"}}
	for i := 0; i < 100; i++ {
		if err := q.SendMessage("/a", msg); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
	}
	blocked := make(chan error, 1)
	go func() { blocked <- q.SendMessage("/b", msg) }()
	for q.senders.Load() == 0 {
		time.Sleep(time.Millisecond)
	}

	closed := make(chan struct{})
	go func() {
		q.Close(10 * time.Second)
		close(closed)
	}()
	for !q.closing.Load() {
		time.Sleep(time.Millisecond)
	}
	if err := q.SendMessage("/c", msg); err != ErrQueueClosed {
		t.Fatalf("expected ErrQueueClosed after Close started, got %v", err)
	}

	unblock()
	if err := <-blocked; err != nil {
		t.Fatalf("blocked sender: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
	if got := hits.Load(); got != 101 {
		t.Fatalf("expected both accepted events delivered, got %d", got)
	}
}
