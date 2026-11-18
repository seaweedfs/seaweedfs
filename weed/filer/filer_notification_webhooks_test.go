package filer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestManagedNotificationWebhooks(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Key string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		keys = append(keys, body.Key)
		mu.Unlock()
	}))
	defer server.Close()
	received := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), keys...)
	}

	store := newStubFilerStore()
	f := newTestFiler(t, store, nil)
	t.Cleanup(func() {
		if q := f.webhooks.queue.Load(); q != nil {
			q.Close(time.Second)
		}
	})

	saveConf := func(content string) {
		store.entries[NotificationWebhooksConfPath] = &Entry{FullPath: util.FullPath(NotificationWebhooksConfPath), Content: []byte(content)}
	}
	confEvent := &filer_pb.SubscribeMetadataResponse{
		Directory:         DirectoryEtcSeaweedFS,
		EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: NotificationWebhooksConfName}},
	}

	f.LoadNotificationWebhooks()
	if f.webhooks.queue.Load() != nil {
		t.Fatal("expected no webhook queue without stored targets")
	}

	saveConf(fmt.Sprintf(`{"targets":[{"name":"data","enabled":true,"endpoint":%q,"bearer_token":"tok","path_prefixes":["/"]}]}`, server.URL))
	f.maybeReloadNotificationWebhooks(confEvent)
	if f.webhooks.queue.Load() == nil {
		t.Fatal("expected a webhook queue after the targets were stored")
	}

	ctx := context.Background()
	f.NotifyUpdateEvent(ctx, nil, store.entries[NotificationWebhooksConfPath], false, false, nil)
	f.NotifyUpdateEvent(ctx, nil, &Entry{FullPath: "/data/a.txt"}, false, false, nil)
	waitForWebhook(t, func() bool { return len(received()) == 1 })
	time.Sleep(200 * time.Millisecond)
	if got := received(); len(got) != 1 || got[0] != "/data/a.txt" {
		t.Fatalf("expected only /data/a.txt to be delivered, got %v", got)
	}

	running := f.webhooks.queue.Load()
	f.maybeReloadNotificationWebhooks(confEvent)
	if f.webhooks.queue.Load() != running {
		t.Fatal("unchanged targets must keep the running queue")
	}
	saveConf(`{"targets":[{"name":"bad name","enabled":true,"endpoint":"https://example.com"}]}`)
	f.maybeReloadNotificationWebhooks(confEvent)
	if f.webhooks.queue.Load() != running {
		t.Fatal("invalid targets must keep the running queue")
	}

	delete(store.entries, NotificationWebhooksConfPath)
	f.maybeReloadNotificationWebhooks(&filer_pb.SubscribeMetadataResponse{
		Directory:         DirectoryEtcSeaweedFS,
		EventNotification: &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: NotificationWebhooksConfName}},
	})
	if f.webhooks.queue.Load() != nil {
		t.Fatal("expected deleting the stored targets to stop the queue")
	}
}

func waitForWebhook(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestManagedNotificationWebhooksRetryAfterReadError(t *testing.T) {
	retry := notificationWebhooksRetry
	t.Cleanup(func() { notificationWebhooksRetry = retry })
	notificationWebhooksRetry = 10 * time.Millisecond

	store := newStubFilerStore()
	f := newTestFiler(t, store, nil)
	t.Cleanup(func() {
		f.webhooks.reloadMu.Lock()
		if f.webhooks.retry != nil {
			f.webhooks.retry.Stop()
		}
		f.webhooks.reloadMu.Unlock()
		if q := f.webhooks.queue.Load(); q != nil {
			q.Close(time.Second)
		}
	})

	store.mu.Lock()
	store.entries[NotificationWebhooksConfPath] = &Entry{
		FullPath: util.FullPath(NotificationWebhooksConfPath),
		Content:  []byte(`{"targets":[{"name":"data","enabled":true,"endpoint":"http://127.0.0.1:1/hook"}]}`),
	}
	store.findErr = errors.New("store unavailable")
	store.mu.Unlock()

	f.LoadNotificationWebhooks()
	if f.webhooks.queue.Load() != nil {
		t.Fatal("expected no webhook queue while the stored targets cannot be read")
	}

	store.mu.Lock()
	store.findErr = nil
	store.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for f.webhooks.queue.Load() == nil {
		if time.Now().After(deadline) {
			t.Fatal("stored webhook targets were not applied after the read recovered")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestManagedNotificationWebhooksStopOnShutdown(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, nil)
	store.entries[NotificationWebhooksConfPath] = &Entry{
		FullPath: util.FullPath(NotificationWebhooksConfPath),
		Content:  []byte(`{"targets":[{"name":"data","enabled":true,"endpoint":"http://127.0.0.1:1/hook"}]}`),
	}

	f.LoadNotificationWebhooks()
	if f.webhooks.queue.Load() == nil {
		t.Fatal("expected a webhook queue for the stored targets")
	}

	close(f.deletionQuit)
	deadline := time.Now().Add(5 * time.Second)
	for f.webhooks.queue.Load() != nil {
		if time.Now().After(deadline) {
			t.Fatal("webhook queue still installed after shutdown")
		}
		time.Sleep(10 * time.Millisecond)
	}

	store.entries[NotificationWebhooksConfPath] = &Entry{
		FullPath: util.FullPath(NotificationWebhooksConfPath),
		Content:  []byte(`{"targets":[{"name":"late","enabled":true,"endpoint":"http://127.0.0.1:1/late"}]}`),
	}
	f.reloadNotificationWebhooks()
	if f.webhooks.queue.Load() != nil {
		t.Fatal("a reload after shutdown installed a webhook queue")
	}
}
