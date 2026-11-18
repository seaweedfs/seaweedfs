package filer

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification/webhook"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	NotificationWebhooksConfName = "notification_webhooks.json"
	notificationWebhooksDrain    = 30 * time.Second
)

var notificationWebhooksRetry = 5 * time.Second

var NotificationWebhooksConfPath = string(util.NewFullPath(DirectoryEtcSeaweedFS, NotificationWebhooksConfName))

// managedWebhooks runs the webhook targets edited from the admin UI, next to
// whatever notification.toml configures.
type managedWebhooks struct {
	reloadMu sync.Mutex
	applied  []byte
	retry    *time.Timer
	queue    atomic.Pointer[webhook.Queue]
}

func (f *Filer) LoadNotificationWebhooks() {
	f.reloadNotificationWebhooks()
	go func() {
		<-f.deletionQuit
		f.webhooks.reloadMu.Lock()
		defer f.webhooks.reloadMu.Unlock()
		if f.webhooks.retry != nil {
			f.webhooks.retry.Stop()
		}
		if q := f.webhooks.queue.Swap(nil); q != nil {
			go q.Close(notificationWebhooksDrain)
		}
	}()
}

func (f *Filer) maybeReloadNotificationWebhooks(event *filer_pb.SubscribeMetadataResponse) {
	if event == nil || event.EventNotification == nil {
		return
	}
	n := event.EventNotification
	touched := n.OldEntry != nil && n.OldEntry.Name == NotificationWebhooksConfName && event.Directory == DirectoryEtcSeaweedFS
	if n.NewEntry != nil && n.NewEntry.Name == NotificationWebhooksConfName && filer_pb.MetadataEventTargetDirectory(event) == DirectoryEtcSeaweedFS {
		touched = true
	}
	if touched {
		f.reloadNotificationWebhooks()
	}
}

// reloadNotificationWebhooks reads the stored targets rather than the event's
// copy, so concurrent change events cannot apply an older version last.
func (f *Filer) reloadNotificationWebhooks() {
	f.webhooks.reloadMu.Lock()
	defer f.webhooks.reloadMu.Unlock()
	select {
	case <-f.deletionQuit:
		return
	default:
	}

	var content []byte
	entry, err := f.FindEntry(context.Background(), util.FullPath(NotificationWebhooksConfPath))
	switch {
	case err == filer_pb.ErrNotFound:
	case err != nil:
		glog.Errorf("read %s: %v", NotificationWebhooksConfPath, err)
		f.retryNotificationWebhooks()
		return
	case len(entry.Content) > 0:
		content = entry.Content
	default:
		if content, err = f.readEntry(entry.GetChunks(), entry.Size()); err != nil {
			glog.Errorf("read %s: %v", NotificationWebhooksConfPath, err)
			f.retryNotificationWebhooks()
			return
		}
	}

	if f.webhooks.applied != nil && bytes.Equal(content, f.webhooks.applied) {
		return
	}
	conf, err := webhook.ParseManagedConfig(content)
	if err != nil {
		glog.Errorf("keep current webhook targets, %s: %v", NotificationWebhooksConfPath, err)
		return
	}
	queue, err := webhook.NewManagedQueue(conf)
	if err != nil {
		glog.Errorf("keep current webhook targets, start %s: %v", NotificationWebhooksConfPath, err)
		return
	}
	glog.V(0).Infof("apply %d webhook targets from %s", len(conf.Targets), NotificationWebhooksConfPath)
	f.webhooks.applied = append([]byte{}, content...)
	if old := f.webhooks.queue.Swap(queue); old != nil {
		go old.Close(notificationWebhooksDrain)
	}
}

// retryNotificationWebhooks keeps at most one pending retry, since no later
// event replays the change whose read failed.
func (f *Filer) retryNotificationWebhooks() {
	if f.webhooks.retry != nil {
		f.webhooks.retry.Stop()
	}
	f.webhooks.retry = time.AfterFunc(notificationWebhooksRetry, f.reloadNotificationWebhooks)
}

// sendToNotificationWebhooks hands a send that raced a reload to the queue
// that replaced the closing one.
func (f *Filer) sendToNotificationWebhooks(key string, message *filer_pb.EventNotification) {
	for {
		q := f.webhooks.queue.Load()
		if q == nil {
			return
		}
		err := q.SendMessage(key, message)
		if errors.Is(err, webhook.ErrQueueClosed) && f.webhooks.queue.Load() != q {
			continue
		}
		if err != nil {
			glog.Error(err)
		}
		return
	}
}

// isNotificationWebhooksConf keeps the stored targets, bearer tokens
// included, out of the events sent to notification queues.
func isNotificationWebhooksConf(oldEntry, newEntry *Entry) bool {
	return (oldEntry != nil && string(oldEntry.FullPath) == NotificationWebhooksConfPath) ||
		(newEntry != nil && string(newEntry.FullPath) == NotificationWebhooksConfPath)
}
