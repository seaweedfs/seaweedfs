package filer

import (
	"bytes"
	"context"
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

var NotificationWebhooksConfPath = string(util.NewFullPath(DirectoryEtcSeaweedFS, NotificationWebhooksConfName))

// managedWebhooks runs the webhook targets edited from the admin UI, next to
// whatever notification.toml configures.
// sendMu keeps a replaced queue open until every sender that loaded it has
// handed its event over.
type managedWebhooks struct {
	reloadMu sync.Mutex
	applied  []byte
	sendMu   sync.RWMutex
	queue    atomic.Pointer[webhook.Queue]
}

func (f *Filer) LoadNotificationWebhooks() {
	if !f.ManagedWebhooksEnabled {
		return
	}
	f.reloadNotificationWebhooks()
}

func (f *Filer) maybeReloadNotificationWebhooks(event *filer_pb.SubscribeMetadataResponse) {
	if !f.ManagedWebhooksEnabled || event == nil || event.EventNotification == nil {
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

	var content []byte
	entry, err := f.FindEntry(context.Background(), util.FullPath(NotificationWebhooksConfPath))
	switch {
	case err == filer_pb.ErrNotFound:
	case err != nil:
		glog.Errorf("read %s: %v", NotificationWebhooksConfPath, err)
		return
	case len(entry.Content) > 0:
		content = entry.Content
	default:
		if content, err = f.readEntry(entry.GetChunks(), entry.Size()); err != nil {
			glog.Errorf("read %s: %v", NotificationWebhooksConfPath, err)
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
	f.webhooks.sendMu.Lock()
	old := f.webhooks.queue.Swap(queue)
	f.webhooks.sendMu.Unlock()
	if old != nil {
		go old.Close(notificationWebhooksDrain)
	}
}

func (f *Filer) sendToNotificationWebhooks(key string, message *filer_pb.EventNotification) {
	f.webhooks.sendMu.RLock()
	defer f.webhooks.sendMu.RUnlock()
	if q := f.webhooks.queue.Load(); q != nil {
		if err := q.SendMessage(key, message); err != nil {
			glog.Error(err)
		}
	}
}

// isNotificationWebhooksConf keeps the stored targets, bearer tokens
// included, out of the events sent to notification queues.
func isNotificationWebhooksConf(oldEntry, newEntry *Entry) bool {
	return (oldEntry != nil && string(oldEntry.FullPath) == NotificationWebhooksConfPath) ||
		(newEntry != nil && string(newEntry.FullPath) == NotificationWebhooksConfPath)
}
