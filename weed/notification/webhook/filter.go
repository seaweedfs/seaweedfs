package webhook

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type filter struct {
	eventTypes   map[eventType]bool
	pathPrefixes []string
}

func newFilter(cfg *config) *filter {
	f := &filter{
		eventTypes:   make(map[eventType]bool),
		pathPrefixes: cfg.pathPrefixes,
	}

	if len(cfg.eventTypes) == 0 {
		f.eventTypes[eventTypeCreate] = true
		f.eventTypes[eventTypeDelete] = true
		f.eventTypes[eventTypeUpdate] = true
		f.eventTypes[eventTypeRename] = true
	} else {
		for _, et := range cfg.eventTypes {
			t := eventType(et)
			if !t.valid() {
				glog.Warningf("invalid event type: %v", t)

				continue
			}

			f.eventTypes[t] = true
		}
	}

	return f
}

func (f *filter) shouldPublish(key string, notification *filer_pb.EventNotification) bool {
	if !f.matchesPath(key, notification) {
		return false
	}

	eventType := detectEventType(key, notification)

	return f.eventTypes[eventType]
}

func (f *filter) matchesPath(key string, notification *filer_pb.EventNotification) bool {
	if len(f.pathPrefixes) == 0 {
		return true
	}

	if f.matchesAnyPathPrefix(key) {
		return true
	}

	if notification != nil && notification.NewEntry != nil && notification.NewParentPath != "" {
		newKey := string(util.FullPath(notification.NewParentPath).Child(notification.NewEntry.Name))
		return f.matchesAnyPathPrefix(newKey)
	}

	return false
}

func (f *filter) matchesAnyPathPrefix(key string) bool {
	for _, prefix := range f.pathPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}
