package webhook

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type filter struct {
	eventTypes   map[EventType]bool
	pathPrefixes []string
}

func newFilter(cfg *config) *filter {
	f := &filter{
		eventTypes:   make(map[EventType]bool),
		pathPrefixes: cfg.pathPrefixes,
	}

	if len(cfg.eventTypes) == 0 {
		f.eventTypes[EventTypeCreate] = true
		f.eventTypes[EventTypeDelete] = true
		f.eventTypes[EventTypeUpdate] = true
		f.eventTypes[EventTypeRename] = true
	} else {
		for _, et := range cfg.eventTypes {
			t := EventType(et)
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
	if !f.matchesPath(key) {
		return false
	}

	eventType := f.detectEventType(notification)

	return f.eventTypes[eventType]
}

func (f *filter) matchesPath(key string) bool {
	if len(f.pathPrefixes) == 0 {
		return true
	}

	for _, prefix := range f.pathPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}

	return false
}

func (f *filter) detectEventType(notification *filer_pb.EventNotification) EventType {
	return detectEventType(notification)
}
