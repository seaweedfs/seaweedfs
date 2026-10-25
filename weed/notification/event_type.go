package notification

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	EventTypeCreate = "create"
	EventTypeDelete = "delete"
	EventTypeUpdate = "update"
	EventTypeRename = "rename"
)

// ValidEventType reports whether t is an event type a queue can filter on.
func ValidEventType(t string) bool {
	switch t {
	case EventTypeCreate, EventTypeDelete, EventTypeUpdate, EventTypeRename:
		return true
	default:
		return false
	}
}

// DetectEventType classifies an entry-change notification. key is the old
// entry's path when one exists.
func DetectEventType(key string, notification *filer_pb.EventNotification) string {
	hasOldEntry := notification.OldEntry != nil
	hasNewEntry := notification.NewEntry != nil

	if !hasOldEntry && hasNewEntry {
		return EventTypeCreate
	}

	if hasOldEntry && !hasNewEntry {
		return EventTypeDelete
	}

	if hasOldEntry && hasNewEntry {
		oldDir, _ := util.FullPath(key).DirAndName()
		newDir := notification.NewParentPath
		if newDir == "" {
			newDir = oldDir
		}
		if oldDir != newDir || notification.OldEntry.Name != notification.NewEntry.Name {
			return EventTypeRename
		}

		return EventTypeUpdate
	}

	return EventTypeUpdate
}
