package kafka

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// Empty eventTypes means publish every event. A non-nil map publishes only those types.
// The names and classification match the webhook notifier.
func (k *KafkaQueue) setEventTypes(types []string) {
	if len(types) == 0 {
		k.eventTypes = nil
		return
	}

	allowed := make(map[string]struct{}, len(types))
	for _, et := range types {
		if !validKafkaEventType(et) {
			glog.Warningf("invalid event type: %v", et)
			continue
		}
		allowed[et] = struct{}{}
	}
	k.eventTypes = allowed
}

func (k *KafkaQueue) allowsEvent(key string, message proto.Message) bool {
	if k.eventTypes == nil {
		return true
	}

	notification, ok := message.(*filer_pb.EventNotification)
	if !ok || notification == nil {
		return false
	}
	_, allowed := k.eventTypes[detectKafkaEventType(key, notification)]
	return allowed
}

func validKafkaEventType(t string) bool {
	switch t {
	case "create", "delete", "update", "rename":
		return true
	default:
		return false
	}
}

func detectKafkaEventType(key string, notification *filer_pb.EventNotification) string {
	hasOldEntry := notification.OldEntry != nil
	hasNewEntry := notification.NewEntry != nil

	if !hasOldEntry && hasNewEntry {
		return "create"
	}

	if hasOldEntry && !hasNewEntry {
		return "delete"
	}

	if hasOldEntry && hasNewEntry {
		oldDir, _ := util.FullPath(key).DirAndName()
		newDir := notification.NewParentPath
		if newDir == "" {
			newDir = oldDir
		}
		if oldDir != newDir || notification.OldEntry.Name != notification.NewEntry.Name {
			return "rename"
		}

		return "update"
	}

	return "update"
}
