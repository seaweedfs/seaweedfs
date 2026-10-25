package kafka

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
		if !notification.ValidEventType(et) {
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

	n, ok := message.(*filer_pb.EventNotification)
	if !ok || n == nil {
		return false
	}
	_, allowed := k.eventTypes[notification.DetectEventType(key, n)]
	return allowed
}
