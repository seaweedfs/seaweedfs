package kafka

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/protobuf/proto"
)

func TestKafkaEventTypes(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		eventTypes   []string
		notification *filer_pb.EventNotification
		wantType     string
		wantAllow    bool
	}{
		{
			name:       "create event allowed",
			key:        "/test/test.txt",
			eventTypes: []string{"create", "delete"},
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			wantType:  "create",
			wantAllow: true,
		},
		{
			name:       "create event filtered out",
			key:        "/test/test.txt",
			eventTypes: []string{"delete", "update"},
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			wantType:  "create",
			wantAllow: false,
		},
		{
			name:       "delete event allowed",
			key:        "/test/test.txt",
			eventTypes: []string{"create", "delete"},
			notification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			wantType:  "delete",
			wantAllow: true,
		},
		{
			name:       "update event allowed",
			key:        "/test/test.txt",
			eventTypes: []string{"update"},
			notification: &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: "test.txt"},
				NewEntry:      &filer_pb.Entry{Name: "test.txt"},
				NewParentPath: "/test",
			},
			wantType:  "update",
			wantAllow: true,
		},
		{
			name:       "rename event allowed",
			key:        "/old/path/old.txt",
			eventTypes: []string{"rename"},
			notification: &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: "old.txt"},
				NewEntry:      &filer_pb.Entry{Name: "new.txt"},
				NewParentPath: "/new/path",
			},
			wantType:  "rename",
			wantAllow: true,
		},
		{
			name:       "rename across directories allowed",
			key:        "/old/path/file.txt",
			eventTypes: []string{"rename"},
			notification: &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: "file.txt"},
				NewEntry:      &filer_pb.Entry{Name: "file.txt"},
				NewParentPath: "/new/path",
			},
			wantType:  "rename",
			wantAllow: true,
		},
		{
			name:       "rename event filtered out",
			key:        "/old/path/old.txt",
			eventTypes: []string{"create", "delete", "update"},
			notification: &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: "old.txt"},
				NewEntry:      &filer_pb.Entry{Name: "new.txt"},
				NewParentPath: "/new/path",
			},
			wantType:  "rename",
			wantAllow: false,
		},
		{
			name:       "empty filter publishes every event",
			key:        "/test/test.txt",
			eventTypes: []string{},
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			wantType:  "create",
			wantAllow: true,
		},
		{
			name:       "invalid type does not remove a valid one",
			key:        "/test/test.txt",
			eventTypes: []string{"create", "bogus"},
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			wantType:  "create",
			wantAllow: true,
		},
		{
			name:       "only invalid types publish nothing",
			key:        "/test/test.txt",
			eventTypes: []string{"bogus"},
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			wantType:  "create",
			wantAllow: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType := notification.DetectEventType(tt.key, tt.notification)
			if gotType != tt.wantType {
				t.Errorf("DetectEventType() = %v, want %v", gotType, tt.wantType)
			}

			q := &KafkaQueue{}
			q.setEventTypes(tt.eventTypes)
			if got := q.allowsEvent(tt.key, tt.notification); got != tt.wantAllow {
				t.Errorf("allowsEvent() = %v, want %v", got, tt.wantAllow)
			}
		})
	}
}

func TestKafkaEventFilterUnsetPublishesOtherMessages(t *testing.T) {
	q := &KafkaQueue{}
	if !q.allowsEvent("/test/test.txt", &filer_pb.Entry{Name: "test.txt"}) {
		t.Fatal("unset filter dropped a message")
	}
}

func TestKafkaEventFilterDropsUnclassifiedMessages(t *testing.T) {
	q := &KafkaQueue{}
	q.setEventTypes([]string{"create"})
	var message proto.Message = &filer_pb.Entry{Name: "test.txt"}
	if q.allowsEvent("/test/test.txt", message) {
		t.Fatal("filtered queue published a message that is not an event notification")
	}
}
