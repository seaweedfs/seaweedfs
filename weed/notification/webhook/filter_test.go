package webhook

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestFilterEventTypes(t *testing.T) {
	tests := []struct {
		name          string
		eventTypes    []string
		notification  *filer_pb.EventNotification
		expectedType  eventType
		shouldPublish bool
	}{
		{
			name:       "create event - allowed",
			eventTypes: []string{"create", "delete"},
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			expectedType:  eventTypeCreate,
			shouldPublish: true,
		},
		{
			name:       "create event - not allowed",
			eventTypes: []string{"delete", "update"},
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			expectedType:  eventTypeCreate,
			shouldPublish: false,
		},
		{
			name:       "delete event - allowed",
			eventTypes: []string{"create", "delete"},
			notification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			expectedType:  eventTypeDelete,
			shouldPublish: true,
		},
		{
			name:       "update event - allowed",
			eventTypes: []string{"update"},
			notification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: "test.txt"},
				NewEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			expectedType:  eventTypeUpdate,
			shouldPublish: true,
		},
		{
			name:       "rename event - allowed",
			eventTypes: []string{"rename"},
			notification: &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: "old.txt"},
				NewEntry:      &filer_pb.Entry{Name: "new.txt"},
				NewParentPath: "/new/path",
			},
			expectedType:  eventTypeRename,
			shouldPublish: true,
		},
		{
			name:       "rename event - not allowed",
			eventTypes: []string{"create", "delete", "update"},
			notification: &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: "old.txt"},
				NewEntry:      &filer_pb.Entry{Name: "new.txt"},
				NewParentPath: "/new/path",
			},
			expectedType:  eventTypeRename,
			shouldPublish: false,
		},
		{
			name:       "all events allowed when empty",
			eventTypes: []string{},
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			expectedType:  eventTypeCreate,
			shouldPublish: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config{eventTypes: tt.eventTypes}
			f := newFilter(cfg)

			eventType := detectEventType(tt.notification)
			if eventType != tt.expectedType {
				t.Errorf("detectEventType() = %v, want %v", eventType, tt.expectedType)
			}

			shouldPublish := f.shouldPublish("/test/path", tt.notification)
			if shouldPublish != tt.shouldPublish {
				t.Errorf("shouldPublish() = %v, want %v", shouldPublish, tt.shouldPublish)
			}
		})
	}
}

func TestFilterPathPrefixes(t *testing.T) {
	tests := []struct {
		name          string
		pathPrefixes  []string
		key           string
		shouldPublish bool
	}{
		{
			name:          "matches single prefix",
			pathPrefixes:  []string{"/data/"},
			key:           "/data/file.txt",
			shouldPublish: true,
		},
		{
			name:          "matches one of multiple prefixes",
			pathPrefixes:  []string{"/data/", "/logs/", "/tmp/"},
			key:           "/logs/app.log",
			shouldPublish: true,
		},
		{
			name:          "no match",
			pathPrefixes:  []string{"/data/", "/logs/"},
			key:           "/other/file.txt",
			shouldPublish: false,
		},
		{
			name:          "empty prefixes allows all",
			pathPrefixes:  []string{},
			key:           "/any/path/file.txt",
			shouldPublish: true,
		},
		{
			name:          "exact prefix match",
			pathPrefixes:  []string{"/data"},
			key:           "/data",
			shouldPublish: true,
		},
		{
			name:          "partial match not allowed",
			pathPrefixes:  []string{"/data/"},
			key:           "/database/file.txt",
			shouldPublish: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config{
				pathPrefixes: tt.pathPrefixes,
				eventTypes:   []string{"create"},
			}
			f := newFilter(cfg)

			notification := &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "test.txt"},
			}

			shouldPublish := f.shouldPublish(tt.key, notification)
			if shouldPublish != tt.shouldPublish {
				t.Errorf("shouldPublish() = %v, want %v", shouldPublish, tt.shouldPublish)
			}
		})
	}
}

func TestFilterCombined(t *testing.T) {
	cfg := &config{
		eventTypes:   []string{"create", "update"},
		pathPrefixes: []string{"/data/", "/logs/"},
	}
	f := newFilter(cfg)

	tests := []struct {
		name          string
		key           string
		notification  *filer_pb.EventNotification
		shouldPublish bool
	}{
		{
			name: "allowed event and path",
			key:  "/data/file.txt",
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file.txt"},
			},
			shouldPublish: true,
		},
		{
			name: "allowed event but wrong path",
			key:  "/other/file.txt",
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file.txt"},
			},
			shouldPublish: false,
		},
		{
			name: "wrong event but allowed path",
			key:  "/data/file.txt",
			notification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: "file.txt"},
			},
			shouldPublish: false,
		},
		{
			name: "wrong event and wrong path",
			key:  "/other/file.txt",
			notification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: "file.txt"},
			},
			shouldPublish: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldPublish := f.shouldPublish(tt.key, tt.notification)
			if shouldPublish != tt.shouldPublish {
				t.Errorf("shouldPublish() = %v, want %v", shouldPublish, tt.shouldPublish)
			}
		})
	}
}
