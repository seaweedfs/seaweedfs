package webhook

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestFilterEventTypes(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		eventTypes    []string
		notification  *filer_pb.EventNotification
		expectedType  eventType
		shouldPublish bool
	}{
		{
			name:       "create event - allowed",
			key:        "/test/test.txt",
			eventTypes: []string{"create", "delete"},
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			expectedType:  eventTypeCreate,
			shouldPublish: true,
		},
		{
			name:       "create event - not allowed",
			key:        "/test/test.txt",
			eventTypes: []string{"delete", "update"},
			notification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			expectedType:  eventTypeCreate,
			shouldPublish: false,
		},
		{
			name:       "delete event - allowed",
			key:        "/test/test.txt",
			eventTypes: []string{"create", "delete"},
			notification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: "test.txt"},
			},
			expectedType:  eventTypeDelete,
			shouldPublish: true,
		},
		{
			name:       "update event - allowed",
			key:        "/test/test.txt",
			eventTypes: []string{"update"},
			notification: &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: "test.txt"},
				NewEntry:      &filer_pb.Entry{Name: "test.txt"},
				NewParentPath: "/test",
			},
			expectedType:  eventTypeUpdate,
			shouldPublish: true,
		},
		{
			name:       "rename event - allowed",
			key:        "/old/path/old.txt",
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
			name:       "rename event same name different parent - allowed",
			key:        "/old/path/file.txt",
			eventTypes: []string{"rename"},
			notification: &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: "file.txt"},
				NewEntry:      &filer_pb.Entry{Name: "file.txt"},
				NewParentPath: "/new/path",
			},
			expectedType:  eventTypeRename,
			shouldPublish: true,
		},
		{
			name:       "rename event - not allowed",
			key:        "/old/path/old.txt",
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
			key:        "/test/test.txt",
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

			eventType := detectEventType(tt.key, tt.notification)
			if eventType != tt.expectedType {
				t.Errorf("detectEventType() = %v, want %v", eventType, tt.expectedType)
			}

			shouldPublish := f.shouldPublish(tt.key, tt.notification)
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
		eventTypes    []string
		key           string
		notification  *filer_pb.EventNotification
		shouldPublish bool
	}{
		{
			name:          "matches single prefix",
			pathPrefixes:  []string{"/data/"},
			eventTypes:    []string{"create"},
			key:           "/data/file.txt",
			notification:  &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "file.txt"}},
			shouldPublish: true,
		},
		{
			name:          "matches one of multiple prefixes",
			pathPrefixes:  []string{"/data/", "/logs/", "/tmp/"},
			eventTypes:    []string{"create"},
			key:           "/logs/app.log",
			notification:  &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "app.log"}},
			shouldPublish: true,
		},
		{
			name:          "no match",
			pathPrefixes:  []string{"/data/", "/logs/"},
			eventTypes:    []string{"create"},
			key:           "/other/file.txt",
			notification:  &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "file.txt"}},
			shouldPublish: false,
		},
		{
			name:          "empty prefixes allows all",
			pathPrefixes:  []string{},
			eventTypes:    []string{"create"},
			key:           "/any/path/file.txt",
			notification:  &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "file.txt"}},
			shouldPublish: true,
		},
		{
			name:          "exact prefix match",
			pathPrefixes:  []string{"/data"},
			eventTypes:    []string{"create"},
			key:           "/data",
			notification:  &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "data"}},
			shouldPublish: true,
		},
		{
			name:          "partial match not allowed",
			pathPrefixes:  []string{"/data/"},
			eventTypes:    []string{"create"},
			key:           "/database/file.txt",
			notification:  &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "file.txt"}},
			shouldPublish: false,
		},
		{
			name:         "rename matches destination prefix",
			pathPrefixes: []string{"/watched/"},
			eventTypes:   []string{"rename"},
			key:          "/outside/old.txt",
			notification: &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: "old.txt"},
				NewEntry:      &filer_pb.Entry{Name: "new.txt"},
				NewParentPath: "/watched",
			},
			shouldPublish: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config{
				pathPrefixes: tt.pathPrefixes,
				eventTypes:   tt.eventTypes,
			}
			f := newFilter(cfg)

			shouldPublish := f.shouldPublish(tt.key, tt.notification)
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
