package filer_pb

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestFileIdSize(t *testing.T) {
	fileIdStr := "11745,0293434534cbb9892b"

	fid, _ := ToFileIdObject(fileIdStr)
	bytes, _ := proto.Marshal(fid)

	println(len(fileIdStr))
	println(len(bytes))
}

func TestMetadataEventMatchesSubscription(t *testing.T) {
	event := &SubscribeMetadataResponse{
		Directory: "/tmp",
		EventNotification: &EventNotification{
			OldEntry:      &Entry{Name: "old-name"},
			NewEntry:      &Entry{Name: "new-name"},
			NewParentPath: "/watched",
		},
	}

	tests := []struct {
		name         string
		pathPrefix   string
		pathPrefixes []string
		directories  []string
	}{
		{
			name:       "primary path prefix matches rename target",
			pathPrefix: "/watched/new-name",
		},
		{
			name:         "additional path prefix matches rename target",
			pathPrefix:   "/data",
			pathPrefixes: []string{"/watched"},
		},
		{
			name:        "directory watch matches rename target directory",
			pathPrefix:  "/data",
			directories: []string{"/watched"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !MetadataEventMatchesSubscription(event, tt.pathPrefix, tt.pathPrefixes, tt.directories) {
				t.Fatalf("MetadataEventMatchesSubscription returned false")
			}
		})
	}
}

func TestMetadataEventTouchesDirectoryHelpers(t *testing.T) {
	renameInto := &SubscribeMetadataResponse{
		Directory: "/tmp",
		EventNotification: &EventNotification{
			OldEntry:      &Entry{Name: "filer.conf"},
			NewEntry:      &Entry{Name: "filer.conf"},
			NewParentPath: "/etc/seaweedfs",
		},
	}
	if got := MetadataEventTargetDirectory(renameInto); got != "/etc/seaweedfs" {
		t.Fatalf("MetadataEventTargetDirectory = %q, want /etc/seaweedfs", got)
	}
	if !MetadataEventTouchesDirectory(renameInto, "/etc/seaweedfs") {
		t.Fatalf("expected rename target to touch /etc/seaweedfs")
	}

	renameOut := &SubscribeMetadataResponse{
		Directory: "/etc/remote",
		EventNotification: &EventNotification{
			OldEntry:      &Entry{Name: "remote.conf"},
			NewEntry:      &Entry{Name: "remote.conf"},
			NewParentPath: "/tmp",
		},
	}
	if !MetadataEventTouchesDirectoryPrefix(renameOut, "/etc/remote") {
		t.Fatalf("expected rename source to touch /etc/remote")
	}
}
