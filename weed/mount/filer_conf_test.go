package mount

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestIsFilerConfUpdateEventMatchesRenameTarget(t *testing.T) {
	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/tmp",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: filer.FilerConfName},
			NewEntry:      &filer_pb.Entry{Name: filer.FilerConfName},
			NewParentPath: filer.DirectoryEtcSeaweedFS,
		},
	}

	if !isFilerConfUpdateEvent(event, filer.DirectoryEtcSeaweedFS, filer.FilerConfName) {
		t.Fatalf("expected rename target to match filer.conf watcher")
	}
}
