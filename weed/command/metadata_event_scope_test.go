package command

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestMetadataEventDirectoryMembershipUsesDirectoryBoundaries(t *testing.T) {
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: filer.DirectoryEtcRemote,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "remote.conf"},
			NewEntry:      &filer_pb.Entry{Name: "remote.conf"},
			NewParentPath: "/etc/remote-sibling",
		},
	}

	sourceInDir, targetInDir := metadataEventDirectoryMembership(resp, filer.DirectoryEtcRemote)
	if !sourceInDir {
		t.Fatal("expected source directory to match")
	}
	if targetInDir {
		t.Fatal("did not expect sibling target directory to match")
	}
}

func TestMetadataEventUpdatesAndRemovesDirectory(t *testing.T) {
	tests := []struct {
		name        string
		resp        *filer_pb.SubscribeMetadataResponse
		wantUpdate  bool
		wantRemoval bool
	}{
		{
			name: "rename out",
			resp: &filer_pb.SubscribeMetadataResponse{
				Directory: filer.DirectoryEtcRemote,
				EventNotification: &filer_pb.EventNotification{
					OldEntry:      &filer_pb.Entry{Name: "remote.conf"},
					NewEntry:      &filer_pb.Entry{Name: "remote.conf"},
					NewParentPath: "/tmp",
				},
			},
			wantUpdate:  false,
			wantRemoval: true,
		},
		{
			name: "rename into",
			resp: &filer_pb.SubscribeMetadataResponse{
				Directory: "/tmp",
				EventNotification: &filer_pb.EventNotification{
					OldEntry:      &filer_pb.Entry{Name: "remote.conf"},
					NewEntry:      &filer_pb.Entry{Name: "remote.conf"},
					NewParentPath: filer.DirectoryEtcRemote,
				},
			},
			wantUpdate:  true,
			wantRemoval: false,
		},
		{
			name: "rename within",
			resp: &filer_pb.SubscribeMetadataResponse{
				Directory: filer.DirectoryEtcRemote,
				EventNotification: &filer_pb.EventNotification{
					OldEntry:      &filer_pb.Entry{Name: "remote.conf"},
					NewEntry:      &filer_pb.Entry{Name: "renamed.conf"},
					NewParentPath: filer.DirectoryEtcRemote,
				},
			},
			wantUpdate:  true,
			wantRemoval: false,
		},
		{
			name: "delete",
			resp: &filer_pb.SubscribeMetadataResponse{
				Directory: filer.DirectoryEtcRemote,
				EventNotification: &filer_pb.EventNotification{
					OldEntry: &filer_pb.Entry{Name: "remote.conf"},
				},
			},
			wantUpdate:  false,
			wantRemoval: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := metadataEventUpdatesDirectory(tt.resp, filer.DirectoryEtcRemote); got != tt.wantUpdate {
				t.Fatalf("metadataEventUpdatesDirectory() = %v, want %v", got, tt.wantUpdate)
			}
			if got := metadataEventRemovesFromDirectory(tt.resp, filer.DirectoryEtcRemote); got != tt.wantRemoval {
				t.Fatalf("metadataEventRemovesFromDirectory() = %v, want %v", got, tt.wantRemoval)
			}
		})
	}
}
