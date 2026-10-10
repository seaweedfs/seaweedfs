package command

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
)

type recordingRemoteMaker struct{ client *recordingRemote }

func (m recordingRemoteMaker) Make(*remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return m.client, nil
}
func (m recordingRemoteMaker) HasBucket() bool { return true }

func TestGatewayMetadataOnlyDeleteKeepsTheRemoteObject(t *testing.T) {
	remote := &recordingRemote{}
	remote_storage.RemoteStorageClientMakers["gatewaytest"] = recordingRemoteMaker{client: remote}
	t.Cleanup(func() { delete(remote_storage.RemoteStorageClientMakers, "gatewaytest") })
	option := &RemoteGatewayOptions{
		bucketsDir:  "/buckets",
		mappings:    &remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{"/buckets/b": {Name: "gatewaytest", Bucket: "b", Path: "/"}}},
		remoteConfs: map[string]*remote_pb.RemoteConf{"gatewaytest": {Name: "gatewaytest", Type: "gatewaytest"}},
	}
	process, err := option.makeBucketedEventProcessor(nil)
	if err != nil {
		t.Fatal(err)
	}
	deleteEvent := func(extended map[string][]byte) *filer_pb.SubscribeMetadataResponse {
		return &filer_pb.SubscribeMetadataResponse{
			Directory: "/buckets/b/dir",
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: "obj.bin", Attributes: &filer_pb.FuseAttributes{}, Extended: extended},
			},
		}
	}

	if err := process(deleteEvent(map[string][]byte{filer.ExtKeepRemoteObjectKey: []byte("true")})); err != nil {
		t.Fatal(err)
	}
	if len(remote.deletes) != 0 {
		t.Fatalf("deletes = %+v, want none for a metadata-only delete", remote.deletes)
	}
	if err := process(deleteEvent(nil)); err != nil {
		t.Fatal(err)
	}
	if len(remote.deletes) != 1 {
		t.Fatalf("deletes = %+v, want the ordinary delete", remote.deletes)
	}
}
