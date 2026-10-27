package command

import (
	"slices"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
)

type recordingRemoteMaker struct{ client *recordingRemote }

func (m recordingRemoteMaker) Make(*remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return m.client, nil
}
func (m recordingRemoteMaker) HasBucket() bool { return true }

func gatewayTestOption(storageType string) (*RemoteGatewayOptions, *recordingRemote) {
	remote := &recordingRemote{}
	remote_storage.RemoteStorageClientMakers[storageType] = recordingRemoteMaker{client: remote}
	filerAddress := "localhost:1"
	return &RemoteGatewayOptions{
		filerAddress: &filerAddress,
		bucketsDir:   "/buckets",
		mappings: &remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{
			"/buckets/gold": {Name: storageType, Bucket: "gold", Path: "/"},
		}},
		remoteConfs: map[string]*remote_pb.RemoteConf{storageType: {Name: storageType, Type: storageType}},
	}, remote
}

// TestGatewayDroppedLocalCopyIsNotUploaded: remote.uncache drops a synced
// entry's chunks and clears its sync stamp; shouldSendToRemote reads the
// cleared stamp in the remote mtime's second as dirty, so the update reaches
// the upload. The object at the key is the entry's only content and the
// chunkless upload would replace it with zero bytes, so it is skipped.
func TestGatewayDroppedLocalCopyIsNotUploaded(t *testing.T) {
	option, remote := gatewayTestOption("gatewaytest-uncache")
	t.Cleanup(func() { delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-uncache") })
	process, err := option.makeBucketedEventProcessor(&source.FilerSource{})
	if err != nil {
		t.Fatal(err)
	}

	resp := droppedCopyUpdate(
		droppedCopyEntry(droppedCopySynced, chunk("3,01", "e1")),
		droppedCopyEntry(droppedCopyStamp()),
	)
	if !shouldSendToRemote(resp.EventNotification.NewEntry) {
		t.Fatal("precondition: the cleared stamp reads the entry as dirty")
	}
	if err := process(resp); err != nil {
		t.Fatal(err)
	}
	if len(remote.writes) != 0 {
		t.Errorf("writes = %+v, want none: the remote object is the entry's content and an upload would zero it", remote.writes)
	}
	if len(remote.deletes) != 0 || len(remote.metadataUpdates) != 0 {
		t.Errorf("deletes = %+v, metadata updates = %+v, want the remote object left alone", remote.deletes, remote.metadataUpdates)
	}
}

// TestGatewayUncachedRewriteIsStillUploaded: a rewrite of an uncached entry
// holds local data, so the dropped-copy skip does not apply and the rewrite
// uploads. The stamp cannot reach a filer in the test, so the call errors;
// the upload is what is asserted.
func TestGatewayUncachedRewriteIsStillUploaded(t *testing.T) {
	option, remote := gatewayTestOption("gatewaytest-rewrite")
	t.Cleanup(func() { delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-rewrite") })
	process, err := option.makeBucketedEventProcessor(&source.FilerSource{})
	if err != nil {
		t.Fatal(err)
	}

	rewrite := entryWith(droppedCopyName, droppedCopyStamp(), chunk("4,02", "e2"))
	resp := droppedCopyUpdate(droppedCopyEntry(droppedCopyStamp()), rewrite)
	if err := process(resp); err != nil {
		t.Logf("stamp against the test filer fails as expected: %v", err)
	}
	if len(remote.writes) != 1 {
		t.Fatalf("writes = %+v, want the rewrite uploaded once", remote.writes)
	}
	if !slices.Equal(remote.writtenChunks[0], []string{"4,02"}) {
		t.Errorf("uploaded chunks %v, want the rewrite's [4,02]", remote.writtenChunks[0])
	}
}

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
