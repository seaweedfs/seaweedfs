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

// TestGatewayRemoteOnlyRenameCopiesTheObject: a remote-only entry holds its
// content only as the object the renamed-from path maps to. The rename copies
// that object to the destination key, stamps the entry so reads stop going
// through the pending-source marker, and removes the source. Uploading the
// chunkless entry would put a zero-byte object over it, and deleting the
// source first would lose the only copy.
func TestGatewayRemoteOnlyRenameCopiesTheObject(t *testing.T) {
	option, remote := gatewayTestOption("gatewaytest-rename")
	t.Cleanup(func() { delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-rename") })

	oldPath := "/buckets/gold/dir/old.txt"
	stamp := &filer_pb.RemoteEntry{StorageName: "gatewaytest-rename", RemoteETag: `"abc"`, RemoteSize: 5, RemoteMtime: 100}
	oldEntry := &filer_pb.Entry{Name: "old.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 100, FileSize: 5}, RemoteEntry: stamp}
	current := &filer_pb.Entry{
		Name:        "new.txt",
		Attributes:  &filer_pb.FuseAttributes{Mtime: 200, FileSize: 5},
		RemoteEntry: stamp,
		Extended:    map[string][]byte{filer.ExtRemoteRenameSourceKey: []byte(oldPath)},
	}
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/gold/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: oldEntry, NewParentPath: "/buckets/gold/dir", NewEntry: current,
		},
	}
	filerClient := &stubFilerClient{entries: map[string]*filer_pb.Entry{"/buckets/gold/dir/new.txt": current}}

	remote.objects = map[string][]byte{"/dir/old.txt": []byte("hello")}
	remote.statETag = `"abc"`
	remote.statMtime = 100

	if err := gatewayRemoteOnlyRename(filerClient, option, resp, current); err != nil {
		t.Fatal(err)
	}
	if len(remote.writes) != 1 || remote.writes[0].Path != "/dir/new.txt" {
		t.Fatalf("writes = %+v, want the source object copied to /dir/new.txt", remote.writes)
	}
	if string(remote.written[0]) != "hello" {
		t.Errorf("copied %q, want the source object's content", remote.written[0])
	}
	if filerClient.updates != 1 {
		t.Errorf("stamped the entry %d times, want 1 to clear the pending-source marker", filerClient.updates)
	}
	if len(remote.deletes) != 1 || remote.deletes[0].Path != "/dir/old.txt" {
		t.Errorf("deletes = %+v, want the source object removed", remote.deletes)
	}
}

// TestGatewayRemoteOnlyRenameAlreadyComplete: when the destination already
// holds what the stamp describes (an earlier run copied it), the rename only
// refreshes the stamp and removes the source.
func TestGatewayRemoteOnlyRenameAlreadyComplete(t *testing.T) {
	option, remote := gatewayTestOption("gatewaytest-rename-complete")
	t.Cleanup(func() { delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-rename-complete") })

	oldPath := "/buckets/gold/dir/old.txt"
	stamp := &filer_pb.RemoteEntry{StorageName: "gatewaytest-rename-complete", RemoteETag: `"abc"`, RemoteSize: 5, RemoteMtime: 100}
	oldEntry := &filer_pb.Entry{Name: "old.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 100, FileSize: 5}, RemoteEntry: stamp}
	current := &filer_pb.Entry{
		Name:        "new.txt",
		Attributes:  &filer_pb.FuseAttributes{Mtime: 200, FileSize: 5},
		RemoteEntry: stamp,
		Extended:    map[string][]byte{filer.ExtRemoteRenameSourceKey: []byte(oldPath)},
	}
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/gold/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: oldEntry, NewParentPath: "/buckets/gold/dir", NewEntry: current,
		},
	}
	filerClient := &stubFilerClient{entries: map[string]*filer_pb.Entry{"/buckets/gold/dir/new.txt": current}}

	remote.objects = map[string][]byte{"/dir/old.txt": []byte("hello"), "/dir/new.txt": []byte("hello")}
	remote.statETag = `"abc"`
	remote.statMtime = 100

	if err := gatewayRemoteOnlyRename(filerClient, option, resp, current); err != nil {
		t.Fatal(err)
	}
	if len(remote.writes) != 0 {
		t.Errorf("writes = %+v, want none: the destination already holds the content", remote.writes)
	}
	if len(remote.deletes) != 1 || remote.deletes[0].Path != "/dir/old.txt" {
		t.Errorf("deletes = %+v, want the source object removed", remote.deletes)
	}
}

// TestGatewayRenameReleasesPendingSource: a rename whose old entry read a
// pending source (an earlier rename's deferred copy had not finished) leaves
// that source behind once the renamed entry no longer reads through a
// marker. The gateway deletes it, guarded by the old entry's stamp.
func TestGatewayRenameReleasesPendingSource(t *testing.T) {
	option, remote := gatewayTestOption("gatewaytest-release")
	t.Cleanup(func() { delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-release") })

	stamp := &filer_pb.RemoteEntry{StorageName: "gatewaytest-release", RemoteETag: `"abc"`, RemoteSize: 5, RemoteMtime: 100}
	oldEntry := &filer_pb.Entry{
		Name:        "mid.txt",
		Attributes:  &filer_pb.FuseAttributes{Mtime: 100, FileSize: 5},
		RemoteEntry: stamp,
		Extended:    map[string][]byte{filer.ExtRemoteRenameSourceKey: []byte("/buckets/gold/dir/orig.txt")},
	}
	newEntry := &filer_pb.Entry{Name: "new.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 200, FileSize: 5}, RemoteEntry: stamp}
	message := &filer_pb.EventNotification{OldEntry: oldEntry, NewParentPath: "/buckets/gold/dir", NewEntry: newEntry}
	filerClient := &stubFilerClient{entries: map[string]*filer_pb.Entry{
		"/buckets/gold/dir/new.txt": newEntry,
	}}
	remote.objects = map[string][]byte{"/dir/orig.txt": []byte("hello")}
	remote.statETag = `"abc"`
	remote.statMtime = 100

	if err := releaseGatewayRenamedSource(filerClient, option, message); err != nil {
		t.Fatal(err)
	}
	if len(remote.deletes) != 1 || remote.deletes[0].Path != "/dir/orig.txt" {
		t.Errorf("deletes = %+v, want the pending source released", remote.deletes)
	}
}

// TestGatewayRenameKeepsHeldPendingSource: the pending source stays while a
// remote-only entry still holds its path.
func TestGatewayRenameKeepsHeldPendingSource(t *testing.T) {
	option, remote := gatewayTestOption("gatewaytest-release")
	t.Cleanup(func() { delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-release") })

	stamp := &filer_pb.RemoteEntry{StorageName: "gatewaytest-release", RemoteETag: `"abc"`, RemoteSize: 5, RemoteMtime: 100}
	oldEntry := &filer_pb.Entry{
		Name:        "mid.txt",
		Attributes:  &filer_pb.FuseAttributes{Mtime: 100, FileSize: 5},
		RemoteEntry: stamp,
		Extended:    map[string][]byte{filer.ExtRemoteRenameSourceKey: []byte("/buckets/gold/dir/orig.txt")},
	}
	newEntry := &filer_pb.Entry{Name: "new.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 200, FileSize: 5}, RemoteEntry: stamp}
	heldSource := &filer_pb.Entry{Name: "orig.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 300, FileSize: 5}, RemoteEntry: stamp}
	message := &filer_pb.EventNotification{OldEntry: oldEntry, NewParentPath: "/buckets/gold/dir", NewEntry: newEntry}
	filerClient := &stubFilerClient{entries: map[string]*filer_pb.Entry{
		"/buckets/gold/dir/new.txt":  newEntry,
		"/buckets/gold/dir/orig.txt": heldSource,
	}}
	remote.objects = map[string][]byte{"/dir/orig.txt": []byte("hello")}
	remote.statETag = `"abc"`
	remote.statMtime = 100

	if err := releaseGatewayRenamedSource(filerClient, option, message); err != nil {
		t.Fatal(err)
	}
	if len(remote.deletes) != 0 {
		t.Errorf("deletes = %+v, want the pending source kept while an entry holds its path", remote.deletes)
	}
}

// TestGatewayRemoteOnlyRenameSourceOutOfCoverage: a rename source outside the
// gateway's buckets is left untouched; when the destination already holds
// the content the rename only refreshes the stamp instead of failing.
func TestGatewayRemoteOnlyRenameSourceOutOfCoverage(t *testing.T) {
	option, remote := gatewayTestOption("gatewaytest-outsrc")
	t.Cleanup(func() { delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-outsrc") })

	stamp := &filer_pb.RemoteEntry{StorageName: "gatewaytest-outsrc", RemoteETag: `"abc"`, RemoteSize: 5, RemoteMtime: 100}
	oldEntry := &filer_pb.Entry{Name: "old.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 100, FileSize: 5}, RemoteEntry: stamp}
	current := &filer_pb.Entry{
		Name:        "new.txt",
		Attributes:  &filer_pb.FuseAttributes{Mtime: 200, FileSize: 5},
		RemoteEntry: stamp,
		Extended:    map[string][]byte{filer.ExtRemoteRenameSourceKey: []byte("/unmanaged/dir/old.txt")},
	}
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/gold/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: oldEntry, NewParentPath: "/buckets/gold/dir", NewEntry: current,
		},
	}
	filerClient := &stubFilerClient{entries: map[string]*filer_pb.Entry{"/buckets/gold/dir/new.txt": current}}

	remote.objects = map[string][]byte{"/dir/new.txt": []byte("hello")}
	remote.statETag = `"abc"`
	remote.statMtime = 100

	if err := gatewayRemoteOnlyRename(filerClient, option, resp, current); err != nil {
		t.Fatalf("the completed destination should finalize without touching the out-of-coverage source: %v", err)
	}
	if filerClient.updates != 1 {
		t.Errorf("stamped the entry %d times, want 1 to clear the pending-source marker", filerClient.updates)
	}
	if len(remote.deletes) != 0 || len(remote.writes) != 0 {
		t.Errorf("deletes = %+v, writes = %+v, want the unmanaged source and destination left alone", remote.deletes, remote.writes)
	}
}

// TestGatewayRenameOutKeepsRemoteOnlyObject: a remote-only entry renamed out
// of the gateway's coverage keeps the remote object: it is the entry's only
// content.
func TestGatewayRenameOutKeepsRemoteOnlyObject(t *testing.T) {
	option, remote := gatewayTestOption("gatewaytest-out")
	t.Cleanup(func() { delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-out") })
	process, err := option.makeBucketedEventProcessor(&source.FilerSource{})
	if err != nil {
		t.Fatal(err)
	}

	stamp := &filer_pb.RemoteEntry{StorageName: "gatewaytest-out", RemoteETag: `"abc"`, RemoteSize: 5, RemoteMtime: 100}
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/gold/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "old.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 100, FileSize: 5}, RemoteEntry: stamp},
			NewParentPath: "/outside/dir",
			NewEntry:      &filer_pb.Entry{Name: "new.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 200, FileSize: 5}, RemoteEntry: stamp},
		},
	}
	if err := process(resp); err != nil {
		t.Fatal(err)
	}
	if len(remote.deletes) != 0 {
		t.Errorf("deletes = %+v, want the remote-only object kept", remote.deletes)
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
