package command

import (
	"slices"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
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

// TestGatewayUncachedRewriteWaitsForTheSupersessionLookup: a rewrite of an
// uncached entry holds local data, so the dropped-copy skip does not apply
// and the rewrite is an upload. The lookup that guards the upload cannot
// reach a filer in the test, so the event fails with that error and nothing
// is uploaded: an unanswered lookup never lets a snapshot overwrite the
// remote object, and the processor retries the event.
func TestGatewayUncachedRewriteWaitsForTheSupersessionLookup(t *testing.T) {
	option, remote := gatewayTestOption("gatewaytest-rewrite")
	t.Cleanup(func() { delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-rewrite") })
	process, err := option.makeBucketedEventProcessor(&source.FilerSource{})
	if err != nil {
		t.Fatal(err)
	}

	rewrite := entryWith(droppedCopyName, droppedCopyStamp(), chunk("4,02", "e2"))
	resp := droppedCopyUpdate(droppedCopyEntry(droppedCopyStamp()), rewrite)
	err = process(resp)
	if err == nil || !strings.Contains(err.Error(), "supersession lookup") {
		t.Fatalf("process = %v, want the supersession lookup error", err)
	}
	if len(remote.writes) != 0 {
		t.Fatalf("writes = %+v, want none until the lookup answers", remote.writes)
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

func TestGatewayUnmountedBucketDeleteIsSkipped(t *testing.T) {
	remote := &recordingRemote{}
	remote_storage.RemoteStorageClientMakers["gatewaytest"] = recordingRemoteMaker{client: remote}
	t.Cleanup(func() { delete(remote_storage.RemoteStorageClientMakers, "gatewaytest") })
	option := &RemoteGatewayOptions{
		bucketsDir:  "/buckets",
		mappings:    &remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{}},
		remoteConfs: map[string]*remote_pb.RemoteConf{"gatewaytest": {Name: "gatewaytest", Type: "gatewaytest"}},
	}
	process, err := option.makeBucketedEventProcessor(nil)
	if err != nil {
		t.Fatal(err)
	}

	deleteBucketEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{
				Name:        "unmounted-bucket",
				IsDirectory: true,
			},
		},
	}

	// Deleting a bucket that is not mounted (for instance because it was already unmounted
	// by an earlier copy of the delete event) should return nil and not error.
	if err := process(deleteBucketEvent); err != nil {
		t.Fatalf("expected nil for unmounted bucket delete, got: %v", err)
	}
}

// TestGatewayCreatesABucketAfterTheMappingFileIsDeleted: deleting
// mount.mapping empties the mapping table, and a later bucket create still
// records its mount in it.
func TestGatewayCreatesABucketAfterTheMappingFileIsDeleted(t *testing.T) {
	option, remote := gatewayTestOption("gatewaytest-mapping-reset")
	t.Cleanup(func() {
		delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-mapping-reset")
		remote_storage.ResetRemoteStorageClients()
	})
	createAt, noFilter, noSuffix := "gatewaytest-mapping-reset", "", false
	option.createBucketAt, option.include, option.exclude, option.createBucketRandomSuffix = &createAt, &noFilter, &noFilter, &noSuffix
	process, err := option.makeBucketedEventProcessor(nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := process(&filer_pb.SubscribeMetadataResponse{
		Directory:         filer.DirectoryEtcRemote,
		EventNotification: &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: filer.REMOTE_STORAGE_MOUNT_FILE}},
	}); err != nil {
		t.Fatal(err)
	}
	// saving the new mount needs a filer, so the create itself errors
	_ = process(&filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets",
		EventNotification: &filer_pb.EventNotification{
			NewParentPath: "/buckets",
			NewEntry:      &filer_pb.Entry{Name: "fresh", IsDirectory: true},
		},
	})

	if !slices.Equal(remote.bucketCreates, []string{"fresh"}) {
		t.Errorf("bucket creates = %v, want [fresh]", remote.bucketCreates)
	}
	if loc := option.mappings.Mappings["/buckets/fresh"]; loc.GetBucket() != "fresh" {
		t.Errorf("mapping for /buckets/fresh = %v, want bucket fresh", loc)
	}
}

// TestGatewayDeleteOfAnAbsentObjectSucceeds: a repeated delete finds the
// object gone, which GCS reports as ErrRemoteObjectNotFound; the event has
// nothing left to do and must not hold the sync offset.
func TestGatewayDeleteOfAnAbsentObjectSucceeds(t *testing.T) {
	option, remote := gatewayTestOption("gatewaytest-absent-delete")
	t.Cleanup(func() {
		delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-absent-delete")
		remote_storage.ResetRemoteStorageClients()
	})
	remote.deleteErr = remote_storage.ErrRemoteObjectNotFound
	process, err := option.makeBucketedEventProcessor(nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := process(&filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/gold/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "obj.bin", Attributes: &filer_pb.FuseAttributes{}},
		},
	}); err != nil {
		t.Fatalf("delete of an absent object: %v", err)
	}
	if len(remote.deletes) != 1 || remote.deletes[0].Path != "/dir/obj.bin" {
		t.Errorf("deletes = %+v, want /dir/obj.bin", remote.deletes)
	}
}

// TestGatewayKeepsTheDefaultStorageAcrossMappingChanges: the default storage
// for new buckets is derived at startup and never saved in mount.mapping, so
// replacing or deleting that file must not drop it.
func TestGatewayKeepsTheDefaultStorageAcrossMappingChanges(t *testing.T) {
	for _, tt := range []struct {
		name  string
		event *filer_pb.EventNotification
	}{
		{"mapping updated", &filer_pb.EventNotification{NewParentPath: filer.DirectoryEtcRemote, NewEntry: &filer_pb.Entry{Name: filer.REMOTE_STORAGE_MOUNT_FILE}}},
		{"mapping deleted", &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: filer.REMOTE_STORAGE_MOUNT_FILE}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			option, remote := gatewayTestOption("gatewaytest-default-storage")
			t.Cleanup(func() {
				delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-default-storage")
				remote_storage.ResetRemoteStorageClients()
			})
			option.mappings.PrimaryBucketStorageName = "gatewaytest-default-storage"
			createAt, noFilter, noSuffix := "", "", false
			option.createBucketAt, option.include, option.exclude, option.createBucketRandomSuffix = &createAt, &noFilter, &noFilter, &noSuffix
			process, err := option.makeBucketedEventProcessor(nil)
			if err != nil {
				t.Fatal(err)
			}

			if err := process(&filer_pb.SubscribeMetadataResponse{Directory: filer.DirectoryEtcRemote, EventNotification: tt.event}); err != nil {
				t.Fatal(err)
			}
			// saving the new mount needs a filer, so the create itself errors
			_ = process(&filer_pb.SubscribeMetadataResponse{
				Directory: "/buckets",
				EventNotification: &filer_pb.EventNotification{
					NewParentPath: "/buckets",
					NewEntry:      &filer_pb.Entry{Name: "fresh", IsDirectory: true},
				},
			})

			if !slices.Equal(remote.bucketCreates, []string{"fresh"}) {
				t.Errorf("bucket creates = %v, want [fresh] in the default storage", remote.bucketCreates)
			}
		})
	}
}

// TestGatewayKeptDeleteMarkerRetry: once a lock is released, the kept marker's
// delete goes to the object key the bucket mount maps the version to, and the
// marker is stamped with the mount's storage.
func TestGatewayKeptDeleteMarkerRetry(t *testing.T) {
	const versionsDir = "/buckets/gold/dir/held.bin.versions"
	option, remote := gatewayTestOption("gatewaytest-kept-marker")
	t.Cleanup(func() {
		delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-kept-marker")
		remote_storage.ResetRemoteStorageClients()
	})
	filerClient := &stubFilerClient{entries: map[string]*filer_pb.Entry{
		versionsDir: {Name: "held.bin.versions", IsDirectory: true, Extended: map[string][]byte{
			s3_constants.ExtLatestVersionFileNameKey:    []byte("v_marker"),
			s3_constants.ExtLatestVersionIsDeleteMarker: []byte("true"),
		}},
		versionsDir + "/v_marker": {
			Name:       "v_marker",
			Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669},
			Extended:   map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true")},
		},
	}}

	if err := syncGatewayKeptDeleteMarker(filerClient, option, versionsDir, "v_old"); err != nil {
		t.Fatal(err)
	}
	if len(remote.deletes) != 1 || remote.deletes[0].Bucket != "gold" || remote.deletes[0].Path != "/dir/held.bin" {
		t.Errorf("deletes = %+v, want gold:/dir/held.bin", remote.deletes)
	}
	stamped := filerClient.updated.GetEntry()
	if filerClient.updates != 1 || stamped.GetName() != "v_marker" || stamped.GetRemoteEntry().GetStorageName() != "gatewaytest-kept-marker" {
		t.Errorf("stamps = %d (%v), want v_marker stamped with the mount's storage", filerClient.updates, stamped)
	}

	if err := syncGatewayKeptDeleteMarker(filerClient, option, "/buckets/unmounted/held.bin.versions", "v_old"); err != nil {
		t.Fatal(err)
	}
	if len(remote.deletes) != 1 || filerClient.updates != 1 {
		t.Errorf("deletes = %+v, stamps = %d; want nothing more for an unmounted bucket", remote.deletes, filerClient.updates)
	}
}

// TestGatewayLockReleaseLooksUpTheKeptMarker: an update or delete that
// releases a version's lock reads the key's versions from the gateway's filer,
// which the test cannot reach, so the event fails with that filer's address
// instead of being skipped. A change to an unlocked version is skipped
// without a lookup.
func TestGatewayLockReleaseLooksUpTheKeptMarker(t *testing.T) {
	const versionsDir = "/buckets/gold/dir/held.bin.versions"
	option, remote := gatewayTestOption("gatewaytest-lock-release")
	t.Cleanup(func() {
		delete(remote_storage.RemoteStorageClientMakers, "gatewaytest-lock-release")
		remote_storage.ResetRemoteStorageClients()
	})
	filerGrpcAddress := pb.ServerAddress(*option.filerAddress).ToGrpcAddress()
	process, err := option.makeBucketedEventProcessor(nil)
	if err != nil {
		t.Fatal(err)
	}
	version := func(extended map[string][]byte) *filer_pb.Entry {
		return &filer_pb.Entry{Name: "v_old", Attributes: &filer_pb.FuseAttributes{FileSize: 8192}, Extended: extended}
	}
	held := map[string][]byte{s3_constants.ExtLegalHoldKey: []byte(s3_constants.LegalHoldOn)}
	released := map[string][]byte{s3_constants.ExtLegalHoldKey: []byte(s3_constants.LegalHoldOff)}

	for _, tc := range []struct {
		name       string
		event      *filer_pb.EventNotification
		wantLookup bool
	}{
		{"hold released", &filer_pb.EventNotification{OldEntry: version(held), NewParentPath: versionsDir, NewEntry: version(released)}, true},
		{"held version deleted", &filer_pb.EventNotification{OldEntry: version(held)}, true},
		{"unlocked version updated", &filer_pb.EventNotification{OldEntry: version(nil), NewParentPath: versionsDir, NewEntry: version(released)}, false},
		{"unlocked version deleted", &filer_pb.EventNotification{OldEntry: version(nil)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := process(&filer_pb.SubscribeMetadataResponse{Directory: versionsDir, EventNotification: tc.event})
			if tc.wantLookup {
				if err == nil || !strings.Contains(err.Error(), filerGrpcAddress) {
					t.Errorf("process = %v, want an unanswered lookup on %s", err, filerGrpcAddress)
				}
			} else if err != nil {
				t.Errorf("process = %v, want the change skipped", err)
			}
			if len(remote.deletes) != 0 || len(remote.writes) != 0 {
				t.Errorf("deletes = %+v, writes = %+v; want the remote untouched", remote.deletes, remote.writes)
			}
		})
	}
}
