package command

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Which side holds an entry's content, and the two processors that keep the
// remote in step with it.
//
// An entry with local data (chunks or inline content) is authoritative: the
// update uploads it. A chunkless entry stamped with a RemoteEntry is
// remote-only: its content is the remote object its stamp describes, so the
// chunkless entry is never uploaded, since it would write zero bytes over
// the only copy, and the object goes only while the stamp still describes it
// (remote_storage.StillCurrent). While the entry carries a
// filer.ExtRemoteRenameSourceKey marker the content is the object at the
// renamed-from path, until the rename's copy or upload restamps the entry
// and clears it. A dropped local copy (remote.uncache, eviction,
// remote.meta.sync) keeps the remote object's size; a size that moved off
// the stamp (a truncate) is new content and uploads.
//
// processUpdateEvent serves filer remote.sync over one mounted directory;
// makeBucketedEventProcessor in filer_remote_gateway_buckets.go serves filer
// remote.gateway over the bucketed space. They implement the same protocol
// and must change together: a guard added to one and not the other is how
// the zero-byte re-upload on remote.uncache happened.

func followUpdatesAndUploadToRemote(option *RemoteSyncOptions, filerSource *source.FilerSource, mountedDir string) error {

	// read filer remote storage mount mappings
	_, _, remoteStorageMountLocation, remoteStorage, detectErr := filer.DetectMountInfo(option.grpcDialOption, pb.ServerAddress(*option.filerAddress), mountedDir)
	if detectErr != nil {
		return fmt.Errorf("read mount info: %w", detectErr)
	}

	eachEntryFunc, err := option.makeEventProcessor(remoteStorage, mountedDir, remoteStorageMountLocation, filerSource)
	if err != nil {
		return err
	}

	lastOffsetTs := collectLastSyncOffset(option, option.grpcDialOption, pb.ServerAddress(*option.filerAddress), mountedDir, *option.timeAgo)
	processor := NewMetadataProcessor(eachEntryFunc, 128, lastOffsetTs.UnixNano())

	var lastLogTsNs = time.Now().UnixNano()
	processEventFnWithOffset := pb.AddOffsetFunc(func(resp *filer_pb.SubscribeMetadataResponse) error {
		processor.AddSyncJob(resp)
		return nil
	}, 3*time.Second, func(counter int64, lastTsNs int64) error {
		offsetTsNs := processor.processedTsWatermark.Load()
		if offsetTsNs == 0 {
			return nil
		}
		// use processor.processedTsWatermark instead of the lastTsNs from the most recent job
		now := time.Now().UnixNano()
		glog.V(0).Infof("remote sync %s progressed to %v %0.2f/sec", *option.filerAddress, time.Unix(0, offsetTsNs), float64(counter)/(float64(now-lastLogTsNs)/1e9))
		lastLogTsNs = now
		return remote_storage.SetSyncOffset(option.grpcDialOption, pb.ServerAddress(*option.filerAddress), mountedDir, resumeTsNsFor(processor))
	})

	option.clientEpoch++

	prefix := mountedDir
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             "filer.remote.sync",
		ClientId:               option.clientId,
		ClientEpoch:            option.clientEpoch,
		SelfSignature:          0,
		PathPrefix:             prefix,
		AdditionalPathPrefixes: []string{filer.DirectoryEtcRemote},
		DirectoriesToWatch:     nil,
		StartTsNs:              lastOffsetTs.UnixNano(),
		StopTsNs:               0,
		EventErrorType:         pb.RetryForeverOnError,
		GetResumeTsNs: func() int64 {
			return resumeTsNsFor(processor)
		},
		Resubscribe: processor.ResubscribeCh(),
	}

	return pb.FollowMetadata(pb.ServerAddress(*option.filerAddress), option.grpcDialOption, metadataFollowOption, processEventFnWithOffset)
}

func (option *RemoteSyncOptions) makeEventProcessor(remoteStorage *remote_pb.RemoteConf, mountedDir string, remoteStorageMountLocation *remote_pb.RemoteStorageLocation, filerSource *source.FilerSource) (pb.ProcessMetadataFunc, error) {
	client, err := remote_storage.GetRemoteStorage(remoteStorage)
	if err != nil {
		return nil, err
	}

	handleEtcRemoteChanges := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		if metadataEventUpdatesDirectory(resp, filer.DirectoryEtcRemote) {
			if message.NewEntry.Name == filer.REMOTE_STORAGE_MOUNT_FILE {
				mappings, readErr := filer.UnmarshalRemoteStorageMappings(message.NewEntry.Content)
				if readErr != nil {
					return fmt.Errorf("unmarshal mappings: %w", readErr)
				}
				if remoteLoc, found := mappings.Mappings[mountedDir]; found {
					if remoteStorageMountLocation.Bucket != remoteLoc.Bucket || remoteStorageMountLocation.Path != remoteLoc.Path {
						glog.Fatalf("Unexpected mount changes %+v => %+v", remoteStorageMountLocation, remoteLoc)
					}
				} else {
					glog.V(0).Infof("unmounted %s exiting ...", mountedDir)
					os.Exit(0)
				}
			}
			if message.NewEntry.Name == remoteStorage.Name+filer.REMOTE_STORAGE_CONF_SUFFIX {
				conf := &remote_pb.RemoteConf{}
				if err := proto.Unmarshal(message.NewEntry.Content, conf); err != nil {
					return fmt.Errorf("unmarshal %s/%s: %v", filer.DirectoryEtcRemote, message.NewEntry.Name, err)
				}
				remoteStorage = conf
				if newClient, err := remote_storage.GetRemoteStorage(remoteStorage); err == nil {
					client = newClient
				} else {
					return err
				}
			}
		}
		if metadataEventRemovesFromDirectory(resp, filer.DirectoryEtcRemote) &&
			message.OldEntry.Name == filer.REMOTE_STORAGE_MOUNT_FILE {
			glog.V(0).Infof("unmounted %s exiting ...", mountedDir)
			os.Exit(0)
		}

		return nil
	}

	eachEntryFunc := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		sourceInEtcRemote, targetInEtcRemote := metadataEventDirectoryMembership(resp, filer.DirectoryEtcRemote)
		if sourceInEtcRemote || targetInEtcRemote {
			return handleEtcRemoteChanges(resp)
		}

		if filer_pb.IsEmpty(resp) {
			return nil
		}
		mustSend := false
		var renameSrc util.FullPath
		if message.OldEntry != nil && message.NewEntry != nil {
			reshaped, inMount := reshapeCrossMountRename(mountedDir, resp)
			if !inMount {
				return nil
			}
			// a create reshaped from a rename into the mount can carry a
			// RemoteEntry earned at the old path: it stamps the old key, so
			// it cannot vouch for this destination
			mustSend = filer_pb.IsCreate(reshaped)
			if mustSend {
				renameSrc = util.NewFullPath(resp.Directory, message.OldEntry.Name)
			}
			resp, message = reshaped, reshaped.EventNotification
		}
		if filer_pb.IsCreate(resp) {
			return processCreateEvent(option, filerSource, *option.storageClass, client, mountedDir, remoteStorageMountLocation, resp, mustSend, renameSrc)
		}
		if filer_pb.IsDelete(resp) {
			return processDeleteEvent(filerSource, client, mountedDir, remoteStorageMountLocation, resp)
		}
		if message.OldEntry != nil && message.NewEntry != nil {
			return processUpdateEvent(option, filerSource, *option.storageClass, client, mountedDir, remoteStorageMountLocation, resp)
		}

		return nil
	}
	return eachEntryFunc, nil
}

// reshapeCrossMountRename maps an update event onto the side of it the mount
// covers. The subscription delivers a rename when either of its paths lies
// under the mounted directory, and only a path under it maps to a remote
// key: toRemoteStorageLocation slices the mount prefix off, which panics on
// a shorter path and names a key in the wrong tree on a longer one. A rename
// into the mount is the creation of the new entry, a rename out of it the
// deletion of the old entry's object, unless that entry is remote-only and
// the object holds its only copy. An update with both paths inside is
// returned as is; one with neither is not the mount's (false).
func reshapeCrossMountRename(mountedDir string, resp *filer_pb.SubscribeMetadataResponse) (*filer_pb.SubscribeMetadataResponse, bool) {
	message := resp.EventNotification
	oldInMount := isUnderMount(mountedDir, resp.Directory, message.OldEntry.Name)
	newInMount := isUnderMount(mountedDir, message.NewParentPath, message.NewEntry.Name)
	switch {
	case oldInMount && newInMount:
		return resp, true
	case newInMount:
		glog.V(0).Infof("rename into mount %s: %s/%s -> %s/%s, creating", mountedDir, resp.Directory, message.OldEntry.Name, message.NewParentPath, message.NewEntry.Name)
		return &filer_pb.SubscribeMetadataResponse{
			Directory:         message.NewParentPath,
			EventNotification: &filer_pb.EventNotification{NewEntry: message.NewEntry, NewParentPath: message.NewParentPath, Signatures: message.Signatures},
			TsNs:              resp.TsNs,
			MutationTsNs:      resp.MutationTsNs,
		}, true
	case oldInMount:
		if isRemoteOnly(message.OldEntry) {
			glog.V(0).Infof("rename out of mount %s: %s/%s -> %s/%s, keeping the remote-only object", mountedDir, resp.Directory, message.OldEntry.Name, message.NewParentPath, message.NewEntry.Name)
			return nil, false
		}
		glog.V(0).Infof("rename out of mount %s: %s/%s -> %s/%s, deleting", mountedDir, resp.Directory, message.OldEntry.Name, message.NewParentPath, message.NewEntry.Name)
		return &filer_pb.SubscribeMetadataResponse{
			Directory:         resp.Directory,
			EventNotification: &filer_pb.EventNotification{OldEntry: message.OldEntry, Signatures: message.Signatures},
			TsNs:              resp.TsNs,
			MutationTsNs:      resp.MutationTsNs,
		}, true
	default:
		return nil, false
	}
}

// isUnderMount reports whether dir/name is the mounted directory or lies
// below it; a trailing slash on the mount flag does not change the answer.
func isUnderMount(mountedDir string, dir string, name string) bool {
	mount := strings.TrimSuffix(mountedDir, "/")
	path := string(util.NewFullPath(dir, name))
	return path == mount || strings.HasPrefix(path, mount+"/")
}

// processCreateEvent uploads a created entry to the remote key its path maps
// to. mustSend skips the sync-stamp check: a create reshaped out of a rename
// into the mount inherits a RemoteEntry stamped at the old path's key, which
// says nothing about whether this destination holds the content. renameSrc is
// the path that rename came from, and is empty for a genuine create.
func processCreateEvent(
	filerClient filer_pb.FilerClient,
	filerSource filer_pb.FilerClient,
	storageClass string,
	client remote_storage.RemoteStorageClient,
	mountedDir string,
	remoteStorageMountLocation *remote_pb.RemoteStorageLocation,
	resp *filer_pb.SubscribeMetadataResponse,
	mustSend bool,
	renameSrc util.FullPath,
) error {
	message := resp.EventNotification
	if isMultipartUploadFile(message.NewParentPath, message.NewEntry.Name) {
		return nil
	}
	if isSystemPath(message.NewParentPath, message.NewEntry.Name) {
		glog.V(2).Infof("skipping create of system path: %s/%s", message.NewParentPath, message.NewEntry.Name)
		return nil
	}
	// Propagate delete markers as deletions on the remote.
	// Delete markers are zero-content version entries, so they
	// would be filtered out by the HasData check below.
	if isDeleteMarker(message.NewEntry) {
		if newParent, newName, ok := rewriteVersionedSourcePath(message.NewParentPath, message.NewEntry.Name); ok {
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(newParent, newName), remoteStorageMountLocation)
			return syncDeleteMarker(client, filerClient, message, dest)
		}
		return nil
	}
	// A create reshaped from a rename into the mount (mustSend) writes an empty
	// entry as a zero-byte object; a dropped local copy still skips since the
	// rename already copied its object to the new key. A stamped entry whose
	// size diverged since (a truncate) is not a dropped copy and sends its
	// new content.
	if !filer.HasData(message.NewEntry) && !(mustSend && !isDroppedLocalCopy(message.NewEntry)) {
		return nil
	}
	glog.V(2).Infof("create: %s/%s ts_ns=%d", filer_pb.MetadataEventTargetDirectory(resp), message.GetNewEntry().GetName(), resp.TsNs)
	if !mustSend && !shouldSendToRemote(message.NewEntry) {
		glog.V(2).Infof("skipping creating: %s/%s ts_ns=%d", filer_pb.MetadataEventTargetDirectory(resp), message.GetNewEntry().GetName(), resp.TsNs)
		return nil
	}
	// Rewrite internal versioning paths to the original S3 key
	// to prevent double-versioning when central also has versioning enabled
	parentPath, entryName := message.NewParentPath, message.NewEntry.Name
	isRewrittenVersion := false
	if newParent, newName, ok := rewriteVersionedSourcePath(parentPath, entryName); ok {
		glog.V(0).Infof("rewrite versioned path %s/%s -> %s/%s", parentPath, entryName, newParent, newName)
		parentPath, entryName = newParent, newName
		isRewrittenVersion = true
	}
	dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(parentPath, entryName), remoteStorageMountLocation)
	if message.NewEntry.IsDirectory {
		glog.V(0).Infof("mkdir  %s", remote_storage.FormatLocation(dest))
		return client.WriteDirectory(dest, remoteWriteEntry(message.NewEntry, storageClass))
	}
	remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.NewParentPath, remoteWriteEntry(message.NewEntry, storageClass), dest)
	if errors.Is(writeErr, errSuperseded) {
		glog.Errorf("skipping %s: %v", remote_storage.FormatLocation(dest), writeErr)
		if mustSend {
			return recoverSupersededCreate(filerClient, filerSource, client, mountedDir, remoteStorageMountLocation, message.NewParentPath, message.NewEntry.Name, renameSrc, dest, storageClass)
		}
		return nil
	}
	if writeErr != nil {
		return writeErr
	}
	// Skip updateLocalEntry for versioned rewrites: the logical
	// object (e.g. file.xml) has no filer entry in versioned
	// buckets, and stamping the internal v_* entry with a
	// RemoteEntry for the logical key is semantically wrong.
	// Replay is safe because S3 PutObject is idempotent.
	if isRewrittenVersion {
		return nil
	}
	return updateLocalEntry(filerClient, message.NewParentPath, message.NewEntry, remoteEntry)
}

// processDeleteEvent removes the remote object an entry mapped to. A remote
// object that is already absent counts as deleted: the entry was never
// uploaded (created and deleted faster than the sync ran, or a replay of an
// event the inline delete already handled), and GCS reports that case as
// ErrRemoteObjectNotFound where S3 and Azure answer an idempotent success.
// Returning the error would pin the sync offset on an event that has nothing
// left to do. A file whose path a remote-only entry has taken since keeps its
// object: a rename copied that entry's content to this key, and it exists
// nowhere else.
func processDeleteEvent(
	filerSource filer_pb.FilerClient,
	client remote_storage.RemoteStorageClient,
	mountedDir string,
	remoteStorageMountLocation *remote_pb.RemoteStorageLocation,
	resp *filer_pb.SubscribeMetadataResponse,
) error {
	message := resp.EventNotification
	// Skip deletion of internal version files; individual version
	// deletes should not propagate to the remote object
	if isVersionedPath(resp.Directory, message.OldEntry.Name, message.OldEntry.IsDirectory) {
		glog.V(2).Infof("skipping delete of internal version path: %s/%s", resp.Directory, message.OldEntry.Name)
		return nil
	}
	if isSystemPath(resp.Directory, message.OldEntry.Name) {
		glog.V(2).Infof("skipping delete of system path: %s/%s", resp.Directory, message.OldEntry.Name)
		return nil
	}
	if filer.IsMetadataOnlyDelete(message.OldEntry) {
		glog.V(2).Infof("skipping remote delete of metadata-only delete: %s/%s", resp.Directory, message.OldEntry.Name)
		return nil
	}
	glog.V(2).Infof("delete: %s/%s ts_ns=%d", resp.Directory, message.GetOldEntry().GetName(), resp.TsNs)
	dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(resp.Directory, message.OldEntry.Name), remoteStorageMountLocation)
	if !message.OldEntry.IsDirectory {
		// a deleted entry whose rename copy was still pending read its
		// content from the source its marker names, which nothing else
		// releases
		if err := releasePendingSource(filerSource, client, mountedDir, remoteStorageMountLocation, message.OldEntry); err != nil {
			return err
		}
		current, err := currentEntry(filerSource, resp.Directory, message.OldEntry.Name)
		if err != nil {
			return err
		}
		if isRemoteOnly(current) {
			glog.V(0).Infof("keep %s: a remote-only entry has taken its path since", remote_storage.FormatLocation(dest))
			return nil
		}
	}
	if message.OldEntry.IsDirectory {
		glog.V(0).Infof("rmdir  %s", remote_storage.FormatLocation(dest))
		return client.RemoveDirectory(dest)
	}
	// The object at the key may have been replaced since the delete: a rename
	// copies another entry's content onto it, a rewrite uploads over it, and a
	// copy can land before its entry is visible. Delete only an object the
	// event's stamp still describes.
	glog.V(0).Infof("delete %s", remote_storage.FormatLocation(dest))
	return deleteRemoteFileIfUnchanged(client, dest, message.OldEntry.GetRemoteEntry())
}

// releasePendingSource deletes the source a pending-rename marker on entry
// names, once nothing needs it. The filer never deletes a marked source: a
// concurrent rename can copy onto that key before its entry is visible. So
// the object goes only while it is still the one entry's stamp describes, and
// no remote-only entry holds its path.
func releasePendingSource(filerSource filer_pb.FilerClient, client remote_storage.RemoteStorageClient, mountedDir string, remoteStorageMountLocation *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry) error {
	src := util.FullPath(entry.GetExtended()[filer.ExtRemoteRenameSourceKey])
	if src == "" || !strings.HasPrefix(string(src), strings.TrimSuffix(mountedDir, "/")+"/") {
		return nil
	}
	srcDest := toRemoteStorageLocation(util.FullPath(mountedDir), src, remoteStorageMountLocation)
	srcDir, srcName := src.DirAndName()
	current, err := currentEntry(filerSource, srcDir, srcName)
	if err != nil {
		return err
	}
	if isRemoteOnly(current) {
		glog.V(0).Infof("keep %s: a remote-only entry holds the pending rename's source", remote_storage.FormatLocation(srcDest))
		return nil
	}
	glog.V(0).Infof("delete %s, the source of a pending rename", remote_storage.FormatLocation(srcDest))
	return deleteRemoteFileIfUnchanged(client, srcDest, entry.GetRemoteEntry())
}

// deleteRemoteFile deletes the object and treats an already-absent object as
// deleted.
func deleteRemoteFile(client remote_storage.RemoteStorageClient, dest *remote_pb.RemoteStorageLocation) error {
	if err := client.DeleteFile(dest); err != nil && !errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
		return err
	}
	return nil
}

// deleteRemoteFileIfUnchanged deletes the object only while it is still the
// one stamp describes. Backends offering a conditional delete bind the check
// and the delete into one preconditioned request; the rest fall back to a
// guarded stat-then-delete.
func deleteRemoteFileIfUnchanged(client remote_storage.RemoteStorageClient, dest *remote_pb.RemoteStorageLocation, stamp *filer_pb.RemoteEntry) error {
	if deleter, ok := client.(remote_storage.RemoteStorageConditionalDeleter); ok {
		deleted, err := deleter.DeleteFileIfUnchanged(dest, stamp)
		if err != nil {
			return err
		}
		if !deleted {
			glog.V(0).Infof("keep %s: the object changed since the delete", remote_storage.FormatLocation(dest))
		}
		return nil
	}
	live, err := client.StatFile(dest)
	if err != nil && !errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
		return err
	}
	if err == nil && !remote_storage.StillCurrent(stamp, live) {
		glog.V(0).Infof("keep %s: the object changed since the delete", remote_storage.FormatLocation(dest))
		return nil
	}
	return deleteRemoteFile(client, dest)
}

func processUpdateEvent(
	filerClient filer_pb.FilerClient,
	filerSource filer_pb.FilerClient,
	storageClass string,
	client remote_storage.RemoteStorageClient,
	mountedDir string,
	remoteStorageMountLocation *remote_pb.RemoteStorageLocation,
	resp *filer_pb.SubscribeMetadataResponse,
) error {
	message := resp.EventNotification
	if isMultipartUploadFile(message.NewParentPath, message.NewEntry.Name) {
		return nil
	}
	if isVersionedPath(message.NewParentPath, message.NewEntry.Name, message.NewEntry.IsDirectory) {
		glog.V(2).Infof("skipping update of internal version path: %s/%s", message.NewParentPath, message.NewEntry.Name)
		return nil
	}
	if isSystemPath(message.NewParentPath, message.NewEntry.Name) {
		glog.V(2).Infof("skipping update of system path: %s/%s", message.NewParentPath, message.NewEntry.Name)
		return nil
	}
	oldDest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(resp.Directory, message.OldEntry.Name), remoteStorageMountLocation)
	dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(message.NewParentPath, message.NewEntry.Name), remoteStorageMountLocation)
	eventSource := renameSource{path: util.NewFullPath(resp.Directory, message.OldEntry.Name), loc: oldDest, stamp: message.OldEntry.GetRemoteEntry()}
	// sourceOf is where a remote-only entry's content is: the source its
	// pending-rename marker names, which may be an earlier rename's old key,
	// or else the event's old key
	sourceOf := func(entry *filer_pb.Entry, fallback renameSource) renameSource {
		if src := string(entry.Extended[filer.ExtRemoteRenameSourceKey]); strings.HasPrefix(src, strings.TrimSuffix(mountedDir, "/")+"/") {
			// a pending entry carries the stamp of the source it reads
			return renameSource{path: util.FullPath(src), loc: toRemoteStorageLocation(util.FullPath(mountedDir), util.FullPath(src), remoteStorageMountLocation), stamp: entry.GetRemoteEntry()}
		}
		return fallback
	}
	if proto.Equal(oldDest, dest) && !shouldSendToRemote(message.NewEntry) {
		glog.V(2).Infof("skipping updating: %s/%s ts_ns=%d", filer_pb.MetadataEventTargetDirectory(resp), message.GetNewEntry().GetName(), resp.TsNs)
		return nil
	}
	if message.NewEntry.IsDirectory {
		return client.WriteDirectory(dest, remoteWriteEntry(message.NewEntry, storageClass))
	}
	if isMetadataOnlyUpdate(resp.Directory, message) {
		remoteEntry, err := liveRemoteEntry(filerClient, message.NewParentPath, message.NewEntry)
		if errors.Is(err, filer_pb.ErrNotFound) {
			glog.V(2).Infof("skipping updating deleted entry: %s/%s ts_ns=%d", filer_pb.MetadataEventTargetDirectory(resp), message.GetNewEntry().GetName(), resp.TsNs)
			return nil
		}
		if err != nil {
			return err
		}
		if remoteEntry != nil {
			glog.V(2).Infof("update meta: %s/%s ts_ns=%d", filer_pb.MetadataEventTargetDirectory(resp), message.GetNewEntry().GetName(), resp.TsNs)
			updated, err := client.UpdateFileMetadata(dest, message.OldEntry, remoteWriteEntry(message.NewEntry, storageClass))
			if err != nil {
				return err
			}
			// a metadata write can rewrite the object (S3 copies it in place
			// for a Content-Encoding change, minting a new ETag): restamp from
			// the object the update produced when the backend reports it
			if updated != nil {
				remoteEntry = updated
			}
			return updateLocalEntry(filerClient, message.NewParentPath, message.NewEntry, remoteEntry)
		}
		glog.V(0).Infof("never replicated, uploading %s", remote_storage.FormatLocation(dest))
	}
	if proto.Equal(oldDest, dest) && isDroppedLocalCopy(message.NewEntry) {
		// The update dropped the local copy: remote.uncache, eviction under
		// storage pressure, or remote.meta.sync pulling a newer remote
		// version. Each clears the sync stamp, so shouldSendToRemote reads
		// the entry as dirty when the remote mtime shares its second. The
		// content is the object at this very key, and uploading the chunkless
		// entry would replace it with zero bytes.
		glog.V(2).Infof("skipping update of remote-only entry: %s/%s ts_ns=%d", filer_pb.MetadataEventTargetDirectory(resp), message.GetNewEntry().GetName(), resp.TsNs)
		return nil
	}
	glog.V(2).Infof("update: %s/%s ts_ns=%d", filer_pb.MetadataEventTargetDirectory(resp), message.GetNewEntry().GetName(), resp.TsNs)
	if !proto.Equal(oldDest, dest) {
		// A renamed entry that holds no local data now (the snapshot was
		// remote-only, or remote.uncache ran since) has its content only as a
		// remote object. Remote-only reads resolve by the entry's own path, so
		// the rename completes only once the destination object holds it. The
		// filer's current state decides, not the snapshot: an entry rewritten
		// since the event has local data again, and the rewrite's bytes are
		// what the destination must hold.
		current, err := currentEntry(filerSource, message.NewParentPath, message.NewEntry.Name)
		if err != nil {
			return err
		}
		if isRemoteOnly(current) {
			source := sourceOf(current, eventSource)
			if err := completeRemoteOnlyRename(filerClient, client, message.NewParentPath, current, source, dest, storageClass); err != nil {
				return err
			}
			// a pending entry the filer copied on this rename leaves the
			// source its marker named
			if pending := sourceOf(message.OldEntry, renameSource{}); pending.loc != nil && !proto.Equal(pending.loc, source.loc) {
				return releasePendingSource(filerSource, client, mountedDir, remoteStorageMountLocation, message.OldEntry)
			}
			return nil
		}
		if current == nil && len(message.NewEntry.Extended[filer.ExtRemoteRenameSourceKey]) > 0 {
			// the rename's copy was pending and the entry has moved on: a
			// later rename carries the source in its own marker and finishes
			// or releases it, so this one leaves it
			glog.V(0).Infof("keep %s: the pending rename moved on", remote_storage.FormatLocation(oldDest))
			return nil
		}
		glog.V(0).Infof("delete %s", remote_storage.FormatLocation(oldDest))
		if err := client.DeleteFile(oldDest); err != nil {
			if isMultipartUploadFile(resp.Directory, message.OldEntry.Name) || isSystemPath(resp.Directory, message.OldEntry.Name) {
				return nil
			}
			if !errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
				return err
			}
		}
	}
	remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.NewParentPath, remoteWriteEntry(message.NewEntry, storageClass), dest)
	if errors.Is(writeErr, errSuperseded) {
		glog.Errorf("skipping %s: %v", remote_storage.FormatLocation(dest), writeErr)
		if !proto.Equal(oldDest, dest) {
			if err := uploadCurrentEntry(filerClient, filerSource, client, message.NewParentPath, message.NewEntry.Name, eventSource, dest, storageClass, sourceOf); err != nil {
				return err
			}
			return releaseRenamedSource(filerSource, client, mountedDir, remoteStorageMountLocation, message)
		}
		return nil
	}
	if writeErr != nil {
		return writeErr
	}
	if err := updateLocalEntry(filerClient, message.NewParentPath, message.NewEntry, remoteEntry); err != nil {
		return err
	}
	if !proto.Equal(oldDest, dest) {
		return releaseRenamedSource(filerSource, client, mountedDir, remoteStorageMountLocation, message)
	}
	return nil
}

// releaseRenamedSource releases a pending source the renamed entry's old
// version read, once the destination holds the content on the remote under
// its own key: only after the upload and its stamp, which clears the marker,
// and only if the entry does not read through a marker any more. Released
// earlier, an upload that fails followed by remote.uncache would leave the
// entry reading a deleted source.
func releaseRenamedSource(filerSource filer_pb.FilerClient, client remote_storage.RemoteStorageClient, mountedDir string, remoteStorageMountLocation *remote_pb.RemoteStorageLocation, message *filer_pb.EventNotification) error {
	if len(message.OldEntry.GetExtended()[filer.ExtRemoteRenameSourceKey]) == 0 {
		return nil
	}
	current, err := currentEntry(filerSource, message.NewParentPath, message.NewEntry.Name)
	if err != nil {
		return err
	}
	if current != nil && len(current.Extended[filer.ExtRemoteRenameSourceKey]) > 0 {
		return nil
	}
	return releasePendingSource(filerSource, client, mountedDir, remoteStorageMountLocation, message.OldEntry)
}

// currentEntry returns what the filer holds at dir/name now, nil when the
// entry is gone.
func currentEntry(filerSource filer_pb.FilerClient, dir, name string) (*filer_pb.Entry, error) {
	current, _, _, err := filer_pb.GetEntry(context.Background(), filerSource, util.NewFullPath(dir, name))
	if errors.Is(err, filer_pb.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return current, nil
}

// isRemoteOnly reports an entry whose content exists only as its remote
// object: no local data, a RemoteEntry with a size.
func isRemoteOnly(entry *filer_pb.Entry) bool {
	return entry != nil && !filer.HasData(entry) && entry.IsInRemoteOnly()
}

// isDroppedLocalCopy reports a remote-only entry whose size is still the
// remote object's: its local copy was dropped and the object describes it.
// Uncache and eviction keep the size, and remote.meta.sync sets it from the
// object. A truncate drops the chunks too but changes the size, and the
// truncated content has to reach the remote.
func isDroppedLocalCopy(entry *filer_pb.Entry) bool {
	return isRemoteOnly(entry) && entry.GetAttributes().GetFileSize() == uint64(entry.RemoteEntry.RemoteSize)
}

// completeRemoteOnlyRename finishes a rename whose content exists only on the
// remote. When the destination object already holds what the entry's stamp
// describes (the rename ran before, its offset was not persisted, and
// remote.uncache followed), the old key goes. Otherwise the old object is
// copied over the destination, the entry is stamped with the copy, and the
// old key goes. With neither object present the content is lost: the event
// fails and holds the offset for recovery.
func completeRemoteOnlyRename(filerClient filer_pb.FilerClient, client remote_storage.RemoteStorageClient, dir string, current *filer_pb.Entry, source renameSource, dest *remote_pb.RemoteStorageLocation, storageClass string) error {
	oldDest := source.loc
	// the source's recorded stamp; a retry can find the entry already
	// stamped with the copy, whose ETag a multipart source does not share
	sourceStamp := source.stamp
	if sourceStamp == nil {
		sourceStamp = proto.Clone(current.RemoteEntry).(*filer_pb.RemoteEntry)
	}
	if existing, err := client.StatFile(dest); err == nil {
		if remote_storage.Describes(current.RemoteEntry, existing) {
			glog.V(0).Infof("%s already holds the renamed content", remote_storage.FormatLocation(dest))
			if len(current.Extended[filer.ExtRemoteRenameSourceKey]) > 0 {
				// reads still go to the source until the stamp clears the marker
				if err := updateLocalEntry(filerClient, dir, current, existing); err != nil {
					return err
				}
			}
			return deleteRenameSource(filerClient, client, dir, current.Name, source, sourceStamp)
		}
		glog.V(0).Infof("%s holds an object the entry does not describe (size %d, want %d); replacing it from %s", remote_storage.FormatLocation(dest), existing.RemoteSize, current.RemoteEntry.RemoteSize, remote_storage.FormatLocation(oldDest))
	} else if !errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
		return err
	}
	stat, err := client.StatFile(oldDest)
	if errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
		return fmt.Errorf("%s: content is on neither %s nor %s", util.NewFullPath(dir, current.Name), remote_storage.FormatLocation(oldDest), remote_storage.FormatLocation(dest))
	}
	if err != nil {
		return err
	}
	reader, err := openRemoteObject(client, oldDest, stat.RemoteSize)
	if err != nil {
		return err
	}
	defer reader.Close()
	glog.V(0).Infof("copy %s -> %s", remote_storage.FormatLocation(oldDest), remote_storage.FormatLocation(dest))
	remoteEntry, err := client.WriteFile(dest, remoteWriteEntry(current, storageClass), reader)
	if err != nil {
		return err
	}
	if err := updateLocalEntry(filerClient, dir, current, remoteEntry); err != nil {
		return err
	}
	return deleteRenameSource(filerClient, client, dir, current.Name, source, sourceStamp)
}

// renameSource is where a renamed remote-only entry's content was read
// from: the path, the object it maps to, and the stamp describing it.
type renameSource struct {
	path  util.FullPath
	loc   *remote_pb.RemoteStorageLocation
	stamp *filer_pb.RemoteEntry
}

// deleteRenameSource deletes a rename's old key once the renamed entry no
// longer reads it: an entry still carrying the pending-source marker (its
// stamp lost a race) keeps the source, and the event is retried. The key is
// left alone when the object there is no longer the one stamp describes, as
// when another rename copied onto it, visible yet or not.
func deleteRenameSource(filerClient filer_pb.FilerClient, client remote_storage.RemoteStorageClient, dir, name string, source renameSource, stamp *filer_pb.RemoteEntry) error {
	current, err := currentEntry(filerClient, dir, name)
	if err != nil {
		return err
	}
	if current != nil && len(current.Extended[filer.ExtRemoteRenameSourceKey]) > 0 {
		return fmt.Errorf("%s still reads %s", util.NewFullPath(dir, name), remote_storage.FormatLocation(source.loc))
	}
	if err := deleteRemoteFileIfUnchanged(client, source.loc, stamp); err != nil {
		return err
	}
	return nil
}

// openRemoteObject streams the object when the client can, and reads it whole
// otherwise.
func openRemoteObject(client remote_storage.RemoteStorageClient, loc *remote_pb.RemoteStorageLocation, size int64) (io.ReadCloser, error) {
	if streamer, ok := client.(remote_storage.RemoteStorageStreamReader); ok {
		return streamer.ReadFileAsStream(context.Background(), loc, 0, size)
	}
	data, err := client.ReadFile(loc, 0, size)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// uploadCurrentEntry uploads what the filer holds at dir/name now, when the
// event that superseded this rename will not. A rename whose snapshot is
// superseded has already deleted the old key; the rewrite behind it in the
// log uploads the destination itself unless shouldSendToRemote skips it on
// the inherited RemoteEntry, whose RemoteMtime can equal the rewrite's mtime
// within the same second. Only that case uploads here, so the content goes
// up once. A remote-only entry is finished the way completeRemoteOnlyRename
// finishes a rename: its stamp can already describe the destination (a sync
// plus remote.uncache in the meantime), and only then is it complete.
func uploadCurrentEntry(filerClient filer_pb.FilerClient, filerSource filer_pb.FilerClient, client remote_storage.RemoteStorageClient, dir, name string, oldSource renameSource, dest *remote_pb.RemoteStorageLocation, storageClass string, sourceOf func(*filer_pb.Entry, renameSource) renameSource) error {
	current, _, _, err := filer_pb.GetEntry(context.Background(), filerSource, util.NewFullPath(dir, name))
	if errors.Is(err, filer_pb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if current.IsDirectory {
		return nil
	}
	if isRemoteOnly(current) {
		return completeRemoteOnlyRename(filerClient, client, dir, current, sourceOf(current, oldSource), dest, storageClass)
	}
	if shouldSendToRemote(current) {
		glog.V(0).Infof("leaving %s to the rewrite that superseded the rename", remote_storage.FormatLocation(dest))
		return nil
	}
	glog.V(0).Infof("uploading the current %s in place of the superseded rename", remote_storage.FormatLocation(dest))
	remoteEntry, writeErr := retriedWriteFile(client, filerSource, dir, remoteWriteEntry(current, storageClass), dest)
	if errors.Is(writeErr, errSuperseded) {
		glog.Errorf("skipping %s: %v", remote_storage.FormatLocation(dest), writeErr)
		return nil
	}
	if writeErr != nil {
		return writeErr
	}
	return updateLocalEntry(filerClient, dir, current, remoteEntry)
}

// recoverSupersededCreate finishes a create reshaped from a rename into the
// mount when the filer has moved past the entry the event described. A plain
// superseded create skips because the superseding event follows in the log; a
// reshaped one has no later event for this destination. remote.uncache accepts
// the rename's inherited stamp and drops the chunks, and the update it logs
// skips the upload through isDroppedLocalCopy, so the content would exist
// nowhere. What the filer holds now decides: local content uploads here only
// when the superseding event will not (the stamp-edge case uploadCurrentEntry
// covers), and remote-only content copies from the object its stamp
// describes, the way completeRemoteOnlyRename finishes a rename.
func recoverSupersededCreate(
	filerClient filer_pb.FilerClient,
	filerSource filer_pb.FilerClient,
	client remote_storage.RemoteStorageClient,
	mountedDir string,
	remoteStorageMountLocation *remote_pb.RemoteStorageLocation,
	dir, name string,
	renameSrc util.FullPath,
	dest *remote_pb.RemoteStorageLocation,
	storageClass string,
) error {
	current, err := currentEntry(filerSource, dir, name)
	if err != nil {
		return err
	}
	if current == nil || current.IsDirectory {
		return nil
	}
	if isRemoteOnly(current) {
		return copyRenameInSource(filerClient, client, mountedDir, remoteStorageMountLocation, dir, current, renameSrc, dest, storageClass)
	}
	if shouldSendToRemote(current) {
		glog.V(0).Infof("leaving %s to the event that superseded the rename", remote_storage.FormatLocation(dest))
		return nil
	}
	glog.V(0).Infof("uploading the current %s in place of the superseded rename", remote_storage.FormatLocation(dest))
	remoteEntry, writeErr := retriedWriteFile(client, filerSource, dir, remoteWriteEntry(current, storageClass), dest)
	if errors.Is(writeErr, errSuperseded) {
		glog.Errorf("skipping %s: %v", remote_storage.FormatLocation(dest), writeErr)
		return nil
	}
	if writeErr != nil {
		return writeErr
	}
	return updateLocalEntry(filerClient, dir, current, remoteEntry)
}

// copyRenameInSource gives the destination key the content of a remote-only
// entry a rename carried into the mount. The object sits at the key the
// renamed-from path maps to on its own mount's remote, or where a
// pending-rename marker under this mount points. The source object is left
// alone: the source side removes it through its own delete event. When the
// destination already holds what the stamp describes (an earlier run copied
// it), only the stamp is refreshed. With no object on either side the event
// fails and holds the offset.
func copyRenameInSource(
	filerClient filer_pb.FilerClient,
	client remote_storage.RemoteStorageClient,
	mountedDir string,
	remoteStorageMountLocation *remote_pb.RemoteStorageLocation,
	dir string,
	current *filer_pb.Entry,
	renameSrc util.FullPath,
	dest *remote_pb.RemoteStorageLocation,
	storageClass string,
) error {
	srcClient, srcLoc, err := renameInSource(filerClient, client, mountedDir, remoteStorageMountLocation, current, renameSrc)
	if err != nil {
		return err
	}
	if existing, err := client.StatFile(dest); err == nil {
		if remote_storage.Describes(current.RemoteEntry, existing) {
			glog.V(0).Infof("%s already holds the renamed content", remote_storage.FormatLocation(dest))
			return updateLocalEntry(filerClient, dir, current, existing)
		}
		glog.V(0).Infof("%s holds an object the entry does not describe (size %d, want %d); replacing it from %s", remote_storage.FormatLocation(dest), existing.RemoteSize, current.RemoteEntry.RemoteSize, remote_storage.FormatLocation(srcLoc))
	} else if !errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
		return err
	}
	stat, err := srcClient.StatFile(srcLoc)
	if errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
		return fmt.Errorf("%s: content is on neither %s nor %s", util.NewFullPath(dir, current.Name), remote_storage.FormatLocation(srcLoc), remote_storage.FormatLocation(dest))
	}
	if err != nil {
		return err
	}
	reader, err := openRemoteObject(srcClient, srcLoc, stat.RemoteSize)
	if err != nil {
		return err
	}
	defer reader.Close()
	glog.V(0).Infof("copy %s -> %s", remote_storage.FormatLocation(srcLoc), remote_storage.FormatLocation(dest))
	remoteEntry, err := client.WriteFile(dest, remoteWriteEntry(current, storageClass), reader)
	if err != nil {
		return err
	}
	return updateLocalEntry(filerClient, dir, current, remoteEntry)
}

// renameInSource locates the object holding a remote-only rename-in entry's
// content and the client that serves it. A pending-rename marker under this
// mount names a key on this remote; otherwise the renamed-from path is
// resolved through the filer's mount mappings to its own mount's remote
// location, served by this client when the source shares this storage.
func renameInSource(
	filerClient filer_pb.FilerClient,
	client remote_storage.RemoteStorageClient,
	mountedDir string,
	remoteStorageMountLocation *remote_pb.RemoteStorageLocation,
	current *filer_pb.Entry,
	renameSrc util.FullPath,
) (remote_storage.RemoteStorageClient, *remote_pb.RemoteStorageLocation, error) {
	if src := util.FullPath(current.Extended[filer.ExtRemoteRenameSourceKey]); strings.HasPrefix(string(src), strings.TrimSuffix(mountedDir, "/")+"/") {
		return client, toRemoteStorageLocation(util.FullPath(mountedDir), src, remoteStorageMountLocation), nil
	}
	mappings, err := readMountMappings(filerClient)
	if err != nil {
		return nil, nil, err
	}
	srcMount, srcRemoteLoc, err := filer.FindMountedRemoteMapping(mappings, string(renameSrc))
	if err != nil {
		return nil, nil, fmt.Errorf("rename source %s: %w", renameSrc, err)
	}
	srcLoc := filer.MapFullPathToRemoteStorageLocation(util.FullPath(srcMount), srcRemoteLoc, renameSrc)
	if srcRemoteLoc.Name == remoteStorageMountLocation.Name {
		return client, srcLoc, nil
	}
	conf, err := readRemoteStorageConf(filerClient, srcRemoteLoc.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("rename source %s: %w", renameSrc, err)
	}
	srcClient, err := remote_storage.GetRemoteStorage(conf)
	if err != nil {
		return nil, nil, fmt.Errorf("rename source %s: %w", renameSrc, err)
	}
	return srcClient, srcLoc, nil
}

// readMountMappings reads the filer's remote mount table, the file
// DetectMountInfo reads at startup, through the event's filer client.
func readMountMappings(filerClient filer_pb.FilerClient) (mappings *remote_pb.RemoteStorageMapping, err error) {
	var content []byte
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var readErr error
		content, readErr = filer.ReadInsideFiler(context.Background(), client, filer.DirectoryEtcRemote, filer.REMOTE_STORAGE_MOUNT_FILE)
		return readErr
	})
	if err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return nil, err
	}
	return filer.UnmarshalRemoteStorageMappings(content)
}

// readRemoteStorageConf reads a remote storage's configuration entry.
func readRemoteStorageConf(filerClient filer_pb.FilerClient, storageName string) (*remote_pb.RemoteConf, error) {
	var content []byte
	if err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var readErr error
		content, readErr = filer.ReadInsideFiler(context.Background(), client, filer.DirectoryEtcRemote, storageName+filer.REMOTE_STORAGE_CONF_SUFFIX)
		return readErr
	}); err != nil {
		return nil, err
	}
	conf := &remote_pb.RemoteConf{}
	if err := proto.Unmarshal(content, conf); err != nil {
		return nil, fmt.Errorf("unmarshal %s/%s: %w", filer.DirectoryEtcRemote, storageName+filer.REMOTE_STORAGE_CONF_SUFFIX, err)
	}
	return conf, nil
}

// isSuperseded reports whether the filer has moved past the entry an event
// described: it is deleted, it no longer references every chunk the event
// named, or it references chunks the event did not. Dropped chunks are the ones
// the filer deletes when an entry is updated, so they are gone from the volume
// servers, or about to be, and no retry of the upload can succeed. Added chunks
// mean the event was a snapshot of a write still in progress. A mount flushes
// metadata while it is still uploading chunks in parallel, so such a snapshot
// can have holes, and the stream reader stops at the first one: every retry
// sends a short body and fails the same way. Chunks are compared by file id,
// not content: a rewrite stores even identical bytes under new ids and drops
// the old ones. A replay from an earlier offset (-timeAgo) re-emits such
// events. Failing one holds the sync offset before it, so every restart of the
// subscription replays it into the same failure, and progress on everything
// after it in the log is never persisted. Skipping is safe: the event that
// superseded this one follows in the log, and the delete removes the remote
// object or the later write uploads the current content. A lookup that fails
// for any other reason keeps the write failure, so the event is retried.
func isSuperseded(filerClient filer_pb.FilerClient, dir string, entry *filer_pb.Entry) bool {
	current, _, _, err := filer_pb.GetEntry(context.Background(), filerClient, util.NewFullPath(dir, entry.Name))
	if errors.Is(err, filer_pb.ErrNotFound) {
		return true
	}
	if err != nil {
		return false
	}
	if !filer.HasData(entry) && filer.HasData(current) {
		// The event described an entry without data (remote-only, or empty);
		// the filer has written to it since. Uploading the snapshot would put
		// an empty object where the write belongs.
		return true
	}
	if len(entry.Content) > 0 || len(current.Content) > 0 {
		return !bytes.Equal(entry.Content, current.Content)
	}
	return len(filer.DoMinusChunks(entry.GetChunks(), current.GetChunks())) > 0 ||
		len(filer.DoMinusChunks(current.GetChunks(), entry.GetChunks())) > 0
}

// remoteUploadReader streams the entry's full declared size through
// ChunkReadAt, which zero-fills any range no chunk view covers: a sparse
// file, a mid-write snapshot flushed while its chunk uploads were still
// running, and a chunkless entry all upload the bytes a read of the entry
// returns. The chunk stream reader stops at the first gap instead, handing
// the backend a short body on every retry — a hole that never fills wedges
// the event, and a backend that does not verify the length publishes a
// truncated object.
func remoteUploadReader(filerClient filer_pb.FilerClient, entry *filer_pb.Entry) (reader io.Reader, closeFn func() error, err error) {
	if len(entry.Content) > 0 {
		return bytes.NewReader(entry.Content), nil, nil
	}
	size := int64(filer.FileSize(entry))
	lookupFileIdFn := filer.LookupFn(filerClient)
	chunkViews, err := filer.ViewFromChunks(context.Background(), lookupFileIdFn, entry.GetChunks(), 0, math.MaxInt64)
	if err != nil {
		return nil, nil, err
	}
	readerAt := filer.NewChunkReaderAtFromClient(context.Background(),
		filer.NewReaderCache(8, chunk_cache.NewChunkCacheInMemory(8), lookupFileIdFn, nil),
		chunkViews, size, filer.DefaultPrefetchCount)
	return io.NewSectionReader(readerAt, 0, size), readerAt.Close, nil
}

// errSuperseded marks an upload that cannot produce the entry an event
// described: the filer moved past it (isSuperseded). The caller skips the
// event instead of failing it; the event carrying the entry's current chunks
// follows in the log and uploads them.
var errSuperseded = errors.New("superseded or incomplete since the event was logged")

// retriedWriteFile uploads the entry, retrying transient failures. The filer is
// asked whether the entry is superseded before the first attempt and after
// every failed one, and the upload stops at once when it is. Before: a backlog
// (a restart resuming from an old offset) carries every intermediate version
// of a hot file, and uploading each one in turn is wasted bandwidth that can
// trip the remote's per-object mutation rate limit; only the version the filer
// still holds is worth sending. After: a dead chunk reads as a transient
// "RequestError" from the SDK, and waiting out the backoff on it buys nothing.
func retriedWriteFile(client remote_storage.RemoteStorageClient, filerSource filer_pb.FilerClient, dir string, newEntry *filer_pb.Entry, dest *remote_pb.RemoteStorageLocation) (remoteEntry *filer_pb.RemoteEntry, err error) {
	if isSuperseded(filerSource, dir, newEntry) {
		return nil, fmt.Errorf("%s %w", util.NewFullPath(dir, newEntry.Name), errSuperseded)
	}
	err = util.RetryOnError("writeFile", func(err error) bool {
		return !errors.Is(err, errSuperseded) && util.IsTransientError(err)
	}, func() error {
		reader, closeFn, readerErr := remoteUploadReader(filerSource, newEntry)
		if readerErr != nil {
			if isSuperseded(filerSource, dir, newEntry) {
				return fmt.Errorf("%s %w: %w", util.NewFullPath(dir, newEntry.Name), errSuperseded, readerErr)
			}
			return readerErr
		}
		if closeFn != nil {
			defer closeFn()
		}
		glog.V(0).Infof("create %s", remote_storage.FormatLocation(dest))
		var writeErr error
		remoteEntry, writeErr = client.WriteFile(dest, newEntry, reader)
		if writeErr != nil && isSuperseded(filerSource, dir, newEntry) {
			return fmt.Errorf("%s %w: %w", util.NewFullPath(dir, newEntry.Name), errSuperseded, writeErr)
		}
		return writeErr
	})
	if err != nil && !errors.Is(err, errSuperseded) {
		glog.Errorf("write to %s: %v", dest, err)
	}
	return
}

func collectLastSyncOffset(filerClient filer_pb.FilerClient, grpcDialOption grpc.DialOption, filerAddress pb.ServerAddress, mountedDir string, timeAgo time.Duration) time.Time {
	// 1. specified by timeAgo
	// 2. last offset timestamp for this directory
	// 3. directory creation time
	var lastOffsetTs time.Time
	if timeAgo == 0 {
		mountedDirEntry, _, _, err := filer_pb.GetEntry(context.Background(), filerClient, util.FullPath(mountedDir))
		if err != nil {
			glog.V(0).Infof("get mounted directory %s: %v", mountedDir, err)
			return time.Now()
		}

		lastOffsetTsNs, err := remote_storage.GetSyncOffset(grpcDialOption, filerAddress, mountedDir)
		if mountedDirEntry != nil {
			if err == nil && mountedDirEntry.Attributes.Crtime < lastOffsetTsNs/1000000 {
				lastOffsetTs = time.Unix(0, lastOffsetTsNs)
				glog.V(0).Infof("resume from %v", lastOffsetTs)
			} else {
				lastOffsetTs = time.Unix(mountedDirEntry.Attributes.Crtime, 0)
			}
		} else {
			lastOffsetTs = time.Now()
		}
	} else {
		lastOffsetTs = time.Now().Add(-timeAgo)
		if lastOffsetTsNs, err := remote_storage.GetSyncOffset(grpcDialOption, filerAddress, mountedDir); err == nil && lastOffsetTsNs > 0 {
			if savedOffsetTs := time.Unix(0, lastOffsetTsNs); savedOffsetTs.Before(lastOffsetTs) {
				lastOffsetTs = savedOffsetTs
			}
		}
	}
	return lastOffsetTs
}

func toRemoteStorageLocation(mountDir, sourcePath util.FullPath, remoteMountLocation *remote_pb.RemoteStorageLocation) *remote_pb.RemoteStorageLocation {
	source := string(sourcePath[len(mountDir):])
	dest := util.FullPath(remoteMountLocation.Path).Child(source)
	return &remote_pb.RemoteStorageLocation{
		Name:   remoteMountLocation.Name,
		Bucket: remoteMountLocation.Bucket,
		Path:   string(dest),
	}
}

// isMetadataOnlyUpdate reports whether an update leaves the entry at the same
// path with the same content, so the remote object needs at most its metadata
// rewritten -- provided it is already there, which liveRemoteEntry establishes.
func isMetadataOnlyUpdate(dir string, message *filer_pb.EventNotification) bool {
	if dir != message.NewParentPath || message.OldEntry.Name != message.NewEntry.Name {
		return false
	}
	return filer.IsSameData(message.OldEntry, message.NewEntry)
}

// liveRemoteEntry returns the RemoteEntry showing the entry's object is on the
// remote, or nil when it never got there. The event's own is not enough: a
// chmod right after a write is logged before the sync has uploaded the write
// and stamped the entry, and treating it as unreplicated would upload twice.
// Returns filer_pb.ErrNotFound when the entry has since been deleted.
func liveRemoteEntry(filerClient filer_pb.FilerClient, dir string, entry *filer_pb.Entry) (*filer_pb.RemoteEntry, error) {
	if entry.RemoteEntry != nil {
		return entry.RemoteEntry, nil
	}
	current, _, _, err := filer_pb.GetEntry(context.Background(), filerClient, util.NewFullPath(dir, entry.Name))
	if err != nil {
		return nil, err
	}
	return current.RemoteEntry, nil
}

func shouldSendToRemote(entry *filer_pb.Entry) bool {
	if entry.RemoteEntry == nil {
		return true
	}
	if entry.RemoteEntry.RemoteMtime != entry.Attributes.Mtime {
		return entry.RemoteEntry.RemoteMtime < entry.Attributes.Mtime
	}
	// Equal seconds cannot tell a rewrite from the version already
	// uploaded; the sync stamp can, the same check remote.uncache uses.
	return entry.RemoteEntry.LastLocalSyncTsNs < entry.Attributes.Mtime*1e9+int64(entry.Attributes.MtimeNs)
}

// remoteWriteEntry returns the entry as remote storage should see it: the
// storage class attribute is dropped, or overridden by -storageClass. The
// event entry is left untouched so updateLocalEntry still compares the entry
// the filer stored.
func remoteWriteEntry(entry *filer_pb.Entry, storageClass string) *filer_pb.Entry {
	clone := proto.Clone(entry).(*filer_pb.Entry)
	if storageClass == "" {
		delete(clone.Extended, s3_constants.AmzStorageClass)
	} else {
		if clone.Extended == nil {
			clone.Extended = map[string][]byte{}
		}
		clone.Extended[s3_constants.AmzStorageClass] = []byte(storageClass)
	}
	return clone
}

// updateLocalEntry stamps the entry an event described with its RemoteEntry.
// The write carries IF_ENTRY_EQUAL over the event's entry: the filer deletes
// every stored chunk absent from an updated entry, so a snapshot older than
// the live entry (the file was rewritten while its upload was in flight, or
// the event is a replay) would delete the live chunks. A failed precondition
// means the filer moved past this event; the event that superseded it follows
// in the log and stamps the current entry, so the stale stamp is skipped the
// same way a superseded upload is.
func updateLocalEntry(filerClient filer_pb.FilerClient, dir string, entry *filer_pb.Entry, remoteEntry *filer_pb.RemoteEntry) error {
	remoteEntry = proto.Clone(remoteEntry).(*filer_pb.RemoteEntry)
	remoteEntry.LastLocalSyncTsNs = time.Now().UnixNano()
	expected := proto.Clone(entry).(*filer_pb.Entry)
	entry.RemoteEntry = remoteEntry
	delete(entry.Extended, filer.ExtRemoteRenameSourceKey)
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     entry,
			Condition: ifEntryEqual(expected),
		})
		return err
	})
	if isFailedPrecondition(err) {
		glog.Errorf("skipping stale stamp of %s: %v", util.NewFullPath(dir, entry.Name), err)
		return nil
	}
	if isEntryGone(err) {
		glog.Errorf("skipping stamp of %s: deleted since the event was logged: %v", util.NewFullPath(dir, entry.Name), err)
		return nil
	}
	return err
}

// isEntryGone reports an UpdateEntry the filer refused because the entry no
// longer exists: a delete that followed the event superseded the stamp, and
// the delete's own event follows in the log. A filer with the typed answer
// returns codes.NotFound; an older filer returns a plain error of the form
// "not found <path>: <cause>", so the cause (the message's tail, never the
// path) is matched against filer_pb.ErrNotFound's text.
func isEntryGone(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
		return true
	}
	return strings.HasSuffix(strings.TrimSpace(err.Error()), filer_pb.ErrNotFound.Error())
}

// ifEntryEqual builds the precondition that the stored entry still equals the
// one the event described: chunk fids, inline content, and metadata alike.
func ifEntryEqual(entry *filer_pb.Entry) *filer_pb.WriteCondition {
	return &filer_pb.WriteCondition{
		Clauses: []*filer_pb.WriteCondition_Clause{{Kind: filer_pb.WriteCondition_IF_ENTRY_EQUAL, ExpectedEntry: entry}},
	}
}

// isFailedPrecondition reports a write condition the filer refused, through
// any wrapping WithFilerClient added.
func isFailedPrecondition(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.FailedPrecondition
}

func isMultipartUploadFile(dir string, name string) bool {
	return isMultipartUploadDir(dir) && strings.HasSuffix(name, ".part")
}

func isMultipartUploadDir(dir string) bool {
	return strings.HasPrefix(dir, "/buckets/") &&
		strings.Contains(dir, "/"+s3_constants.MultipartUploadsFolder+"/")
}

// isSystemPath returns true if the dir and name refer to the internal .system folder
// under /buckets (such as the bucket-owner index at /buckets/.system/owners/...)
// or any entry inside it. These paths are internal to SeaweedFS and must not
// be synced to remote storage.
func isSystemPath(dir string, name string) bool {
	fullPath := string(util.NewFullPath(dir, name))
	trimmed := strings.Trim(fullPath, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) >= 1 && parts[0] == s3_constants.SystemFolder {
		return true
	}
	if len(parts) >= 2 && parts[1] == s3_constants.SystemFolder {
		return true
	}
	return false
}

// isDeleteMarker returns true if the entry is an S3 delete marker
// (a zero-content version entry with ExtDeleteMarkerKey set to "true").
func isDeleteMarker(entry *filer_pb.Entry) bool {
	if entry == nil || entry.Extended == nil {
		return false
	}
	return string(entry.Extended[s3_constants.ExtDeleteMarkerKey]) == "true"
}

// syncDeleteMarker propagates a delete marker to the remote storage and
// persists a local sync marker so that replaying the same event is a no-op.
func syncDeleteMarker(
	client remote_storage.RemoteStorageClient,
	filerClient filer_pb.FilerClient,
	message *filer_pb.EventNotification,
	dest *remote_pb.RemoteStorageLocation,
) error {
	glog.V(0).Infof("delete (marker) %s", remote_storage.FormatLocation(dest))
	if err := deleteRemoteFile(client, dest); err != nil {
		return err
	}
	return updateLocalEntry(filerClient, message.NewParentPath, message.NewEntry, &filer_pb.RemoteEntry{
		StorageName: dest.Name,
		RemoteMtime: message.NewEntry.Attributes.GetMtime(),
	})
}

// isVersionedPath returns true if the dir/name refers to an internal
// versioning path (.versions directory or a version file inside it).
// These paths are SeaweedFS-internal and must not be synced to remote
// storage as-is, because the remote S3 endpoint may apply its own
// versioning, leading to double-versioned paths.
//
// For directories: matches only when the entry name ends with the
// VersionsFolder suffix (e.g. "file.xml.versions").
// For files: matches only when the parent directory ends with
// VersionsFolder and the file name has the "v_" prefix used by
// the internal version file naming convention.
func isVersionedPath(dir string, name string, isDir bool) bool {
	if !strings.HasPrefix(dir, "/buckets/") {
		return false
	}
	if isDir {
		return strings.HasSuffix(name, s3_constants.VersionsFolder)
	}
	return strings.HasSuffix(dir, s3_constants.VersionsFolder) && strings.HasPrefix(name, "v_")
}

// rewriteVersionedSourcePath rewrites an internal versioning path to the
// original S3 object key. When a file is uploaded to a versioned bucket,
// SeaweedFS stores it internally as:
//
//	/buckets/{bucket}/{key}.versions/v_{versionId}
//
// This function strips the ".versions/v_{versionId}" suffix and returns
// the original parent directory and object name, so the remote destination
// points to the logical S3 key rather than the internal version storage path.
//
// Returns (newDir, newName, true) if the path was rewritten, or
// (dir, name, false) if the path is not a versioned path.
func rewriteVersionedSourcePath(dir string, name string) (string, string, bool) {
	if !strings.HasPrefix(dir, "/buckets/") {
		return dir, name, false
	}
	if !strings.HasSuffix(dir, s3_constants.VersionsFolder) {
		return dir, name, false
	}
	if !strings.HasPrefix(name, "v_") {
		return dir, name, false
	}
	// dir  = "/buckets/bucket/path/to/file.xml.versions"
	// name = "v_abc123"
	// Original object: dir without ".versions" suffix → "/buckets/bucket/path/to/file.xml"
	originalObjectPath := dir[:len(dir)-len(s3_constants.VersionsFolder)]
	lastSlash := strings.LastIndex(originalObjectPath, "/")
	if lastSlash < 0 {
		return dir, name, false
	}
	newDir := originalObjectPath[:lastSlash]
	if lastSlash == 0 {
		newDir = "/"
	}
	return newDir, originalObjectPath[lastSlash+1:], true
}
