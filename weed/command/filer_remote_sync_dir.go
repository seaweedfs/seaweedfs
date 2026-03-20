package command

import (
	"context"
	"fmt"
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
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

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
		if resp.EventNotification.NewEntry != nil {
			if *option.storageClass == "" {
				delete(resp.EventNotification.NewEntry.Extended, s3_constants.AmzStorageClass)
			} else {
				resp.EventNotification.NewEntry.Extended[s3_constants.AmzStorageClass] = []byte(*option.storageClass)
			}
		}

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
		return remote_storage.SetSyncOffset(option.grpcDialOption, pb.ServerAddress(*option.filerAddress), mountedDir, offsetTsNs)
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
		if message.NewEntry == nil {
			return nil
		}
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

		return nil
	}

	eachEntryFunc := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		if strings.HasPrefix(resp.Directory, filer.DirectoryEtcRemote) {
			return handleEtcRemoteChanges(resp)
		}

		if filer_pb.IsEmpty(resp) {
			return nil
		}
		if filer_pb.IsCreate(resp) {
			if isMultipartUploadFile(message.NewParentPath, message.NewEntry.Name) {
				return nil
			}
			// Propagate delete markers as deletions on the remote.
			// Delete markers are zero-content version entries, so they
			// would be filtered out by the HasData check below.
			if isDeleteMarker(message.NewEntry) {
				if newParent, newName, ok := rewriteVersionedSourcePath(message.NewParentPath, message.NewEntry.Name); ok {
					dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(newParent, newName), remoteStorageMountLocation)
					return syncDeleteMarker(client, option, message, dest)
				}
				return nil
			}
			if !filer.HasData(message.NewEntry) {
				return nil
			}
			glog.V(2).Infof("create: %+v", resp)
			if !shouldSendToRemote(message.NewEntry) {
				glog.V(2).Infof("skipping creating: %+v", resp)
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
				return client.WriteDirectory(dest, message.NewEntry)
			}
			glog.V(0).Infof("create %s", remote_storage.FormatLocation(dest))
			remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.NewEntry, dest)
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
			return updateLocalEntry(option, message.NewParentPath, message.NewEntry, remoteEntry)
		}
		if filer_pb.IsDelete(resp) {
			// Skip deletion of internal version files; individual version
			// deletes should not propagate to the remote object
			if isVersionedPath(resp.Directory, message.OldEntry.Name, message.OldEntry.IsDirectory) {
				glog.V(2).Infof("skipping delete of internal version path: %s/%s", resp.Directory, message.OldEntry.Name)
				return nil
			}
			glog.V(2).Infof("delete: %+v", resp)
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(resp.Directory, message.OldEntry.Name), remoteStorageMountLocation)
			if message.OldEntry.IsDirectory {
				glog.V(0).Infof("rmdir  %s", remote_storage.FormatLocation(dest))
				return client.RemoveDirectory(dest)
			}
			glog.V(0).Infof("delete %s", remote_storage.FormatLocation(dest))
			return client.DeleteFile(dest)
		}
		if message.OldEntry != nil && message.NewEntry != nil {
			if isMultipartUploadFile(message.NewParentPath, message.NewEntry.Name) {
				return nil
			}
			// Skip updates to internal version paths
			if isVersionedPath(message.NewParentPath, message.NewEntry.Name, message.NewEntry.IsDirectory) {
				glog.V(2).Infof("skipping update of internal version path: %s/%s", message.NewParentPath, message.NewEntry.Name)
				return nil
			}
			oldDest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(resp.Directory, message.OldEntry.Name), remoteStorageMountLocation)
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(message.NewParentPath, message.NewEntry.Name), remoteStorageMountLocation)
			if !shouldSendToRemote(message.NewEntry) {
				glog.V(2).Infof("skipping updating: %+v", resp)
				return nil
			}
			if message.NewEntry.IsDirectory {
				return client.WriteDirectory(dest, message.NewEntry)
			}
			if resp.Directory == message.NewParentPath && message.OldEntry.Name == message.NewEntry.Name {
				if filer.IsSameData(message.OldEntry, message.NewEntry) {
					glog.V(2).Infof("update meta: %+v", resp)
					return client.UpdateFileMetadata(dest, message.OldEntry, message.NewEntry)
				}
			}
			glog.V(2).Infof("update: %+v", resp)
			glog.V(0).Infof("delete %s", remote_storage.FormatLocation(oldDest))
			if err := client.DeleteFile(oldDest); err != nil {
				if isMultipartUploadFile(resp.Directory, message.OldEntry.Name) {
					return nil
				}
			}
			remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.NewEntry, dest)
			if writeErr != nil {
				return writeErr
			}
			return updateLocalEntry(option, message.NewParentPath, message.NewEntry, remoteEntry)
		}

		return nil
	}
	return eachEntryFunc, nil
}

func retriedWriteFile(client remote_storage.RemoteStorageClient, filerSource *source.FilerSource, newEntry *filer_pb.Entry, dest *remote_pb.RemoteStorageLocation) (remoteEntry *filer_pb.RemoteEntry, err error) {
	var writeErr error
	err = util.Retry("writeFile", func() error {
		reader := filer.NewFileReader(filerSource, newEntry)
		glog.V(0).Infof("create %s", remote_storage.FormatLocation(dest))
		remoteEntry, writeErr = client.WriteFile(dest, newEntry, reader)
		if writeErr != nil {
			return writeErr
		}
		return nil
	})
	if err != nil {
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
		mountedDirEntry, err := filer_pb.GetEntry(context.Background(), filerClient, util.FullPath(mountedDir))
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

func shouldSendToRemote(entry *filer_pb.Entry) bool {
	if entry.RemoteEntry == nil {
		return true
	}
	if entry.RemoteEntry.RemoteMtime < entry.Attributes.Mtime {
		return true
	}
	return false
}

func updateLocalEntry(filerClient filer_pb.FilerClient, dir string, entry *filer_pb.Entry, remoteEntry *filer_pb.RemoteEntry) error {
	remoteEntry.LastLocalSyncTsNs = time.Now().UnixNano()
	entry.RemoteEntry = remoteEntry
	return filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     entry,
		})
		return err
	})
}

func isMultipartUploadFile(dir string, name string) bool {
	return isMultipartUploadDir(dir) && strings.HasSuffix(name, ".part")
}

func isMultipartUploadDir(dir string) bool {
	return strings.HasPrefix(dir, "/buckets/") &&
		strings.Contains(dir, "/"+s3_constants.MultipartUploadsFolder+"/")
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
	if err := client.DeleteFile(dest); err != nil {
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
