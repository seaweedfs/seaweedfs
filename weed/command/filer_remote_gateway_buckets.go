package command

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

func (option *RemoteGatewayOptions) followBucketUpdatesAndUploadToRemote(filerSource *source.FilerSource) error {

	// read filer remote storage mount mappings
	if detectErr := option.collectRemoteStorageConf(); detectErr != nil {
		return fmt.Errorf("read mount info: %w", detectErr)
	}

	eachEntryFunc, err := option.makeBucketedEventProcessor(filerSource)
	if err != nil {
		return err
	}

	lastOffsetTs := collectLastSyncOffset(option, option.grpcDialOption, pb.ServerAddress(*option.filerAddress), option.bucketsDir, *option.timeAgo)
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
		now := time.Now().UnixNano()
		glog.V(0).Infof("remote sync %s progressed to %v %0.2f/sec", *option.filerAddress, time.Unix(0, offsetTsNs), float64(counter)/(float64(now-lastLogTsNs)/1e9))
		lastLogTsNs = now
		return remote_storage.SetSyncOffset(option.grpcDialOption, pb.ServerAddress(*option.filerAddress), option.bucketsDir, resumeTsNsFor(processor))
	})

	option.clientEpoch++

	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             "filer.remote.sync",
		ClientId:               option.clientId,
		ClientEpoch:            option.clientEpoch,
		SelfSignature:          0,
		PathPrefix:             option.bucketsDir + "/",
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

// makeBucketedEventProcessor is the gateway half of the sync protocol
// documented atop filer_remote_sync_dir.go: the same source-of-truth rules
// applied per bucket, with each entry's object resolved through its own
// bucket's mount. Guards added to processUpdateEvent belong here too.
func (option *RemoteGatewayOptions) makeBucketedEventProcessor(filerSource *source.FilerSource) (pb.ProcessMetadataFunc, error) {

	handleCreateBucket := func(entry *filer_pb.Entry) error {
		if !entry.IsDirectory {
			return nil
		}
		if entry.RemoteEntry != nil {
			// this directory is imported from "remote.mount.buckets" or "remote.mount"
			return nil
		}
		if option.mappings.PrimaryBucketStorageName != "" && *option.createBucketAt == "" {
			*option.createBucketAt = option.mappings.PrimaryBucketStorageName
			glog.V(0).Infof("%s is set as the primary remote storage", *option.createBucketAt)
		}
		if len(option.mappings.Mappings) == 1 && *option.createBucketAt == "" {
			for k := range option.mappings.Mappings {
				*option.createBucketAt = k
				glog.V(0).Infof("%s is set as the only remote storage", *option.createBucketAt)
			}
		}
		if *option.createBucketAt == "" {
			return nil
		}
		remoteConf, found := option.remoteConfs[*option.createBucketAt]
		if !found {
			return fmt.Errorf("un-configured remote storage %s", *option.createBucketAt)
		}

		client, err := remote_storage.GetRemoteStorage(remoteConf)
		if err != nil {
			return err
		}

		bucketName := strings.ToLower(entry.Name)
		if *option.include != "" {
			if ok, _ := filepath.Match(*option.include, entry.Name); !ok {
				return nil
			}
		}
		if *option.exclude != "" {
			if ok, _ := filepath.Match(*option.exclude, entry.Name); ok {
				return nil
			}
		}

		bucketPath := util.FullPath(option.bucketsDir).Child(entry.Name)
		remoteLocation, found := option.mappings.Mappings[string(bucketPath)]
		if !found {
			if *option.createBucketRandomSuffix {
				// https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
				if len(bucketName)+5 > 63 {
					bucketName = bucketName[:58]
				}
				bucketName = fmt.Sprintf("%s-%04d", bucketName, rand.Uint32()%10000)
			}
			remoteLocation = &remote_pb.RemoteStorageLocation{
				Name:   *option.createBucketAt,
				Bucket: bucketName,
				Path:   "/",
			}
			// need to add new mapping here before getting updates from metadata tailing
			option.mappings.Mappings[string(bucketPath)] = remoteLocation
		} else {
			bucketName = remoteLocation.Bucket
		}

		glog.V(0).Infof("create bucket %s", bucketName)
		if err := client.CreateBucket(bucketName); err != nil {
			return fmt.Errorf("create bucket %s in %s: %v", bucketName, remoteConf.Name, err)
		}

		return filer.InsertMountMapping(option, string(bucketPath), remoteLocation)

	}
	handleDeleteBucket := func(entry *filer_pb.Entry) error {
		if !entry.IsDirectory {
			return nil
		}

		client, remoteStorageMountLocation, err := option.findRemoteStorageClient(entry.Name)
		if err != nil {
			return fmt.Errorf("findRemoteStorageClient %s: %v", entry.Name, err)
		}

		glog.V(0).Infof("delete remote bucket %s", remoteStorageMountLocation.Bucket)
		if err := client.DeleteBucket(remoteStorageMountLocation.Bucket); err != nil {
			return fmt.Errorf("delete remote bucket %s: %v", remoteStorageMountLocation.Bucket, err)
		}

		bucketPath := util.FullPath(option.bucketsDir).Child(entry.Name)

		return filer.DeleteMountMapping(option, string(bucketPath))
	}

	handleEtcRemoteChanges := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		if metadataEventUpdatesDirectory(resp, filer.DirectoryEtcRemote) {
			// update
			if message.NewEntry.Name == filer.REMOTE_STORAGE_MOUNT_FILE {
				newMappings, readErr := filer.UnmarshalRemoteStorageMappings(message.NewEntry.Content)
				if readErr != nil {
					return fmt.Errorf("unmarshal mappings: %w", readErr)
				}
				option.mappings = newMappings
			}
			if strings.HasSuffix(message.NewEntry.Name, filer.REMOTE_STORAGE_CONF_SUFFIX) {
				conf := &remote_pb.RemoteConf{}
				if err := proto.Unmarshal(message.NewEntry.Content, conf); err != nil {
					return fmt.Errorf("unmarshal %s/%s: %v", filer.DirectoryEtcRemote, message.NewEntry.Name, err)
				}
				option.remoteConfs[conf.Name] = conf
			}
		} else if metadataEventRemovesFromDirectory(resp, filer.DirectoryEtcRemote) {
			// deletion
			if message.OldEntry.Name == filer.REMOTE_STORAGE_MOUNT_FILE {
				option.mappings = &remote_pb.RemoteStorageMapping{}
			}
			if strings.HasSuffix(message.OldEntry.Name, filer.REMOTE_STORAGE_CONF_SUFFIX) {
				conf := &remote_pb.RemoteConf{}
				if err := proto.Unmarshal(message.OldEntry.Content, conf); err != nil {
					return fmt.Errorf("unmarshal %s/%s: %v", filer.DirectoryEtcRemote, message.OldEntry.Name, err)
				}
				delete(option.remoteConfs, conf.Name)
			}
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
		if filer_pb.IsCreate(resp) {
			if message.NewParentPath == option.bucketsDir {
				if isSystemPath(message.NewParentPath, message.NewEntry.Name) {
					return nil
				}
				return handleCreateBucket(message.NewEntry)
			}
			if isMultipartUploadFile(message.NewParentPath, message.NewEntry.Name) {
				return nil
			}
			if isSystemPath(message.NewParentPath, message.NewEntry.Name) {
				return nil
			}
			// Propagate delete markers as deletions on the remote.
			// Delete markers are zero-content version entries, so they
			// would be filtered out by the HasData check below.
			if isDeleteMarker(message.NewEntry) {
				if newParent, newName, ok := rewriteVersionedSourcePath(message.NewParentPath, message.NewEntry.Name); ok {
					bucket, remoteStorageMountLocation, remoteStorage, ok := option.detectBucketInfo(newParent)
					if !ok {
						return nil
					}
					client, err := remote_storage.GetRemoteStorage(remoteStorage)
					if err != nil {
						return err
					}
					dest := toRemoteStorageLocation(bucket, util.NewFullPath(newParent, newName), remoteStorageMountLocation)
					return syncDeleteMarker(client, option, message, dest)
				}
				return nil
			}
			if !filer.HasData(message.NewEntry) {
				return nil
			}
			bucket, remoteStorageMountLocation, remoteStorage, ok := option.detectBucketInfo(message.NewParentPath)
			if !ok {
				return nil
			}
			client, err := remote_storage.GetRemoteStorage(remoteStorage)
			if err != nil {
				return err
			}
			glog.V(2).Infof("create: %s/%s ts_ns=%d", filer_pb.MetadataEventTargetDirectory(resp), message.GetNewEntry().GetName(), resp.TsNs)
			if !shouldSendToRemote(message.NewEntry) {
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
			dest := toRemoteStorageLocation(bucket, util.NewFullPath(parentPath, entryName), remoteStorageMountLocation)
			if message.NewEntry.IsDirectory {
				glog.V(0).Infof("mkdir  %s", remote_storage.FormatLocation(dest))
				return client.WriteDirectory(dest, message.NewEntry)
			}
			glog.V(0).Infof("create %s", remote_storage.FormatLocation(dest))
			remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.NewParentPath, message.NewEntry, dest)
			if errors.Is(writeErr, errSuperseded) {
				glog.Errorf("skipping %s: %v", remote_storage.FormatLocation(dest), writeErr)
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
			return updateLocalEntry(option, message.NewParentPath, message.NewEntry, remoteEntry)
		}
		if filer_pb.IsDelete(resp) {
			if filer.IsMetadataOnlyDelete(message.OldEntry) {
				glog.V(2).Infof("skipping remote delete of metadata-only delete: %s/%s", resp.Directory, message.OldEntry.Name)
				return nil
			}
			if resp.Directory == option.bucketsDir {
				if isSystemPath(resp.Directory, message.OldEntry.Name) {
					return nil
				}
				return handleDeleteBucket(message.OldEntry)
			}
			// Skip deletion of internal version files; individual version
			// deletes should not propagate to the remote object
			if isVersionedPath(resp.Directory, message.OldEntry.Name, message.OldEntry.IsDirectory) {
				glog.V(2).Infof("skipping delete of internal version path: %s/%s", resp.Directory, message.OldEntry.Name)
				return nil
			}
			if isSystemPath(resp.Directory, message.OldEntry.Name) {
				return nil
			}
			bucket, remoteStorageMountLocation, remoteStorage, ok := option.detectBucketInfo(resp.Directory)
			if !ok {
				return nil
			}
			client, err := remote_storage.GetRemoteStorage(remoteStorage)
			if err != nil {
				return err
			}
			glog.V(2).Infof("delete: %s/%s ts_ns=%d", resp.Directory, message.GetOldEntry().GetName(), resp.TsNs)
			dest := toRemoteStorageLocation(bucket, util.NewFullPath(resp.Directory, message.OldEntry.Name), remoteStorageMountLocation)
			if message.OldEntry.IsDirectory {
				glog.V(0).Infof("rmdir  %s", remote_storage.FormatLocation(dest))
				return client.RemoveDirectory(dest)
			}
			glog.V(0).Infof("delete %s", remote_storage.FormatLocation(dest))
			return client.DeleteFile(dest)
		}
		if message.OldEntry != nil && message.NewEntry != nil {
			if resp.Directory == option.bucketsDir {
				if message.NewParentPath == option.bucketsDir {
					if message.OldEntry.Name == message.NewEntry.Name {
						return nil
					}
					if isSystemPath(resp.Directory, message.OldEntry.Name) || isSystemPath(message.NewParentPath, message.NewEntry.Name) {
						return nil
					}
					if err := handleCreateBucket(message.NewEntry); err != nil {
						return err
					}
					if err := handleDeleteBucket(message.OldEntry); err != nil {
						return err
					}
				}
			}
			if isMultipartUploadFile(message.NewParentPath, message.NewEntry.Name) {
				return nil
			}
			// Skip updates to internal version paths
			if isVersionedPath(message.NewParentPath, message.NewEntry.Name, message.NewEntry.IsDirectory) {
				glog.V(2).Infof("skipping update of internal version path: %s/%s", message.NewParentPath, message.NewEntry.Name)
				return nil
			}
			if isSystemPath(message.NewParentPath, message.NewEntry.Name) {
				return nil
			}
			oldBucket, oldRemoteStorageMountLocation, oldRemoteStorage, oldOk := option.detectBucketInfo(resp.Directory)
			newBucket, newRemoteStorageMountLocation, newRemoteStorage, newOk := option.detectBucketInfo(message.NewParentPath)
			if oldOk && newOk {
				client, err := remote_storage.GetRemoteStorage(oldRemoteStorage)
				if err != nil {
					return err
				}
				if resp.Directory == message.NewParentPath && message.OldEntry.Name == message.NewEntry.Name {
					// update the same entry; the sync-stamp check holds only
					// for updates in place: a rename keeps the mtime, so a
					// synced or pending-stamped rename must still run
					if !shouldSendToRemote(message.NewEntry) {
						glog.V(2).Infof("skipping updating: %s/%s ts_ns=%d", filer_pb.MetadataEventTargetDirectory(resp), message.GetNewEntry().GetName(), resp.TsNs)
						return nil
					}
					if message.NewEntry.IsDirectory {
						// update directory property
						return nil
					}
					if message.OldEntry.RemoteEntry != nil && filer.IsSameData(message.OldEntry, message.NewEntry) {
						glog.V(2).Infof("update meta: %s/%s ts_ns=%d", filer_pb.MetadataEventTargetDirectory(resp), message.GetNewEntry().GetName(), resp.TsNs)
						oldDest := toRemoteStorageLocation(oldBucket, util.NewFullPath(resp.Directory, message.OldEntry.Name), oldRemoteStorageMountLocation)
						updated, err := client.UpdateFileMetadata(oldDest, message.OldEntry, message.NewEntry)
						if err != nil {
							return err
						}
						remoteEntry := message.NewEntry.RemoteEntry
						if remoteEntry == nil {
							remoteEntry = message.OldEntry.RemoteEntry
						}
						// the metadata write can rewrite the object (S3 copies
						// it in place for a Content-Encoding change, minting a
						// new ETag): restamp from the object the update
						// produced when the backend reports it
						if updated != nil {
							remoteEntry = updated
						}
						return updateLocalEntry(option, message.NewParentPath, message.NewEntry, remoteEntry)
					}
					if isDroppedLocalCopy(message.NewEntry) {
						// remote.uncache, eviction, or remote.meta.sync dropped
						// the local copy: the object at this key is the entry's
						// content, and uploading the chunkless entry replaces
						// it with zero bytes
						glog.V(2).Infof("skipping update of remote-only entry: %s/%s ts_ns=%d", filer_pb.MetadataEventTargetDirectory(resp), message.GetNewEntry().GetName(), resp.TsNs)
						return nil
					}
					newDest := toRemoteStorageLocation(newBucket, util.NewFullPath(message.NewParentPath, message.NewEntry.Name), newRemoteStorageMountLocation)
					remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.NewParentPath, message.NewEntry, newDest)
					if errors.Is(writeErr, errSuperseded) {
						glog.Errorf("skipping %s: %v", remote_storage.FormatLocation(newDest), writeErr)
						return nil
					}
					if writeErr != nil {
						return writeErr
					}
					return updateLocalEntry(option, message.NewParentPath, message.NewEntry, remoteEntry)
				}
			}

			// the following is entry rename
			var oldDest *remote_pb.RemoteStorageLocation
			if oldOk {
				oldDest = toRemoteStorageLocation(oldBucket, util.NewFullPath(resp.Directory, message.OldEntry.Name), oldRemoteStorageMountLocation)
			}
			if newOk && !message.NewEntry.IsDirectory {
				// A remote-only entry's content is the object its
				// renamed-from path maps to; copying it on the remote
				// completes the rename. Deleting the source and uploading
				// the chunkless entry would lose the only copy.
				current, err := currentEntry(filerSource, message.NewParentPath, message.NewEntry.Name)
				if err != nil {
					return err
				}
				if isRemoteOnly(current) {
					if err := gatewayRemoteOnlyRename(option, option, resp, current); err != nil {
						return err
					}
					// a pending entry the filer copied on this rename
					// leaves the source its marker named
					return releaseGatewayRenamedSource(filerSource, option, message)
				}
				if current == nil && len(message.NewEntry.Extended[filer.ExtRemoteRenameSourceKey]) > 0 {
					// the rename's copy was pending and the entry has moved
					// on: a later rename carries the source in its own
					// marker and finishes or releases it, so this leaves it
					glog.V(0).Infof("keep %s: the pending rename moved on", util.NewFullPath(resp.Directory, message.OldEntry.Name))
					return nil
				}
			}
			if oldOk {
				client, err := remote_storage.GetRemoteStorage(oldRemoteStorage)
				if err != nil {
					return err
				}
				if message.OldEntry.IsDirectory {
					return client.RemoveDirectory(oldDest)
				}
				if !newOk && isRemoteOnly(message.OldEntry) {
					glog.V(0).Infof("keep %s: the renamed entry's only content", remote_storage.FormatLocation(oldDest))
					return nil
				}
				glog.V(0).Infof("delete %s", remote_storage.FormatLocation(oldDest))
				if err := deleteRemoteFileIfUnchanged(client, oldDest, message.OldEntry.GetRemoteEntry()); err != nil {
					return err
				}
			}
			if newOk {
				client, err := remote_storage.GetRemoteStorage(newRemoteStorage)
				if err != nil {
					return err
				}
				newDest := toRemoteStorageLocation(newBucket, util.NewFullPath(message.NewParentPath, message.NewEntry.Name), newRemoteStorageMountLocation)
				if message.NewEntry.IsDirectory {
					return client.WriteDirectory(newDest, message.NewEntry)
				}
				remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.NewParentPath, message.NewEntry, newDest)
				if errors.Is(writeErr, errSuperseded) {
					glog.Errorf("skipping %s: %v", remote_storage.FormatLocation(newDest), writeErr)
					return releaseGatewayRenamedSource(filerSource, option, message)
				}
				if writeErr != nil {
					return writeErr
				}
				if err := updateLocalEntry(option, message.NewParentPath, message.NewEntry, remoteEntry); err != nil {
					return err
				}
				return releaseGatewayRenamedSource(filerSource, option, message)
			}
		}

		return nil
	}
	return eachEntryFunc, nil
}

// gatewayRemoteOnlyRename completes a rename whose content exists only as
// the object the renamed-from path maps to: the chunkless entry has nothing
// to upload. When the destination already holds what the entry's stamp
// describes (an earlier run copied it), only the stamp is refreshed.
// Otherwise the source object is copied over, the entry is stamped with the
// copy so reads stop resolving through the pending-source marker, and the
// source goes only while it is still the object the stamp describes. A source
// outside the gateway's coverage is never touched; the rename then completes
// only out of what the destination already holds. With neither object present
// the content is lost: the event fails and holds the offset for recovery.
func gatewayRemoteOnlyRename(filerClient filer_pb.FilerClient, option *RemoteGatewayOptions, resp *filer_pb.SubscribeMetadataResponse, current *filer_pb.Entry) error {
	message := resp.EventNotification
	srcPath := util.FullPath(current.Extended[filer.ExtRemoteRenameSourceKey])
	if srcPath == "" {
		srcPath = util.NewFullPath(resp.Directory, message.OldEntry.Name)
	}
	newBucket, newMountLocation, newStorage, ok := option.detectBucketInfo(message.NewParentPath)
	if !ok {
		return nil
	}
	client, err := remote_storage.GetRemoteStorage(newStorage)
	if err != nil {
		return err
	}
	dest := toRemoteStorageLocation(newBucket, util.NewFullPath(message.NewParentPath, current.Name), newMountLocation)
	stamp := message.OldEntry.GetRemoteEntry()
	if stamp == nil {
		stamp = proto.Clone(current.RemoteEntry).(*filer_pb.RemoteEntry)
	}

	// a source outside the gateway's coverage stays untouched; the rename
	// may only complete out of what the destination already holds
	srcDir, _ := srcPath.DirAndName()
	var srcClient remote_storage.RemoteStorageClient
	var source renameSource
	if srcBucket, srcMountLocation, srcStorage, ok := option.detectBucketInfo(srcDir); ok {
		if srcClient, err = remote_storage.GetRemoteStorage(srcStorage); err != nil {
			return err
		}
		source = renameSource{path: srcPath, loc: toRemoteStorageLocation(srcBucket, srcPath, srcMountLocation), stamp: stamp}
	}
	if existing, statErr := client.StatFile(dest); statErr == nil {
		if remote_storage.Describes(current.RemoteEntry, existing) {
			glog.V(0).Infof("%s already holds the renamed content", remote_storage.FormatLocation(dest))
			if len(current.Extended[filer.ExtRemoteRenameSourceKey]) > 0 {
				if err := updateLocalEntry(filerClient, message.NewParentPath, current, existing); err != nil {
					return err
				}
			}
			if srcClient == nil {
				return nil
			}
			return deleteRenameSource(filerClient, srcClient, message.NewParentPath, current.Name, source, stamp)
		}
		glog.V(0).Infof("%s holds an object the entry does not describe (size %d, want %d); replacing it from %s", remote_storage.FormatLocation(dest), existing.RemoteSize, current.RemoteEntry.RemoteSize, srcPath)
	} else if !errors.Is(statErr, remote_storage.ErrRemoteObjectNotFound) {
		return statErr
	}
	if srcClient == nil {
		return fmt.Errorf("rename source %s is not under a mounted bucket and %s does not hold the content", srcPath, remote_storage.FormatLocation(dest))
	}
	stat, err := srcClient.StatFile(source.loc)
	if errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
		return fmt.Errorf("%s: content is on neither %s nor %s", util.NewFullPath(message.NewParentPath, current.Name), remote_storage.FormatLocation(source.loc), remote_storage.FormatLocation(dest))
	}
	if err != nil {
		return err
	}
	reader, err := openRemoteObject(srcClient, source.loc, stat.RemoteSize)
	if err != nil {
		return err
	}
	defer reader.Close()
	glog.V(0).Infof("copy %s -> %s", remote_storage.FormatLocation(source.loc), remote_storage.FormatLocation(dest))
	remoteEntry, err := client.WriteFile(dest, current, reader)
	if err != nil {
		return err
	}
	if err := updateLocalEntry(filerClient, message.NewParentPath, current, remoteEntry); err != nil {
		return err
	}
	return deleteRenameSource(filerClient, srcClient, message.NewParentPath, current.Name, source, stamp)
}

// releaseGatewayRenamedSource releases the pending source a rename's old
// entry read, once the renamed entry no longer reads through a marker: the
// gateway-side releaseRenamedSource, with the source resolved through its
// own bucket's mount.
func releaseGatewayRenamedSource(filerSource filer_pb.FilerClient, option *RemoteGatewayOptions, message *filer_pb.EventNotification) error {
	srcPath := util.FullPath(message.OldEntry.GetExtended()[filer.ExtRemoteRenameSourceKey])
	if srcPath == "" {
		return nil
	}
	current, err := currentEntry(filerSource, message.NewParentPath, message.NewEntry.Name)
	if err != nil {
		return err
	}
	if current != nil && len(current.Extended[filer.ExtRemoteRenameSourceKey]) > 0 {
		return nil
	}
	srcDir, srcName := srcPath.DirAndName()
	srcBucket, srcMountLocation, srcStorage, ok := option.detectBucketInfo(srcDir)
	if !ok {
		return nil
	}
	current, err = currentEntry(filerSource, srcDir, srcName)
	if err != nil {
		return err
	}
	if isRemoteOnly(current) {
		glog.V(0).Infof("keep %s: a remote-only entry holds the pending rename's source", srcPath)
		return nil
	}
	srcClient, err := remote_storage.GetRemoteStorage(srcStorage)
	if err != nil {
		return err
	}
	srcLoc := toRemoteStorageLocation(srcBucket, srcPath, srcMountLocation)
	glog.V(0).Infof("delete %s, the source of a pending rename", remote_storage.FormatLocation(srcLoc))
	return deleteRemoteFileIfUnchanged(srcClient, srcLoc, message.OldEntry.GetRemoteEntry())
}

func (option *RemoteGatewayOptions) findRemoteStorageClient(bucketName string) (client remote_storage.RemoteStorageClient, remoteStorageMountLocation *remote_pb.RemoteStorageLocation, err error) {
	bucket := util.FullPath(option.bucketsDir).Child(bucketName)

	var isMounted bool
	remoteStorageMountLocation, isMounted = option.mappings.Mappings[string(bucket)]
	if !isMounted {
		return nil, remoteStorageMountLocation, fmt.Errorf("%s is not mounted", bucket)
	}
	remoteConf, hasClient := option.remoteConfs[remoteStorageMountLocation.Name]
	if !hasClient {
		return nil, remoteStorageMountLocation, fmt.Errorf("%s mounted to un-configured %+v", bucket, remoteStorageMountLocation)
	}

	client, err = remote_storage.GetRemoteStorage(remoteConf)
	if err != nil {
		return nil, remoteStorageMountLocation, err
	}
	return client, remoteStorageMountLocation, nil
}

func (option *RemoteGatewayOptions) detectBucketInfo(actualDir string) (bucket util.FullPath, remoteStorageMountLocation *remote_pb.RemoteStorageLocation, remoteConf *remote_pb.RemoteConf, ok bool) {
	bucket, ok = extractBucketPath(option.bucketsDir, actualDir)
	if !ok {
		return "", nil, nil, false
	}
	var isMounted bool
	remoteStorageMountLocation, isMounted = option.mappings.Mappings[string(bucket)]
	if !isMounted {
		glog.Warningf("%s is not mounted", bucket)
		return "", nil, nil, false
	}
	var hasClient bool
	remoteConf, hasClient = option.remoteConfs[remoteStorageMountLocation.Name]
	if !hasClient {
		glog.Warningf("%s mounted to un-configured %+v", bucket, remoteStorageMountLocation)
		return "", nil, nil, false
	}
	return bucket, remoteStorageMountLocation, remoteConf, true
}

func extractBucketPath(bucketsDir, dir string) (util.FullPath, bool) {
	if bucketPath, ok := util.ExtractBucketPath(bucketsDir, dir, false); ok {
		return util.FullPath(bucketPath), true
	}
	return "", false
}

func (option *RemoteGatewayOptions) collectRemoteStorageConf() (err error) {

	if mappings, err := filer.ReadMountMappings(option.grpcDialOption, pb.ServerAddress(*option.filerAddress)); err != nil {
		if err == filer_pb.ErrNotFound {
			return fmt.Errorf("remote storage is not configured in filer server")
		}
		return err
	} else {
		option.mappings = mappings
	}

	option.remoteConfs = make(map[string]*remote_pb.RemoteConf)
	var lastConfName string
	err = filer_pb.List(context.Background(), option, filer.DirectoryEtcRemote, "", func(entry *filer_pb.Entry, isLast bool) error {
		if !strings.HasSuffix(entry.Name, filer.REMOTE_STORAGE_CONF_SUFFIX) {
			return nil
		}
		conf := &remote_pb.RemoteConf{}
		if err := proto.Unmarshal(entry.Content, conf); err != nil {
			return fmt.Errorf("unmarshal %s/%s: %v", filer.DirectoryEtcRemote, entry.Name, err)
		}
		option.remoteConfs[conf.Name] = conf
		lastConfName = conf.Name
		return nil
	}, "", false, math.MaxUint32)

	if option.mappings.PrimaryBucketStorageName == "" && len(option.remoteConfs) == 1 {
		glog.V(0).Infof("%s is set to the default remote storage", lastConfName)
		option.mappings.PrimaryBucketStorageName = lastConfName
	}

	return
}
