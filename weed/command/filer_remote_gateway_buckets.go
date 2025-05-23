package command

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
	"math"
	"math/rand"
	"path/filepath"
	"strings"
	"time"
)

func (option *RemoteGatewayOptions) followBucketUpdatesAndUploadToRemote(filerSource *source.FilerSource) error {

	// read filer remote storage mount mappings
	if detectErr := option.collectRemoteStorageConf(); detectErr != nil {
		return fmt.Errorf("read mount info: %v", detectErr)
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
		return remote_storage.SetSyncOffset(option.grpcDialOption, pb.ServerAddress(*option.filerAddress), option.bucketsDir, offsetTsNs)
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
	}

	return pb.FollowMetadata(pb.ServerAddress(*option.filerAddress), option.grpcDialOption, metadataFollowOption, processEventFnWithOffset)
}

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
		if message.NewEntry != nil {
			// update
			if message.NewEntry.Name == filer.REMOTE_STORAGE_MOUNT_FILE {
				newMappings, readErr := filer.UnmarshalRemoteStorageMappings(message.NewEntry.Content)
				if readErr != nil {
					return fmt.Errorf("unmarshal mappings: %v", readErr)
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
		} else if message.OldEntry != nil {
			// deletion
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
		if strings.HasPrefix(resp.Directory, filer.DirectoryEtcRemote) {
			return handleEtcRemoteChanges(resp)
		}

		if filer_pb.IsEmpty(resp) {
			return nil
		}
		if filer_pb.IsCreate(resp) {
			if message.NewParentPath == option.bucketsDir {
				return handleCreateBucket(message.NewEntry)
			}
			if isMultipartUploadFile(message.NewParentPath, message.NewEntry.Name) {
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
			glog.V(2).Infof("create: %+v", resp)
			if !shouldSendToRemote(message.NewEntry) {
				glog.V(2).Infof("skipping creating: %+v", resp)
				return nil
			}
			dest := toRemoteStorageLocation(bucket, util.NewFullPath(message.NewParentPath, message.NewEntry.Name), remoteStorageMountLocation)
			if message.NewEntry.IsDirectory {
				glog.V(0).Infof("mkdir  %s", remote_storage.FormatLocation(dest))
				return client.WriteDirectory(dest, message.NewEntry)
			}
			glog.V(0).Infof("create %s", remote_storage.FormatLocation(dest))
			remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.NewEntry, dest)
			if writeErr != nil {
				return writeErr
			}
			return updateLocalEntry(option, message.NewParentPath, message.NewEntry, remoteEntry)
		}
		if filer_pb.IsDelete(resp) {
			if resp.Directory == option.bucketsDir {
				return handleDeleteBucket(message.OldEntry)
			}
			bucket, remoteStorageMountLocation, remoteStorage, ok := option.detectBucketInfo(resp.Directory)
			if !ok {
				return nil
			}
			client, err := remote_storage.GetRemoteStorage(remoteStorage)
			if err != nil {
				return err
			}
			glog.V(2).Infof("delete: %+v", resp)
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
			oldBucket, oldRemoteStorageMountLocation, oldRemoteStorage, oldOk := option.detectBucketInfo(resp.Directory)
			newBucket, newRemoteStorageMountLocation, newRemoteStorage, newOk := option.detectBucketInfo(message.NewParentPath)
			if oldOk && newOk {
				if !shouldSendToRemote(message.NewEntry) {
					glog.V(2).Infof("skipping updating: %+v", resp)
					return nil
				}
				client, err := remote_storage.GetRemoteStorage(oldRemoteStorage)
				if err != nil {
					return err
				}
				if resp.Directory == message.NewParentPath && message.OldEntry.Name == message.NewEntry.Name {
					// update the same entry
					if message.NewEntry.IsDirectory {
						// update directory property
						return nil
					}
					if message.OldEntry.RemoteEntry != nil && filer.IsSameData(message.OldEntry, message.NewEntry) {
						glog.V(2).Infof("update meta: %+v", resp)
						oldDest := toRemoteStorageLocation(oldBucket, util.NewFullPath(resp.Directory, message.OldEntry.Name), oldRemoteStorageMountLocation)
						return client.UpdateFileMetadata(oldDest, message.OldEntry, message.NewEntry)
					} else {
						newDest := toRemoteStorageLocation(newBucket, util.NewFullPath(message.NewParentPath, message.NewEntry.Name), newRemoteStorageMountLocation)
						remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.NewEntry, newDest)
						if writeErr != nil {
							return writeErr
						}
						return updateLocalEntry(option, message.NewParentPath, message.NewEntry, remoteEntry)
					}
				}
			}

			// the following is entry rename
			if oldOk {
				client, err := remote_storage.GetRemoteStorage(oldRemoteStorage)
				if err != nil {
					return err
				}
				oldDest := toRemoteStorageLocation(oldBucket, util.NewFullPath(resp.Directory, message.OldEntry.Name), oldRemoteStorageMountLocation)
				if message.OldEntry.IsDirectory {
					return client.RemoveDirectory(oldDest)
				}
				glog.V(0).Infof("delete %s", remote_storage.FormatLocation(oldDest))
				if err := client.DeleteFile(oldDest); err != nil {
					return err
				}
			}
			if newOk {
				if !shouldSendToRemote(message.NewEntry) {
					glog.V(2).Infof("skipping updating: %+v", resp)
					return nil
				}
				client, err := remote_storage.GetRemoteStorage(newRemoteStorage)
				if err != nil {
					return err
				}
				newDest := toRemoteStorageLocation(newBucket, util.NewFullPath(message.NewParentPath, message.NewEntry.Name), newRemoteStorageMountLocation)
				if message.NewEntry.IsDirectory {
					return client.WriteDirectory(newDest, message.NewEntry)
				}
				remoteEntry, writeErr := retriedWriteFile(client, filerSource, message.NewEntry, newDest)
				if writeErr != nil {
					return writeErr
				}
				return updateLocalEntry(option, message.NewParentPath, message.NewEntry, remoteEntry)
			}
		}

		return nil
	}
	return eachEntryFunc, nil
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
	if !strings.HasPrefix(dir, bucketsDir+"/") {
		return "", false
	}
	parts := strings.SplitN(dir[len(bucketsDir)+1:], "/", 2)
	return util.FullPath(bucketsDir).Child(parts[0]), true
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
