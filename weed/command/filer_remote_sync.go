package command

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/remote_pb"
	"github.com/chrislusf/seaweedfs/weed/remote_storage"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"os"
	"strings"
	"time"
)

type RemoteSyncOptions struct {
	filerAddress       *string
	grpcDialOption     grpc.DialOption
	readChunkFromFiler *bool
	debug              *bool
	timeAgo            *time.Duration
	dir                *string
}

const (
	RemoteSyncKeyPrefix = "remote.sync."
)

var _ = filer_pb.FilerClient(&RemoteSyncOptions{})

func (option *RemoteSyncOptions) WithFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {
	return pb.WithFilerClient(*option.filerAddress, option.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return fn(client)
	})
}
func (option *RemoteSyncOptions) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

var (
	remoteSyncOptions RemoteSyncOptions
)

func init() {
	cmdFilerRemoteSynchronize.Run = runFilerRemoteSynchronize // break init cycle
	remoteSyncOptions.filerAddress = cmdFilerRemoteSynchronize.Flag.String("filer", "localhost:8888", "filer of the SeaweedFS cluster")
	remoteSyncOptions.dir = cmdFilerRemoteSynchronize.Flag.String("dir", "/", "a mounted directory on filer")
	remoteSyncOptions.readChunkFromFiler = cmdFilerRemoteSynchronize.Flag.Bool("filerProxy", false, "read file chunks from filer instead of volume servers")
	remoteSyncOptions.debug = cmdFilerRemoteSynchronize.Flag.Bool("debug", false, "debug mode to print out filer updated remote files")
	remoteSyncOptions.timeAgo = cmdFilerRemoteSynchronize.Flag.Duration("timeAgo", 0, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
}

var cmdFilerRemoteSynchronize = &Command{
	UsageLine: "filer.remote.sync -filer=<filerHost>:<filerPort> -dir=/mount/s3_on_cloud",
	Short:     "resumable continuously write back updates to remote storage if the directory is mounted to the remote storage",
	Long: `resumable continuously write back updates to remote storage if the directory is mounted to the remote storage

	filer.remote.sync listens on filer update events. 
	If any mounted remote file is updated, it will fetch the updated content,
	and write to the remote storage.
`,
}

func runFilerRemoteSynchronize(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	remoteSyncOptions.grpcDialOption = grpcDialOption

	dir := *remoteSyncOptions.dir
	filerAddress := *remoteSyncOptions.filerAddress

	filerSource := &source.FilerSource{}
	filerSource.DoInitialize(
		filerAddress,
		pb.ServerToGrpcAddress(filerAddress),
		"/", // does not matter
		*remoteSyncOptions.readChunkFromFiler,
	)

	fmt.Printf("synchronize %s to remote storage...\n", dir)
	util.RetryForever("filer.remote.sync "+dir, func() error {
		return followUpdatesAndUploadToRemote(&remoteSyncOptions, filerSource, dir)
	}, func(err error) bool {
		if err != nil {
			glog.Errorf("synchronize %s: %v", dir, err)
		}
		return true
	})

	return true
}

func followUpdatesAndUploadToRemote(option *RemoteSyncOptions, filerSource *source.FilerSource, mountedDir string) error {

	// read filer remote storage mount mappings
	_, _, remoteStorageMountLocation, remoteStorage, detectErr := filer.DetectMountInfo(option.grpcDialOption, *option.filerAddress, mountedDir)
	if detectErr != nil {
		return fmt.Errorf("read mount info: %v", detectErr)
	}

	// 1. specified by timeAgo
	// 2. last offset timestamp for this directory
	// 3. directory creation time
	var lastOffsetTs time.Time
	if *option.timeAgo == 0 {
		mountedDirEntry, err := filer_pb.GetEntry(option, util.FullPath(mountedDir))
		if err != nil {
			return fmt.Errorf("lookup %s: %v", mountedDir, err)
		}

		lastOffsetTsNs, err := remote_storage.GetSyncOffset(option.grpcDialOption, *option.filerAddress, mountedDir)
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
		lastOffsetTs = time.Now().Add(-*option.timeAgo)
	}

	client, err := remote_storage.GetRemoteStorage(remoteStorage)
	if err != nil {
		return err
	}

	handleEtcRemoteChanges := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		if message.NewEntry == nil {
			return nil
		}
		if message.NewEntry.Name == filer.REMOTE_STORAGE_MOUNT_FILE {
			mappings, readErr := filer.UnmarshalRemoteStorageMappings(message.NewEntry.Content)
			if readErr != nil {
				return fmt.Errorf("unmarshal mappings: %v", readErr)
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
		if message.NewEntry.Name == remoteStorage.Name + filer.REMOTE_STORAGE_CONF_SUFFIX {
			conf := &remote_pb.RemoteConf{}
			if err := proto.Unmarshal(message.NewEntry.Content, conf); err != nil {
				return fmt.Errorf("unmarshal %s/%s: %v", filer.DirectoryEtcRemote, message.NewEntry.Name, err)
			}
			remoteStorage = conf
			client, err = remote_storage.GetRemoteStorage(remoteStorage)
			return err
		}

		return nil
	}

	eachEntryFunc := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		if strings.HasPrefix(resp.Directory, filer.DirectoryEtcRemote) {
			return handleEtcRemoteChanges(resp)
		}

		if message.OldEntry == nil && message.NewEntry == nil {
			return nil
		}
		if message.OldEntry == nil && message.NewEntry != nil {
			if !filer.HasData(message.NewEntry) {
				return nil
			}
			glog.V(2).Infof("create: %+v", resp)
			if !shouldSendToRemote(message.NewEntry) {
				glog.V(2).Infof("skipping creating: %+v", resp)
				return nil
			}
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(message.NewParentPath, message.NewEntry.Name), remoteStorageMountLocation)
			if message.NewEntry.IsDirectory {
				glog.V(0).Infof("mkdir  %s", remote_storage.FormatLocation(dest))
				return client.WriteDirectory(dest, message.NewEntry)
			}
			glog.V(0).Infof("create %s", remote_storage.FormatLocation(dest))
			reader := filer.NewFileReader(filerSource, message.NewEntry)
			remoteEntry, writeErr := client.WriteFile(dest, message.NewEntry, reader)
			if writeErr != nil {
				return writeErr
			}
			return updateLocalEntry(&remoteSyncOptions, message.NewParentPath, message.NewEntry, remoteEntry)
		}
		if message.OldEntry != nil && message.NewEntry == nil {
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
				return err
			}
			reader := filer.NewFileReader(filerSource, message.NewEntry)
			glog.V(0).Infof("create %s", remote_storage.FormatLocation(dest))
			remoteEntry, writeErr := client.WriteFile(dest, message.NewEntry, reader)
			if writeErr != nil {
				return writeErr
			}
			return updateLocalEntry(&remoteSyncOptions, message.NewParentPath, message.NewEntry, remoteEntry)
		}

		return nil
	}

	processEventFnWithOffset := pb.AddOffsetFunc(eachEntryFunc, 3*time.Second, func(counter int64, lastTsNs int64) error {
		lastTime := time.Unix(0, lastTsNs)
		glog.V(0).Infof("remote sync %s progressed to %v %0.2f/sec", *option.filerAddress, lastTime, float64(counter)/float64(3))
		return remote_storage.SetSyncOffset(option.grpcDialOption, *option.filerAddress, mountedDir, lastTsNs)
	})

	return pb.FollowMetadata(*option.filerAddress, option.grpcDialOption, "filer.remote.sync",
		mountedDir, []string{filer.DirectoryEtcRemote}, lastOffsetTs.UnixNano(), 0, processEventFnWithOffset, false)
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
	if entry.RemoteEntry.LastLocalSyncTsNs/1e9 < entry.Attributes.Mtime {
		return true
	}
	return false
}

func updateLocalEntry(filerClient filer_pb.FilerClient, dir string, entry *filer_pb.Entry, remoteEntry *filer_pb.RemoteEntry) error {
	entry.RemoteEntry = remoteEntry
	return filerClient.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     entry,
		})
		return err
	})
}
