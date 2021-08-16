package command

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/remote_storage"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
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

	// read filer remote storage mount mappings
	_, _, remoteStorageMountLocation, storageConf, detectErr := filer.DetectMountInfo(grpcDialOption, filerAddress, dir)
	if detectErr != nil {
		fmt.Printf("read mount info: %v", detectErr)
		return false
	}

	filerSource := &source.FilerSource{}
	filerSource.DoInitialize(
		filerAddress,
		pb.ServerToGrpcAddress(filerAddress),
		"/", // does not matter
		*remoteSyncOptions.readChunkFromFiler,
	)

	fmt.Printf("synchronize %s to remote storage...\n", dir)
	util.RetryForever("filer.remote.sync "+dir, func() error {
		return followUpdatesAndUploadToRemote(&remoteSyncOptions, filerSource, dir, storageConf, remoteStorageMountLocation)
	}, func(err error) bool {
		if err != nil {
			fmt.Printf("synchronize %s: %v\n", dir, err)
		}
		return true
	})

	return true
}

func followUpdatesAndUploadToRemote(option *RemoteSyncOptions, filerSource *source.FilerSource, mountedDir string, remoteStorage *filer_pb.RemoteConf, remoteStorageMountLocation *filer_pb.RemoteStorageLocation) error {

	dirHash := util.HashStringToLong(mountedDir)

	// 1. specified by timeAgo
	// 2. last offset timestamp for this directory
	// 3. directory creation time
	var lastOffsetTs time.Time
	if *option.timeAgo == 0 {
		mountedDirEntry, err := filer_pb.GetEntry(option, util.FullPath(mountedDir))
		if err != nil {
			return fmt.Errorf("lookup %s: %v", mountedDir, err)
		}

		lastOffsetTsNs, err := getOffset(option.grpcDialOption, *option.filerAddress, RemoteSyncKeyPrefix, int32(dirHash))
		if err == nil && mountedDirEntry.Attributes.Crtime < lastOffsetTsNs/1000000 {
			lastOffsetTs = time.Unix(0, lastOffsetTsNs)
			glog.V(0).Infof("resume from %v", lastOffsetTs)
		} else {
			lastOffsetTs = time.Unix(mountedDirEntry.Attributes.Crtime, 0)
		}
	} else {
		lastOffsetTs = time.Now().Add(-*option.timeAgo)
	}

	client, err := remote_storage.GetRemoteStorage(remoteStorage)
	if err != nil {
		return err
	}

	eachEntryFunc := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		if message.OldEntry == nil && message.NewEntry == nil {
			return nil
		}
		if message.OldEntry == nil && message.NewEntry != nil {
			if len(message.NewEntry.Chunks) == 0 {
				return nil
			}
			fmt.Printf("create: %+v\n", resp)
			if !shouldSendToRemote(message.NewEntry) {
				fmt.Printf("skipping creating: %+v\n", resp)
				return nil
			}
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(message.NewParentPath, message.NewEntry.Name), remoteStorageMountLocation)
			if message.NewEntry.IsDirectory {
				return client.WriteDirectory(dest, message.NewEntry)
			}
			reader := filer.NewChunkStreamReader(filerSource, message.NewEntry.Chunks)
			remoteEntry, writeErr := client.WriteFile(dest, message.NewEntry, reader)
			if writeErr != nil {
				return writeErr
			}
			return updateLocalEntry(&remoteSyncOptions, message.NewParentPath, message.NewEntry, remoteEntry)
		}
		if message.OldEntry != nil && message.NewEntry == nil {
			fmt.Printf("delete: %+v\n", resp)
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(resp.Directory, message.OldEntry.Name), remoteStorageMountLocation)
			return client.DeleteFile(dest)
		}
		if message.OldEntry != nil && message.NewEntry != nil {
			oldDest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(resp.Directory, message.OldEntry.Name), remoteStorageMountLocation)
			dest := toRemoteStorageLocation(util.FullPath(mountedDir), util.NewFullPath(message.NewParentPath, message.NewEntry.Name), remoteStorageMountLocation)
			if !shouldSendToRemote(message.NewEntry) {
				fmt.Printf("skipping updating: %+v\n", resp)
				return nil
			}
			if message.NewEntry.IsDirectory {
				return client.WriteDirectory(dest, message.NewEntry)
			}
			if resp.Directory == message.NewParentPath && message.OldEntry.Name == message.NewEntry.Name {
				if isSameChunks(message.OldEntry.Chunks, message.NewEntry.Chunks) {
					fmt.Printf("update meta: %+v\n", resp)
					return client.UpdateFileMetadata(dest, message.OldEntry, message.NewEntry)
				}
			}
			fmt.Printf("update: %+v\n", resp)
			if err := client.DeleteFile(oldDest); err != nil {
				return err
			}
			reader := filer.NewChunkStreamReader(filerSource, message.NewEntry.Chunks)
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
		return setOffset(option.grpcDialOption, *option.filerAddress, RemoteSyncKeyPrefix, int32(dirHash), lastTsNs)
	})

	return pb.FollowMetadata(*option.filerAddress, option.grpcDialOption,
		"filer.remote.sync", mountedDir, lastOffsetTs.UnixNano(), 0, processEventFnWithOffset, false)
}

func toRemoteStorageLocation(mountDir, sourcePath util.FullPath, remoteMountLocation *filer_pb.RemoteStorageLocation) *filer_pb.RemoteStorageLocation {
	source := string(sourcePath[len(mountDir):])
	dest := util.FullPath(remoteMountLocation.Path).Child(source)
	return &filer_pb.RemoteStorageLocation{
		Name:   remoteMountLocation.Name,
		Bucket: remoteMountLocation.Bucket,
		Path:   string(dest),
	}
}

func isSameChunks(a, b []*filer_pb.FileChunk) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if !proto.Equal(x, y) {
			return false
		}
	}
	return true
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
