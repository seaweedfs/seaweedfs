package command

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

type RemoteSyncOptions struct {
	filerAddress       *string
	storageClass       *string
	grpcDialOption     grpc.DialOption
	readChunkFromFiler *bool
	timeAgo            *time.Duration
	dir                *string
	allMounts          *bool
	mountsCheck        *time.Duration
	clientId           int32
	clientEpoch        int32
	resumeTsNs         int64
	// what resumeTsNs was earned under: the directory's remote location, and
	// the offset stored when it was kept (resumeFor)
	resumeLoc      *remote_pb.RemoteStorageLocation
	resumeStoredNs int64
}

var _ = filer_pb.FilerClient(&RemoteSyncOptions{})

func (option *RemoteSyncOptions) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return pb.WithFilerClient(streamingMode, option.clientId, pb.ServerAddress(*option.filerAddress), option.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return fn(client)
	})
}
func (option *RemoteSyncOptions) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (option *RemoteSyncOptions) GetDataCenter() string {
	return ""
}

var (
	remoteSyncOptions RemoteSyncOptions
)

func init() {
	cmdFilerRemoteSynchronize.Run = runFilerRemoteSynchronize // break init cycle
	remoteSyncOptions.filerAddress = cmdFilerRemoteSynchronize.Flag.String("filer", "localhost:8888", "filer of the SeaweedFS cluster")
	remoteSyncOptions.dir = cmdFilerRemoteSynchronize.Flag.String("dir", "", "a mounted directory on filer")
	remoteSyncOptions.allMounts = cmdFilerRemoteSynchronize.Flag.Bool("allMounts", false, "write back every mounted directory from this one process, following mounts and unmounts (instead of -dir)")
	remoteSyncOptions.mountsCheck = cmdFilerRemoteSynchronize.Flag.Duration("mountsCheckInterval", 5*time.Second, "with -allMounts, how often the mount table is read for new and removed mounts")
	remoteSyncOptions.storageClass = cmdFilerRemoteSynchronize.Flag.String("storageClass", "", "override amz storage class, empty to delete")
	remoteSyncOptions.readChunkFromFiler = cmdFilerRemoteSynchronize.Flag.Bool("filerProxy", false, "read file chunks from filer instead of volume servers")
	remoteSyncOptions.timeAgo = cmdFilerRemoteSynchronize.Flag.Duration("timeAgo", 0, "start time before now, skipping previous metadata changes. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	remoteSyncOptions.clientId = util.RandomInt32()
}

var cmdFilerRemoteSynchronize = &Command{
	UsageLine: "filer.remote.sync",
	Short:     "resumable continuously write back updates to remote storage",
	Long: `resumable continuously write back updates to remote storage

	filer.remote.sync listens on filer update events. 
	If any mounted remote file is updated, it will fetch the updated content,
	and write to the remote storage.

		weed filer.remote.sync -dir=/mount/s3_on_cloud

	In a versioned bucket every version is written to the object's key, so the
	remote keeps earlier versions only when versioning is enabled on it. A delete
	marker over a version with a legal hold or retention keeps the remote object
	until a later change to that version releases the lock.

	With -allMounts, one process writes back every mounted directory, each
	from its own position as a separate -dir process would, and starts or
	stops a directory's write-back as it is mounted or unmounted:

		weed filer.remote.sync -allMounts

	The metadata sync starting time is determined with the following priority order:
	1. specified by timeAgo
	2. last sync timestamp for this directory
	3. directory creation time

`,
}

func runFilerRemoteSynchronize(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	remoteSyncOptions.grpcDialOption = grpcDialOption

	dir := *remoteSyncOptions.dir
	filerAddress := pb.ServerAddress(*remoteSyncOptions.filerAddress)

	filerSource := &source.FilerSource{}
	filerSource.DoInitialize(
		filerAddress.ToHttpAddress(),
		filerAddress.ToGrpcAddress(),
		"/", // does not matter
		*remoteSyncOptions.readChunkFromFiler,
	)

	if *remoteSyncOptions.allMounts {
		if dir != "" {
			glog.Fatalf("-dir and -allMounts are exclusive")
		}
		fmt.Printf("synchronize every mounted directory to remote storage...\n")
		newMountSupervisor(&remoteSyncOptions, filerSource).run(nil)
		return true
	}

	if dir != "" {
		fmt.Printf("synchronize %s to remote storage...\n", dir)
		util.RetryUntil("filer.remote.sync "+dir, func() error {
			return followUpdatesAndUploadToRemote(&remoteSyncOptions, filerSource, dir)
		}, func(err error) bool {
			if err != nil {
				glog.Errorf("synchronize %s: %v", dir, err)
			}
			return true
		})
		return true
	}

	return true

}
