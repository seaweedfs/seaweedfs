package command

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"time"
)

type RemoteSyncOptions struct {
	filerAddress       *string
	storageClass       *string
	grpcDialOption     grpc.DialOption
	readChunkFromFiler *bool
	timeAgo            *time.Duration
	dir                *string
	clientId           int32
	clientEpoch        int32
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
