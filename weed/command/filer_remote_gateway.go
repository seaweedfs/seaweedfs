package command

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"os"
	"time"
)

type RemoteGatewayOptions struct {
	filerAddress             *string
	grpcDialOption           grpc.DialOption
	readChunkFromFiler       *bool
	timeAgo                  *time.Duration
	createBucketAt           *string
	createBucketRandomSuffix *bool
	include                  *string
	exclude                  *string

	mappings    *remote_pb.RemoteStorageMapping
	remoteConfs map[string]*remote_pb.RemoteConf
	bucketsDir  string
	clientId    int32
	clientEpoch int32
}

var _ = filer_pb.FilerClient(&RemoteGatewayOptions{})

func (option *RemoteGatewayOptions) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return pb.WithFilerClient(streamingMode, option.clientId, pb.ServerAddress(*option.filerAddress), option.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return fn(client)
	})
}
func (option *RemoteGatewayOptions) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (option *RemoteGatewayOptions) GetDataCenter() string {
	return ""
}

var (
	remoteGatewayOptions RemoteGatewayOptions
)

func init() {
	cmdFilerRemoteGateway.Run = runFilerRemoteGateway // break init cycle
	remoteGatewayOptions.filerAddress = cmdFilerRemoteGateway.Flag.String("filer", "localhost:8888", "filer of the SeaweedFS cluster")
	remoteGatewayOptions.createBucketAt = cmdFilerRemoteGateway.Flag.String("createBucketAt", "", "one remote storage name to create new buckets in")
	remoteGatewayOptions.createBucketRandomSuffix = cmdFilerRemoteGateway.Flag.Bool("createBucketWithRandomSuffix", true, "add randomized suffix to bucket name to avoid conflicts")
	remoteGatewayOptions.readChunkFromFiler = cmdFilerRemoteGateway.Flag.Bool("filerProxy", false, "read file chunks from filer instead of volume servers")
	remoteGatewayOptions.timeAgo = cmdFilerRemoteGateway.Flag.Duration("timeAgo", 0, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	remoteGatewayOptions.include = cmdFilerRemoteGateway.Flag.String("include", "", "pattens of new bucket names, e.g., s3*")
	remoteGatewayOptions.exclude = cmdFilerRemoteGateway.Flag.String("exclude", "", "pattens of new bucket names, e.g., local*")
	remoteGatewayOptions.clientId = util.RandomInt32()
}

var cmdFilerRemoteGateway = &Command{
	UsageLine: "filer.remote.gateway",
	Short:     "resumable continuously write back bucket creation, deletion, and other local updates to remote object store",
	Long: `resumable continuously write back bucket creation, deletion, and other local updates to remote object store

	filer.remote.gateway listens on filer local buckets update events. 
	If any bucket is created, deleted, or updated, it will mirror the changes to remote object store.

		weed filer.remote.gateway -createBucketAt=cloud1

`,
}

func runFilerRemoteGateway(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	remoteGatewayOptions.grpcDialOption = grpcDialOption

	filerAddress := pb.ServerAddress(*remoteGatewayOptions.filerAddress)

	filerSource := &source.FilerSource{}
	filerSource.DoInitialize(
		filerAddress.ToHttpAddress(),
		filerAddress.ToGrpcAddress(),
		"/", // does not matter
		*remoteGatewayOptions.readChunkFromFiler,
	)

	remoteGatewayOptions.bucketsDir = "/buckets"
	// check buckets again
	remoteGatewayOptions.WithFilerClient(false, func(filerClient filer_pb.SeaweedFilerClient) error {
		resp, err := filerClient.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return err
		}
		remoteGatewayOptions.bucketsDir = resp.DirBuckets
		return nil
	})

	// read filer remote storage mount mappings
	if detectErr := remoteGatewayOptions.collectRemoteStorageConf(); detectErr != nil {
		fmt.Fprintf(os.Stderr, "read mount info: %v\n", detectErr)
		return true
	}

	// synchronize /buckets folder
	fmt.Printf("synchronize buckets in %s ...\n", remoteGatewayOptions.bucketsDir)
	util.RetryUntil("filer.remote.sync buckets", func() error {
		return remoteGatewayOptions.followBucketUpdatesAndUploadToRemote(filerSource)
	}, func(err error) bool {
		if err != nil {
			glog.Errorf("synchronize %s: %v", remoteGatewayOptions.bucketsDir, err)
		}
		return true
	})
	return true

}
