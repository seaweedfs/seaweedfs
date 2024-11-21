package shell

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"net/url"
	"strconv"
	"strings"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/seaweedfs/seaweedfs/weed/wdclient/exclusive_locks"
)

type ShellOptions struct {
	Masters        *string
	GrpcDialOption grpc.DialOption
	// shell transient context
	FilerHost    string
	FilerPort    int64
	FilerGroup   *string
	FilerAddress pb.ServerAddress
	Directory    string
}

type CommandEnv struct {
	env          map[string]string
	MasterClient *wdclient.MasterClient
	option       *ShellOptions
	locker       *exclusive_locks.ExclusiveLocker
	noLock       bool
}

func NewCommandEnv(options *ShellOptions) *CommandEnv {
	ce := &CommandEnv{
		env:          make(map[string]string),
		MasterClient: wdclient.NewMasterClient(options.GrpcDialOption, *options.FilerGroup, pb.AdminShellClient, "", "", "", *pb.ServerAddresses(*options.Masters).ToServiceDiscovery()),
		option:       options,
		noLock:       false,
	}
	ce.locker = exclusive_locks.NewExclusiveLocker(ce.MasterClient, "shell")
	return ce
}

func (ce *CommandEnv) parseUrl(input string) (path string, err error) {
	if strings.HasPrefix(input, "http") {
		err = fmt.Errorf("http://<filer>:<port> prefix is not supported any more")
		return
	}
	if !strings.HasPrefix(input, "/") {
		input = util.Join(ce.option.Directory, input)
	}
	return input, err
}

func (ce *CommandEnv) isDirectory(path string) bool {

	return ce.checkDirectory(path) == nil

}

func (ce *CommandEnv) confirmIsLocked(args []string) error {

	if ce.locker.IsLocked() {
		return nil
	}
	ce.locker.SetMessage(fmt.Sprintf("%v", args))

	return fmt.Errorf("need to run \"lock\" first to continue")

}

func (ce *CommandEnv) isLocked() bool {
	if ce == nil {
		return true
	}
	if ce.noLock {
		return true
	}
	return ce.locker.IsLocked()
}

func (ce *CommandEnv) checkDirectory(path string) error {

	dir, name := util.FullPath(path).DirAndName()

	exists, err := filer_pb.Exists(ce, dir, name, true)

	if !exists {
		return fmt.Errorf("%s is not a directory", path)
	}

	return err

}

var _ = filer_pb.FilerClient(&CommandEnv{})

func (ce *CommandEnv) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithGrpcFilerClient(streamingMode, 0, ce.option.FilerAddress, ce.option.GrpcDialOption, fn)

}

func (ce *CommandEnv) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (ce *CommandEnv) GetDataCenter() string {
	return ce.MasterClient.DataCenter
}

func parseFilerUrl(entryPath string) (filerServer string, filerPort int64, path string, err error) {
	if strings.HasPrefix(entryPath, "http") {
		var u *url.URL
		u, err = url.Parse(entryPath)
		if err != nil {
			return
		}
		filerServer = u.Hostname()
		portString := u.Port()
		if portString != "" {
			filerPort, err = strconv.ParseInt(portString, 10, 32)
		}
		path = u.Path
	} else {
		err = fmt.Errorf("path should have full url /path/to/dirOrFile : %s", entryPath)
	}
	return
}

func findInputDirectory(args []string) (input string) {
	input = "."
	if len(args) > 0 {
		input = args[len(args)-1]
		if strings.HasPrefix(input, "-") {
			input = "."
		}
	}
	return input
}

func readNeedleMeta(grpcDialOption grpc.DialOption, volumeServer pb.ServerAddress, volumeId uint32, needleValue needle_map.NeedleValue) (resp *volume_server_pb.ReadNeedleMetaResponse, err error) {
	err = operation.WithVolumeServerClient(false, volumeServer, grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			if resp, err = client.ReadNeedleMeta(context.Background(), &volume_server_pb.ReadNeedleMetaRequest{
				VolumeId: volumeId,
				NeedleId: uint64(needleValue.Key),
				Offset:   needleValue.Offset.ToActualOffset(),
				Size:     int32(needleValue.Size),
			}); err != nil {
				return err
			}
			return nil
		},
	)
	return
}

func readNeedleStatus(grpcDialOption grpc.DialOption, sourceVolumeServer pb.ServerAddress, volumeId uint32, needleValue needle_map.NeedleValue) (resp *volume_server_pb.VolumeNeedleStatusResponse, err error) {
	err = operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			if resp, err = client.VolumeNeedleStatus(context.Background(), &volume_server_pb.VolumeNeedleStatusRequest{
				VolumeId: volumeId,
				NeedleId: uint64(needleValue.Key),
			}); err != nil {
				return err
			}
			return nil
		},
	)
	return
}

func getCollectionName(commandEnv *CommandEnv, bucket string) string {
	if *commandEnv.option.FilerGroup != "" {
		return fmt.Sprintf("%s_%s", *commandEnv.option.FilerGroup, bucket)
	}
	return bucket
}
