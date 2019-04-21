package shell

import (
	"context"
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

func init() {
	commands = append(commands, &commandVolumeUnmount{})
}

type commandVolumeUnmount struct {
}

func (c *commandVolumeUnmount) Name() string {
	return "volume.unmount"
}

func (c *commandVolumeUnmount) Help() string {
	return `unmount a volume from one volume server

	volume.unmount <volume server host:port> <volume id>

	This command unmounts a volume from one volume server.

`
}

func (c *commandVolumeUnmount) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	if len(args) != 2 {
		fmt.Fprintf(writer, "received args: %+v\n", args)
		return fmt.Errorf("need 2 args of <volume server host:port> <volume id>")
	}
	sourceVolumeServer, volumeIdString := args[0], args[1]

	volumeId, err := needle.NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("wrong volume id format %s: %v", volumeId, err)
	}

	ctx := context.Background()
	return unmountVolume(ctx, commandEnv.option.GrpcDialOption, volumeId, sourceVolumeServer)

}


func unmountVolume(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer string) (err error) {
	return operation.WithVolumeServerClient(sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, unmountErr := volumeServerClient.VolumeUnmount(ctx, &volume_server_pb.VolumeUnmountRequest{
			VolumeId: uint32(volumeId),
		})
		return unmountErr
	})
}
