package shell

import (
	"context"
	"flag"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandVolumeMount{})
}

type commandVolumeMount struct {
}

func (c *commandVolumeMount) Name() string {
	return "volume.mount"
}

func (c *commandVolumeMount) Help() string {
	return `mount a volume from one volume server

	volume.mount -node <volume server host:port> -volumeId <volume id>

	This command mounts a volume from one volume server.

`
}

func (c *commandVolumeMount) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeMount) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volMountCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := volMountCommand.Int("volumeId", 0, "the volume id")
	nodeStr := volMountCommand.String("node", "", "the volume server <host>:<port>")
	if err = volMountCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	sourceVolumeServer := pb.ServerAddress(*nodeStr)

	volumeId := needle.VolumeId(*volumeIdInt)

	return mountVolume(commandEnv.option.GrpcDialOption, volumeId, sourceVolumeServer)

}

func mountVolume(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer pb.ServerAddress) (err error) {
	return operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, mountErr := volumeServerClient.VolumeMount(context.Background(), &volume_server_pb.VolumeMountRequest{
			VolumeId: uint32(volumeId),
		})
		return mountErr
	})
}
