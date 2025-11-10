package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandVolumeServerLeave{})
}

type commandVolumeServerLeave struct {
}

func (c *commandVolumeServerLeave) Name() string {
	return "volumeServer.leave"
}

func (c *commandVolumeServerLeave) Help() string {
	return `stop a volume server from sending heartbeats to the master

	volumeServer.leave -node <volume server host:port> [-apply]

	This command enables gracefully shutting down the volume server.
	The volume server will stop sending heartbeats to the master.
	After draining the traffic for a few seconds, you can safely shut down the volume server.

	This operation is not revocable unless the volume server is restarted.
`
}

func (c *commandVolumeServerLeave) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeServerLeave) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	vsLeaveCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeServer := vsLeaveCommand.String("node", "", "<host>:<port> of the volume server")
	applyChanges := vsLeaveCommand.Bool("apply", false, "apply the changes")
	// TODO: remove this alias
	applyChangesAlias := vsLeaveCommand.Bool("force", false, "apply the changes (alias for -apply)")
	if err = vsLeaveCommand.Parse(args); err != nil {
		return nil
	}

	handleDeprecatedForceFlag(writer, vsLeaveCommand, applyChangesAlias, applyChanges)
	infoAboutSimulationMode(writer, *applyChanges, "-apply")

	if err = commandEnv.confirmIsLocked(args); err != nil && *applyChanges {
		return
	}

	if *volumeServer == "" {
		return fmt.Errorf("need to specify volume server by -node=<host>:<port>")
	}

	return volumeServerLeave(commandEnv.option.GrpcDialOption, pb.ServerAddress(*volumeServer), writer, *applyChanges)

}

func volumeServerLeave(grpcDialOption grpc.DialOption, volumeServer pb.ServerAddress, writer io.Writer, applyChanges bool) (err error) {
	if !applyChanges {
		fmt.Fprintf(writer, "Would ask volume server %s to leave (dry-run)\n", volumeServer)
		return nil
	}
	return operation.WithVolumeServerClient(false, volumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, leaveErr := volumeServerClient.VolumeServerLeave(context.Background(), &volume_server_pb.VolumeServerLeaveRequest{})
		if leaveErr != nil {
			fmt.Fprintf(writer, "ask volume server %s to leave: %v\n", volumeServer, leaveErr)
		} else {
			fmt.Fprintf(writer, "stopped heartbeat in volume server %s. After a few seconds to drain traffic, it will be safe to stop the volume server.\n", volumeServer)
		}
		return leaveErr
	})
}
