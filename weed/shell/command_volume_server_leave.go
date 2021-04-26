package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
	"io"
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

	volume.unmount -node <volume server host:port> -force

	This command enables gracefully shutting down the volume server.
	The volume server will stop sending heartbeats to the master.
	After draining the traffic for a few seconds, you can safely shut down the volume server.

	This operation is not revocable unless the volume server is restarted.
`
}

func (c *commandVolumeServerLeave) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(); err != nil {
		return
	}

	vsLeaveCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeServer := vsLeaveCommand.String("node", "", "<host>:<port> of the volume server")
	if err = vsLeaveCommand.Parse(args); err != nil {
		return nil
	}

	if *volumeServer == "" {
		return fmt.Errorf("need to specify volume server by -node=<host>:<port>")
	}

	return volumeServerLeave(commandEnv.option.GrpcDialOption, *volumeServer, writer)

}

func volumeServerLeave(grpcDialOption grpc.DialOption, volumeServer string, writer io.Writer) (err error) {
	return operation.WithVolumeServerClient(volumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, leaveErr := volumeServerClient.VolumeServerLeave(context.Background(), &volume_server_pb.VolumeServerLeaveRequest{})
		if leaveErr != nil {
			fmt.Fprintf(writer, "ask volume server %s to leave: %v\n", volumeServer, leaveErr)
		} else {
			fmt.Fprintf(writer, "stopped heartbeat in volume server %s. After a few seconds to drain traffic, it will be safe to stop the volume server.\n", volumeServer)
		}
		return leaveErr
	})
}
