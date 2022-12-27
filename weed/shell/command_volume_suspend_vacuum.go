package shell

import (
	"context"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandSuspendVacuum{})
}

type commandSuspendVacuum struct {
}

func (c *commandSuspendVacuum) Name() string {
	return "volume.suspendvacuum"
}

func (c *commandSuspendVacuum) Help() string {
	return `suspend periodic vacuuming

	volume.suspendvacuum

`
}

func (c *commandSuspendVacuum) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		_, err = client.SuspendVacuum(context.Background(), &master_pb.SuspendVacuumRequest{})
		return err
	})

	return
}
