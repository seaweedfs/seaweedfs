package shell

import (
	"context"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandDisableVacuum{})
}

type commandDisableVacuum struct {
}

func (c *commandDisableVacuum) Name() string {
	return "volume.vacuum.disable"
}

func (c *commandDisableVacuum) Help() string {
	return `disable vacuuming request from Master, however volume.vacuum still works.

	volume.vacuum.disable

`
}

func (c *commandDisableVacuum) HasTag(CommandTag) bool {
	return false
}

func (c *commandDisableVacuum) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		_, err = client.DisableVacuum(context.Background(), &master_pb.DisableVacuumRequest{})
		return err
	})

	return
}
