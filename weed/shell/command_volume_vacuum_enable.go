package shell

import (
	"context"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandEnableVacuum{})
}

type commandEnableVacuum struct {
}

func (c *commandEnableVacuum) Name() string {
	return "volume.vacuum.enable"
}

func (c *commandEnableVacuum) Help() string {
	return `enable vacuuming request from Master

	volume.vacuum.enable

`
}

func (c *commandEnableVacuum) HasTag(CommandTag) bool {
	return false
}

func (c *commandEnableVacuum) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		_, err = client.EnableVacuum(context.Background(), &master_pb.EnableVacuumRequest{})
		return err
	})

	return
}
