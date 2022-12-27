package shell

import (
	"context"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandResumeVacuum{})
}

type commandResumeVacuum struct {
}

func (c *commandResumeVacuum) Name() string {
	return "volume.resumevacuum"
}

func (c *commandResumeVacuum) Help() string {
	return `resume periodic vacuuming

	volume.resumevacuum

`
}

func (c *commandResumeVacuum) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		_, err = client.ResumeVacuum(context.Background(), &master_pb.ResumeVacuumRequest{})
		return err
	})

	return
}
