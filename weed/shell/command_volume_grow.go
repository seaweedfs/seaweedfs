package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"io"
)

func init() {
	Commands = append(Commands, &commandGrow{})
}

type commandGrow struct {
}

func (c *commandGrow) Name() string {
	return "volume.grow"
}

func (c *commandGrow) Help() string {
	return `grow volumes

	volume.grow [-collection=<collection name>] [-dataCenter=<data center name>]

`
}

func (c *commandGrow) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volumeVacuumCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	growCount := volumeVacuumCommand.Uint("count", 2, "")
	collection := volumeVacuumCommand.String("collection", "", "grow this collection")
	dataCenter := volumeVacuumCommand.String("dataCenter", "", "grow volumes only from the specified data center")

	if err = volumeVacuumCommand.Parse(args); err != nil {
		return nil
	}

	assignRequest := &master_pb.AssignRequest{
		Count:               0,
		Collection:          *collection,
		WritableVolumeCount: uint32(*growCount),
	}
	if *dataCenter != "" {
		assignRequest.DataCenter = *dataCenter
	}

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		_, err := client.Assign(context.Background(), assignRequest)

		if err != nil {
			return fmt.Errorf("Assign: %v", err)
		}
		return nil
	})

	if err != nil {
		return
	}

	return nil
}
