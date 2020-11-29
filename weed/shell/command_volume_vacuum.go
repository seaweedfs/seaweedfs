package shell

import (
	"context"
	"flag"
	"io"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandVacuum{})
}

type commandVacuum struct {
}

func (c *commandVacuum) Name() string {
	return "volume.vacuum"
}

func (c *commandVacuum) Help() string {
	return `compact volumes if deleted entries are more than the limit

	volume.vacuum [-garbageThreshold=0.3]

`
}

func (c *commandVacuum) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(); err != nil {
		return
	}

	volumeVacuumCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	garbageThreshold := volumeVacuumCommand.Float64("garbageThreshold", 0.3, "vacuum when garbage is more than this limit")
	if err = volumeVacuumCommand.Parse(args); err != nil {
		return nil
	}

	err = commandEnv.MasterClient.WithClient(func(client master_pb.SeaweedClient) error {
		_, err = client.VacuumVolume(context.Background(), &master_pb.VacuumVolumeRequest{
			GarbageThreshold: float32(*garbageThreshold),
		})
		return err
	})
	if err != nil {
		return
	}

	return nil
}
