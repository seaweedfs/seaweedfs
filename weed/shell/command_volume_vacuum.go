package shell

import (
	"context"
	"flag"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
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

	volume.vacuum [-garbageThreshold=0.3] [-collection=<collection name>] [-volumeId=<volume id>]

`
}

func (c *commandVacuum) HasTag(CommandTag) bool {
	return false
}

func (c *commandVacuum) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volumeVacuumCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	garbageThreshold := volumeVacuumCommand.Float64("garbageThreshold", 0.3, "vacuum when garbage is more than this limit")
	collection := volumeVacuumCommand.String("collection", "", "vacuum this collection")
	volumeId := volumeVacuumCommand.Uint("volumeId", 0, "the volume id")
	if err = volumeVacuumCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		_, err = client.VacuumVolume(context.Background(), &master_pb.VacuumVolumeRequest{
			GarbageThreshold: float32(*garbageThreshold),
			VolumeId:         uint32(*volumeId),
			Collection:       *collection,
		})
		return err
	})
	if err != nil {
		return
	}

	return nil
}
