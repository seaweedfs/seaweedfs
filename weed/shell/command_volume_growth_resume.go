package shell

import (
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandVolumeGrowthResume{})
}

type commandVolumeGrowthResume struct {
}

func (c *commandVolumeGrowthResume) Name() string {
	return "volume.growth.resume"
}

func (c *commandVolumeGrowthResume) Help() string {
	return `resume volume growth frozen after detected master state loss

	volume.growth.resume

	Volume growth freezes automatically when a volume server reports a
	topology id that differs from this cluster's, which means the master
	state was rebuilt and max volume id knowledge may be incomplete. Run
	this after verifying the cluster inventory so new ids are not reused.

`
}

func (c *commandVolumeGrowthResume) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeGrowthResume) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	return commandEnv.MasterClient.WithClient(context.Background(), false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ResumeVolumeGrowth(context.Background(), &master_pb.ResumeVolumeGrowthRequest{})
		if err != nil {
			return err
		}
		if resp.WasFrozen {
			fmt.Fprintf(writer, "volume growth resumed\n")
		} else {
			fmt.Fprintf(writer, "volume growth was not frozen\n")
		}
		return nil
	})
}
