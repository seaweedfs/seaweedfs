package shell

import (
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeDelete{})
}

type commandVolumeDelete struct {
}

func (c *commandVolumeDelete) Name() string {
	return "volume.delete"
}

func (c *commandVolumeDelete) Help() string {
	return `delete a live volume from one volume server

	volume.delete <volume server host:port> <volume id>

	This command deletes a volume from one volume server.

`
}

func (c *commandVolumeDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(); err != nil {
		return
	}

	if len(args) != 2 {
		fmt.Fprintf(writer, "received args: %+v\n", args)
		return fmt.Errorf("need 2 args of <volume server host:port> <volume id>")
	}
	sourceVolumeServer, volumeIdString := args[0], args[1]

	volumeId, err := needle.NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("wrong volume id format %s: %v", volumeId, err)
	}

	return deleteVolume(commandEnv.option.GrpcDialOption, volumeId, sourceVolumeServer)

}
