package shell

import (
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeCopy{})
}

type commandVolumeCopy struct {
}

func (c *commandVolumeCopy) Name() string {
	return "volume.copy"
}

func (c *commandVolumeCopy) Help() string {
	return `copy a volume from one volume server to another volume server

	volume.copy <source volume server host:port> <target volume server host:port> <volume id>

	This command copies a volume from one volume server to another volume server.
	Usually you will want to unmount the volume first before copying.

`
}

func (c *commandVolumeCopy) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(); err != nil {
		return
	}

	if len(args) != 3 {
		fmt.Fprintf(writer, "received args: %+v\n", args)
		return fmt.Errorf("need 3 args of <source volume server host:port> <target volume server host:port> <volume id>")
	}
	sourceVolumeServer, targetVolumeServer, volumeIdString := args[0], args[1], args[2]

	volumeId, err := needle.NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("wrong volume id format %s: %v", volumeId, err)
	}

	if sourceVolumeServer == targetVolumeServer {
		return fmt.Errorf("source and target volume servers are the same!")
	}

	_, err = copyVolume(commandEnv.option.GrpcDialOption, volumeId, sourceVolumeServer, targetVolumeServer)
	return
}
