package shell

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
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

	volume.copy -source <source volume server host:port> -target <target volume server host:port> -volumeId <volume id>

	This command copies a volume from one volume server to another volume server.
	Usually you will want to unmount the volume first before copying.

`
}

func (c *commandVolumeCopy) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeCopy) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volCopyCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := volCopyCommand.Int("volumeId", 0, "the volume id")
	sourceNodeStr := volCopyCommand.String("source", "", "the source volume server <host>:<port>")
	targetNodeStr := volCopyCommand.String("target", "", "the target volume server <host>:<port>")
	if err = volCopyCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	sourceVolumeServer, targetVolumeServer := pb.ServerAddress(*sourceNodeStr), pb.ServerAddress(*targetNodeStr)

	volumeId := needle.VolumeId(*volumeIdInt)

	if sourceVolumeServer == targetVolumeServer {
		return fmt.Errorf("source and target volume servers are the same!")
	}

	_, err = copyVolume(commandEnv.option.GrpcDialOption, writer, volumeId, sourceVolumeServer, targetVolumeServer, "", 0)
	return
}
