package shell

import (
	"flag"
	"io"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeMark{})
}

type commandVolumeMark struct {
}

func (c *commandVolumeMark) Name() string {
	return "volume.mark"
}

func (c *commandVolumeMark) Help() string {
	return `Mark volume readonly from one volume server

	volume.mark -node <volume server host:port> -volumeId <volume id> -readonly true
`
}

func (c *commandVolumeMark) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(); err != nil {
		return
	}

	volMarkCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := volMarkCommand.Int("volumeId", 0, "the volume id")
	nodeStr := volMarkCommand.String("node", "", "the volume server <host>:<port>")
	writable := volMarkCommand.Bool("writable", true, "volume mark writable/readonly")
	if err = volMarkCommand.Parse(args); err != nil {
		return nil
	}

	sourceVolumeServer := *nodeStr

	volumeId := needle.VolumeId(*volumeIdInt)

	return markVolumeWritable(commandEnv.option.GrpcDialOption, volumeId, sourceVolumeServer, *writable)
}
