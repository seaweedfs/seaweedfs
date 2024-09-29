package shell

import (
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
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
	return `Mark volume writable or readonly from one volume server

	volume.mark -node <volume server host:port> -volumeId <volume id> -writable or -readonly
`
}

func (c *commandVolumeMark) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeMark) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volMarkCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := volMarkCommand.Int("volumeId", 0, "the volume id")
	nodeStr := volMarkCommand.String("node", "", "the volume server <host>:<port>")
	writable := volMarkCommand.Bool("writable", false, "volume mark writable")
	readonly := volMarkCommand.Bool("readonly", false, "volume mark readonly")
	if err = volMarkCommand.Parse(args); err != nil {
		return nil
	}
	markWritable := false
	if (*writable && *readonly) || (!*writable && !*readonly) {
		return fmt.Errorf("use -readonly or -writable")
	} else if *writable {
		markWritable = true
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	sourceVolumeServer := pb.ServerAddress(*nodeStr)

	volumeId := needle.VolumeId(*volumeIdInt)

	return markVolumeWritable(commandEnv.option.GrpcDialOption, volumeId, sourceVolumeServer, markWritable, true)
}
