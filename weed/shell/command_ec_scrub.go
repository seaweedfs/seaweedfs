package shell

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func init() {
	Commands = append(Commands, &commandEcVolumeScrub{})
}

type commandEcVolumeScrub struct {
}

func (c *commandEcVolumeScrub) Name() string {
	return "ec.scrub"
}

func (c *commandEcVolumeScrub) Help() string {
	return `scrubs EC volume contents on volume servers.

	Supports either scrubbing only needle data, or deep scrubbing file contents as well.

	Scrubbing can be limited to specific EC volume IDs for specific volume servers.
	By default, all volume IDs across all servers are processed.
`
}

func (c *commandEcVolumeScrub) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcVolumeScrub) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	volScrubCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	nodesStr := volScrubCommand.String("node", "", "comma-separated list of volume server <host>:<port> (optional)")
	volumeIDsStr := volScrubCommand.String("volumeId", "", "comma-separated EC volume IDs to process (optional)")
	mode := volScrubCommand.String("mode", "local", "scrubbing mode (index/local/full/checksum)")
	maxParallelization := volScrubCommand.Int("maxParallelization", DefaultMaxParallelization, "run up to X tasks in parallel, whenever possible")
	showDetails := volScrubCommand.Bool("details", false, "display scrub result details, if available")
	forceDeletedNeedlesCheck := volScrubCommand.Bool("forceDeletedNeedlesCheck", false, "force strict verification of deleted needles (full mode only); may report false positives when EC indexes disagree")

	if err = volScrubCommand.Parse(args); err != nil {
		return err
	}
	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	volumeServerAddrs := []pb.ServerAddress{}
	if *nodesStr != "" {
		for _, addr := range strings.Split(*nodesStr, ",") {
			volumeServerAddrs = append(volumeServerAddrs, pb.ServerAddress(addr))
		}
	} else {
		dns, err := collectDataNodes(commandEnv, 0)
		if err != nil {
			return err
		}
		for _, dn := range dns {
			volumeServerAddrs = append(volumeServerAddrs, pb.ServerAddress(dn.Address))
		}
	}

	volumeIDs := []uint32{}
	if *volumeIDsStr != "" {
		for _, vids := range strings.Split(*volumeIDsStr, ",") {
			vids = strings.TrimSpace(vids)
			if vids == "" {
				continue
			}
			if vid, err := strconv.ParseUint(vids, 10, 32); err == nil {
				volumeIDs = append(volumeIDs, uint32(vid))
			} else {
				return fmt.Errorf("invalid volume ID %q", vids)
			}
		}
	}

	var scrubMode volume_server_pb.VolumeScrubMode
	switch strings.ToUpper(*mode) {
	case "INDEX":
		scrubMode = volume_server_pb.VolumeScrubMode_INDEX
	case "LOCAL":
		scrubMode = volume_server_pb.VolumeScrubMode_LOCAL
	case "FULL":
		scrubMode = volume_server_pb.VolumeScrubMode_FULL
	case "CHECKSUM":
		scrubMode = volume_server_pb.VolumeScrubMode_CHECKSUM
	default:
		return fmt.Errorf("unsupported scrubbing mode %q", *mode)
	}
	fmt.Fprintf(writer, "using %s mode\n", scrubMode.String())
	if *forceDeletedNeedlesCheck && scrubMode != volume_server_pb.VolumeScrubMode_FULL {
		return fmt.Errorf("deleted needle checks are only supported for FULL scrubs")
	}

	return ec.ScrubEcVolumes(commandEnv.ecEnv(), writer, volumeServerAddrs, volumeIDs, scrubMode, *forceDeletedNeedlesCheck, *maxParallelization, *showDetails)
}
