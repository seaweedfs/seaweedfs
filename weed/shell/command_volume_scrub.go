package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandVolumeScrub{})
}

type commandVolumeScrub struct {
	env               *CommandEnv
	volumeServerAddrs []pb.ServerAddress
	volumeIDs         []uint32
	mode              volume_server_pb.VolumeScrubMode
	grpcDialOption    grpc.DialOption
}

func (c *commandVolumeScrub) Name() string {
	return "volume.scrub"
}

func (c *commandVolumeScrub) Help() string {
	return `scrubs volume contents on volume servers.

	Supports either scrubbing only needle data, or deep scrubbing file contents as well.

	Scrubbing can be limited to specific volume IDs for specific volume servers.
	By default, all volume IDs across all servers are processed.

`
}

func (c *commandVolumeScrub) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeScrub) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	volScrubCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	nodesStr := volScrubCommand.String("node", "", "comma-separated list of volume server <host>:<port> (optional)")
	volumeIDsStr := volScrubCommand.String("volumeId", "", "comma-separated volume IDs to process (optional)")
	// TODO: switch default mode to LOCAL, once implemented.
	mode := volScrubCommand.String("mode", "INDEX", "scrubbing mode (INDEX/FULL)")
	// TODO: add per-node parallelization

	if err = volScrubCommand.Parse(args); err != nil {
		return err
	}
	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	c.volumeServerAddrs = []pb.ServerAddress{}
	if *nodesStr != "" {
		for _, addr := range strings.Split(*nodesStr, ",") {
			c.volumeServerAddrs = append(c.volumeServerAddrs, pb.ServerAddress(addr))
		}
	} else {
		dns, err := collectDataNodes(commandEnv, 0)
		if err != nil {
			return err
		}
		for _, dn := range dns {
			c.volumeServerAddrs = append(c.volumeServerAddrs, pb.ServerAddress(dn.Address))
		}
	}

	c.volumeIDs = []uint32{}
	if *volumeIDsStr != "" {
		for _, vids := range strings.Split(*volumeIDsStr, ",") {
			vids = strings.TrimSpace(vids)
			if vids == "" {
				continue
			}
			if vid, err := strconv.ParseUint(vids, 10, 32); err == nil {
				c.volumeIDs = append(c.volumeIDs, uint32(vid))
			} else {
				return fmt.Errorf("invalid volume ID %q", vids)
			}
		}
	}

	switch strings.ToUpper(*mode) {
	case "INDEX":
		c.mode = volume_server_pb.VolumeScrubMode_INDEX
	case "FULL":
		c.mode = volume_server_pb.VolumeScrubMode_FULL
	default:
		return fmt.Errorf("unsupported scrubbing mode %q", *mode)
	}
	fmt.Fprintf(writer, "using %s mode\n", c.mode.String())
	c.env = commandEnv

	return c.scrubVolumes(writer)
}

func (c *commandVolumeScrub) scrubVolumes(writer io.Writer) error {
	var brokenVolumesStr []string
	var details []string
	var totalVolumes, brokenVolumes, totalFiles uint64

	for i, addr := range c.volumeServerAddrs {
		fmt.Fprintf(writer, "Scrubbing %s (%d/%d)...\n", addr.String(), i+1, len(c.volumeServerAddrs))

		err := operation.WithVolumeServerClient(false, addr, c.env.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			res, err := volumeServerClient.ScrubVolume(context.Background(), &volume_server_pb.ScrubVolumeRequest{
				Mode:      c.mode,
				VolumeIds: c.volumeIDs,
			})
			if err != nil {
				return err
			}

			totalVolumes += res.GetTotalVolumes()
			totalFiles += res.GetTotalFiles()
			brokenVolumes += uint64(len(res.GetBrokenVolumeIds()))
			for _, d := range res.GetDetails() {
				details = append(details, fmt.Sprintf("[%s] %s", addr, d))
			}
			for _, vid := range res.GetBrokenVolumeIds() {
				brokenVolumesStr = append(brokenVolumesStr, fmt.Sprintf("%s:%v", addr, vid))
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	fmt.Fprintf(writer, "Scrubbed %d files and %d volumes on %d nodes\n", totalFiles, totalVolumes, len(c.volumeServerAddrs))
	if brokenVolumes != 0 {
		fmt.Fprintf(writer, "\nGot scrub failures on %d volumes :(\n", brokenVolumes)
		fmt.Fprintf(writer, "Affected volumes: %s\n", strings.Join(brokenVolumesStr, ", "))
		if len(details) != 0 {
			fmt.Fprintf(writer, "Details:\n\t%s\n", strings.Join(details, "\n\t"))
		}
	}
	return nil
}
