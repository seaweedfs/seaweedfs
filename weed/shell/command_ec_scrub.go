package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandEcVolumeScrub{})
}

type commandEcVolumeScrub struct {
	env               *CommandEnv
	grpcDialOption    grpc.DialOption

	writer io.Writer
	volumeServerAddrs []pb.ServerAddress
	volumeIDs         []uint32
	mode              volume_server_pb.VolumeScrubMode
	showDetails bool
	forceDeletedNeedlesCheck bool
	maxParallelization int
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
	forceDeletedNeedlesCheck := volScrubCommand.Bool("forceDeletedNeedlesCheck", false, "forces strict verification of deleted needles; can cause false positives for EC volumes not fully matching local indexes")

	if err = volScrubCommand.Parse(args); err != nil {
		return err
	}
	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	c.env = commandEnv
	c.writer = writer
	c.maxParallelization = *maxParallelization
	c.showDetails = *showDetails
	c.forceDeletedNeedlesCheck = *forceDeletedNeedlesCheck

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
	case "LOCAL":
		c.mode = volume_server_pb.VolumeScrubMode_LOCAL
	case "FULL":
		c.mode = volume_server_pb.VolumeScrubMode_FULL
	case "CHECKSUM":
		c.mode = volume_server_pb.VolumeScrubMode_CHECKSUM
	default:
		return fmt.Errorf("unsupported scrubbing mode %q", *mode)
	}
	fmt.Fprintf(writer, "using %s mode\n", c.mode.String())

	return c.doScrub()
}


func (c *commandEcVolumeScrub) write(format string, a ...any) {
	fmt.Fprintf(c.writer, format, a...)
}

func (c *commandEcVolumeScrub) doScrub() error {
	var brokenVolumesStr, brokenShardsStr []string
	var details []string
	var totalVolumes, brokenVolumes, brokenShards, totalFiles uint64
	var mu sync.Mutex

	ewg := NewErrorWaitGroup(c.maxParallelization)
	count := 0
	for _, addr := range c.volumeServerAddrs {
		ewg.Add(func() error {
			mu.Lock()
			count++
			c.write("Scrubbing %s (%d/%d)...\n", addr.String(), count, len(c.volumeServerAddrs))
			mu.Unlock()

			err := operation.WithVolumeServerClient(false, addr, c.env.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				res, err := volumeServerClient.ScrubEcVolume(context.Background(), &volume_server_pb.ScrubEcVolumeRequest{
					Mode:      c.mode,
					VolumeIds: c.volumeIDs,
					ForceDeletedNeedlesCheck: c.forceDeletedNeedlesCheck,
				})
				if err != nil {
					return err
				}

				mu.Lock()
				defer mu.Unlock()

				totalVolumes += res.GetTotalVolumes()
				totalFiles += res.GetTotalFiles()
				brokenVolumes += uint64(len(res.GetBrokenVolumeIds()))
				brokenShards += uint64(len(res.GetBrokenShardInfos()))
				for _, d := range res.GetDetails() {
					details = append(details, fmt.Sprintf("[%s] %s", addr, d))
				}
				for _, vid := range res.GetBrokenVolumeIds() {
					brokenVolumesStr = append(brokenVolumesStr, fmt.Sprintf("%s:%v", addr, vid))
				}
				for _, si := range res.GetBrokenShardInfos() {
					brokenShardsStr = append(brokenShardsStr, fmt.Sprintf("%s:%v:%v", addr, si.VolumeId, si.ShardId))
				}

				return nil
			})
			return err
		})
	}
	if err := ewg.Wait(); err != nil {
		return err
	}

	c.write("Scrubbed %d EC files and %d volumes on %d nodes\n", totalFiles, totalVolumes, len(c.volumeServerAddrs))
	if brokenVolumes != 0 {
		c.write("\nGot scrub failures on %d EC volumes and %d EC shards :(\n", brokenVolumes, brokenShards)
		c.write("Affected volumes: %s\n", strings.Join(brokenVolumesStr, ", "))
		if len(brokenShardsStr) != 0 {
			c.write("Affected shards:  %s\n", strings.Join(brokenShardsStr, ", "))
		}
		if c.showDetails && len(details) != 0 {
			c.write("Details:\n\t%s\n", strings.Join(details, "\n\t"))
		}
	}
	return nil
}
