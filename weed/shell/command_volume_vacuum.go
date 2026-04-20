package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

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
	volumeIds := volumeVacuumCommand.String("volumeId", "", "comma-separated list of volume IDs")
	if err = volumeVacuumCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	var volumeIdInts []uint32
	if *volumeIds != "" {
		for _, volumeIdStr := range strings.Split(*volumeIds, ",") {
			volumeIdStr = strings.TrimSpace(volumeIdStr)
			if volumeIdInt, err := strconv.ParseUint(volumeIdStr, 10, 32); err == nil {
				volumeIdInts = append(volumeIdInts, uint32(volumeIdInt))
			} else {
				return fmt.Errorf("parse volumeId string %s to int: %v", volumeIdStr, err)
			}
		}
	} else {
		volumeIdInts = append(volumeIdInts, 0)
	}

	// Reject unknown ids up front. The master's VacuumVolume RPC silently
	// iterates matching volumes, so a typo or an already-deleted volume just
	// returns success — making this command look like it worked when nothing
	// happened.
	if *volumeIds != "" {
		topo, _, err := collectTopologyInfo(commandEnv, 0)
		if err != nil {
			return fmt.Errorf("collect topology: %w", err)
		}
		known := make(map[uint32]bool)
		eachDataNode(topo, func(_ DataCenterId, _ RackId, dn *master_pb.DataNodeInfo) {
			for _, disk := range dn.DiskInfos {
				for _, vi := range disk.VolumeInfos {
					known[vi.Id] = true
				}
			}
		})
		// Dedupe via a set so "volume.vacuum -volumeId 5,5,5" on a missing
		// volume 5 reports [5] once instead of [5 5 5].
		missingSet := make(map[uint32]bool)
		for _, vid := range volumeIdInts {
			if !known[vid] {
				missingSet[vid] = true
			}
		}
		if len(missingSet) > 0 {
			missing := make([]uint32, 0, len(missingSet))
			for vid := range missingSet {
				missing = append(missing, vid)
			}
			sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
			return fmt.Errorf("volume(s) not found on master: %v", missing)
		}
	}

	for _, volumeId := range volumeIdInts {
		err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
			_, err = client.VacuumVolume(context.Background(), &master_pb.VacuumVolumeRequest{
				GarbageThreshold: float32(*garbageThreshold),
				VolumeId:         volumeId,
				Collection:       *collection,
			})
			return err
		})
		if err != nil {
			return err
		}
	}

	return nil
}
