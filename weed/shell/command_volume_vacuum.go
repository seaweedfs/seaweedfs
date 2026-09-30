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

	Without -volumeId this runs the same sweep as the automatic vacuum, which skips
	read-only volumes. Name a read-only volume with -volumeId to vacuum it anyway.

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

	topo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return fmt.Errorf("collect topology: %w", err)
	}

	// Reject unknown ids up front. The master's VacuumVolume RPC silently
	// iterates matching volumes, so a typo or an already-deleted volume just
	// returns success — making this command look like it worked when nothing
	// happened.
	if *volumeIds != "" {
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
	} else if skipped := readOnlyVolumesAboveThreshold(topo, *collection, *garbageThreshold); len(skipped) > 0 {
		// The sweep says nothing about the volumes it leaves alone, so an
		// operator on a full disk sees the command return and nothing change.
		fmt.Fprintf(writer, "%d read-only volume(s) hold garbage above %.2f and are skipped by the sweep: %v\n", len(skipped), *garbageThreshold, skipped)
		fmt.Fprintf(writer, "vacuum them explicitly with -volumeId\n")
	}

	for _, volumeId := range volumeIdInts {
		err = commandEnv.MasterClient.WithClient(context.Background(), false, func(client master_pb.SeaweedClient) error {
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

// readOnlyVolumesAboveThreshold lists the volumes a sweep leaves alone: read-only
// on any replica, in the collection when one is given, with a garbage ratio at
// or above the threshold. The ratio uses the sizes the master reports; the
// volume server's own check is the authority, so this is a hint, not a verdict.
func readOnlyVolumesAboveThreshold(topo *master_pb.TopologyInfo, collection string, garbageThreshold float64) []uint32 {
	seen := make(map[uint32]bool)
	eachDataNode(topo, func(_ DataCenterId, _ RackId, dn *master_pb.DataNodeInfo) {
		for _, disk := range dn.DiskInfos {
			for _, v := range disk.VolumeInfos {
				if !v.ReadOnly || v.Size == 0 || (collection != "" && v.Collection != collection) {
					continue
				}
				if float64(v.DeletedByteCount)/float64(v.Size) >= garbageThreshold {
					seen[v.Id] = true
				}
			}
		}
	})
	vids := make([]uint32, 0, len(seen))
	for vid := range seen {
		vids = append(vids, vid)
	}
	sort.Slice(vids, func(i, j int) bool { return vids[i] < vids[j] })
	return vids
}
