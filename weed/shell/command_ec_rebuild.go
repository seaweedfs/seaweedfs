package shell

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func init() {
	Commands = append(Commands, &commandEcRebuild{})
}

type commandEcRebuild struct {
}

func (c *commandEcRebuild) Name() string {
	return "ec.rebuild"
}

func (c *commandEcRebuild) Help() string {
	return `find and rebuild missing ec shards among volume servers

	ec.rebuild [-c EACH_COLLECTION|<collection_name>] [-apply] [-maxParallelization N] [-diskType=<disk_type>]

	Before rebuilding, asks volume servers to recover any shards left unmounted by
	a missing .ecx index (the index resides only on a peer server). Such shards are
	invisible to the master, so recovering them first avoids regenerating data that
	is actually present (issue #10104).

	Options:
	  -collection: specify a collection name, or "EACH_COLLECTION" to process all collections
	  -apply: actually perform the rebuild operations (default is dry-run mode)
	  -maxParallelization: number of volumes to rebuild concurrently (default: 10)
	                       Increase for faster rebuilds with more system resources.
	                       Decrease if experiencing resource contention or instability.
	  -diskType: disk type for EC shards (hdd, ssd, or empty for default hdd)

	Algorithm:

	For each type of volume server (different max volume count limit){
		for each collection {
			rebuildEcVolumes()
		}
	}

	func rebuildEcVolumes(){
		idealWritableVolumes = totalWritableVolumes / numVolumeServers
		for {
			sort all volume servers ordered by the number of local writable volumes
			pick the volume server A with the lowest number of writable volumes x
			pick the volume server B with the highest number of writable volumes y
			if y > idealWritableVolumes and x +1 <= idealWritableVolumes {
				if B has a writable volume id v that A does not have {
					move writable volume v from A to B
				}
			}
		}
	}

`
}

func (c *commandEcRebuild) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcRebuild) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fixCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := fixCommand.String("collection", "EACH_COLLECTION", "collection name, or \"EACH_COLLECTION\" for each collection")
	volumeIdsStr := fixCommand.String("volumeIds", "", "optional comma-separated list of volume ID to process; defaults to all volumes in the collection")
	maxParallelization := fixCommand.Int("maxParallelization", DefaultMaxParallelization, "run up to X tasks in parallel, whenever possible")
	applyChanges := fixCommand.Bool("apply", false, "apply the changes")
	diskTypeStr := fixCommand.String("diskType", "", "disk type for EC shards (hdd, ssd, or empty for default hdd)")
	// TODO: remove this alias
	applyChangesAlias := fixCommand.Bool("force", false, "apply the changes (alias for -apply)")
	if err = fixCommand.Parse(args); err != nil {
		return nil
	}
	handleDeprecatedForceFlag(writer, fixCommand, applyChangesAlias, applyChanges)
	infoAboutSimulationMode(writer, *applyChanges, "-apply")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	diskType := types.ToDiskType(*diskTypeStr)

	// collect all ec nodes
	allEcNodes, _, err := collectEcNodes(commandEnv, diskType)
	if err != nil {
		return err
	}

	var collections []string
	if *collection == "EACH_COLLECTION" {
		collections, err = ListCollectionNames(commandEnv, false, true)
		if err != nil {
			return err
		}
	} else {
		collections = []string{*collection}
	}

	var volumeIds []needle.VolumeId
	if *volumeIdsStr != "" {
		for _, vidStr := range strings.Split(*volumeIdsStr, ",") {
			vidStr = strings.TrimSpace(vidStr)
			if len(vidStr) == 0 {
				continue
			}
			if vid, err := strconv.ParseUint(vidStr, 10, 32); err == nil {
				volumeIds = append(volumeIds, needle.VolumeId(vid))
			} else {
				return fmt.Errorf("invalid volume ID %q", vidStr)
			}
		}
	}

	return ec.RebuildEcVolumes(commandEnv.ecEnv(), allEcNodes, writer, collections, volumeIds, diskType, *maxParallelization, *applyChanges)
}
