package shell

import (
	"fmt"
	"io"
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func init() {
	Commands = append(Commands, &commandVolumeCheckDuplicateIds{})
}

type commandVolumeCheckDuplicateIds struct {
}

func (c *commandVolumeCheckDuplicateIds) Name() string {
	return "volume.check.duplicate_ids"
}

func (c *commandVolumeCheckDuplicateIds) Help() string {
	return `find volume ids registered under more than one layout

	volume.check.duplicate_ids

	Volume ids are global. The same id under two collections, replica
	placements, TTLs, or disk types means the allocator reused an id, for
	example after master state loss. See volume.growth.resume.

`
}

func (c *commandVolumeCheckDuplicateIds) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeCheckDuplicateIds) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	topoInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	// vid -> layout key -> locations
	seen := make(map[needle.VolumeId]map[string][]string)
	for _, dc := range topoInfo.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for diskKey, di := range dn.DiskInfos {
					for _, vi := range di.VolumeInfos {
						rp, _ := super_block.NewReplicaPlacementFromByte(byte(vi.ReplicaPlacement))
						key := fmt.Sprintf("collection=%q replication=%s ttl=%s disk=%s",
							vi.Collection, rp.String(), needle.LoadTTLFromUint32(vi.Ttl), vi.DiskType)
						layouts := seen[needle.VolumeId(vi.Id)]
						if layouts == nil {
							layouts = make(map[string][]string)
							seen[needle.VolumeId(vi.Id)] = layouts
						}
						layouts[key] = append(layouts[key], fmt.Sprintf("%s %s", dn.Id, diskKey))
					}
				}
			}
		}
	}

	duplicates := 0
	for _, vid := range sortedKeys(seen) {
		layouts := seen[vid]
		if len(layouts) < 2 {
			continue
		}
		duplicates++
		fmt.Fprintf(writer, "volume %d is registered under %d layouts:\n", vid, len(layouts))
		for _, key := range sortedKeysString(layouts) {
			fmt.Fprintf(writer, "  %-60s %v\n", key, layouts[key])
		}
	}
	if duplicates == 0 {
		fmt.Fprintf(writer, "no duplicate volume ids found\n")
	}
	return nil
}

func sortedKeys[V any](m map[needle.VolumeId]V) []needle.VolumeId {
	keys := make([]needle.VolumeId, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func sortedKeysString[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
