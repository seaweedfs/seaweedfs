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

	// vid -> layout key -> locations; ec shards tracked per collection since
	// shards plus regular copies of the same encoded volume are legitimate
	seen := make(map[needle.VolumeId]map[string][]string)
	ecSeen := make(map[needle.VolumeId]map[string][]string)
	collections := make(map[needle.VolumeId]map[string]bool)
	ecCollections := make(map[needle.VolumeId]map[string]bool)
	add := func(m map[needle.VolumeId]map[string][]string, id uint32, key, location string) {
		layouts := m[needle.VolumeId(id)]
		if layouts == nil {
			layouts = make(map[string][]string)
			m[needle.VolumeId(id)] = layouts
		}
		layouts[key] = append(layouts[key], location)
	}
	addCollection := func(m map[needle.VolumeId]map[string]bool, id uint32, collection string) {
		if m[needle.VolumeId(id)] == nil {
			m[needle.VolumeId(id)] = make(map[string]bool)
		}
		m[needle.VolumeId(id)][collection] = true
	}
	for _, dc := range topoInfo.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for diskKey, di := range dn.DiskInfos {
					for _, vi := range di.VolumeInfos {
						rp, _ := super_block.NewReplicaPlacementFromByte(byte(vi.ReplicaPlacement))
						add(seen, vi.Id, fmt.Sprintf("collection=%q replication=%s ttl=%s disk=%s",
							vi.Collection, rp.String(), needle.LoadTTLFromUint32(vi.Ttl), vi.DiskType),
							fmt.Sprintf("%s %s", dn.Id, diskKey))
						addCollection(collections, vi.Id, vi.Collection)
					}
					for _, si := range di.EcShardInfos {
						add(ecSeen, si.Id, fmt.Sprintf("collection=%q ec_shards disk=%s", si.Collection, si.DiskType),
							fmt.Sprintf("%s %s", dn.Id, diskKey))
						addCollection(ecCollections, si.Id, si.Collection)
					}
				}
			}
		}
	}

	vids := make(map[needle.VolumeId]struct{}, len(seen)+len(ecSeen))
	for vid := range seen {
		vids[vid] = struct{}{}
	}
	for vid := range ecSeen {
		vids[vid] = struct{}{}
	}

	duplicates := 0
	for _, vid := range sortedKeys(vids) {
		layouts := seen[vid]
		shardLayouts := ecSeen[vid]
		ecForeign := false
		for collection := range ecCollections[vid] {
			if !collections[vid][collection] {
				ecForeign = true
			}
		}
		if len(layouts) < 2 && len(shardLayouts) < 2 && !ecForeign {
			continue
		}
		duplicates++
		fmt.Fprintf(writer, "volume %d is registered under %d layouts:\n", vid, len(layouts)+len(shardLayouts))
		for _, key := range sortedKeysString(layouts) {
			fmt.Fprintf(writer, "  %-60s %v\n", key, layouts[key])
		}
		for _, key := range sortedKeysString(shardLayouts) {
			fmt.Fprintf(writer, "  %-60s %v\n", key, shardLayouts[key])
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
