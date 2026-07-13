package shell

import (
	"flag"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

func init() {
	Commands = append(Commands, &commandEcVolumeHealth{})
}

type commandEcVolumeHealth struct {
	writer      io.Writer
	dataNodes   []*master_pb.DataNodeInfo
	volumeIDMap map[uint32]bool
}

func (c *commandEcVolumeHealth) Name() string {
	return "ec.health"
}

func (c *commandEcVolumeHealth) Help() string {
	return `Verifies the overall shard health of EC volumes.
`
}

func (c *commandEcVolumeHealth) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcVolumeHealth) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	volHealthCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIDsStr := volHealthCommand.String("volumeIds", "", "comma-separated EC volume IDs to process (optional)")
	showDetails := volHealthCommand.Bool("details", false, "display result details, if available")

	if err = volHealthCommand.Parse(args); err != nil {
		return err
	}
	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	c.writer = writer

	c.volumeIDMap = map[uint32]bool{}
	if *volumeIDsStr != "" {
		for _, vids := range strings.Split(*volumeIDsStr, ",") {
			vids = strings.TrimSpace(vids)
			if vids == "" {
				continue
			}
			if vid, err := strconv.ParseUint(vids, 10, 32); err == nil {
				c.volumeIDMap[uint32(vid)] = true
			} else {
				return fmt.Errorf("invalid volume ID %q", vids)
			}
		}
	}

	c.dataNodes, err = collectDataNodes(commandEnv, 0)
	if err != nil {
		return err
	}

	return c.checkEcVolumes(*showDetails)
}

func (c *commandEcVolumeHealth) write(format string, a ...any) {
	fmt.Fprintf(c.writer, format, a...)
}

func (c *commandEcVolumeHealth) isVolumeIDValid(vid uint32) bool {
	if len(c.volumeIDMap) == 0 {
		return true
	}
	if _, ok := c.volumeIDMap[vid]; ok {
		return true
	}

	return false
}

// TODO: check shard sizes?
func (c *commandEcVolumeHealth) checkEcVolumes(showDetails bool) error {
	shardAddressMap := map[uint32]map[erasure_coding.ShardId][]string{}

	// collect EC shard details
	volumesFound := false
	for _, dni := range c.dataNodes {
		nodeAddress := dni.GetAddress()
		for _, di := range dni.GetDiskInfos() {
			for _, eci := range di.GetEcShardInfos() {
				vid := eci.GetId()
				if !c.isVolumeIDValid(vid) {
					continue
				}
				volumesFound = true

				if _, ok := shardAddressMap[vid]; !ok {
					shardAddressMap[vid] = map[erasure_coding.ShardId][]string{}
				}

				sinfo := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci)
				for _, sid := range sinfo.Ids() {
					if _, ok := shardAddressMap[vid][sid]; !ok {
						shardAddressMap[vid][sid] = []string{}
					}
					shardAddressMap[vid][sid] = append(shardAddressMap[vid][sid], nodeAddress)
				}
			}
		}
	}

	if !volumesFound {
		return fmt.Errorf("no EC volumes found")
	}

	// keep the shard address map nicely sorted, for the sake of readability.
	for _, sm := range shardAddressMap {
		for _, addrs := range sm {
			slices.Sort(addrs)
		}
	}

	// check each volume, and compute shard counts
	underreplicatedVolumeIDs := []uint32{}
	overreplicatedVolumeIDs := []uint32{}
	shardCountMap := map[uint32]int{}

	for vid, sm := range shardAddressMap {
		for _, addrs := range sm {
			shardCountMap[vid] += len(addrs)
		}
		under := false
		over := false

		for sid := range erasure_coding.TotalShardsCount {
			sid := erasure_coding.ShardId(sid)
			if len(sm[sid]) == 0 {
				under = true
			}
			if len(sm[sid]) > 1 {
				over = true
			}
		}

		if under {
			underreplicatedVolumeIDs = append(underreplicatedVolumeIDs, vid)
		}
		if !under && over {
			// don't flag EC volumes as over-replicated if some shards are missing, regardless
			// of others being repeated.
			overreplicatedVolumeIDs = append(overreplicatedVolumeIDs, vid)
		}
	}

	slices.Sort(underreplicatedVolumeIDs)
	slices.Sort(overreplicatedVolumeIDs)

	// ...and display results
	if len(underreplicatedVolumeIDs) == 0 && len(overreplicatedVolumeIDs) == 0 {
		c.write("EC volumes are healthy.\n")
		return nil
	}

	if len(underreplicatedVolumeIDs) != 0 {
		c.write("Found %d/%d under-replicated EC volumes: %v\n", len(underreplicatedVolumeIDs), len(shardAddressMap), underreplicatedVolumeIDs)
	}
	if len(overreplicatedVolumeIDs) != 0 {
		c.write("Found %d/%d over-replicated EC volumes: %v\n", len(overreplicatedVolumeIDs), len(shardAddressMap), overreplicatedVolumeIDs)
	}

	if showDetails {
		shardTypeDesc := func(sid int) string {
			if sid < erasure_coding.DataShardsCount {
				return ""
			}
			return " (parity)"
		}

		if len(underreplicatedVolumeIDs) != 0 {
			c.write("\n")
			for _, vid := range underreplicatedVolumeIDs {
				c.write("Shards map for under-replicated EC volume %v (%d/%d shards):\n", vid, shardCountMap[vid], erasure_coding.TotalShardsCount)
				for sid := range erasure_coding.TotalShardsCount {
					if addrs, ok := shardAddressMap[vid][erasure_coding.ShardId(sid)]; ok {
						c.write("\t%02d%s => %v\n", sid, shardTypeDesc(sid), addrs)
					} else {
						c.write("\t%02d%s is missing\n", sid, shardTypeDesc(sid))
					}
				}
			}
		}

		if len(overreplicatedVolumeIDs) != 0 {
			c.write("\n")
			for _, vid := range overreplicatedVolumeIDs {
				c.write("Shards map for over-replicated EC volume %v (%d/%d shards):\n", vid, shardCountMap[vid], erasure_coding.TotalShardsCount)
				for sid := range erasure_coding.TotalShardsCount {
					if addrs, ok := shardAddressMap[vid][erasure_coding.ShardId(sid)]; ok {
						c.write("\t%02d%s => %v\n", sid, shardTypeDesc(sid), addrs)
					} else {
						c.write("\t%02d%s is missing\n", sid, shardTypeDesc(sid))
					}
				}
			}
		}
	}

	return nil
}
