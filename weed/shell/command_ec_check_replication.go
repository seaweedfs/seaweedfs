package shell

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/ec"
)

func init() {
	Commands = append(Commands, &commandEcCheckReplication{})
}

type commandEcCheckReplication struct {
}

func (c *commandEcCheckReplication) Name() string {
	return "ec.check.replication"
}

func (c *commandEcCheckReplication) Help() string {
	return `check EC volumes for under- or over-replicated shards

	ec.check.replication [-volumeIds=<id>,<id>...] [-details]

	Reports EC volumes whose shards are:
	  - under-replicated: at least one shard is missing from every node
	  - over-replicated:  at least one shard has more than one copy, whether on
	                      different nodes or on more than one disk of a node

	Over-replication is normal and transient while ec.balance or ec.encode is
	running, since shards are copied before the redundant copies are deleted;
	re-check once those operations finish before treating it as a problem.

	Each volume is checked against its own data+parity ratio rather than a fixed
	shard count, so volumes with non-default erasure coding ratios are reported
	correctly.

	Options:
	  -volumeIds: comma-separated EC volume IDs to check (default: all EC volumes)
	  -details:   print the per-shard node placement for each flagged volume
`
}

func (c *commandEcCheckReplication) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcCheckReplication) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	checkReplicationCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIDsStr := checkReplicationCommand.String("volumeIds", "", "comma-separated EC volume IDs to process (optional)")
	showDetails := checkReplicationCommand.Bool("details", false, "display result details, if available")

	if err = checkReplicationCommand.Parse(args); err != nil {
		return err
	}
	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	volumeIDMap := map[uint32]bool{}
	if *volumeIDsStr != "" {
		for _, vids := range strings.Split(*volumeIDsStr, ",") {
			vids = strings.TrimSpace(vids)
			if vids == "" {
				continue
			}
			if vid, err := strconv.ParseUint(vids, 10, 32); err == nil {
				volumeIDMap[uint32(vid)] = true
			} else {
				return fmt.Errorf("invalid volume ID %q", vids)
			}
		}
	}

	dataNodes, err := collectDataNodes(commandEnv, 0)
	if err != nil {
		return err
	}

	return ec.CheckEcVolumeReplication(writer, dataNodes, volumeIDMap, *showDetails)
}

// ecCheckReplicationRunner holds the state for a single ec.check.replication invocation.
