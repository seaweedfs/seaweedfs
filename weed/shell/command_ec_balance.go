package shell

import (
	"flag"
	"fmt"
	"io"
)

func init() {
	Commands = append(Commands, &commandEcBalance{})
}

type commandEcBalance struct {
}

func (c *commandEcBalance) Name() string {
	return "ec.balance"
}

func (c *commandEcBalance) Help() string {
	return `balance all ec shards among all racks and volume servers

	ec.balance [-c EACH_COLLECTION|<collection_name>] [-force] [-dataCenter <data_center>] [-shardReplicaPlacement <replica_placement>]

	Algorithm:
	` + ecBalanceAlgorithmDescription
}

func (c *commandEcBalance) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcBalance) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	balanceCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := balanceCommand.String("collection", "EACH_COLLECTION", "collection name, or \"EACH_COLLECTION\" for each collection")
	dc := balanceCommand.String("dataCenter", "", "only apply the balancing for this dataCenter")
	shardReplicaPlacement := balanceCommand.String("shardReplicaPlacement", "", "replica placement for EC shards, or master default if empty")
	maxParallelization := balanceCommand.Int("maxParallelization", 10, "run up to X tasks in parallel, whenever possible")
	applyBalancing := balanceCommand.Bool("force", false, "apply the balancing plan")
	if err = balanceCommand.Parse(args); err != nil {
		return nil
	}
	infoAboutSimulationMode(writer, *applyBalancing, "-force")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	var collections []string
	if *collection == "EACH_COLLECTION" {
		collections, err = ListCollectionNames(commandEnv, false, true)
		if err != nil {
			return err
		}
	} else {
		collections = append(collections, *collection)
	}
	fmt.Printf("balanceEcVolumes collections %+v\n", len(collections))

	rp, err := parseReplicaPlacementArg(commandEnv, *shardReplicaPlacement)
	if err != nil {
		return err
	}

	return EcBalance(commandEnv, collections, *dc, rp, *maxParallelization, *applyBalancing)
}
