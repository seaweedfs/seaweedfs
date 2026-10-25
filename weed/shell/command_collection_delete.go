package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandCollectionDelete{})
}

type commandCollectionDelete struct {
}

func (c *commandCollectionDelete) Name() string {
	return "collection.delete"
}

func (c *commandCollectionDelete) Help() string {
	return `delete specified collection

	collection.delete -collection <collection_name> -apply

`
}

func (c *commandCollectionDelete) HasTag(CommandTag) bool {
	return false
}

func (c *commandCollectionDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	colDeleteCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collectionName := colDeleteCommand.String("collection", "", "collection to delete. Use '"+CollectionDefault+"' for the empty-named collection.")
	applyBalancing := colDeleteCommand.Bool("apply", false, "apply the collection")
	// TODO: remove this alias
	applyBalancingAlias := colDeleteCommand.Bool("force", false, "apply the collection (alias for -apply)")

	if err = colDeleteCommand.Parse(args); err != nil {
		return nil
	}

	handleDeprecatedForceFlag(writer, colDeleteCommand, applyBalancingAlias, applyBalancing)
	infoAboutSimulationMode(writer, *applyBalancing, "-apply")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	if *collectionName == "" {
		return fmt.Errorf("empty collection name is not allowed")
	}

	// The legacy "_default_" spelling stays accepted for compatibility;
	// CollectionDefault is the shared keyword every other -collection flag
	// uses for the empty-named collection.
	if *collectionName == CollectionDefault+"_" {
		*collectionName = ""
	}
	*collectionName = resolveCollectionName(*collectionName)

	if !*applyBalancing {
		fmt.Fprintf(writer, "collection '%s' will be deleted. Use -apply to apply the change.\n", *collectionName)
		return nil
	}

	err = commandEnv.MasterClient.WithClient(context.Background(), false, func(client master_pb.SeaweedClient) error {
		_, err = client.CollectionDelete(context.Background(), &master_pb.CollectionDeleteRequest{
			Name: *collectionName,
		})
		return err
	})
	if err != nil {
		return
	}

	fmt.Fprintf(writer, "collection %s is deleted.\n", *collectionName)

	return nil
}
