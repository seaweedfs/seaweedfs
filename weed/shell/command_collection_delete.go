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

	collection.delete -collection <collection_name> -force

`
}

func (c *commandCollectionDelete) HasTag(CommandTag) bool {
	return false
}

func (c *commandCollectionDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	colDeleteCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collectionName := colDeleteCommand.String("collection", "", "collection to delete. Use '_default_' for the empty-named collection.")
	applyBalancing := colDeleteCommand.Bool("force", false, "apply the collection")
	if err = colDeleteCommand.Parse(args); err != nil {
		return nil
	}
	infoAboutSimulationMode(writer, *applyBalancing, "-force")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	if *collectionName == "" {
		return fmt.Errorf("empty collection name is not allowed")
	}

	if *collectionName == "_default_" {
		*collectionName = ""
	}

	if !*applyBalancing {
		fmt.Fprintf(writer, "collection '%s' will be deleted. Use -force to apply the change.\n", *collectionName)
		return nil
	}

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
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
