package shell

import (
	"context"
	"fmt"
	"github.com/joeslay/seaweedfs/weed/pb/master_pb"
	"io"
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

	collection.delete <collection_name>

`
}

func (c *commandCollectionDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if len(args) == 0 {
		return nil
	}

	collectionName := args[0]

	ctx := context.Background()
	err = commandEnv.MasterClient.WithClient(ctx, func(client master_pb.SeaweedClient) error {
		_, err = client.CollectionDelete(ctx, &master_pb.CollectionDeleteRequest{
			Name: collectionName,
		})
		return err
	})
	if err != nil {
		return
	}

	fmt.Fprintf(writer, "collection %s is deleted.\n", collectionName)

	return nil
}
