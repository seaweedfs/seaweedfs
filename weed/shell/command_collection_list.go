package shell

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"io"
)

func init() {
	commands = append(commands, &commandCollectionList{})
}

type commandCollectionList struct {
}

func (c *commandCollectionList) Name() string {
	return "collection.list"
}

func (c *commandCollectionList) Help() string {
	return `list all collections`
}

func (c *commandCollectionList) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	collections, err := ListCollectionNames(commandEnv)

	if err != nil {
		return err
	}

	for _, c := range collections {
		fmt.Fprintf(writer, "collection:\"%s\"\n", c)
	}

	fmt.Fprintf(writer, "Total %d collections.\n", len(collections))

	return nil
}

func ListCollectionNames(commandEnv *commandEnv) (collections []string, err error) {
	var resp *master_pb.CollectionListResponse
	ctx := context.Background()
	err = commandEnv.masterClient.WithClient(ctx, func(client master_pb.SeaweedClient) error {
		resp, err = client.CollectionList(ctx, &master_pb.CollectionListRequest{})
		return err
	})
	if err != nil {
		return
	}
	for _, c := range resp.Collections {
		collections = append(collections, c.Name)
	}
	return
}
