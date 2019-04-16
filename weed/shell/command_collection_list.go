package shell

import (
	"context"
	"fmt"
	"io"

	"github.com/HZ89/seaweedfs/weed/pb/master_pb"
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

	var resp *master_pb.CollectionListResponse
	ctx := context.Background()
	err = commandEnv.masterClient.WithClient(ctx, func(client master_pb.SeaweedClient) error {
		resp, err = client.CollectionList(ctx, &master_pb.CollectionListRequest{})
		return err
	})

	if err != nil {
		return err
	}

	for _, c := range resp.Collections {
		fmt.Fprintf(writer, "collection:\"%s\"\n", c.GetName())
	}

	fmt.Fprintf(writer, "Total %d collections.\n", len(resp.Collections))

	return nil
}
