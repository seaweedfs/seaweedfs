package shell

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"io"
)

func init() {
	Commands = append(Commands, &commandCollectionList{})
}

type commandCollectionList struct {
}

func (c *commandCollectionList) Name() string {
	return "collection.list"
}

func (c *commandCollectionList) Help() string {
	return `list all collections`
}

func (c *commandCollectionList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	collections, err := ListCollectionNames(commandEnv, true, true)

	if err != nil {
		return err
	}

	for _, c := range collections {
		fmt.Fprintf(writer, "collection:\"%s\"\n", c)
	}

	fmt.Fprintf(writer, "Total %d collections.\n", len(collections))

	return nil
}

func ListCollectionNames(commandEnv *CommandEnv, includeNormalVolumes, includeEcVolumes bool) (collections []string, err error) {
	var resp *master_pb.CollectionListResponse
	err = commandEnv.MasterClient.WithClient(func(client master_pb.SeaweedClient) error {
		resp, err = client.CollectionList(context.Background(), &master_pb.CollectionListRequest{
			IncludeNormalVolumes: includeNormalVolumes,
			IncludeEcVolumes:     includeEcVolumes,
		})
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
