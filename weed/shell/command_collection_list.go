package shell

import (
	"context"
	"fmt"
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
	return "# list all collections"
}

func (c *commandCollectionList) Do(args []string, commandEnv *commandEnv, writer io.Writer) error {

	resp, err := commandEnv.masterClient.CollectionList(context.Background())

	if err != nil {
		return err
	}

	for _, c := range resp.Collections {
		fmt.Fprintf(writer, "collection:\"%s\"\treplication:\"%s\"\tTTL:\"%s\"\n", c.GetName(), c.GetReplication(), c.GetTtl())
	}

	fmt.Fprintf(writer, "Total %d collections.\n", len(resp.Collections))

	return nil
}
