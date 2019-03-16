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
	return "\t\t # list all collections"
}

func (c *commandCollectionList) Do(args []string, commandEnv *commandEnv, writer io.Writer) error {

	resp, err := commandEnv.masterClient.CollectionList(context.Background())

	if err != nil {
		return err
	}

	for _, c := range resp.Collections {
		fmt.Fprintf(writer, "collection:\"%s\"\n", c.GetName())
	}

	fmt.Fprintf(writer, "Total %d collections.\n", len(resp.Collections))

	return nil
}
