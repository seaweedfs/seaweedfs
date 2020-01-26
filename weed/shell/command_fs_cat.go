package shell

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func init() {
	Commands = append(Commands, &commandFsCat{})
}

type commandFsCat struct {
}

func (c *commandFsCat) Name() string {
	return "fs.cat"
}

func (c *commandFsCat) Help() string {
	return `stream the file content on to the screen

	fs.cat /dir/file_name
	fs.cat http://<filer_server>:<port>/dir/file_name
`
}

func (c *commandFsCat) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	input := findInputDirectory(args)

	filerServer, filerPort, path, err := commandEnv.parseUrl(input)
	if err != nil {
		return err
	}

	ctx := context.Background()

	if commandEnv.isDirectory(ctx, filerServer, filerPort, path) {
		return fmt.Errorf("%s is a directory", path)
	}

	dir, name := filer2.FullPath(path).DirAndName()

	return commandEnv.withFilerClient(ctx, filerServer, filerPort, func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Name:      name,
			Directory: dir,
		}
		respLookupEntry, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			return err
		}

		return filer2.StreamContent(commandEnv.MasterClient, writer, respLookupEntry.Entry.Chunks, 0, math.MaxInt32)

	})

}
