package shell

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	weed_server "github.com/chrislusf/seaweedfs/weed/server"
)

func init() {
	commands = append(commands, &commandFsCat{})
}

type commandFsCat struct {
}

func (c *commandFsCat) Name() string {
	return "fs.cat"
}

func (c *commandFsCat) Help() string {
	return `stream the file content on to the screen

	fs.cat /dir/
	fs.cat /dir/file_name
	fs.cat /dir/file_prefix
	fs.cat http://<filer_server>:<port>/dir/
	fs.cat http://<filer_server>:<port>/dir/file_name
	fs.cat http://<filer_server>:<port>/dir/file_prefix
`
}

func (c *commandFsCat) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

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

	return commandEnv.withFilerClient(ctx, filerServer, filerPort, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Name:      name,
			Directory: dir,
		}
		respLookupEntry, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			return err
		}

		return weed_server.StreamContent(commandEnv.masterClient, writer, respLookupEntry.Entry.Chunks, 0, math.MaxInt32)

	})

}
