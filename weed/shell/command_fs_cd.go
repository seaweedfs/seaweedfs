package shell

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"io"
	"strings"
)

func init() {
	commands = append(commands, &commandFsCd{})
}

type commandFsCd struct {
}

func (c *commandFsCd) Name() string {
	return "fs.cd"
}

func (c *commandFsCd) Help() string {
	return `change directory to http://<filer_server>:<port>/dir/

	The full path can be too long to type. For example,
		fs.ls http://<filer_server>:<port>/some/path/to/file_name

	can be simplified as

		fs.cd http://<filer_server>:<port>/some/path
		fs.ls to/file_name
`
}

func (c *commandFsCd) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	input := ""
	if len(args) > 0 {
		input = args[len(args)-1]
	}

	filerServer, filerPort, path, err := commandEnv.parseUrl(input)
	if err != nil {
		return err
	}

	dir, name := filer2.FullPath(path).DirAndName()
	if strings.HasSuffix(path, "/") {
		if path == "/" {
			dir, name = "/", ""
		} else {
			dir, name = filer2.FullPath(path[0:len(path)-1]).DirAndName()
		}
	}

	ctx := context.Background()

	err = commandEnv.withFilerClient(ctx, filerServer, filerPort, func(client filer_pb.SeaweedFilerClient) error {

		resp, listErr := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          dir,
			Prefix:             name,
			StartFromFileName:  name,
			InclusiveStartFrom: true,
			Limit:              1,
		})
		if listErr != nil {
			return listErr
		}

		if path == "" || path == "/" {
			return nil
		}

		if len(resp.Entries) == 0 {
			return fmt.Errorf("entry not found")
		}

		if resp.Entries[0].Name != name {
			println("path", path, "dir", dir, "name", name, "found", resp.Entries[0].Name)
			return fmt.Errorf("not a valid directory, found %s", resp.Entries[0].Name)
		}

		if !resp.Entries[0].IsDirectory {
			return fmt.Errorf("not a directory")
		}

		return nil
	})

	if err == nil {
		commandEnv.option.FilerHost = filerServer
		commandEnv.option.FilerPort = filerPort
		commandEnv.option.Directory = path
	}

	return err
}
