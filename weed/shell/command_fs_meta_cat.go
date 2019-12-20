package shell

import (
	"context"
	"fmt"
	"io"

	"github.com/golang/protobuf/jsonpb"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func init() {
	Commands = append(Commands, &commandFsMetaCat{})
}

type commandFsMetaCat struct {
}

func (c *commandFsMetaCat) Name() string {
	return "fs.meta.cat"
}

func (c *commandFsMetaCat) Help() string {
	return `print out the meta data content for a file or directory

	fs.meta.cat /dir/
	fs.meta.cat /dir/file_name
	fs.meta.cat http://<filer_server>:<port>/dir/
	fs.meta.cat http://<filer_server>:<port>/dir/file_name
`
}

func (c *commandFsMetaCat) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	input := findInputDirectory(args)

	filerServer, filerPort, path, err := commandEnv.parseUrl(input)
	if err != nil {
		return err
	}

	ctx := context.Background()

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

		m := jsonpb.Marshaler{
			EmitDefaults: true,
			Indent:       "  ",
		}

		text, marshalErr := m.MarshalToString(respLookupEntry.Entry)
		if marshalErr != nil {
			return fmt.Errorf("marshal meta: %v", marshalErr)
		}

		fmt.Fprintf(writer, "%s\n", text)

		return nil

	})

}
