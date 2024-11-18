package shell

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
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
`
}

func (c *commandFsCat) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsCat) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	if commandEnv.isDirectory(path) {
		return fmt.Errorf("%s is a directory", path)
	}

	dir, name := util.FullPath(path).DirAndName()

	return commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Name:      name,
			Directory: dir,
		}
		respLookupEntry, err := filer_pb.LookupEntry(client, request)
		if err != nil {
			return err
		}

		if len(respLookupEntry.Entry.Content) > 0 {
			_, err = writer.Write(respLookupEntry.Entry.Content)
			return err
		}

		return filer.StreamContent(commandEnv.MasterClient, writer, respLookupEntry.Entry.GetChunks(), 0, int64(filer.FileSize(respLookupEntry.Entry)))

	})

}
