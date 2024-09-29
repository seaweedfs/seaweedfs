package shell

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"os"
	"time"
)

func init() {
	Commands = append(Commands, &commandFsMkdir{})
}

type commandFsMkdir struct {
}

func (c *commandFsMkdir) Name() string {
	return "fs.mkdir"
}

func (c *commandFsMkdir) Help() string {
	return `create a directory

	fs.mkdir path/to/dir
`
}

func (c *commandFsMkdir) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMkdir) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	dir, name := util.FullPath(path).DirAndName()

	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		_, createErr := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry: &filer_pb.Entry{
				Name:        name,
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0777 | os.ModeDir),
				},
			},
		})
		return createErr
	})

	return
}
