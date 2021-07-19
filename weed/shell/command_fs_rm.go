package shell

import (
	"context"
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandFsRm{})
}

type commandFsRm struct {
}

func (c *commandFsRm) Name() string {
	return "fs.rm"
}

func (c *commandFsRm) Help() string {
	return `remove a file or a folder, recursively delete all files and folders

	fs.rm <entry1>

	fs.rm /dir/file_name
	fs.rm /dir
`
}

func (c *commandFsRm) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if len(args) != 1 {
		return fmt.Errorf("need to have arguments")
	}

	targetPath, err := commandEnv.parseUrl(args[0])
	if err != nil {
		return err
	}

	targetDir, targetName := util.FullPath(targetPath).DirAndName()

	return commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.DeleteEntryRequest{
			Directory:            targetDir,
			Name:                 targetName,
			IgnoreRecursiveError: true,
			IsDeleteData:         true,
			IsRecursive:          true,
			IsFromOtherCluster:   false,
			Signatures:           nil,
		}
		_, err = client.DeleteEntry(context.Background(), request)

		if err == nil {
			fmt.Fprintf(writer, "remove: %s\n", targetPath)
		}

		return err

	})

}
