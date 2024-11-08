package shell

import (
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandFsMv{})
}

type commandFsMv struct {
}

func (c *commandFsMv) Name() string {
	return "fs.mv"
}

func (c *commandFsMv) Help() string {
	return `move or rename a file or a folder

	fs.mv  <source entry> <destination entry> 

	fs.mv /dir/file_name /dir2/filename2
	fs.mv /dir/file_name /dir2

	fs.mv /dir/dir2 /dir3/dir4/
	fs.mv /dir/dir2 /dir3/new_dir

`
}

func (c *commandFsMv) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMv) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if len(args) != 2 {
		return fmt.Errorf("need to have 2 arguments")
	}

	sourcePath, err := commandEnv.parseUrl(args[0])
	if err != nil {
		return err
	}

	destinationPath, err := commandEnv.parseUrl(args[1])
	if err != nil {
		return err
	}

	sourceDir, sourceName := util.FullPath(sourcePath).DirAndName()

	destinationDir, destinationName := util.FullPath(destinationPath).DirAndName()

	return commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		// collect destination entry info
		destinationRequest := &filer_pb.LookupDirectoryEntryRequest{
			Name:      destinationDir,
			Directory: destinationName,
		}
		respDestinationLookupEntry, err := filer_pb.LookupEntry(client, destinationRequest)

		var targetDir, targetName string

		// moving a file or folder
		if err == nil && respDestinationLookupEntry.Entry.IsDirectory {
			// to a directory
			targetDir = util.Join(destinationDir, destinationName)
			targetName = sourceName
		} else {
			// to a file or folder
			targetDir = destinationDir
			targetName = destinationName
		}

		request := &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: sourceDir,
			OldName:      sourceName,
			NewDirectory: targetDir,
			NewName:      targetName,
		}

		_, err = client.AtomicRenameEntry(context.Background(), request)

		fmt.Fprintf(writer, "move: %s => %s\n", sourcePath, util.NewFullPath(targetDir, targetName))

		return err

	})

}
