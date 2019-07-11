package shell

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
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

func (c *commandFsMv) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	filerServer, filerPort, sourcePath, err := commandEnv.parseUrl(args[0])
	if err != nil {
		return err
	}

	_, _, destinationPath, err := commandEnv.parseUrl(args[1])
	if err != nil {
		return err
	}

	ctx := context.Background()


	sourceDir, sourceName := filer2.FullPath(sourcePath).DirAndName()

	destinationDir, destinationName := filer2.FullPath(destinationPath).DirAndName()


	return commandEnv.withFilerClient(ctx, filerServer, filerPort, func(client filer_pb.SeaweedFilerClient) error {

		// collect destination entry info
		destinationRequest := &filer_pb.LookupDirectoryEntryRequest{
			Name:      destinationDir,
			Directory: destinationName,
		}
		respDestinationLookupEntry, err := client.LookupDirectoryEntry(ctx, destinationRequest)

		var targetDir, targetName string

		// moving a file or folder
		if err == nil && respDestinationLookupEntry.Entry.IsDirectory {
			// to a directory
			targetDir = filepath.ToSlash(filepath.Join(destinationDir, destinationName))
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

		_, err = client.AtomicRenameEntry(ctx, request)

		fmt.Fprintf(writer, "move: %s => %s\n", sourcePath, filer2.NewFullPath(targetDir, targetName))

		return err

	})

}
