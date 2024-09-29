package shell

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
	return `remove file and directory entries

	fs.rm [-rf] <entry1> <entry2> ...

	fs.rm /dir/file_name1 dir/file_name2
	fs.rm /dir

	The option "-r" can be recursive.
	The option "-f" can be ignored by recursive error.
`
}

func (c *commandFsRm) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsRm) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	isRecursive := false
	ignoreRecursiveError := false
	var entries []string
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") {
			entries = append(entries, arg)
			continue
		}
		for _, t := range arg {
			switch t {
			case 'r':
				isRecursive = true
			case 'f':
				ignoreRecursiveError = true
			}
		}
	}
	if len(entries) < 1 {
		return fmt.Errorf("need to have arguments")
	}

	commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for _, entry := range entries {
			targetPath, err := commandEnv.parseUrl(entry)
			if err != nil {
				fmt.Fprintf(writer, "rm: %s: %v\n", targetPath, err)
				continue
			}

			targetDir, targetName := util.FullPath(targetPath).DirAndName()

			lookupRequest := &filer_pb.LookupDirectoryEntryRequest{
				Directory: targetDir,
				Name:      targetName,
			}
			_, err = filer_pb.LookupEntry(client, lookupRequest)
			if err != nil {
				fmt.Fprintf(writer, "rm: %s: %v\n", targetPath, err)
				continue
			}

			request := &filer_pb.DeleteEntryRequest{
				Directory:            targetDir,
				Name:                 targetName,
				IgnoreRecursiveError: ignoreRecursiveError,
				IsDeleteData:         true,
				IsRecursive:          isRecursive,
				IsFromOtherCluster:   false,
				Signatures:           nil,
			}
			if resp, err := client.DeleteEntry(context.Background(), request); err != nil {
				fmt.Fprintf(writer, "rm: %s: %v\n", targetPath, err)
			} else {
				if resp.Error != "" {
					fmt.Fprintf(writer, "rm: %s: %v\n", targetPath, resp.Error)
				}
			}
		}
		return nil
	})

	return
}
