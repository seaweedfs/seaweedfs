package shell

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"
)

func init() {
	commands = append(commands, &commandFsLs{})
}

type commandFsLs struct {
}

func (c *commandFsLs) Name() string {
	return "fs.ls"
}

func (c *commandFsLs) Help() string {
	return `list all files under a directory

	fs.ls [-l] [-a] http://<filer_server>:<port>/dir/
	fs.ls [-l] [-a] http://<filer_server>:<port>/dir/file_name
	fs.ls [-l] [-a] http://<filer_server>:<port>/dir/file_prefix
`
}

func (c *commandFsLs) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	var isLongFormat, showHidden bool
	for _, arg := range args {
		switch arg {
		case "-a":
			showHidden = true
		case "-l":
			isLongFormat = true
		}
	}

	input := ""
	if len(args) > 0 {
		input = args[len(args)-1]
		if strings.HasPrefix(input, "-") {
			input = ""
		}
	}

	filerServer, filerPort, path, err := commandEnv.parseUrl(input)
	if err != nil {
		return err
	}
	if input == "" && !strings.HasSuffix(path, "/") {
		path = path + "/"
	}

	dir, name := filer2.FullPath(path).DirAndName()
	// println("path", path, "dir", dir, "name", name)
	if strings.HasSuffix(path, "/") {
		if path == "/" {
			dir, name = "/", ""
		} else {
			dir, name = path[0 : len(path)-1], ""
		}
	}

	ctx := context.Background()

	return commandEnv.withFilerClient(ctx, filerServer, filerPort, func(client filer_pb.SeaweedFilerClient) error {

		return paginateOneDirectory(ctx, writer, client, dir, name, 1000, isLongFormat, showHidden)

	})

}

func paginateOneDirectory(ctx context.Context, writer io.Writer, client filer_pb.SeaweedFilerClient, dir, name string, paginateSize int, isLongFormat, showHidden bool) (err error) {

	entryCount := 0
	paginatedCount := -1
	startFromFileName := ""

	for paginatedCount == -1 || paginatedCount == paginateSize {
		resp, listErr := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          dir,
			Prefix:             name,
			StartFromFileName:  startFromFileName,
			InclusiveStartFrom: false,
			Limit:              uint32(paginateSize),
		})
		if listErr != nil {
			err = listErr
			return
		}

		paginatedCount = len(resp.Entries)

		for _, entry := range resp.Entries {

			if !showHidden && strings.HasPrefix(entry.Name, ".") {
				continue
			}

			entryCount++

			if isLongFormat {
				fileMode := os.FileMode(entry.Attributes.FileMode)
				userName, groupNames := entry.Attributes.UserName, entry.Attributes.GroupName
				if userName == "" {
					if user, userErr := user.LookupId(strconv.Itoa(int(entry.Attributes.Uid))); userErr == nil {
						userName = user.Username
					}
				}
				groupName := ""
				if len(groupNames) > 0 {
					groupName = groupNames[0]
				}
				if groupName == "" {
					if group, groupErr := user.LookupGroupId(strconv.Itoa(int(entry.Attributes.Gid))); groupErr == nil {
						groupName = group.Name
					}
				}

				if dir == "/" {
					// just for printing
					dir = ""
				}
				fmt.Fprintf(writer, "%s %3d %s %s %6d %s/%s\n",
					fileMode, len(entry.Chunks),
					userName, groupName,
					filer2.TotalSize(entry.Chunks), dir, entry.Name)
			} else {
				fmt.Fprintf(writer, "%s\n", entry.Name)
			}

			startFromFileName = entry.Name

		}
	}

	if isLongFormat {
		fmt.Fprintf(writer, "total %d\n", entryCount)
	}

	return

}
