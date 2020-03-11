package shell

import (
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func init() {
	Commands = append(Commands, &commandFsLs{})
}

type commandFsLs struct {
}

func (c *commandFsLs) Name() string {
	return "fs.ls"
}

func (c *commandFsLs) Help() string {
	return `list all files under a directory

	fs.ls [-l] [-a] /dir/
	fs.ls [-l] [-a] /dir/file_name
	fs.ls [-l] [-a] /dir/file_prefix
	fs.ls [-l] [-a] http://<filer_server>:<port>/dir/
	fs.ls [-l] [-a] http://<filer_server>:<port>/dir/file_name
	fs.ls [-l] [-a] http://<filer_server>:<port>/dir/file_prefix
`
}

func (c *commandFsLs) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	var isLongFormat, showHidden bool
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") {
			break
		}
		for _, t := range arg {
			switch t {
			case 'a':
				showHidden = true
			case 'l':
				isLongFormat = true
			}
		}
	}

	input := findInputDirectory(args)

	filerServer, filerPort, path, err := commandEnv.parseUrl(input)
	if err != nil {
		return err
	}

	if commandEnv.isDirectory(filerServer, filerPort, path) {
		path = path + "/"
	}

	dir, name := filer2.FullPath(path).DirAndName()
	entryCount := 0

	err = filer2.ReadDirAllEntries(commandEnv.getFilerClient(filerServer, filerPort), filer2.FullPath(dir), name, func(entry *filer_pb.Entry, isLast bool) {

		if !showHidden && strings.HasPrefix(entry.Name, ".") {
			return
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

	})

	if isLongFormat && err == nil {
		fmt.Fprintf(writer, "total %d\n", entryCount)
	}

	return
}
