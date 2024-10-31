package shell

import (
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
`
}

func (c *commandFsLs) HasTag(CommandTag) bool {
	return false
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

	path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	if commandEnv.isDirectory(path) {
		path = path + "/"
	}

	dir, name := util.FullPath(path).DirAndName()
	entryCount := 0

	err = filer_pb.ReadDirAllEntries(commandEnv, util.FullPath(dir), name, func(entry *filer_pb.Entry, isLast bool) error {

		if !showHidden && strings.HasPrefix(entry.Name, ".") {
			return nil
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

			if strings.HasSuffix(dir, "/") {
				// just for printing
				dir = dir[:len(dir)-1]
			}
			fmt.Fprintf(writer, "%s %3d %s %s %6d %s/%s\n",
				fileMode, len(entry.GetChunks()),
				userName, groupName,
				filer.FileSize(entry), dir, entry.Name)
		} else {
			fmt.Fprintf(writer, "%s\n", entry.Name)
		}

		return nil
	})

	if isLongFormat && err == nil {
		fmt.Fprintf(writer, "total %d\n", entryCount)
	}

	return
}
