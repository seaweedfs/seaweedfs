package shell

import (
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandFsMetaNotify{})
}

type commandFsMetaNotify struct {
}

func (c *commandFsMetaNotify) Name() string {
	return "fs.meta.notify"
}

func (c *commandFsMetaNotify) Help() string {
	return `recursively send directory and file meta data to notification message queue

	fs.meta.notify	# send meta data from current directory to notification message queue

	The message queue will use it to trigger replication from this filer.

`
}

func (c *commandFsMetaNotify) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMetaNotify) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	util.LoadConfiguration("notification", true)
	v := util.GetViper()
	notification.LoadConfiguration(v, "notification.")

	var dirCount, fileCount uint64

	err = filer_pb.TraverseBfs(commandEnv, util.FullPath(path), func(parentPath util.FullPath, entry *filer_pb.Entry) {

		if entry.IsDirectory {
			dirCount++
		} else {
			fileCount++
		}

		notifyErr := notification.Queue.SendMessage(
			string(parentPath.Child(entry.Name)),
			&filer_pb.EventNotification{
				NewEntry: entry,
			},
		)

		if notifyErr != nil {
			fmt.Fprintf(writer, "fail to notify new entry event for %s: %v\n", parentPath.Child(entry.Name), notifyErr)
		}

	})

	if err == nil {
		fmt.Fprintf(writer, "\ntotal notified %d directories, %d files\n", dirCount, fileCount)
	}

	return err

}
