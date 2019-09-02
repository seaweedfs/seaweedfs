package shell

import (
	"context"
	"fmt"
	"io"

	"github.com/joeslay/seaweedfs/weed/filer2"
	"github.com/joeslay/seaweedfs/weed/notification"
	"github.com/joeslay/seaweedfs/weed/pb/filer_pb"
	"github.com/joeslay/seaweedfs/weed/util"
	"github.com/spf13/viper"
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
	return `recursively send directory and file meta data to notifiction message queue

	fs.meta.notify	# send meta data from current directory to notification message queue

	The message queue will use it to trigger replication from this filer.

`
}

func (c *commandFsMetaNotify) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	filerServer, filerPort, path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	util.LoadConfiguration("notification", true)
	v := viper.GetViper()
	notification.LoadConfiguration(v.Sub("notification"))

	ctx := context.Background()

	return commandEnv.withFilerClient(ctx, filerServer, filerPort, func(client filer_pb.SeaweedFilerClient) error {

		var dirCount, fileCount uint64

		err = doTraverse(ctx, writer, client, filer2.FullPath(path), func(parentPath filer2.FullPath, entry *filer_pb.Entry) error {

			if entry.IsDirectory {
				dirCount++
			} else {
				fileCount++
			}

			return notification.Queue.SendMessage(
				string(parentPath.Child(entry.Name)),
				&filer_pb.EventNotification{
					NewEntry: entry,
				},
			)

		})

		if err == nil {
			fmt.Fprintf(writer, "\ntotal notified %d directories, %d files\n", dirCount, fileCount)
		}

		return err

	})

}
