package shell

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	Commands = append(Commands, &commandFsMetaSave{})
}

type commandFsMetaSave struct {
}

func (c *commandFsMetaSave) Name() string {
	return "fs.meta.save"
}

func (c *commandFsMetaSave) Help() string {
	return `save all directory and file meta data to a local file for metadata backup.

	fs.meta.save /             # save from the root
	fs.meta.save /path/to/save # save from the directory /path/to/save
	fs.meta.save .             # save from current directory
	fs.meta.save               # save from current directory

	The meta data will be saved into a local <filer_host>-<port>-<time>.meta file.
	These meta data can be later loaded by fs.meta.load command, 

	This assumes there are no deletions, so this is different from taking a snapshot.

`
}

func (c *commandFsMetaSave) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	filerServer, filerPort, path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	ctx := context.Background()

	return commandEnv.withFilerClient(ctx, filerServer, filerPort, func(client filer_pb.SeaweedFilerClient) error {

		t := time.Now()
		fileName := fmt.Sprintf("%s-%d-%4d%02d%02d-%02d%02d%02d.meta",
			filerServer, filerPort, t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())

		dst, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return nil
		}
		defer dst.Close()

		var dirCount, fileCount uint64

		sizeBuf := make([]byte, 4)

		err = doTraverse(ctx, writer, client, filer2.FullPath(path), func(parentPath filer2.FullPath, entry *filer_pb.Entry) error {

			protoMessage := &filer_pb.FullEntry{
				Dir:   string(parentPath),
				Entry: entry,
			}

			bytes, err := proto.Marshal(protoMessage)
			if err != nil {
				return fmt.Errorf("marshall error: %v", err)
			}

			util.Uint32toBytes(sizeBuf, uint32(len(bytes)))

			dst.Write(sizeBuf)
			dst.Write(bytes)

			if entry.IsDirectory {
				dirCount++
			} else {
				fileCount++
			}

			println(parentPath.Child(entry.Name))

			return nil

		})

		if err == nil {
			fmt.Fprintf(writer, "\ntotal %d directories, %d files", dirCount, fileCount)
			fmt.Fprintf(writer, "\nmeta data for http://%s:%d%s is saved to %s\n", filerServer, filerPort, path, fileName)
		}

		return err

	})

}
func doTraverse(ctx context.Context, writer io.Writer, client filer_pb.SeaweedFilerClient, parentPath filer2.FullPath, fn func(parentPath filer2.FullPath, entry *filer_pb.Entry) error) (err error) {

	paginatedCount := -1
	startFromFileName := ""
	paginateSize := 1000

	for paginatedCount == -1 || paginatedCount == paginateSize {
		resp, listErr := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          string(parentPath),
			Prefix:             "",
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

			if err = fn(parentPath, entry); err != nil {
				return err
			}

			if entry.IsDirectory {
				subDir := fmt.Sprintf("%s/%s", parentPath, entry.Name)
				if parentPath == "/" {
					subDir = "/" + entry.Name
				}
				if err = doTraverse(ctx, writer, client, filer2.FullPath(subDir), fn); err != nil {
					return err
				}
			}
			startFromFileName = entry.Name

		}
	}

	return

}
