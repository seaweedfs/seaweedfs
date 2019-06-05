package shell

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	Commands = append(Commands, &commandFsMetaLoad{})
}

type commandFsMetaLoad struct {
}

func (c *commandFsMetaLoad) Name() string {
	return "fs.meta.load"
}

func (c *commandFsMetaLoad) Help() string {
	return `load saved filer meta data to restore the directory and file structure

	fs.meta.load <filer_host>-<port>-<time>.meta

`
}

func (c *commandFsMetaLoad) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if len(args) == 0 {
		fmt.Fprintf(writer, "missing a metadata file\n")
		return nil
	}

	filerServer, filerPort, path, err := commandEnv.parseUrl(findInputDirectory(nil))
	if err != nil {
		return err
	}

	fileName := args[len(args)-1]

	dst, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil
	}
	defer dst.Close()

	var dirCount, fileCount uint64

	ctx := context.Background()

	err = commandEnv.withFilerClient(ctx, filerServer, filerPort, func(client filer_pb.SeaweedFilerClient) error {

		sizeBuf := make([]byte, 4)

		for {
			if n, err := dst.Read(sizeBuf); n != 4 {
				if err == io.EOF {
					return nil
				}
				return err
			}

			size := util.BytesToUint32(sizeBuf)

			data := make([]byte, int(size))

			if n, err := dst.Read(data); n != len(data) {
				return err
			}

			fullEntry := &filer_pb.FullEntry{}
			if err = proto.Unmarshal(data, fullEntry); err != nil {
				return err
			}

			if _, err = client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
				Directory: fullEntry.Dir,
				Entry:     fullEntry.Entry,
			}); err != nil {
				return err
			}

			fmt.Fprintf(writer, "load %s\n", filer2.FullPath(fullEntry.Dir).Child(fullEntry.Entry.Name))

			if fullEntry.Entry.IsDirectory {
				dirCount++
			} else {
				fileCount++
			}

		}

	})

	if err == nil {
		fmt.Fprintf(writer, "\ntotal %d directories, %d files", dirCount, fileCount)
		fmt.Fprintf(writer, "\n%s is loaded to http://%s:%d%s\n", fileName, filerServer, filerPort, path)
	}

	return err
}
