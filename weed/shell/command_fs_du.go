package shell

import (
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandFsDu{})
}

type commandFsDu struct {
}

func (c *commandFsDu) Name() string {
	return "fs.du"
}

func (c *commandFsDu) Help() string {
	return `show disk usage

	fs.du /dir
	fs.du /dir/file_name
	fs.du /dir/file_prefix
`
}

func (c *commandFsDu) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsDu) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	if commandEnv.isDirectory(path) {
		path = path + "/"
	}

	var blockCount, byteCount uint64
	dir, name := util.FullPath(path).DirAndName()
	blockCount, byteCount, err = duTraverseDirectory(writer, commandEnv, dir, name)

	if name == "" && err == nil {
		fmt.Fprintf(writer, "block:%4d\tlogical size:%10d\t%s\n", blockCount, byteCount, dir)
	}

	return

}

func duTraverseDirectory(writer io.Writer, filerClient filer_pb.FilerClient, dir, name string) (blockCount, byteCount uint64, err error) {

	err = filer_pb.ReadDirAllEntries(filerClient, util.FullPath(dir), name, func(entry *filer_pb.Entry, isLast bool) error {

		var fileBlockCount, fileByteCount uint64

		if entry.IsDirectory {
			subDir := fmt.Sprintf("%s/%s", dir, entry.Name)
			if dir == "/" {
				subDir = "/" + entry.Name
			}
			numBlock, numByte, err := duTraverseDirectory(writer, filerClient, subDir, "")
			if err == nil {
				blockCount += numBlock
				byteCount += numByte
			}
		} else {
			fileBlockCount = uint64(len(entry.GetChunks()))
			fileByteCount = filer.FileSize(entry)
			blockCount += fileBlockCount
			byteCount += fileByteCount
		}

		if name != "" && !entry.IsDirectory {
			fmt.Fprintf(writer, "block:%4d\tlogical size:%10d\t%s/%s\n", fileBlockCount, fileByteCount, dir, entry.Name)
		}
		return nil
	})
	return
}
