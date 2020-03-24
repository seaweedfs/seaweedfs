package shell

import (
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
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

func (c *commandFsDu) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	filerServer, filerPort, path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	if commandEnv.isDirectory(filerServer, filerPort, path) {
		path = path + "/"
	}

	var blockCount, byteCount uint64
	dir, name := util.FullPath(path).DirAndName()
	blockCount, byteCount, err = duTraverseDirectory(writer, commandEnv.getFilerClient(filerServer, filerPort), dir, name)

	if name == "" && err == nil {
		fmt.Fprintf(writer, "block:%4d\tbyte:%10d\t%s\n", blockCount, byteCount, dir)
	}

	return

}

func duTraverseDirectory(writer io.Writer, filerClient filer_pb.FilerClient, dir, name string) (blockCount, byteCount uint64, err error) {

	err = filer_pb.ReadDirAllEntries(filerClient, util.FullPath(dir), name, func(entry *filer_pb.Entry, isLast bool) {
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
			blockCount += uint64(len(entry.Chunks))
			byteCount += filer2.TotalSize(entry.Chunks)
		}

		if name != "" && !entry.IsDirectory {
			fmt.Fprintf(writer, "block:%4d\tbyte:%10d\t%s/%s\n", blockCount, byteCount, dir, name)
		}
	})
	return
}

func (env *CommandEnv) withFilerClient(filerServer string, filerPort int64, fn func(filer_pb.SeaweedFilerClient) error) error {

	filerGrpcAddress := fmt.Sprintf("%s:%d", filerServer, filerPort+10000)
	return pb.WithGrpcFilerClient(filerGrpcAddress, env.option.GrpcDialOption, fn)

}

type commandFilerClient struct {
	env         *CommandEnv
	filerServer string
	filerPort   int64
}

func (env *CommandEnv) getFilerClient(filerServer string, filerPort int64) *commandFilerClient {
	return &commandFilerClient{
		env:         env,
		filerServer: filerServer,
		filerPort:   filerPort,
	}
}
func (c *commandFilerClient) WithFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {
	return c.env.withFilerClient(c.filerServer, c.filerPort, fn)
}
func (c *commandFilerClient) AdjustedUrl(hostAndPort string) string {
	return hostAndPort
}
