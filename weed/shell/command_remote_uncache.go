package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
	"strings"
)

func init() {
	Commands = append(Commands, &commandRemoteUncache{})
}

type commandRemoteUncache struct {
}

func (c *commandRemoteUncache) Name() string {
	return "remote.uncache"
}

func (c *commandRemoteUncache) Help() string {
	return `keep the metadata but remote cache the file content for mounted directories or files

	remote.uncache -dir=xxx
	remote.uncache -dir=xxx/some/sub/dir

`
}

func (c *commandRemoteUncache) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	remoteMountCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)

	dir := remoteMountCommand.String("dir", "", "a directory in filer")

	if err = remoteMountCommand.Parse(args); err != nil {
		return nil
	}

	mappings, listErr := filer.ReadMountMappings(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress)
	if listErr != nil {
		return listErr
	}
	if *dir == "" {
		jsonPrintln(writer, mappings)
		fmt.Fprintln(writer, "need to specify '-dir' option")
		return nil
	}

	var localMountedDir string
	for k := range mappings.Mappings {
		if strings.HasPrefix(*dir, k) {
			localMountedDir = k
		}
	}
	if localMountedDir == "" {
		jsonPrintln(writer, mappings)
		fmt.Fprintf(writer, "%s is not mounted\n", *dir)
		return nil
	}

	// pull content from remote
	if err = c.uncacheContentData(commandEnv, writer, util.FullPath(*dir)); err != nil {
		return fmt.Errorf("cache content data: %v", err)
	}

	return nil
}

func (c *commandRemoteUncache) uncacheContentData(commandEnv *CommandEnv, writer io.Writer, dirToCache util.FullPath) error {

	return recursivelyTraverseDirectory(commandEnv, dirToCache, func(dir util.FullPath, entry *filer_pb.Entry) bool {
		if !mayHaveCachedToLocal(entry) {
			return true // true means recursive traversal should continue
		}
		entry.RemoteEntry.LocalMtime = 0
		entry.Chunks = nil

		println(dir, entry.Name)

		err := commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
			_, updateErr := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
				Directory: string(dir),
				Entry:     entry,
			})
			return updateErr
		})
		if err != nil {
			fmt.Fprintf(writer, "uncache %+v: %v\n", dir.Child(entry.Name), err)
			return false
		}

		return true
	})
}
