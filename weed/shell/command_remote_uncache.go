package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

	This is designed to run regularly. So you can add it to some cronjob.
	If a file is not synchronized with the remote copy, the file will be skipped to avoid loss of data.

	remote.uncache -dir=/xxx
	remote.uncache -dir=/xxx/some/sub/dir
	remote.uncache -dir=/xxx/some/sub/dir -include=*.pdf
	remote.uncache -dir=/xxx/some/sub/dir -exclude=*.txt
	remote.uncache -minSize=1024000    # uncache files larger than 100K
	remote.uncache -minAge=3600        # uncache files older than 1 hour

`
}

func (c *commandRemoteUncache) HasTag(CommandTag) bool {
	return false
}

func (c *commandRemoteUncache) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	remoteUncacheCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)

	dir := remoteUncacheCommand.String("dir", "", "a directory in filer")
	fileFiler := newFileFilter(remoteUncacheCommand)

	if err = remoteUncacheCommand.Parse(args); err != nil {
		return nil
	}

	mappings, listErr := filer.ReadMountMappings(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress)
	if listErr != nil {
		return listErr
	}
	if *dir != "" {
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
		if err = c.uncacheContentData(commandEnv, writer, util.FullPath(*dir), fileFiler); err != nil {
			return fmt.Errorf("uncache content data: %v", err)
		}
		return nil
	}

	for key, _ := range mappings.Mappings {
		if err := c.uncacheContentData(commandEnv, writer, util.FullPath(key), fileFiler); err != nil {
			return err
		}
	}

	return nil
}

func (c *commandRemoteUncache) uncacheContentData(commandEnv *CommandEnv, writer io.Writer, dirToCache util.FullPath, fileFilter *FileFilter) error {

	return recursivelyTraverseDirectory(commandEnv, dirToCache, func(dir util.FullPath, entry *filer_pb.Entry) bool {

		if !mayHaveCachedToLocal(entry) {
			return true // true means recursive traversal should continue
		}

		if !fileFilter.matches(entry) {
			return true
		}

		if entry.RemoteEntry.LastLocalSyncTsNs/1e9 < entry.Attributes.Mtime {
			return true // should not uncache an entry that is not synchronized with remote
		}

		entry.RemoteEntry.LastLocalSyncTsNs = 0
		entry.Chunks = nil

		fmt.Fprintf(writer, "Uncache %+v ... ", dir.Child(entry.Name))

		err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
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
		fmt.Fprintf(writer, "Done\n")

		return true
	})
}

type FileFilter struct {
	include *string
	exclude *string
	minSize *int64
	maxSize *int64
	minAge  *int64
	maxAge  *int64
}

func newFileFilter(remoteMountCommand *flag.FlagSet) (ff *FileFilter) {
	ff = &FileFilter{}
	ff.include = remoteMountCommand.String("include", "", "pattens of file names, e.g., *.pdf, *.html, ab?d.txt")
	ff.exclude = remoteMountCommand.String("exclude", "", "pattens of file names, e.g., *.pdf, *.html, ab?d.txt")
	ff.minSize = remoteMountCommand.Int64("minSize", -1, "minimum file size in bytes")
	ff.maxSize = remoteMountCommand.Int64("maxSize", -1, "maximum file size in bytes")
	ff.minAge = remoteMountCommand.Int64("minAge", -1, "minimum file age in seconds")
	ff.maxAge = remoteMountCommand.Int64("maxAge", -1, "maximum file age in seconds")
	return
}

func (ff *FileFilter) matches(entry *filer_pb.Entry) bool {
	if *ff.include != "" {
		if ok, _ := filepath.Match(*ff.include, entry.Name); !ok {
			return false
		}
	}
	if *ff.exclude != "" {
		if ok, _ := filepath.Match(*ff.exclude, entry.Name); ok {
			return false
		}
	}
	if *ff.minSize != -1 {
		if int64(entry.Attributes.FileSize) < *ff.minSize {
			return false
		}
	}
	if *ff.maxSize != -1 {
		if int64(entry.Attributes.FileSize) > *ff.maxSize {
			return false
		}
	}
	if *ff.minAge != -1 {
		if entry.Attributes.Crtime+*ff.minAge > time.Now().Unix() {
			return false
		}
	}
	if *ff.maxAge != -1 {
		if entry.Attributes.Crtime+*ff.maxAge < time.Now().Unix() {
			return false
		}
	}
	return true
}
