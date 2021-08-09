package shell

import (
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
	"strings"
)

func init() {
	Commands = append(Commands, &commandRemoteCache{})
}

type commandRemoteCache struct {
}

func (c *commandRemoteCache) Name() string {
	return "remote.cache"
}

func (c *commandRemoteCache) Help() string {
	return `cache the file content for mounted directories or files

	# assume a remote storage is configured to name "s3_1"
	remote.configure -name=s3_1 -type=s3 -access_key=xxx -secret_key=yyy
	# mount and pull one bucket
	remote.mount -dir=xxx -remote=s3_1/bucket

	# after mount, run one of these command to cache the content of the files
	remote.cache -dir=xxx
	remote.cache -dir=xxx/some/sub/dir

`
}

func (c *commandRemoteCache) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

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
	var remoteStorageMountedLocation *filer_pb.RemoteStorageLocation
	for k, loc := range mappings.Mappings {
		if strings.HasPrefix(*dir, k) {
			localMountedDir, remoteStorageMountedLocation = k, loc
		}
	}
	if localMountedDir == "" {
		jsonPrintln(writer, mappings)
		fmt.Fprintf(writer, "%s is not mounted\n", *dir)
		return nil
	}

	// find remote storage configuration
	remoteStorageConf, err := filer.ReadRemoteStorageConf(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress, remoteStorageMountedLocation.Name)
	if err != nil {
		return err
	}

	// pull content from remote
	if err = c.cacheContentData(commandEnv, writer, util.FullPath(localMountedDir), remoteStorageMountedLocation, util.FullPath(*dir), remoteStorageConf); err != nil {
		return fmt.Errorf("cache content data: %v", err)
	}

	return nil
}

func recursivelyTraverseDirectory(filerClient filer_pb.FilerClient, dirPath util.FullPath, visitEntry func(dir util.FullPath, entry *filer_pb.Entry) bool) (err error) {

	err = filer_pb.ReadDirAllEntries(filerClient, dirPath, "", func(entry *filer_pb.Entry, isLast bool) error {
		if entry.IsDirectory {
			if !visitEntry(dirPath, entry) {
				return nil
			}
			subDir := dirPath.Child(entry.Name)
			if err := recursivelyTraverseDirectory(filerClient, subDir, visitEntry); err != nil {
				return err
			}
		}else {
			if !visitEntry(dirPath, entry) {
				return nil
			}
		}
		return nil
	})
	return
}

func shouldCacheToLocal(entry *filer_pb.Entry) bool {
	if entry.IsDirectory {
		return false
	}
	if entry.RemoteEntry == nil {
		return false
	}
	if entry.RemoteEntry.LocalMtime == 0 && entry.RemoteEntry.RemoteSize > 0 {
		return true
	}
	return false
}

func mayHaveCachedToLocal(entry *filer_pb.Entry) bool {
	if entry.IsDirectory {
		return false
	}
	if entry.RemoteEntry == nil {
		return false
	}
	if entry.RemoteEntry.LocalMtime > 0 && len(entry.Chunks) > 0 {
		return true
	}
	return false
}

func (c *commandRemoteCache) cacheContentData(commandEnv *CommandEnv, writer io.Writer, localMountedDir util.FullPath, remoteMountedLocation *filer_pb.RemoteStorageLocation, dirToCache util.FullPath, remoteConf *filer_pb.RemoteConf) error {

	return recursivelyTraverseDirectory(commandEnv, dirToCache, func(dir util.FullPath, entry *filer_pb.Entry) bool {
		if !shouldCacheToLocal(entry) {
			return true // true means recursive traversal should continue
		}

		println(dir, entry.Name)

		remoteLocation := filer.MapFullPathToRemoteStorageLocation(localMountedDir, remoteMountedLocation, dir.Child(entry.Name))

		if err := filer.DownloadToLocal(commandEnv, remoteConf, remoteLocation, dir, entry); err != nil {
			fmt.Fprintf(writer, "DownloadToLocal %+v: %v\n", remoteLocation, err)
			return false
		}

		return true
	})
}
