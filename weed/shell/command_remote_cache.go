package shell

import (
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
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

	# assume a remote storage is configured to name "cloud1"
	remote.configure -name=cloud1 -type=s3 -access_key=xxx -secret_key=yyy
	# mount and pull one bucket
	remote.mount -dir=/xxx -remote=cloud1/bucket

	# after mount, run one of these command to cache the content of the files
	remote.cache -dir=/xxx
	remote.cache -dir=/xxx/some/sub/dir
	remote.cache -dir=/xxx/some/sub/dir -include=*.pdf

	This is designed to run regularly. So you can add it to some cronjob.
	If a file is already synchronized with the remote copy, the file will be skipped to avoid unnecessary copy.

	The actual data copying goes through volume severs.

`
}

func (c *commandRemoteCache) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	remoteMountCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)

	dir := remoteMountCommand.String("dir", "", "a directory in filer")
	fileFiler := newFileFilter(remoteMountCommand)

	if err = remoteMountCommand.Parse(args); err != nil {
		return nil
	}

	mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, detectErr := detectMountInfo(commandEnv, writer, *dir)
	if detectErr != nil{
		jsonPrintln(writer, mappings)
		return detectErr
	}

	// pull content from remote
	if err = c.cacheContentData(commandEnv, writer, util.FullPath(localMountedDir), remoteStorageMountedLocation, util.FullPath(*dir), fileFiler, remoteStorageConf); err != nil {
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
		} else {
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
	if entry.RemoteEntry.LastLocalSyncTsNs == 0 && entry.RemoteEntry.RemoteSize > 0 {
		return true
	}
	return false
}

func mayHaveCachedToLocal(entry *filer_pb.Entry) bool {
	if entry.IsDirectory {
		return false
	}
	if entry.RemoteEntry == nil {
		return false // should not uncache an entry that is not in remote
	}
	if entry.RemoteEntry.LastLocalSyncTsNs > 0 && len(entry.Chunks) > 0 {
		return true
	}
	return false
}

func (c *commandRemoteCache) cacheContentData(commandEnv *CommandEnv, writer io.Writer, localMountedDir util.FullPath, remoteMountedLocation *filer_pb.RemoteStorageLocation, dirToCache util.FullPath, fileFilter *FileFilter, remoteConf *filer_pb.RemoteConf) error {

	return recursivelyTraverseDirectory(commandEnv, dirToCache, func(dir util.FullPath, entry *filer_pb.Entry) bool {
		if !shouldCacheToLocal(entry) {
			return true // true means recursive traversal should continue
		}

		if fileFilter.matches(entry) {
			return true
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
