package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"sync"
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
	remote.configure -name=cloud1 -type=s3 -s3.access_key=xxx -s3.secret_key=yyy
	# mount and pull one bucket
	remote.mount -dir=/xxx -remote=cloud1/bucket

	# after mount, run one of these command to cache the content of the files
	remote.cache -dir=/xxx
	remote.cache -dir=/xxx/some/sub/dir
	remote.cache -dir=/xxx/some/sub/dir -include=*.pdf
	remote.cache -dir=/xxx/some/sub/dir -exclude=*.txt
	remote.cache -maxSize=1024000    # cache files smaller than 100K
	remote.cache -maxAge=3600        # cache files less than 1 hour old

	This is designed to run regularly. So you can add it to some cronjob.
	If a file is already synchronized with the remote copy, the file will be skipped to avoid unnecessary copy.

	The actual data copying goes through volume severs in parallel.

`
}

func (c *commandRemoteCache) HasTag(CommandTag) bool {
	return false
}

func (c *commandRemoteCache) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	remoteMountCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)

	dir := remoteMountCommand.String("dir", "", "a mounted directory or one of its sub folders in filer")
	concurrency := remoteMountCommand.Int("concurrent", 32, "concurrent file downloading")
	fileFiler := newFileFilter(remoteMountCommand)

	if err = remoteMountCommand.Parse(args); err != nil {
		return nil
	}

	if *dir != "" {
		if err := c.doCacheOneDirectory(commandEnv, writer, *dir, fileFiler, *concurrency); err != nil {
			return err
		}
		return nil
	}

	mappings, err := filer.ReadMountMappings(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress)
	if err != nil {
		return err
	}

	for key, _ := range mappings.Mappings {
		if err := c.doCacheOneDirectory(commandEnv, writer, key, fileFiler, *concurrency); err != nil {
			return err
		}
	}

	return nil
}

func (c *commandRemoteCache) doCacheOneDirectory(commandEnv *CommandEnv, writer io.Writer, dir string, fileFiler *FileFilter, concurrency int) error {
	mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, detectErr := detectMountInfo(commandEnv, writer, dir)
	if detectErr != nil {
		jsonPrintln(writer, mappings)
		return detectErr
	}

	// pull content from remote
	if err := c.cacheContentData(commandEnv, writer, util.FullPath(localMountedDir), remoteStorageMountedLocation, util.FullPath(dir), fileFiler, remoteStorageConf, concurrency); err != nil {
		return fmt.Errorf("cache content data on %s: %v", localMountedDir, err)
	}

	return nil
}

func recursivelyTraverseDirectory(filerClient filer_pb.FilerClient, dirPath util.FullPath, visitEntry func(dir util.FullPath, entry *filer_pb.Entry) bool) (err error) {

	err = filer_pb.ReadDirAllEntries(context.Background(), filerClient, dirPath, "", func(entry *filer_pb.Entry, isLast bool) error {
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
	if entry.RemoteEntry.LastLocalSyncTsNs > 0 {
		return true
	}
	return false
}

func (c *commandRemoteCache) cacheContentData(commandEnv *CommandEnv, writer io.Writer, localMountedDir util.FullPath, remoteMountedLocation *remote_pb.RemoteStorageLocation, dirToCache util.FullPath, fileFilter *FileFilter, remoteConf *remote_pb.RemoteConf, concurrency int) error {

	var wg sync.WaitGroup
	limitedConcurrentExecutor := util.NewLimitedConcurrentExecutor(concurrency)
	var executionErr error

	traverseErr := recursivelyTraverseDirectory(commandEnv, dirToCache, func(dir util.FullPath, entry *filer_pb.Entry) bool {
		if !shouldCacheToLocal(entry) {
			return true // true means recursive traversal should continue
		}

		if !fileFilter.matches(entry) {
			return true
		}

		wg.Add(1)
		limitedConcurrentExecutor.Execute(func() {
			defer wg.Done()
			fmt.Fprintf(writer, "Cache %+v ...\n", dir.Child(entry.Name))

			remoteLocation := filer.MapFullPathToRemoteStorageLocation(localMountedDir, remoteMountedLocation, dir.Child(entry.Name))

			if err := filer.CacheRemoteObjectToLocalCluster(commandEnv, remoteConf, remoteLocation, dir, entry); err != nil {
				fmt.Fprintf(writer, "CacheRemoteObjectToLocalCluster %+v: %v\n", remoteLocation, err)
				if executionErr == nil {
					executionErr = fmt.Errorf("CacheRemoteObjectToLocalCluster %+v: %v\n", remoteLocation, err)
				}
				return
			}
			fmt.Fprintf(writer, "Cache %+v Done\n", dir.Child(entry.Name))
		})

		return true
	})
	wg.Wait()

	if traverseErr != nil {
		return traverseErr
	}
	if executionErr != nil {
		return executionErr
	}
	return nil
}
