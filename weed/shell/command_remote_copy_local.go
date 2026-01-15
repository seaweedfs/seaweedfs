package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandRemoteCopyLocal{})
}

type commandRemoteCopyLocal struct {
}

func (c *commandRemoteCopyLocal) Name() string {
	return "remote.copy.local"
}

func (c *commandRemoteCopyLocal) Help() string {
	return `copy local files to remote storage

	# assume a remote storage is configured to name "cloud1"
	remote.configure -name=cloud1 -type=s3 -s3.access_key=xxx -s3.secret_key=yyy
	# mount and pull one bucket
	remote.mount -dir=/xxx -remote=cloud1/bucket

	# copy local files to remote storage
	remote.copy.local -dir=/xxx                                    # copy all local-only files
	remote.copy.local -dir=/xxx -concurrent=16                     # with custom concurrency
	remote.copy.local -dir=/xxx -include=*.pdf                     # only copy PDF files
	remote.copy.local -dir=/xxx -exclude=*.tmp                     # exclude temporary files
	remote.copy.local -dir=/xxx -dryRun=true                       # show what would be done without making changes
	remote.copy.local -dir=/xxx -forceUpdate=true                  # force update even if remote exists

	This command will:
	1. Find local files that don't exist on remote storage
	2. Copy these files to remote storage
	3. Update local metadata with remote information

	This is useful when:
	- You deleted filer logs and need to copy existing files
	- You have local files that were never copied to remote
	- You want to ensure all local files are backed up to remote

 `
}

func (c *commandRemoteCopyLocal) HasTag(CommandTag) bool {
	return false
}

func (c *commandRemoteCopyLocal) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	remoteCopyLocalCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)

	dir := remoteCopyLocalCommand.String("dir", "", "a directory in filer")
	concurrency := remoteCopyLocalCommand.Int("concurrent", 16, "concurrent file operations")
	dryRun := remoteCopyLocalCommand.Bool("dryRun", false, "show what would be done without making changes")
	forceUpdate := remoteCopyLocalCommand.Bool("forceUpdate", false, "force update even if remote exists")
	fileFilter := newFileFilter(remoteCopyLocalCommand)

	if err = remoteCopyLocalCommand.Parse(args); err != nil {
		return err
	}

	if *dir == "" {
		return fmt.Errorf("need to specify -dir option")
	}

	mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, detectErr := detectMountInfo(commandEnv, writer, *dir)
	if detectErr != nil {
		jsonPrintln(writer, mappings)
		return detectErr
	}

	// perform local to remote copy
	return c.doLocalToRemoteCopy(commandEnv, writer, util.FullPath(localMountedDir), remoteStorageMountedLocation, util.FullPath(*dir), remoteStorageConf, *concurrency, *dryRun, *forceUpdate, fileFilter)
}

func (c *commandRemoteCopyLocal) doLocalToRemoteCopy(commandEnv *CommandEnv, writer io.Writer, localMountedDir util.FullPath, remoteMountedLocation *remote_pb.RemoteStorageLocation, dirToCopy util.FullPath, remoteConf *remote_pb.RemoteConf, concurrency int, dryRun bool, forceUpdate bool, fileFilter *FileFilter) error {

	// Get remote storage client
	remoteStorage, err := remote_storage.GetRemoteStorage(remoteConf)
	if err != nil {
		return fmt.Errorf("failed to get remote storage: %w", err)
	}

	remote := filer.MapFullPathToRemoteStorageLocation(localMountedDir, remoteMountedLocation, dirToCopy)

	// Step 1: Collect all local files that are part of the remote mount
	localFiles := make(map[string]*filer_pb.Entry)
	err = recursivelyTraverseDirectory(commandEnv, dirToCopy, func(dir util.FullPath, entry *filer_pb.Entry) bool {
		// Only consider files that are part of remote mount
		if isInMountedDirectory(dir, localMountedDir) {
			fullPath := string(dir.Child(entry.Name))
			localFiles[fullPath] = entry
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("failed to traverse local directory: %w", err)
	}

	fmt.Fprintf(writer, "Found %d files/directories in local storage\n", len(localFiles))

	// Step 2: Check which files exist on remote storage
	remoteFiles := make(map[string]bool)
	err = remoteStorage.Traverse(remote, func(remoteDir, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
		localDir := filer.MapRemoteStorageLocationPathToFullPath(localMountedDir, remoteMountedLocation, remoteDir)
		fullPath := string(localDir.Child(name))
		remoteFiles[fullPath] = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to traverse remote storage: %w", err)
	}

	fmt.Fprintf(writer, "Found %d files/directories in remote storage\n", len(remoteFiles))

	// Step 3: Determine files to copy
	var filesToCopy []string
	for localPath, localEntry := range localFiles {
		// Skip directories
		if localEntry.IsDirectory {
			continue
		}

		// Apply file filter
		if !fileFilter.matches(localEntry) {
			continue
		}

		// Check if file needs copying
		needsCopy := false
		if !remoteFiles[localPath] {
			// File doesn't exist on remote
			needsCopy = true
		} else if forceUpdate {
			// Force update requested and file exists on remote
			needsCopy = true
		}

		if needsCopy {
			filesToCopy = append(filesToCopy, localPath)
		}
	}

	fmt.Fprintf(writer, "Files to copy: %d\n", len(filesToCopy))

	if dryRun {
		fmt.Fprintf(writer, "DRY RUN - showing what would be done:\n")
		for _, path := range filesToCopy {
			fmt.Fprintf(writer, "COPY: %s\n", path)
		}
		return nil
	}

	// Step 4: Copy files to remote storage
	if len(filesToCopy) == 0 {
		fmt.Fprintf(writer, "No files to copy\n")
		return nil
	}

	var wg sync.WaitGroup
	limitedConcurrentExecutor := util.NewLimitedConcurrentExecutor(concurrency)
	var firstErr error
	var errOnce sync.Once
	var successCount atomic.Int64
	var outputMu sync.Mutex

	for _, pathToCopy := range filesToCopy {
		wg.Add(1)
		pathToCopyCopy := pathToCopy // Capture for closure
		limitedConcurrentExecutor.Execute(func() {
			defer wg.Done()

			localEntry := localFiles[pathToCopyCopy]
			if localEntry == nil {
				outputMu.Lock()
				fmt.Fprintf(writer, "Warning: skipping copy for %s (local entry not found)\n", pathToCopyCopy)
				outputMu.Unlock()
				return
			}

			outputMu.Lock()
			fmt.Fprintf(writer, "Copying %s... ", pathToCopyCopy)
			outputMu.Unlock()

			dir, _ := util.FullPath(pathToCopyCopy).DirAndName()
			remoteLocation := filer.MapFullPathToRemoteStorageLocation(localMountedDir, remoteMountedLocation, util.FullPath(pathToCopyCopy))

			// Copy the file to remote storage
			err := syncFileToRemote(commandEnv, remoteStorage, remoteConf, remoteLocation, util.FullPath(dir), localEntry)
			if err != nil {
				outputMu.Lock()
				fmt.Fprintf(writer, "failed: %v\n", err)
				outputMu.Unlock()
				errOnce.Do(func() {
					firstErr = err
				})
				return
			}

			successCount.Add(1)
			outputMu.Lock()
			fmt.Fprintf(writer, "done\n")
			outputMu.Unlock()
		})
	}

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	fmt.Fprintf(writer, "Successfully copied %d files to remote storage\n", successCount.Load())
	return nil
}

func syncFileToRemote(commandEnv *CommandEnv, remoteStorage remote_storage.RemoteStorageClient, remoteConf *remote_pb.RemoteConf, remoteLocation *remote_pb.RemoteStorageLocation, dir util.FullPath, localEntry *filer_pb.Entry) error {

	// Upload to remote storage using the same approach as filer_remote_sync
	var remoteEntry *filer_pb.RemoteEntry
	var err error

	err = util.Retry("writeFile", func() error {
		// Create a reader for the file content
		reader := filer.NewFileReader(commandEnv, localEntry)

		remoteEntry, err = remoteStorage.WriteFile(remoteLocation, localEntry, reader)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to upload to remote storage: %w", err)
	}

	// Update local entry with remote information
	return commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		ctx := context.Background()

		// Update local entry with remote information
		localEntry.RemoteEntry = remoteEntry

		// Update the entry
		_, updateErr := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
			Directory: string(dir),
			Entry:     localEntry,
		})
		if updateErr != nil {
			return fmt.Errorf("failed to update local entry: %w", updateErr)
		}

		return nil
	})
}

func isInMountedDirectory(dir util.FullPath, mountedDir util.FullPath) bool {
	if string(dir) == string(mountedDir) {
		return true
	}
	// Ensure mountedDir ends with separator to avoid matching sibling directories
	// e.g., "/mnt/remote2" should not match "/mnt/remote"
	mountedDirStr := string(mountedDir)
	if !strings.HasSuffix(mountedDirStr, "/") {
		mountedDirStr += "/"
	}
	return strings.HasPrefix(string(dir)+"/", mountedDirStr)
}
