package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
	return `comprehensive synchronization and caching between local and remote storage

	# assume a remote storage is configured to name "cloud1"
	remote.configure -name=cloud1 -type=s3 -s3.access_key=xxx -s3.secret_key=yyy
	# mount and pull one bucket
	remote.mount -dir=/xxx -remote=cloud1/bucket

	# comprehensive sync and cache: update metadata, cache content, and remove deleted files
	remote.cache -dir=/xxx                                # sync metadata, cache content, and remove deleted files (default)
	remote.cache -dir=/xxx -cacheContent=false            # sync metadata and cleanup only, no caching
	remote.cache -dir=/xxx -deleteLocalExtra=false        # skip removal of local files missing from remote
	remote.cache -dir=/xxx -concurrent=32                 # with custom concurrency
	remote.cache -dir=/xxx -include=*.pdf                 # only sync PDF files
	remote.cache -dir=/xxx -exclude=*.tmp                 # exclude temporary files
	remote.cache -dir=/xxx -dryRun=true                   # show what would be done without making changes

	This command will:
	1. Synchronize metadata from remote storage
	2. Cache file content from remote by default
	3. Remove local files that no longer exist on remote by default (use -deleteLocalExtra=false to disable)

	This is designed to run regularly. So you can add it to some cronjob.

`
}

func (c *commandRemoteCache) HasTag(CommandTag) bool {
	return false
}

func (c *commandRemoteCache) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	remoteCacheCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)

	dir := remoteCacheCommand.String("dir", "", "a directory in filer")
	cache := remoteCacheCommand.Bool("cacheContent", true, "cache file content from remote")
	deleteLocalExtra := remoteCacheCommand.Bool("deleteLocalExtra", true, "delete local files that no longer exist on remote")
	concurrency := remoteCacheCommand.Int("concurrent", 16, "concurrent file operations")
	dryRun := remoteCacheCommand.Bool("dryRun", false, "show what would be done without making changes")
	fileFiler := newFileFilter(remoteCacheCommand)

	if err = remoteCacheCommand.Parse(args); err != nil {
		return nil
	}

	if *dir == "" {
		return fmt.Errorf("need to specify -dir option")
	}

	mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, detectErr := detectMountInfo(commandEnv, writer, *dir)
	if detectErr != nil {
		jsonPrintln(writer, mappings)
		return detectErr
	}

	// perform comprehensive sync
	return c.doComprehensiveSync(commandEnv, writer, util.FullPath(localMountedDir), remoteStorageMountedLocation, util.FullPath(*dir), remoteStorageConf, *cache, *deleteLocalExtra, *concurrency, *dryRun, fileFiler)
}

func (c *commandRemoteCache) doComprehensiveSync(commandEnv *CommandEnv, writer io.Writer, localMountedDir util.FullPath, remoteMountedLocation *remote_pb.RemoteStorageLocation, dirToSync util.FullPath, remoteConf *remote_pb.RemoteConf, shouldCache bool, deleteLocalExtra bool, concurrency int, dryRun bool, fileFilter *FileFilter) error {

	// visit remote storage
	remoteStorage, err := remote_storage.GetRemoteStorage(remoteConf)
	if err != nil {
		return err
	}

	remote := filer.MapFullPathToRemoteStorageLocation(localMountedDir, remoteMountedLocation, dirToSync)

	// Step 1: Collect all remote files
	remoteFiles := make(map[string]*filer_pb.RemoteEntry)
	err = remoteStorage.Traverse(remote, func(remoteDir, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
		localDir := filer.MapRemoteStorageLocationPathToFullPath(localMountedDir, remoteMountedLocation, remoteDir)
		fullPath := string(localDir.Child(name))
		remoteFiles[fullPath] = remoteEntry
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to traverse remote storage: %w", err)
	}

	fmt.Fprintf(writer, "Found %d files/directories in remote storage\n", len(remoteFiles))

	// Step 2: Collect all local files (only if we need to delete local extra files)
	localFiles := make(map[string]*filer_pb.Entry)
	if deleteLocalExtra {
		err = recursivelyTraverseDirectory(commandEnv, dirToSync, func(dir util.FullPath, entry *filer_pb.Entry) bool {
			if entry.RemoteEntry != nil { // only consider files that are part of remote mount
				fullPath := string(dir.Child(entry.Name))
				localFiles[fullPath] = entry
			}
			return true
		})
		if err != nil {
			return fmt.Errorf("failed to traverse local directory: %w", err)
		}
		fmt.Fprintf(writer, "Found %d files/directories in local storage\n", len(localFiles))
	} else {
		fmt.Fprintf(writer, "Skipping local file collection (deleteLocalExtra=false)\n")
	}

	// Step 3: Determine actions needed
	var filesToDelete []string
	var filesToUpdate []string
	var filesToCache []string

	// Find files to delete (exist locally but not remotely) - only if deleteLocalExtra is enabled
	if deleteLocalExtra {
		for localPath := range localFiles {
			if _, exists := remoteFiles[localPath]; !exists {
				filesToDelete = append(filesToDelete, localPath)
			}
		}
	}

	// Find files to update/cache (exist remotely)
	for remotePath, remoteEntry := range remoteFiles {
		if deleteLocalExtra {
			// When deleteLocalExtra is enabled, we have localFiles to compare with
			if localEntry, exists := localFiles[remotePath]; exists {
				// File exists locally, check if it needs updating
				if localEntry.RemoteEntry == nil ||
					localEntry.RemoteEntry.RemoteETag != remoteEntry.RemoteETag ||
					localEntry.RemoteEntry.RemoteMtime < remoteEntry.RemoteMtime {
					filesToUpdate = append(filesToUpdate, remotePath)
				}
				// Check if it needs caching
				if shouldCache && shouldCacheToLocal(localEntry) && fileFilter.matches(localEntry) {
					filesToCache = append(filesToCache, remotePath)
				}
			} else {
				// File doesn't exist locally, needs to be created
				filesToUpdate = append(filesToUpdate, remotePath)
			}
		} else {
			// When deleteLocalExtra is disabled, we check each file individually
			// All remote files are candidates for update/creation
			filesToUpdate = append(filesToUpdate, remotePath)

			// For caching, we need to check if the local file exists and needs caching
			if shouldCache {
				// We need to look up the local file to check if it needs caching
				localDir, name := util.FullPath(remotePath).DirAndName()
				err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
					lookupResp, lookupErr := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
						Directory: localDir,
						Name:      name,
					})
					if lookupErr == nil {
						localEntry := lookupResp.Entry
						if shouldCacheToLocal(localEntry) && fileFilter.matches(localEntry) {
							filesToCache = append(filesToCache, remotePath)
						}
					}
					return nil // Don't propagate lookup errors here
				})
				if err != nil {
					// Log error but continue
					fmt.Fprintf(writer, "Warning: failed to lookup local file %s for caching check: %v\n", remotePath, err)
				}
			}
		}
	}

	fmt.Fprintf(writer, "Actions needed: %d files to delete, %d files to update, %d files to cache\n",
		len(filesToDelete), len(filesToUpdate), len(filesToCache))

	if dryRun {
		fmt.Fprintf(writer, "DRY RUN - showing what would be done:\n")
		for _, path := range filesToDelete {
			fmt.Fprintf(writer, "DELETE: %s\n", path)
		}
		for _, path := range filesToUpdate {
			fmt.Fprintf(writer, "UPDATE: %s\n", path)
		}
		for _, path := range filesToCache {
			fmt.Fprintf(writer, "CACHE: %s\n", path)
		}
		return nil
	}

	// Step 4: Execute actions
	return commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		ctx := context.Background()

		// Delete files that no longer exist on remote (only if deleteLocalExtra is enabled)
		if deleteLocalExtra {
			for _, pathToDelete := range filesToDelete {
				fmt.Fprintf(writer, "Deleting %s... ", pathToDelete)

				dir, name := util.FullPath(pathToDelete).DirAndName()
				_, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
					Directory:            dir,
					Name:                 name,
					IgnoreRecursiveError: false,
					IsDeleteData:         true,
					IsRecursive:          false,
					IsFromOtherCluster:   false,
				})
				if err != nil {
					fmt.Fprintf(writer, "failed: %v\n", err)
					return err
				}
				fmt.Fprintf(writer, "done\n")
			}
		}

		// Update metadata for files that exist on remote
		for _, pathToUpdate := range filesToUpdate {
			remoteEntry := remoteFiles[pathToUpdate]
			localDir, name := util.FullPath(pathToUpdate).DirAndName()

			fmt.Fprintf(writer, "Updating metadata for %s... ", pathToUpdate)

			// Check if file exists locally
			lookupResp, lookupErr := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
				Directory: string(localDir),
				Name:      name,
			})

			if lookupErr != nil && lookupErr != filer_pb.ErrNotFound {
				fmt.Fprintf(writer, "failed to lookup: %v\n", lookupErr)
				continue
			}

			isDirectory := remoteEntry.RemoteSize == 0 && remoteEntry.RemoteMtime == 0
			if lookupErr == filer_pb.ErrNotFound {
				// Create new entry
				_, createErr := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
					Directory: string(localDir),
					Entry: &filer_pb.Entry{
						Name:        name,
						IsDirectory: isDirectory,
						Attributes: &filer_pb.FuseAttributes{
							FileSize: uint64(remoteEntry.RemoteSize),
							Mtime:    remoteEntry.RemoteMtime,
							FileMode: uint32(0644),
						},
						RemoteEntry: remoteEntry,
					},
				})
				if createErr != nil {
					fmt.Fprintf(writer, "failed to create: %v\n", createErr)
					continue
				}
			} else {
				// Update existing entry
				existingEntry := lookupResp.Entry
				if existingEntry.RemoteEntry == nil {
					// This is a local file, skip to avoid overwriting
					fmt.Fprintf(writer, "skipped (local file)\n")
					continue
				}

				existingEntry.RemoteEntry = remoteEntry
				existingEntry.Attributes.FileSize = uint64(remoteEntry.RemoteSize)
				existingEntry.Attributes.Mtime = remoteEntry.RemoteMtime
				existingEntry.Attributes.Md5 = nil
				existingEntry.Chunks = nil
				existingEntry.Content = nil

				_, updateErr := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
					Directory: string(localDir),
					Entry:     existingEntry,
				})
				if updateErr != nil {
					fmt.Fprintf(writer, "failed to update: %v\n", updateErr)
					continue
				}
			}
			fmt.Fprintf(writer, "done\n")
		}

		// Cache file content if requested
		if shouldCache && len(filesToCache) > 0 {
			fmt.Fprintf(writer, "Caching file content...\n")

			var wg sync.WaitGroup
			limitedConcurrentExecutor := util.NewLimitedConcurrentExecutor(concurrency)
			var executionErr error

			for _, pathToCache := range filesToCache {
				wg.Add(1)
				pathToCacheCopy := pathToCache // Capture for closure
				limitedConcurrentExecutor.Execute(func() {
					defer wg.Done()

					// Get local entry (either from localFiles map or by lookup)
					var localEntry *filer_pb.Entry
					if deleteLocalExtra {
						localEntry = localFiles[pathToCacheCopy]
						if localEntry == nil {
							fmt.Fprintf(writer, "Warning: skipping cache for %s (local entry not found)\n", pathToCacheCopy)
							return
						}
					} else {
						// Look up the local entry since we don't have it in localFiles
						localDir, name := util.FullPath(pathToCacheCopy).DirAndName()
						lookupErr := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
							lookupResp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
								Directory: localDir,
								Name:      name,
							})
							if err == nil {
								localEntry = lookupResp.Entry
							}
							return err
						})
						if lookupErr != nil {
							fmt.Fprintf(writer, "Warning: failed to lookup local entry for caching %s: %v\n", pathToCacheCopy, lookupErr)
							return
						}
					}

					dir, _ := util.FullPath(pathToCacheCopy).DirAndName()
					remoteLocation := filer.MapFullPathToRemoteStorageLocation(localMountedDir, remoteMountedLocation, util.FullPath(pathToCacheCopy))

					fmt.Fprintf(writer, "Caching %s... ", pathToCacheCopy)

					if _, err := filer.CacheRemoteObjectToLocalCluster(commandEnv, remoteConf, remoteLocation, util.FullPath(dir), localEntry); err != nil {
						fmt.Fprintf(writer, "failed: %v\n", err)
						if executionErr == nil {
							executionErr = err
						}
						return
					}
					fmt.Fprintf(writer, "done\n")
				})
			}

			wg.Wait()
			if executionErr != nil {
				return executionErr
			}
		}

		return nil
	})
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
