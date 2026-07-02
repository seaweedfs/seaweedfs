package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandRemoteMetaSync{})
}

type commandRemoteMetaSync struct {
}

func (c *commandRemoteMetaSync) Name() string {
	return "remote.meta.sync"
}

func (c *commandRemoteMetaSync) Help() string {
	return `synchronize the local file meta data with the remote file metadata

	# assume a remote storage is configured to name "cloud1"
	remote.configure -name=cloud1 -type=s3 -s3.access_key=xxx -s3.secret_key=yyy
	# mount and pull one bucket
	remote.mount -dir=/xxx -remote=cloud1/bucket

	After mount, if the remote file can be changed, 
	run this command to synchronize the metadata of the mounted folder or any sub folder

		remote.meta.sync -dir=/xxx
		remote.meta.sync -dir=/xxx/some/subdir

	Local metadata for files and directories removed from the remote is also
	removed by default; pass -delete=false to keep it.

	This is designed to run regularly. So you can add it to some cronjob.

	If there are no other operations changing remote files, this operation is not needed.

`
}

func (c *commandRemoteMetaSync) HasTag(CommandTag) bool {
	return false
}

func (c *commandRemoteMetaSync) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	remoteMetaSyncCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)

	dir := remoteMetaSyncCommand.String("dir", "", "a directory in filer")
	deleteStale := remoteMetaSyncCommand.Bool("delete", true, "remove local metadata of files and directories deleted from remote")

	if err = remoteMetaSyncCommand.Parse(args); err != nil {
		return nil
	}

	mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, detectErr := detectMountInfo(commandEnv, writer, *dir)
	if detectErr != nil {
		jsonPrintln(writer, mappings)
		return detectErr
	}

	// pull metadata from remote
	if err = pullMetadata(commandEnv, writer, util.FullPath(localMountedDir), remoteStorageMountedLocation, util.FullPath(*dir), remoteStorageConf, *deleteStale); err != nil {
		return fmt.Errorf("cache meta data: %w", err)
	}

	return nil
}

func detectMountInfo(commandEnv *CommandEnv, writer io.Writer, dir string) (*remote_pb.RemoteStorageMapping, string, *remote_pb.RemoteStorageLocation, *remote_pb.RemoteConf, error) {
	return filer.DetectMountInfo(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress, dir)
}

/*
This function update entry.RemoteEntry if the remote has any changes.

To pull remote updates, or created for the first time, the criteria is:

	entry == nil or (entry.RemoteEntry != nil and (entry.RemoteEntry.RemoteTag != remote.RemoteTag or entry.RemoteEntry.RemoteMTime < remote.RemoteMTime ))

After the meta pull, the entry.RemoteEntry will have:

	remoteEntry.LastLocalSyncTsNs == 0
	Attributes.FileSize = uint64(remoteEntry.RemoteSize)
	Attributes.Mtime = remoteEntry.RemoteMtime
	remoteEntry.RemoteTag   = actual remote tag
	chunks = nil

When reading the file content or pulling the file content in "remote.cache", the criteria is:

	Attributes.FileSize > 0 and len(chunks) == 0

After caching the file content, the entry.RemoteEntry will be

	remoteEntry.LastLocalSyncTsNs == time.Now.UnixNano()
	Attributes.FileSize = uint64(remoteEntry.RemoteSize)
	Attributes.Mtime = remoteEntry.RemoteMtime
	chunks = non-empty

When "weed filer.remote.sync" to upload local changes to remote, the criteria is:

	Attributes.Mtime > remoteEntry.RemoteMtime

Right after "weed filer.remote.sync", the entry.RemoteEntry will be

	remoteEntry.LastLocalSyncTsNs = time.Now.UnixNano()
	remoteEntry.RemoteSize  = actual remote size, which should equal to entry.Attributes.FileSize
	remoteEntry.RemoteMtime = actual remote mtime, which should be a little greater than entry.Attributes.Mtime
	remoteEntry.RemoteTag   = actual remote tag

If entry does not exist, need to pull meta
If entry.RemoteEntry == nil, this is a new local change and should not be overwritten

	If entry.RemoteEntry.RemoteTag != remoteEntry.RemoteTag {
	  the remote version is updated, need to pull meta
	}
*/

// remoteEntryFileMode returns the POSIX mode for an entry synced from a remote
// whose listing carries no mode. It matches what SeaweedFS S3 assigns native
// objects (files 0660) so a mounted bucket matches the source, deriving the
// directory mode with the same 0111 traversal mask the filer uses for
// auto-created parents (0660 -> 0771).
func remoteEntryFileMode(isDirectory bool) uint32 {
	mode := uint32(0660)
	if isDirectory {
		mode = uint32(os.ModeDir) | mode | 0111
	}
	return mode
}

// remoteChild is one entry of a single remote directory level: a file carries
// its RemoteEntry, a directory carries none.
type remoteChild struct {
	isDirectory bool
	remoteEntry *filer_pb.RemoteEntry
}

func pullMetadata(commandEnv *CommandEnv, writer io.Writer, localMountedDir util.FullPath, remoteMountedLocation *remote_pb.RemoteStorageLocation, dirToCache util.FullPath, remoteConf *remote_pb.RemoteConf, deleteStale bool) error {

	remoteStorage, err := remote_storage.GetRemoteStorage(remoteConf)
	if err != nil {
		return err
	}

	remote := filer.MapFullPathToRemoteStorageLocation(localMountedDir, remoteMountedLocation, dirToCache)

	return commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return pullMetadataDirectory(context.Background(), client, writer, remoteStorage, dirToCache, remote, deleteStale)
	})
}

// pullMetadataDirectory reconciles one local directory with its remote
// counterpart and recurses into every remote subdirectory. Listing with a
// delimiter surfaces subdirectories, including empty ones, as their own
// entries, so directories are materialized locally even when they hold no
// files.
func pullMetadataDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, writer io.Writer, remoteStorage remote_storage.RemoteStorageClient, localDir util.FullPath, remoteLoc *remote_pb.RemoteStorageLocation, deleteStale bool) error {

	remoteChildren := make(map[string]*remoteChild)
	if err := remoteStorage.ListDirectory(ctx, remoteLoc, func(dir, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
		remoteChildren[name] = &remoteChild{isDirectory: isDirectory, remoteEntry: remoteEntry}
		return nil
	}); err != nil {
		return err
	}

	localChildren, err := listLocalDirectory(ctx, client, localDir)
	if err != nil {
		return err
	}

	for name, child := range remoteChildren {
		localPath := localDir.Child(name)
		existingEntry := localChildren[name]

		if existingEntry != nil && existingEntry.IsDirectory != child.isDirectory {
			if existingEntry.RemoteEntry == nil {
				// a local change conflicts with a changed remote type; keep local
				fmt.Fprintf(writer, "%s (skip)\n", localPath)
				continue
			}
			// the remote swapped file for directory (or vice versa); drop the
			// stale entry so it is recreated with the right type, but keep any
			// local-only descendants of a directory
			if existingEntry.IsDirectory {
				if err := deleteRemoteBackedEntry(ctx, client, writer, localDir, name, existingEntry); err != nil {
					return err
				}
				refreshed, err := listLocalDirectory(ctx, client, localDir)
				if err != nil {
					return err
				}
				if refreshed[name] != nil {
					// local-only entries remain, so the directory stays and the
					// remote file cannot take its place
					fmt.Fprintf(writer, "%s (skip)\n", localPath)
					continue
				}
			} else {
				fmt.Fprintf(writer, "%s (delete)\n", localPath)
				if err := deleteLocalEntry(ctx, client, localDir, name); err != nil {
					return err
				}
			}
			existingEntry = nil
		}

		if existingEntry == nil {
			if err := createRemoteEntry(ctx, client, writer, localDir, name, child, remoteLoc.Name); err != nil {
				return err
			}
		} else if !child.isDirectory {
			if existingEntry.RemoteEntry == nil {
				// a local change that should not be overwritten
				fmt.Fprintf(writer, "%s (skip)\n", localPath)
			} else if existingEntry.RemoteEntry.RemoteETag != child.remoteEntry.RemoteETag ||
				existingEntry.RemoteEntry.RemoteMtime != child.remoteEntry.RemoteMtime ||
				existingEntry.RemoteEntry.RemoteSize != child.remoteEntry.RemoteSize {
				fmt.Fprintf(writer, "%s (update)\n", localPath)
				if err := doSaveRemoteEntry(client, string(localDir), existingEntry, child.remoteEntry); err != nil {
					return err
				}
			} else {
				fmt.Fprintf(writer, "%s (skip)\n", localPath)
			}
		}

		if child.isDirectory {
			if err := pullMetadataDirectory(ctx, client, writer, remoteStorage, localPath, childRemoteLocation(remoteLoc, name), deleteStale); err != nil {
				return err
			}
		}
	}

	if deleteStale {
		for name, existingEntry := range localChildren {
			if _, ok := remoteChildren[name]; ok {
				continue
			}
			if err := deleteRemoteBackedEntry(ctx, client, writer, localDir, name, existingEntry); err != nil {
				return err
			}
		}
	}

	return nil
}

// deleteRemoteBackedEntry removes a local entry whose remote source is gone. A
// file is deleted; a directory is descended into locally so its remote-backed
// children are cleaned, and the directory itself is removed only when it was
// remote-backed and holds no remaining local-only entries. Entries without a
// RemoteEntry are local changes and are never touched.
func deleteRemoteBackedEntry(ctx context.Context, client filer_pb.SeaweedFilerClient, writer io.Writer, localDir util.FullPath, name string, existingEntry *filer_pb.Entry) error {
	localPath := localDir.Child(name)

	if !existingEntry.IsDirectory {
		if existingEntry.RemoteEntry == nil {
			return nil
		}
		fmt.Fprintf(writer, "%s (delete)\n", localPath)
		return deleteLocalEntry(ctx, client, localDir, name)
	}

	children, err := listLocalDirectory(ctx, client, localPath)
	if err != nil {
		return err
	}
	for childName, childEntry := range children {
		if err := deleteRemoteBackedEntry(ctx, client, writer, localPath, childName, childEntry); err != nil {
			return err
		}
	}

	if existingEntry.RemoteEntry == nil {
		return nil
	}
	empty, err := isLocalDirectoryEmpty(ctx, client, localPath)
	if err != nil {
		return err
	}
	if empty {
		fmt.Fprintf(writer, "%s (delete)\n", localPath)
		return deleteLocalEntry(ctx, client, localDir, name)
	}
	return nil
}

func deleteLocalEntry(ctx context.Context, client filer_pb.SeaweedFilerClient, localDir util.FullPath, name string) error {
	_, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
		Directory:    string(localDir),
		Name:         name,
		IsDeleteData: true,
	})
	return err
}

func isLocalDirectoryEmpty(ctx context.Context, client filer_pb.SeaweedFilerClient, dir util.FullPath) (bool, error) {
	empty := true
	err := filer_pb.SeaweedList(ctx, client, string(dir), "", func(entry *filer_pb.Entry, isLast bool) error {
		empty = false
		return nil
	}, "", false, 1)
	return empty, err
}

func createRemoteEntry(ctx context.Context, client filer_pb.SeaweedFilerClient, writer io.Writer, localDir util.FullPath, name string, child *remoteChild, storageName string) error {
	attributes := &filer_pb.FuseAttributes{
		FileMode: remoteEntryFileMode(child.isDirectory),
		TtlSec:   0, // Remote entries should not have TTL
	}
	remoteEntry := child.remoteEntry
	if child.isDirectory {
		// remote listings carry no directory timestamp; stamp with sync time
		now := time.Now().Unix()
		attributes.Crtime = now
		attributes.Mtime = now
		// mark the directory remote-backed so it can be reconciled and removed
		// once it disappears from the remote
		remoteEntry = &filer_pb.RemoteEntry{StorageName: storageName}
	} else {
		attributes.FileSize = uint64(child.remoteEntry.RemoteSize)
		attributes.Mtime = child.remoteEntry.RemoteMtime
	}
	_, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory: string(localDir),
		Entry: &filer_pb.Entry{
			Name:        name,
			IsDirectory: child.isDirectory,
			Attributes:  attributes,
			RemoteEntry: remoteEntry,
		},
	})
	fmt.Fprintf(writer, "%s (create)\n", localDir.Child(name))
	return err
}

func childRemoteLocation(remoteLoc *remote_pb.RemoteStorageLocation, name string) *remote_pb.RemoteStorageLocation {
	return &remote_pb.RemoteStorageLocation{
		Name:   remoteLoc.Name,
		Bucket: remoteLoc.Bucket,
		Path:   string(util.FullPath(remoteLoc.Path).Child(name)),
	}
}

func listLocalDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, dir util.FullPath) (map[string]*filer_pb.Entry, error) {
	const paginationLimit = 10000
	entries := make(map[string]*filer_pb.Entry)
	lastFileName := ""
	for {
		count := 0
		if err := filer_pb.SeaweedList(ctx, client, string(dir), "", func(entry *filer_pb.Entry, isLast bool) error {
			entries[entry.Name] = entry
			lastFileName = entry.Name
			count++
			return nil
		}, lastFileName, false, paginationLimit); err != nil {
			return nil, err
		}
		if count < paginationLimit {
			return entries, nil
		}
	}
}
