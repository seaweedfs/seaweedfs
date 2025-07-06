package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
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

	if err = remoteMetaSyncCommand.Parse(args); err != nil {
		return nil
	}

	mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, detectErr := detectMountInfo(commandEnv, writer, *dir)
	if detectErr != nil {
		jsonPrintln(writer, mappings)
		return detectErr
	}

	// pull metadata from remote
	if err = pullMetadata(commandEnv, writer, util.FullPath(localMountedDir), remoteStorageMountedLocation, util.FullPath(*dir), remoteStorageConf); err != nil {
		return fmt.Errorf("cache meta data: %v", err)
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
func pullMetadata(commandEnv *CommandEnv, writer io.Writer, localMountedDir util.FullPath, remoteMountedLocation *remote_pb.RemoteStorageLocation, dirToCache util.FullPath, remoteConf *remote_pb.RemoteConf) error {

	// visit remote storage
	remoteStorage, err := remote_storage.GetRemoteStorage(remoteConf)
	if err != nil {
		return err
	}

	remote := filer.MapFullPathToRemoteStorageLocation(localMountedDir, remoteMountedLocation, dirToCache)

	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		ctx := context.Background()
		err = remoteStorage.Traverse(remote, func(remoteDir, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
			localDir := filer.MapRemoteStorageLocationPathToFullPath(localMountedDir, remoteMountedLocation, remoteDir)
			fmt.Fprint(writer, localDir.Child(name))

			lookupResponse, lookupErr := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
				Directory: string(localDir),
				Name:      name,
			})
			var existingEntry *filer_pb.Entry
			if lookupErr != nil {
				if lookupErr != filer_pb.ErrNotFound {
					return lookupErr
				}
			} else {
				existingEntry = lookupResponse.Entry
			}

			if existingEntry == nil {
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
				fmt.Fprintln(writer, " (create)")
				return createErr
			} else {
				if existingEntry.RemoteEntry == nil {
					// this is a new local change and should not be overwritten
					fmt.Fprintln(writer, " (skip)")
					return nil
				}
				if existingEntry.RemoteEntry.RemoteETag != remoteEntry.RemoteETag || existingEntry.RemoteEntry.RemoteMtime < remoteEntry.RemoteMtime {
					// the remote version is updated, need to pull meta
					fmt.Fprintln(writer, " (update)")
					return doSaveRemoteEntry(client, string(localDir), existingEntry, remoteEntry)
				}
			}
			fmt.Fprintln(writer, " (skip)")
			return nil
		})
		return err
	})

	if err != nil {
		return err
	}

	return nil
}
