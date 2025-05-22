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
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"strings"
	"time"
)

func init() {
	Commands = append(Commands, &commandRemoteMount{})
}

type commandRemoteMount struct {
}

func (c *commandRemoteMount) Name() string {
	return "remote.mount"
}

func (c *commandRemoteMount) Help() string {
	return `mount remote storage and pull its metadata

	# assume a remote storage is configured to name "cloud1"
	remote.configure -name=cloud1 -type=s3 -s3.access_key=xxx -s3.secret_key=yyy

	# mount and pull one bucket
	remote.mount -dir=/xxx -remote=cloud1/bucket
	# mount and pull one directory in the bucket
	remote.mount -dir=/xxx -remote=cloud1/bucket/dir1

	# after mount, start a separate process to write updates to remote storage
	weed filer.remote.sync -filer=<filerHost>:<filerPort> -dir=/xxx

`
}

func (c *commandRemoteMount) HasTag(CommandTag) bool {
	return false
}

func (c *commandRemoteMount) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	remoteMountCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)

	dir := remoteMountCommand.String("dir", "", "a directory in filer")
	nonEmpty := remoteMountCommand.Bool("nonempty", false, "allows the mounting over a non-empty directory")
	remote := remoteMountCommand.String("remote", "", "a directory in remote storage, ex. <storageName>/<bucket>/path/to/dir")

	if err = remoteMountCommand.Parse(args); err != nil {
		return nil
	}

	if *dir == "" {
		_, err = listExistingRemoteStorageMounts(commandEnv, writer)
		return err
	}

	// find configuration for remote storage
	remoteConf, err := filer.ReadRemoteStorageConf(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress, remote_storage.ParseLocationName(*remote))
	if err != nil {
		return fmt.Errorf("find configuration for %s: %v", *remote, err)
	}

	remoteStorageLocation, err := remote_storage.ParseRemoteLocation(remoteConf.Type, *remote)
	if err != nil {
		return err
	}

	// sync metadata from remote
	if err = syncMetadata(commandEnv, writer, *dir, *nonEmpty, remoteConf, remoteStorageLocation); err != nil {
		return fmt.Errorf("pull metadata: %v", err)
	}

	// store a mount configuration in filer
	if err = filer.InsertMountMapping(commandEnv, *dir, remoteStorageLocation); err != nil {
		return fmt.Errorf("save mount mapping: %v", err)
	}

	return nil
}

func listExistingRemoteStorageMounts(commandEnv *CommandEnv, writer io.Writer) (mappings *remote_pb.RemoteStorageMapping, err error) {

	// read current mapping
	mappings, err = filer.ReadMountMappings(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress)
	if err != nil {
		return mappings, err
	}

	jsonPrintln(writer, mappings)

	return

}

func jsonPrintln(writer io.Writer, message proto.Message) error {
	return filer.ProtoToText(writer, message)
}

func syncMetadata(commandEnv *CommandEnv, writer io.Writer, dir string, nonEmpty bool, remoteConf *remote_pb.RemoteConf, remote *remote_pb.RemoteStorageLocation) error {

	// find existing directory, and ensure the directory is empty
	err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		parent, name := util.FullPath(dir).DirAndName()
		_, lookupErr := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: parent,
			Name:      name,
		})
		if lookupErr != nil {
			if strings.Contains(lookupErr.Error(), filer_pb.ErrNotFound.Error()) {
				_, createErr := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
					Directory: parent,
					Entry: &filer_pb.Entry{
						Name:        name,
						IsDirectory: true,
						Attributes: &filer_pb.FuseAttributes{
							Mtime:    time.Now().Unix(),
							Crtime:   time.Now().Unix(),
							FileMode: uint32(0644 | os.ModeDir),
						},
						RemoteEntry: &filer_pb.RemoteEntry{
							StorageName: remoteConf.Name,
						},
					},
				})
				return createErr
			}
		}

		mountToDirIsEmpty := true
		listErr := filer_pb.SeaweedList(context.Background(), client, dir, "", func(entry *filer_pb.Entry, isLast bool) error {
			mountToDirIsEmpty = false
			return nil
		}, "", false, 1)

		if listErr != nil {
			return fmt.Errorf("list %s: %v", dir, listErr)
		}

		if !mountToDirIsEmpty {
			if !nonEmpty {
				return fmt.Errorf("dir %s is not empty", dir)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// pull metadata from remote
	if err = pullMetadata(commandEnv, writer, util.FullPath(dir), remote, util.FullPath(dir), remoteConf); err != nil {
		return fmt.Errorf("cache metadata: %v", err)
	}

	return nil
}

// if an entry has synchronized metadata but has not synchronized content
//
//	entry.Attributes.FileSize == entry.RemoteEntry.RemoteSize
//	entry.Attributes.Mtime    == entry.RemoteEntry.RemoteMtime
//	entry.RemoteEntry.LastLocalSyncTsNs == 0
//
// if an entry has synchronized metadata but has synchronized content before
//
//	entry.Attributes.FileSize == entry.RemoteEntry.RemoteSize
//	entry.Attributes.Mtime    == entry.RemoteEntry.RemoteMtime
//	entry.RemoteEntry.LastLocalSyncTsNs > 0
//
// if an entry has synchronized metadata but has new updates
//
//	entry.Attributes.Mtime * 1,000,000,000    > entry.RemoteEntry.LastLocalSyncTsNs
func doSaveRemoteEntry(client filer_pb.SeaweedFilerClient, localDir string, existingEntry *filer_pb.Entry, remoteEntry *filer_pb.RemoteEntry) error {
	existingEntry.RemoteEntry = remoteEntry
	existingEntry.Attributes.FileSize = uint64(remoteEntry.RemoteSize)
	existingEntry.Attributes.Mtime = remoteEntry.RemoteMtime
	existingEntry.Attributes.Md5 = nil
	existingEntry.Chunks = nil
	existingEntry.Content = nil
	_, updateErr := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
		Directory: localDir,
		Entry:     existingEntry,
	})
	if updateErr != nil {
		return updateErr
	}
	return nil
}
