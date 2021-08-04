package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/remote_storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/jsonpb"
	"io"
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

	# assume a remote storage is configured to name "s3_1"
	remote.configure -name=s3_1 -type=s3 -access_key=xxx -secret_key=yyy

	# mount and pull one bucket
	remote.mount -dir=xxx -remote=s3_1/bucket
	# mount and pull one directory in the bucket
	remote.mount -dir=xxx -remote=s3_1/bucket/dir1

`
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
		return c.listExistingRemoteStorageMounts(commandEnv, writer)
	}

	remoteStorageLocation := remote_storage.ParseLocation(*remote)

	// find configuration for remote storage
	// remotePath is /<bucket>/path/to/dir
	remoteConf, err := c.findRemoteStorageConfiguration(commandEnv, writer, remoteStorageLocation)
	if err != nil {
		return fmt.Errorf("find configuration for %s: %v", *remote, err)
	}

	// pull metadata from remote
	if err = c.pullMetadata(commandEnv, writer, *dir, *nonEmpty, remoteConf, remoteStorageLocation); err != nil {
		return fmt.Errorf("pull metadata: %v", err)
	}

	// store a mount configuration in filer
	if err = c.saveMountMapping(commandEnv, writer, *dir, remoteStorageLocation); err != nil {
		return fmt.Errorf("save mount mapping: %v", err)
	}

	return nil
}

func (c *commandRemoteMount) listExistingRemoteStorageMounts(commandEnv *CommandEnv, writer io.Writer) (err error) {

	// read current mapping
	mappings, readErr := filer.ReadMountMappings(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress)
	if readErr != nil {
		return readErr
	}

	m := jsonpb.Marshaler{
		EmitDefaults: false,
		Indent:       "  ",
	}

	return m.Marshal(writer, mappings)

}

func (c *commandRemoteMount) findRemoteStorageConfiguration(commandEnv *CommandEnv, writer io.Writer, remote *filer_pb.RemoteStorageLocation) (conf *filer_pb.RemoteConf, err error) {

	return filer.ReadRemoteStorageConf(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress, remote.Name)

}

func (c *commandRemoteMount) pullMetadata(commandEnv *CommandEnv, writer io.Writer, dir string, nonEmpty bool, remoteConf *filer_pb.RemoteConf, remote *filer_pb.RemoteStorageLocation) error {

	// find existing directory, and ensure the directory is empty
	err := commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		parent, name := util.FullPath(dir).DirAndName()
		_, lookupErr := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: parent,
			Name:      name,
		})
		if lookupErr != nil {
			return fmt.Errorf("lookup %s: %v", dir, lookupErr)
		}

		mountToDirIsEmpty := true
		listErr := filer_pb.SeaweedList(client, dir, "", func(entry *filer_pb.Entry, isLast bool) error {
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

	// visit remote storage
	remoteStorage, err := remote_storage.GetRemoteStorage(remoteConf)
	if err != nil {
		return err
	}

	err = commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		ctx := context.Background()
		err = remoteStorage.Traverse(remote, func(remoteDir, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
			localDir := dir + remoteDir
			println(util.NewFullPath(localDir, name))

			lookupResponse, lookupErr := filer_pb.LookupEntry(client, &filer_pb.LookupDirectoryEntryRequest{
				Directory: localDir,
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
					Directory: localDir,
					Entry: &filer_pb.Entry{
						Name:        name,
						IsDirectory: isDirectory,
						Attributes: &filer_pb.FuseAttributes{
							FileSize: uint64(remoteEntry.Size),
							Mtime:    remoteEntry.LastModifiedAt,
							FileMode: uint32(0644),
						},
						RemoteEntry: remoteEntry,
					},
				})
				return createErr
			} else {
				if existingEntry.RemoteEntry == nil || existingEntry.RemoteEntry.ETag != remoteEntry.ETag {
					existingEntry.RemoteEntry = remoteEntry
					existingEntry.Attributes.FileSize = uint64(remoteEntry.Size)
					existingEntry.Attributes.Mtime = remoteEntry.LastModifiedAt
					_, updateErr := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
						Directory: localDir,
						Entry:     existingEntry,
					})
					return updateErr
				}
			}
			return nil
		})
		return err
	})

	if err != nil {
		return err
	}

	return nil
}

func (c *commandRemoteMount) saveMountMapping(commandEnv *CommandEnv, writer io.Writer, dir string, remoteStorageLocation *filer_pb.RemoteStorageLocation) (err error) {

	// read current mapping
	var oldContent, newContent []byte
	err = commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		oldContent, err = filer.ReadInsideFiler(client, filer.DirectoryEtcRemote, filer.REMOTE_STORAGE_MOUNT_FILE)
		return err
	})
	if err != nil {
		if err != filer_pb.ErrNotFound {
			return fmt.Errorf("read existing mapping: %v", err)
		}
	}

	// add new mapping
	newContent, err = filer.AddRemoteStorageMapping(oldContent, dir, remoteStorageLocation)
	if err != nil {
		return fmt.Errorf("add mapping %s~%s: %v", dir, remoteStorageLocation, err)
	}

	// save back
	err = commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, filer.DirectoryEtcRemote, filer.REMOTE_STORAGE_MOUNT_FILE, newContent)
	})
	if err != nil {
		return fmt.Errorf("save mapping: %v", err)
	}

	return nil
}
