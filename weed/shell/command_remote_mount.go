package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/remote_storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
	"io"
	"strings"
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

	// find configuration for remote storage
	remoteConf, remotePath, err := c.findRemoteStorageConfiguration(commandEnv, writer, *remote)
	if err != nil {
		return fmt.Errorf("find configuration for %s: %v", *remote, err)
	}

	// pull metadata from remote
	if err = c.pullMetadata(commandEnv, writer, *dir, *nonEmpty, remoteConf, remotePath); err != nil {
		return fmt.Errorf("pull metadata: %v", err)
	}

	// store a mount configuration in filer


	return nil
}

func (c *commandRemoteMount) findRemoteStorageConfiguration(commandEnv *CommandEnv, writer io.Writer, remote string) (conf *filer_pb.RemoteConf, remotePath string, err error) {

	// find remote configuration
	parts := strings.Split(remote, "/")
	if len(parts) == 0 {
		err = fmt.Errorf("wrong remote storage location %s", remote)
		return
	}
	storageName := parts[0]
	remotePath = remote[len(storageName):]

	// read storage configuration data
	var confBytes []byte
	err = commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		confBytes, err = filer.ReadInsideFiler(client, filer.DirectoryEtcRemote, storageName+filer.REMOTE_STORAGE_CONF_SUFFIX)
		return err
	})
	if err != nil {
		err = fmt.Errorf("no remote storage configuration for %s : %v", storageName, err)
		return
	}

	// unmarshal storage configuration
	conf = &filer_pb.RemoteConf{}
	if unMarshalErr := proto.Unmarshal(confBytes, conf); unMarshalErr != nil {
		err = fmt.Errorf("unmarshal %s/%s: %v", filer.DirectoryEtcRemote, storageName, unMarshalErr)
		return
	}

	return
}

func (c *commandRemoteMount) pullMetadata(commandEnv *CommandEnv, writer io.Writer, dir string, nonEmpty bool, remoteConf *filer_pb.RemoteConf, remotePath string) error {

	// find existing directory, and ensure the directory is empty
	var mountToDir *filer_pb.Entry
	err := commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		parent, name := util.FullPath(dir).DirAndName()
		resp, lookupErr := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: parent,
			Name:      name,
		})
		if lookupErr != nil {
			return fmt.Errorf("lookup %s: %v", dir, lookupErr)
		}
		mountToDir = resp.Entry

		mountToDirIsEmpty := false
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
		err = remoteStorage.Traverse(remotePath, func(remoteDir, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
			localDir := dir + remoteDir
			println(util.NewFullPath(localDir, name))

			lookupResponse, lookupErr := filer_pb.LookupEntry(client, &filer_pb.LookupDirectoryEntryRequest{
				Directory: localDir,
				Name:      name,
			})
			if lookupErr != nil {
				if lookupErr != filer_pb.ErrNotFound {
					return lookupErr
				}
			}
			existingEntry := lookupResponse.Entry

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
				if existingEntry.RemoteEntry.ETag != remoteEntry.ETag {
					existingEntry.RemoteEntry = remoteEntry
					existingEntry.Attributes.FileSize = uint64(remoteEntry.Size)
					existingEntry.Attributes.Mtime = remoteEntry.LastModifiedAt
					_, updateErr := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
						Directory: localDir,
						Entry: existingEntry,
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
