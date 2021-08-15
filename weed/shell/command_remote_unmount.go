package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
)

func init() {
	Commands = append(Commands, &commandRemoteUnmount{})
}

type commandRemoteUnmount struct {
}

func (c *commandRemoteUnmount) Name() string {
	return "remote.unmount"
}

func (c *commandRemoteUnmount) Help() string {
	return `unmount remote storage

	# assume a remote storage is configured to name "s3_1"
	remote.configure -name=s3_1 -type=s3 -access_key=xxx -secret_key=yyy
	# mount and pull one bucket
	remote.mount -dir=/xxx -remote=s3_1/bucket

	# unmount the mounted directory and remove its cache
	remote.unmount -dir=/xxx

`
}

func (c *commandRemoteUnmount) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

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
		return jsonPrintln(writer, mappings)
	}

	_, found := mappings.Mappings[*dir]
	if !found {
		return fmt.Errorf("directory %s is not mounted", *dir)
	}

	// purge mounted data
	if err = c.purgeMountedData(commandEnv, *dir); err != nil {
		return fmt.Errorf("purge mounted data: %v", err)
	}

	// store a mount configuration in filer
	if err = c.deleteMountMapping(commandEnv, *dir); err != nil {
		return fmt.Errorf("delete mount mapping: %v", err)
	}

	return nil
}

func (c *commandRemoteUnmount) findRemoteStorageConfiguration(commandEnv *CommandEnv, writer io.Writer, remote *filer_pb.RemoteStorageLocation) (conf *filer_pb.RemoteConf, err error) {

	return filer.ReadRemoteStorageConf(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress, remote.Name)

}

func (c *commandRemoteUnmount) purgeMountedData(commandEnv *CommandEnv, dir string) error {

	// find existing directory, and ensure the directory is empty
	err := commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		parent, name := util.FullPath(dir).DirAndName()
		lookupResp, lookupErr := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: parent,
			Name:      name,
		})
		if lookupErr != nil {
			return fmt.Errorf("lookup %s: %v", dir, lookupErr)
		}

		oldEntry := lookupResp.Entry

		deleteError := filer_pb.DoRemove(client, parent, name, true, true, true, false, nil)
		if deleteError != nil {
			return fmt.Errorf("delete %s: %v", dir, deleteError)
		}

		mkdirErr := filer_pb.DoMkdir(client, parent, name, func(entry *filer_pb.Entry) {
			entry.Attributes = oldEntry.Attributes
			entry.Extended = oldEntry.Extended
		})
		if mkdirErr != nil {
			return fmt.Errorf("mkdir %s: %v", dir, mkdirErr)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *commandRemoteUnmount) deleteMountMapping(commandEnv *CommandEnv, dir string) (err error) {

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
	newContent, err = filer.RemoveRemoteStorageMapping(oldContent, dir)
	if err != nil {
		return fmt.Errorf("delete mount %s: %v", dir, err)
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
