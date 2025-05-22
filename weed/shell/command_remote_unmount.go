package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"time"
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
	remote.configure -name=s3_1 -type=s3 -s3.access_key=xxx -s3.secret_key=yyy
	# mount and pull one bucket
	remote.mount -dir=/xxx -remote=s3_1/bucket

	# unmount the mounted directory and remove its cache
	remote.unmount -dir=/xxx

`
}

func (c *commandRemoteUnmount) HasTag(CommandTag) bool {
	return false
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

	// store a mount configuration in filer
	fmt.Fprintf(writer, "deleting mount for %s ...\n", *dir)
	if err = filer.DeleteMountMapping(commandEnv, *dir); err != nil {
		return fmt.Errorf("delete mount mapping: %v", err)
	}

	// purge mounted data
	fmt.Fprintf(writer, "purge %s ...\n", *dir)
	if err = c.purgeMountedData(commandEnv, *dir); err != nil {
		return fmt.Errorf("purge mounted data: %v", err)
	}

	// reset remote sync offset in case the folder is mounted again
	if err = remote_storage.SetSyncOffset(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress, *dir, time.Now().UnixNano()); err != nil {
		return fmt.Errorf("reset remote.sync offset for %s: %v", *dir, err)
	}

	return nil
}

func (c *commandRemoteUnmount) purgeMountedData(commandEnv *CommandEnv, dir string) error {

	// find existing directory, and ensure the directory is empty
	err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		ctx := context.Background()
		parent, name := util.FullPath(dir).DirAndName()
		lookupResp, lookupErr := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: parent,
			Name:      name,
		})
		if lookupErr != nil {
			return fmt.Errorf("lookup %s: %v", dir, lookupErr)
		}

		oldEntry := lookupResp.Entry

		deleteError := filer_pb.DoRemove(ctx, client, parent, name, true, true, true, false, nil)
		if deleteError != nil {
			return fmt.Errorf("delete %s: %v", dir, deleteError)
		}

		mkdirErr := filer_pb.DoMkdir(ctx, client, parent, name, func(entry *filer_pb.Entry) {
			entry.Attributes = oldEntry.Attributes
			entry.Extended = oldEntry.Extended
			entry.Attributes.Crtime = time.Now().Unix()
			entry.Attributes.Mtime = time.Now().Unix()
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
