package shell

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

func init() {
	Commands = append(Commands, &commandNfsDisable{})
}

type commandNfsDisable struct {
}

func (c *commandNfsDisable) Name() string {
	return "nfs.disable"
}

func (c *commandNfsDisable) Help() string {
	return `disable NFS export support for a filer path

	# preview the filer.conf change
	nfs.disable -path=/exports

	# persist it and delete the inode index rows under the path
	nfs.disable -path=/exports -apply

	Clears the inode_index mark that nfs.enable set on the path, then
	walks the entries under it and removes their inode->path index rows,
	so disabling leaves no index data behind. Pass -keepIndex to skip
	the cleanup walk.
`
}

func (c *commandNfsDisable) HasTag(CommandTag) bool {
	return false
}

func (c *commandNfsDisable) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	nfsDisableCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	path := nfsDisableCommand.String("path", "", "filer path that nfs.enable was applied to")
	apply := nfsDisableCommand.Bool("apply", false, "update filer.conf and delete the inode index rows")
	keepIndex := nfsDisableCommand.Bool("keepIndex", false, "keep existing inode index rows")
	if err = nfsDisableCommand.Parse(args); err != nil {
		return nil
	}
	if !strings.HasPrefix(*path, "/") {
		return fmt.Errorf("-path must be an absolute filer path")
	}

	fc, err := filer.ReadFilerConf(commandEnv.option.FilerAddress, commandEnv.option.GrpcDialOption, commandEnv.MasterClient)
	if err != nil {
		return err
	}

	if locConf, found := fc.GetLocationConf(*path); found && locConf.InodeIndex {
		locConf = filer.ClonePathConf(locConf)
		locConf.InodeIndex = false
		if proto.Equal(locConf, &filer_pb.FilerConf_PathConf{LocationPrefix: *path}) {
			fc.DeleteLocationConf(*path)
		} else if err = fc.SetLocationConf(locConf); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(writer, "inode_index is not set on %s in filer.conf\n", *path)
	}

	var buf bytes.Buffer
	fc.ToText(&buf)
	fmt.Fprint(writer, buf.String())
	fmt.Fprintln(writer)

	if !*apply {
		infoAboutSimulationMode(writer, *apply, "-apply")
		return nil
	}

	if err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(context.Background(), client, filer.DirectoryEtcSeaweedFS, filer.FilerConfName, buf.Bytes())
	}); err != nil && err != filer_pb.ErrNotFound {
		return err
	}

	if *keepIndex {
		return nil
	}
	return cleanupInodeIndex(commandEnv, writer, util.FullPath(*path))
}

func cleanupInodeIndex(commandEnv *CommandEnv, writer io.Writer, exportPath util.FullPath) error {
	var cleared atomic.Int64

	if exportPath != "/" {
		rootEntry, err := lookupPathEntry(commandEnv, exportPath)
		if err == filer_pb.ErrNotFound {
			fmt.Fprintf(writer, "%s does not exist; nothing to clean up\n", exportPath)
			return nil
		}
		if err != nil {
			return err
		}
		if err := removeInodeIndexRow(commandEnv, exportPath, rootEntry.GetAttributes().GetInode(), &cleared); err != nil {
			return err
		}
	}

	err := filer_pb.TraverseBfs(context.Background(), commandEnv, exportPath, func(parentPath util.FullPath, entry *filer_pb.Entry) error {
		fullPath := util.NewFullPath(string(parentPath), entry.Name)
		if err := removeInodeIndexRow(commandEnv, fullPath, entry.GetAttributes().GetInode(), &cleared); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("clean up inode index under %s: %v", exportPath, err)
	}

	fmt.Fprintf(writer, "cleared %d inode index rows\n", cleared.Load())
	return nil
}

func removeInodeIndexRow(commandEnv *CommandEnv, fullPath util.FullPath, inode uint64, cleared *atomic.Int64) error {
	if inode == 0 {
		return nil
	}
	key := filer.InodeIndexKey(inode)
	err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, kvErr := client.KvGet(context.Background(), &filer_pb.KvGetRequest{Key: key})
		if kvErr != nil {
			return kvErr
		}
		if resp.GetError() != "" {
			return errors.New(resp.GetError())
		}
		if len(resp.GetValue()) == 0 {
			return nil
		}
		record, decodeErr := filer.DecodeInodeIndexRecord(resp.GetValue())
		if decodeErr != nil || !record.RemovePath(fullPath) {
			return nil
		}
		// A hard link can keep paths outside the export; only drop the row
		// once no path remains. An empty KvPut value deletes the key.
		var value []byte
		if len(record.Paths) > 0 {
			var encodeErr error
			if value, encodeErr = record.Encode(); encodeErr != nil {
				return encodeErr
			}
		}
		putResp, putErr := client.KvPut(context.Background(), &filer_pb.KvPutRequest{Key: key, Value: value})
		if putErr != nil {
			return putErr
		}
		if putResp.GetError() != "" {
			return errors.New(putResp.GetError())
		}
		cleared.Add(1)
		return nil
	})
	if err != nil {
		return fmt.Errorf("clear inode %d for %s: %v", inode, fullPath, err)
	}
	return nil
}
