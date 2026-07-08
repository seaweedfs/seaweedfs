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
)

func init() {
	Commands = append(Commands, &commandNfsEnable{})
}

type commandNfsEnable struct {
}

func (c *commandNfsEnable) Name() string {
	return "nfs.enable"
}

func (c *commandNfsEnable) Help() string {
	return `enable NFS export support for a filer path

	# preview the filer.conf change
	nfs.enable -path=/exports

	# persist it and backfill the index for existing entries
	nfs.enable -path=/exports -apply

	The weed nfs gateway resolves NFS filehandles through an inode->path
	index. This command marks the path with inode_index in filer.conf, so
	every filer maintains index rows for entries under it (no restarts
	needed), then walks the existing entries and writes their rows so
	pre-existing files are immediately resolvable. Reverse with nfs.disable.
`
}

func (c *commandNfsEnable) HasTag(CommandTag) bool {
	return false
}

func (c *commandNfsEnable) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	nfsEnableCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	path := nfsEnableCommand.String("path", "", "filer path to export over NFS")
	apply := nfsEnableCommand.Bool("apply", false, "update filer.conf and backfill the inode index")
	if err = nfsEnableCommand.Parse(args); err != nil {
		return nil
	}
	if !strings.HasPrefix(*path, "/") {
		return fmt.Errorf("-path must be an absolute filer path")
	}

	fc, err := filer.ReadFilerConf(commandEnv.option.FilerAddress, commandEnv.option.GrpcDialOption, commandEnv.MasterClient)
	if err != nil {
		return err
	}
	if err = fc.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: *path,
		InodeIndex:     true,
	}); err != nil {
		return err
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

	return backfillInodeIndex(commandEnv, writer, util.FullPath(*path))
}

func backfillInodeIndex(commandEnv *CommandEnv, writer io.Writer, exportPath util.FullPath) error {
	var indexed, assigned atomic.Int64

	if exportPath != "/" {
		rootEntry, err := lookupPathEntry(commandEnv, exportPath)
		if err == filer_pb.ErrNotFound {
			fmt.Fprintf(writer, "%s does not exist yet; nothing to backfill\n", exportPath)
			return nil
		}
		if err != nil {
			return err
		}
		changed, err := backfillEntryInodeIndex(commandEnv, exportPath, rootEntry, &assigned)
		if err != nil {
			return err
		}
		if changed {
			indexed.Add(1)
		}
	}

	err := filer_pb.TraverseBfs(context.Background(), commandEnv, exportPath, func(parentPath util.FullPath, entry *filer_pb.Entry) error {
		fullPath := util.NewFullPath(string(parentPath), entry.Name)
		changed, err := backfillEntryInodeIndex(commandEnv, fullPath, entry, &assigned)
		if err != nil {
			return err
		}
		if changed {
			if count := indexed.Add(1); count%10000 == 0 {
				fmt.Fprintf(writer, "indexed %d entries ...\n", count)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("backfill inode index under %s: %v", exportPath, err)
	}

	fmt.Fprintf(writer, "indexed %d entries, assigned %d missing inodes\n", indexed.Load(), assigned.Load())
	return nil
}

func backfillEntryInodeIndex(commandEnv *CommandEnv, fullPath util.FullPath, entry *filer_pb.Entry, assigned *atomic.Int64) (changed bool, err error) {
	inode := entry.GetAttributes().GetInode()
	if inode == 0 {
		// Entry predates filer-assigned inodes: re-save it so the filer
		// assigns and persists one, then read the assignment back.
		dir, name := fullPath.DirAndName()
		if err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			if _, updateErr := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
				Directory: dir,
				Entry:     entry,
			}); updateErr != nil {
				return updateErr
			}
			resp, lookupErr := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
				Directory: dir,
				Name:      name,
			})
			if lookupErr != nil {
				return lookupErr
			}
			inode = resp.Entry.GetAttributes().GetInode()
			return nil
		}); err != nil {
			return false, fmt.Errorf("assign inode for %s: %v", fullPath, err)
		}
		if inode == 0 {
			return false, fmt.Errorf("filer did not assign an inode for %s", fullPath)
		}
		assigned.Add(1)
	}
	return upsertInodeIndexRow(commandEnv, fullPath, inode)
}

func lookupPathEntry(commandEnv *CommandEnv, fullPath util.FullPath) (entry *filer_pb.Entry, err error) {
	dir, name := fullPath.DirAndName()
	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, lookupErr := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if lookupErr != nil {
			return lookupErr
		}
		entry = resp.Entry
		return nil
	})
	return
}

func upsertInodeIndexRow(commandEnv *CommandEnv, fullPath util.FullPath, inode uint64) (changed bool, err error) {
	key := filer.InodeIndexKey(inode)
	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, kvErr := client.KvGet(context.Background(), &filer_pb.KvGetRequest{Key: key})
		if kvErr != nil {
			return kvErr
		}
		if resp.GetError() != "" {
			return errors.New(resp.GetError())
		}
		record, decodeErr := filer.DecodeInodeIndexRecord(resp.GetValue())
		if decodeErr != nil {
			// Unreadable row: rebuild it from this path.
			record = &filer.InodeIndexRecord{}
		}
		if !record.AddPath(fullPath) {
			return nil
		}
		value, encodeErr := record.Encode()
		if encodeErr != nil {
			return encodeErr
		}
		putResp, putErr := client.KvPut(context.Background(), &filer_pb.KvPutRequest{Key: key, Value: value})
		if putErr != nil {
			return putErr
		}
		if putResp.GetError() != "" {
			return errors.New(putResp.GetError())
		}
		changed = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("index inode %d for %s: %v", inode, fullPath, err)
	}
	return changed, nil
}
