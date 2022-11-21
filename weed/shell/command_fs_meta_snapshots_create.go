package shell

import (
	"flag"
	"fmt"
	cp "github.com/otiai10/copy"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/syndtr/goleveldb/leveldb"
	"io"
)

func init() {
	Commands = append(Commands, &commandFsMetaSnapshotsCreate{})
}

type commandFsMetaSnapshotsCreate struct {
}

func (c *commandFsMetaSnapshotsCreate) Name() string {
	return "fs.meta.snapshots.create"
}

func (c *commandFsMetaSnapshotsCreate) Help() string {
	return `create snapshots of meta data from given time range.

	fs.meta.snapshots.create -s yyyy-mm-dd -e yyyy-mm-dd -o

	//These snapshot maybe later used to backup the system to certain timestamp.

`
}

func (c *commandFsMetaSnapshotsCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	db, err := leveldb.OpenFile("/root/go/repos/seaweedfs/weed/snapshots.db", nil)
	if err != nil {
		return err
	}
	defer db.Close()
	fsMetaSnapshotsCreateCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	if err = fsMetaSnapshotsCreateCommand.Parse(args); err != nil {
		return err
	}

	path := filer.SystemLogDir
	count := 0
	err = filer_pb.TraverseBfs(commandEnv, util.FullPath(path), func(parentPath util.FullPath, entry *filer_pb.Entry) {
		if !entry.IsDirectory {
			fmt.Println("%+v", entry)
			err = db.Put([]byte(entry.GetName()), entry.Content, nil)
			count++
			if count%2 == 0 {
				snapshotName := fmt.Sprintf("/root/go/repos/seaweedfs/weed/snapshots-%d.db", count/2)
				e := cp.Copy("/root/go/repos/seaweedfs/weed/snapshots.db", snapshotName)
				println(snapshotName)
				if e != nil {
					println("failed to generate " + snapshotName + e.Error())
				}
			}
		}
	})
	verifying, err := leveldb.OpenFile("/root/go/repos/seaweedfs/weed/snapshots-1.db", nil)
	if err != nil {
		return err
	}
	iter := verifying.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		fmt.Println("1 ---->")
		fmt.Println(string(key))
	}
	verifying.Close()
	verifying, err = leveldb.OpenFile("/root/go/repos/seaweedfs/weed/snapshots-4.db", nil)
	if err != nil {
		return err
	}
	iter = verifying.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		fmt.Println("4 ---->")
		fmt.Println(string(key))
	}
	verifying.Close()
	return err
}
