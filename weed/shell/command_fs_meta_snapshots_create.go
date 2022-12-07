package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	filer_leveldb "github.com/seaweedfs/seaweedfs/weed/filer/leveldb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"path/filepath"
	"time"
)

const LevelDbPath = "/tmp/snapshots.db"
const SnapshotsParentPath = "/seaweedfs_meta_snapshots/"
const DateFormat = "2006-01-02"

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

	fs.meta.snapshots.create -s=2022-08-09 -e=2022-10-12 -f 7   create snapshot starting from 2022-08-09 ending at 2022-10-12 every seven days.

	//These snapshot maybe later used to backup the system to certain timestamp.
`
}

func processMetaDataEvents(store *filer_leveldb.LevelDBStore, data []byte, snapshotCnt int, snapshotCheckPoints []time.Time, homeDir string) (err error) {
	var event filer_pb.SubscribeMetadataResponse
	err = proto.Unmarshal(data, &event)
	if err != nil {
		return err
	}
	fmt.Printf("%+v\n", event)
	eventTime := event.TsNs
	println(time.Unix(0, eventTime).Format(DateFormat))
	for snapshotCnt < len(snapshotCheckPoints) && time.Unix(0, eventTime).After(snapshotCheckPoints[snapshotCnt]) {
		snapshotPath := homeDir + SnapshotsParentPath + snapshotCheckPoints[snapshotCnt].Format(DateFormat)
		err = CreateIfNotExists(snapshotPath, 0755)
		if err != nil {
			return err
		}
		println("generating snapshots of metadata at: " + snapshotPath)
		err = generateSnapshots(homeDir+LevelDbPath, snapshotPath)
		if err != nil {
			return err
		}
		snapshotCnt++
	}
	if snapshotCnt == len(snapshotCheckPoints) {
		return nil
	}
	ctx := context.Background()
	if filer_pb.IsEmpty(&event) {
		return nil
	} else if filer_pb.IsCreate(&event) {
		println("+", util.FullPath(event.EventNotification.NewParentPath).Child(event.EventNotification.NewEntry.Name))
		entry := filer.FromPbEntry(event.EventNotification.NewParentPath, event.EventNotification.NewEntry)
		return store.InsertEntry(ctx, entry)
	} else if filer_pb.IsDelete(&event) {
		println("-", util.FullPath(event.Directory).Child(event.EventNotification.OldEntry.Name))
		return store.DeleteEntry(ctx, util.FullPath(event.Directory).Child(event.EventNotification.OldEntry.Name))
	} else if filer_pb.IsUpdate(&event) {
		println("~", util.FullPath(event.EventNotification.NewParentPath).Child(event.EventNotification.NewEntry.Name))
		entry := filer.FromPbEntry(event.EventNotification.NewParentPath, event.EventNotification.NewEntry)
		return store.UpdateEntry(ctx, entry)
	} else {
		// renaming
		println("-", util.FullPath(event.Directory).Child(event.EventNotification.OldEntry.Name))
		if err := store.DeleteEntry(ctx, util.FullPath(event.Directory).Child(event.EventNotification.OldEntry.Name)); err != nil {
			return err
		}
		println("+", util.FullPath(event.EventNotification.NewParentPath).Child(event.EventNotification.NewEntry.Name))
		return store.InsertEntry(ctx, filer.FromPbEntry(event.EventNotification.NewParentPath, event.EventNotification.NewEntry))
	}
	return nil
}

func generateSnapshots(scrDir, dest string) error {
	entries, err := os.ReadDir(scrDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		sourcePath := filepath.Join(scrDir, entry.Name())
		destPath := filepath.Join(dest, entry.Name())

		fileInfo, err := os.Stat(sourcePath)
		if err != nil {
			return err
		}

		switch fileInfo.Mode() & os.ModeType {
		case os.ModeDir:
			if err := CreateIfNotExists(destPath, 0755); err != nil {
				return err
			}
			if err := generateSnapshots(sourcePath, destPath); err != nil {
				return err
			}
		default:
			if err := Copy(sourcePath, destPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func Copy(srcFile, dstFile string) error {
	out, err := os.Create(dstFile)
	if err != nil {
		return err
	}

	defer out.Close()

	in, err := os.Open(srcFile)
	defer in.Close()
	if err != nil {
		return err
	}

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}

	return nil
}

func Exists(filePath string) bool {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false
	}

	return true
}

func CreateIfNotExists(dir string, perm os.FileMode) error {
	if Exists(dir) {
		return nil
	}

	if err := os.MkdirAll(dir, perm); err != nil {
		return fmt.Errorf("failed to create directory: '%s', error: '%s'", dir, err.Error())
	}

	return nil
}

func (c *commandFsMetaSnapshotsCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	fsMetaSnapshotsCreateCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	start := fsMetaSnapshotsCreateCommand.String("s", "", "start date of metadata compaction in yyyy-mm-dd format")
	end := fsMetaSnapshotsCreateCommand.String("e", "", "end date of metadata compaction in yyyy-mm-dd format")
	frequency := fsMetaSnapshotsCreateCommand.Int("f", 7, "the frequency of generating the metadata snapshots in days")
	if err = fsMetaSnapshotsCreateCommand.Parse(args); err != nil {
		return err
	}
	startDate, err := time.Parse(DateFormat, *start)
	if err != nil {
		return err
	}
	endDate, err := time.Parse(DateFormat, *end)
	if err != nil {
		return err
	}
	var snapshotCheckPoints []time.Time
	for startDate.Before(endDate) || startDate.Equal(endDate) {
		snapshotCheckPoints = append(snapshotCheckPoints, startDate)
		startDate = startDate.AddDate(0, 0, *frequency)
	}
	snapshotCnt := 0
	homeDirname, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	levelDbPath := homeDirname + LevelDbPath
	err = os.RemoveAll(levelDbPath)
	if err != nil {
		return err
	}
	store := &filer_leveldb.LevelDBStore{}
	store.CustomInitialize(levelDbPath)
	path := filer.SystemLogDir
	var processEntry func(entry *filer_pb.Entry, isLast bool) error
	processEntry = func(entry *filer_pb.Entry, isLast bool) error {
		if entry.IsDirectory {
			return filer_pb.ReadDirAllEntries(commandEnv, util.FullPath(path+"/"+entry.Name), "", processEntry)
		}
		totalSize := filer.FileSize(entry)
		buf := mem.Allocate(int(totalSize))
		if err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer.ReadAll(buf, commandEnv.MasterClient, entry.GetChunks())
		}); err != nil && err != filer_pb.ErrNotFound {
			return err
		}
		idx := uint32(0)
		for idx < uint32(totalSize) {
			logEntrySize := util.BytesToUint32(buf[idx : idx+4])
			var logEntry filer_pb.LogEntry
			err = proto.Unmarshal(buf[idx+4:idx+4+logEntrySize], &logEntry)
			if err != nil {
				return err
			}
			idx = idx + 4 + logEntrySize
			err = processMetaDataEvents(store, logEntry.Data, snapshotCnt, snapshotCheckPoints, homeDirname)
			if err != nil {
				return err
			}
		}

		return nil
	}

	err = filer_pb.ReadDirAllEntries(commandEnv, util.FullPath(path), "", processEntry)
	return err
}
