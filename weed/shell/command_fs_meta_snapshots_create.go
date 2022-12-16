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
const DateFormat = "2006-01-02"

func init() {
	Commands = append(Commands, &commandFsMetaSnapshotsCreate{})
}

type commandFsMetaSnapshotsCreate struct {
}

func (c *commandFsMetaSnapshotsCreate) Name() string {
	return "fs.meta.snapshots.create"
}

type SnapshotConfig struct {
	dir string
}

func (c SnapshotConfig) GetString(key string) string {
	return c.dir
}

func (c SnapshotConfig) GetBool(key string) bool {
	panic("implement me")
}

func (c SnapshotConfig) GetInt(key string) int {
	panic("implement me")
}

func (c SnapshotConfig) GetStringSlice(key string) []string {
	panic("implement me")
}

func (c SnapshotConfig) SetDefault(key string, value interface{}) {
	panic("implement me")
}

func (c *commandFsMetaSnapshotsCreate) Help() string {
	return `create snapshots of meta data from given time range.

	fs.meta.snapshots.create -snapshot-interval=7 -snapshot-cnt=3 -path=/your/path
	// fs.meta.snapshots.create will generate desired number of snapshots with desired duration interval from yesterday the generated files will be saved from input path.
	// These snapshot maybe later used to backup the system to certain timestamp.
`
}

func processMetaDataEvents(store *filer_leveldb.LevelDBStore, data []byte, unfinshiedSnapshotCnt int, snapshotCheckPoints []time.Time, homeDir string, snapshotPath string) (todoCnt int, err error) {
	var event filer_pb.SubscribeMetadataResponse
	err = proto.Unmarshal(data, &event)
	if err != nil {
		return unfinshiedSnapshotCnt, err
	}
	eventTime := event.TsNs
	for unfinshiedSnapshotCnt >= 0 && time.Unix(0, eventTime).After(snapshotCheckPoints[unfinshiedSnapshotCnt]) {
		snapshotPath := filepath.Join(homeDir, snapshotPath, snapshotCheckPoints[unfinshiedSnapshotCnt].Format(DateFormat))
		err = CreateIfNotExists(snapshotPath, 0755)
		if err != nil {
			return unfinshiedSnapshotCnt, err
		}
		err = generateSnapshots(homeDir+LevelDbPath, snapshotPath)
		if err != nil {
			return unfinshiedSnapshotCnt, err
		}
		unfinshiedSnapshotCnt--
	}
	if unfinshiedSnapshotCnt < 0 {
		return unfinshiedSnapshotCnt, nil
	}
	ctx := context.Background()
	if filer_pb.IsEmpty(&event) {
		return unfinshiedSnapshotCnt, nil
	} else if filer_pb.IsCreate(&event) {
		entry := filer.FromPbEntry(event.EventNotification.NewParentPath, event.EventNotification.NewEntry)
		return unfinshiedSnapshotCnt, store.InsertEntry(ctx, entry)
	} else if filer_pb.IsDelete(&event) {
		return unfinshiedSnapshotCnt, store.DeleteEntry(ctx, util.FullPath(event.Directory).Child(event.EventNotification.OldEntry.Name))
	} else if filer_pb.IsUpdate(&event) {
		entry := filer.FromPbEntry(event.EventNotification.NewParentPath, event.EventNotification.NewEntry)
		return unfinshiedSnapshotCnt, store.UpdateEntry(ctx, entry)
	} else {
		if err := store.DeleteEntry(ctx, util.FullPath(event.Directory).Child(event.EventNotification.OldEntry.Name)); err != nil {
			return unfinshiedSnapshotCnt, err
		}
		return unfinshiedSnapshotCnt, store.InsertEntry(ctx, filer.FromPbEntry(event.EventNotification.NewParentPath, event.EventNotification.NewEntry))
	}
	return unfinshiedSnapshotCnt, nil
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
	snapshotPath := fsMetaSnapshotsCreateCommand.String("path", "", "the path to store generated snapshot files")
	snapshotCnt := fsMetaSnapshotsCreateCommand.Int("snapshot-cnt", 3, "number of snapshots generated")
	snapshotInterval := fsMetaSnapshotsCreateCommand.Int("snapshot-interval", 7, "the duration interval between each generated snapshot")
	if err = fsMetaSnapshotsCreateCommand.Parse(args); err != nil {
		return err
	}
	yesterday := time.Now().Add(-time.Hour * 24).Format(DateFormat)
	// ensure snapshot start at yesterday 00:00
	snapshotDate, err := time.Parse(DateFormat, yesterday)
	if err != nil {
		return err
	}
	var snapshotCheckPoints []time.Time
	for i := 0; i < *snapshotCnt; i++ {
		snapshotCheckPoints = append(snapshotCheckPoints, snapshotDate)
		snapshotDate = snapshotDate.AddDate(0, 0, -1**snapshotInterval)
	}

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
	config := SnapshotConfig{
		dir: levelDbPath,
	}
	store.Initialize(config, "")
	unfinishedSnapshotCnt := len(snapshotCheckPoints) - 1
	changeLogPath := filer.SystemLogDir
	var processEntry func(entry *filer_pb.Entry, isLast bool) error
	processEntry = func(entry *filer_pb.Entry, isLast bool) error {
		if entry.IsDirectory {
			return filer_pb.ReadDirAllEntries(commandEnv, util.FullPath(changeLogPath+"/"+entry.Name), "", processEntry)
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
			unfinishedSnapshotCnt, err = processMetaDataEvents(store, logEntry.Data, unfinishedSnapshotCnt, snapshotCheckPoints, homeDirname, *snapshotPath)
			if err != nil {
				return err
			}
		}
		// edge case
		// there might be unfinished snapshot left over in the duration gaps.
		// process meta event only triggers snapshots when there are event after the snapshot time
		for unfinishedSnapshotCnt >= 0 {
			generatePath := filepath.Join(homeDirname, *snapshotPath, snapshotCheckPoints[unfinishedSnapshotCnt].Format(DateFormat))
			err = CreateIfNotExists(generatePath, 0755)
			if err != nil {
				return err
			}
			err = generateSnapshots(levelDbPath, generatePath)
			if err != nil {
				return err
			}
			unfinishedSnapshotCnt--
		}

		return nil
	}

	err = filer_pb.ReadDirAllEntries(commandEnv, util.FullPath(changeLogPath), "", processEntry)
	return err
}
