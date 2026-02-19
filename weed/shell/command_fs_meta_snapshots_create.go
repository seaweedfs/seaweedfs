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
	"sort"
	"time"
)

const LevelDbPath = "snapshots.db"
const DateFormat = "2006-01-02"
const SnapshotDirPostFix = "-snapshot"

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

	fs.meta.snapshots.create -interval-days=7 -count=3 -path=/your/path
	// fs.meta.snapshots.create will generate desired number of snapshots with desired duration interval from yesterday the generated files will be saved from input path.
	// These snapshot maybe later used to backup the system to certain timestamp.
	// path input is relative to home directory.
`
}

func processMetaDataEvents(store *filer_leveldb.LevelDBStore, data []byte, count int, snapshotCheckPoints []time.Time, homeDir string, snapshotPath string) (SnapshotCount int, err error) {
	var event filer_pb.SubscribeMetadataResponse
	err = proto.Unmarshal(data, &event)
	if err != nil {
		return count, err
	}
	eventTime := event.TsNs
	for count < len(snapshotCheckPoints) && time.Unix(0, eventTime).After(snapshotCheckPoints[count].Add(-time.Microsecond)) {
		snapshotPath := filepath.Join(homeDir, snapshotPath, snapshotCheckPoints[count].Format(DateFormat)+SnapshotDirPostFix)
		err = createIfNotExists(snapshotPath, 0755)
		if err != nil {
			return count, err
		}
		err = generateSnapshots(filepath.Join(homeDir, LevelDbPath), snapshotPath)
		if err != nil {
			return count, err
		}
		count++
	}
	if count < 0 {
		return count, nil
	}
	ctx := context.Background()
	if filer_pb.IsEmpty(&event) {
		return count, nil
	} else if filer_pb.IsCreate(&event) {
		entry := filer.FromPbEntry(event.EventNotification.NewParentPath, event.EventNotification.NewEntry)
		return count, store.InsertEntry(ctx, entry)
	} else if filer_pb.IsDelete(&event) {
		return count, store.DeleteEntry(ctx, util.FullPath(event.Directory).Child(event.EventNotification.OldEntry.Name))
	} else if filer_pb.IsUpdate(&event) {
		entry := filer.FromPbEntry(event.EventNotification.NewParentPath, event.EventNotification.NewEntry)
		return count, store.UpdateEntry(ctx, entry)
	} else {
		if err := store.DeleteEntry(ctx, util.FullPath(event.Directory).Child(event.EventNotification.OldEntry.Name)); err != nil {
			return count, err
		}
		return count, store.InsertEntry(ctx, filer.FromPbEntry(event.EventNotification.NewParentPath, event.EventNotification.NewEntry))
	}
	return count, nil
}

func processEntryLog(entry *filer_pb.Entry, commandEnv *CommandEnv, snapshotCount int, store *filer_leveldb.LevelDBStore, snapshotsToGenerate []time.Time, homeDirname string, snapshotPath string) (count int, err error) {
	totalSize := filer.FileSize(entry)
	buf := mem.Allocate(int(totalSize))
	if err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.ReadAll(buf, commandEnv.MasterClient, entry.GetChunks())
	}); err != nil && err != filer_pb.ErrNotFound {
		return snapshotCount, err
	}
	idx := uint32(0)
	for idx < uint32(totalSize) {
		logEntrySize := util.BytesToUint32(buf[idx : idx+4])
		var logEntry filer_pb.LogEntry
		err = proto.Unmarshal(buf[idx+4:idx+4+logEntrySize], &logEntry)
		if err != nil {
			return snapshotCount, err
		}
		idx = idx + 4 + logEntrySize
		snapshotCount, err = processMetaDataEvents(store, logEntry.Data, snapshotCount, snapshotsToGenerate, homeDirname, snapshotPath)
		if err != nil {
			return snapshotCount, err
		}
	}
	return snapshotCount, err
}

func generateSnapshots(scrDir, dest string) error {
	entries, err := os.ReadDir(scrDir)
	if err := createIfNotExists(dest, 0755); err != nil {
		return err
	}
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
			if err := createIfNotExists(destPath, 0755); err != nil {
				return err
			}
			if err := generateSnapshots(sourcePath, destPath); err != nil {
				return err
			}
		default:
			if err := copy(sourcePath, destPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func copy(srcFile, dstFile string) error {
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

func exists(filePath string) bool {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false
	}
	return true
}

func createIfNotExists(dir string, perm os.FileMode) error {
	if exists(dir) {
		return nil
	}

	if err := os.MkdirAll(dir, perm); err != nil {
		return fmt.Errorf("failed to create directory: '%s', error: '%s'", dir, err.Error())
	}

	return nil
}

func setupLevelDb(levelDbPath string, levelDbBootstrapPath string) (store *filer_leveldb.LevelDBStore, err error) {
	store = &filer_leveldb.LevelDBStore{}
	err = os.RemoveAll(levelDbPath)
	if err != nil {
		return
	}
	if len(levelDbBootstrapPath) != 0 {
		// copy the latest snapshot as starting point
		err = generateSnapshots(levelDbBootstrapPath, levelDbPath)
		if err != nil {
			return
		}
	}
	config := SnapshotConfig{
		dir: levelDbPath,
	}

	store.Initialize(config, "")
	return
}

func (c *commandFsMetaSnapshotsCreate) Do(args []string, commandEnv *CommandEnv, _writer io.Writer) (err error) {
	fsMetaSnapshotsCreateCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	snapshotPath := fsMetaSnapshotsCreateCommand.String("path", "", "the path to store generated snapshot files")
	count := fsMetaSnapshotsCreateCommand.Int("count", 3, "number of snapshots generated")
	intervalDays := fsMetaSnapshotsCreateCommand.Int("interval-days", 7, "the duration interval between each generated snapshot")
	if err = fsMetaSnapshotsCreateCommand.Parse(args); err != nil {
		return err
	}
	homeDirname, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	snapshotsToRemove, snapshotsToGenerate, levelDbBootstrapPath, err := computeRequirements(homeDirname, *snapshotPath, *count, *intervalDays)
	if err != nil {
		return err
	}
	levelDbPath := filepath.Join(homeDirname, LevelDbPath)
	store, err := setupLevelDb(levelDbPath, filepath.Join(homeDirname, *snapshotPath, levelDbBootstrapPath))
	if err != nil {
		return err
	}
	// sort to make sure we are processing ascending list of snapshots to generate
	sort.Slice(snapshotsToGenerate, func(i, j int) bool {
		return snapshotsToGenerate[i].Before(snapshotsToGenerate[j])
	})
	changeLogPath := filer.SystemLogDir
	var processEntry func(entry *filer_pb.Entry, isLast bool) error
	var levelDbBootstrapDate string
	if levelDbBootstrapPath != "" {
		levelDbBootstrapDate = levelDbBootstrapPath[:len(DateFormat)]
	}
	snapshotCount := 0
	processEntry = func(entry *filer_pb.Entry, isLast bool) error {
		if entry.IsDirectory {
			// skip logs prior to the latest previous snapshot
			if levelDbBootstrapDate != "" && entry.GetName() <= levelDbBootstrapDate {
				return nil
			}
			return filer_pb.ReadDirAllEntries(commandEnv, util.FullPath(changeLogPath+"/"+entry.Name), "", processEntry)
		}
		snapshotCount, err = processEntryLog(entry, commandEnv, snapshotCount, store, snapshotsToGenerate, homeDirname, *snapshotPath)
		return err
	}

	err = filer_pb.ReadDirAllEntries(commandEnv, util.FullPath(changeLogPath), "", processEntry)
	if err != nil {
		return err
	}
	// edge case
	// there might be unfinished snapshot left over in the duration gaps.
	// process meta event only triggers snapshots when there are event after the snapshot time.
	for snapshotCount < len(snapshotsToGenerate) {
		generatePath := filepath.Join(homeDirname, *snapshotPath, snapshotsToGenerate[snapshotCount].Format(DateFormat)+SnapshotDirPostFix)
		err = createIfNotExists(generatePath, 0755)
		if err != nil {
			return err
		}
		err = generateSnapshots(levelDbPath, generatePath)
		if err != nil {
			return err
		}
		snapshotCount++
	}
	// remove previous snapshots.
	for _, snapshot := range snapshotsToRemove {
		err = os.RemoveAll(snapshot)
		if err != nil {
			return err
		}
	}
	return nil
}
