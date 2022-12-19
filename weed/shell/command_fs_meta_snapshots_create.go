package shell

import (
	"context"
	"errors"
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
	"strings"
	"time"
)

const LevelDbPath = "tmp/snapshots.db"
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

func processMetaDataEvents(store *filer_leveldb.LevelDBStore, data []byte, unfinshiedSnapshotCnt int, snapshotCheckPoints []time.Time, homeDir string, snapshotPath string) (todoCnt int, err error) {
	var event filer_pb.SubscribeMetadataResponse
	err = proto.Unmarshal(data, &event)
	if err != nil {
		return unfinshiedSnapshotCnt, err
	}
	eventTime := event.TsNs
	for unfinshiedSnapshotCnt >= 0 && time.Unix(0, eventTime).After(snapshotCheckPoints[unfinshiedSnapshotCnt]) {
		snapshotPath := filepath.Join(homeDir, snapshotPath, snapshotCheckPoints[unfinshiedSnapshotCnt].Format(DateFormat)+SnapshotDirPostFix)
		err = createIfNotExists(snapshotPath, 0755)
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

func computeRequirementsFromDirectory(previousSnapshots []os.DirEntry, homeDirectory string, snapshotPath string, count int, durationDays int) (snapshotsToRemove []string, snapshotsToGenerate []time.Time, err error) {
	lastSnapshotDate, err := time.Parse(DateFormat, previousSnapshots[len(previousSnapshots)-1].Name()[:len(DateFormat)])
	if err != nil {
		return snapshotsToRemove, snapshotsToGenerate, err
	}
	yesterday := time.Now().Add(-time.Hour * 24)
	yesterdayStr := yesterday.Format(DateFormat)
	// ensure snapshot start at yesterday 00:00
	yesterday, err = time.Parse(DateFormat, yesterdayStr)
	if err != nil {
		return snapshotsToRemove, snapshotsToGenerate, err
	}
	gapDays := int(yesterday.Sub(lastSnapshotDate).Hours() / 24)
	// gap too small no snapshot will be generated
	if gapDays < durationDays {
		return snapshotsToRemove, snapshotsToGenerate, errors.New(fmt.Sprintf("last snapshot was generated at %v no need to generate new snapshots", lastSnapshotDate.Format(DateFormat)))
	} else if gapDays > durationDays*count {
		// gap too large generate from yesterday
		// and remove all previous snapshots
		_, snapshotsToGenerate, err = computeRequirementsFromEmpty(homeDirectory, count, durationDays)
		for _, file := range previousSnapshots {
			snapshotsToRemove = append(snapshotsToRemove, filepath.Join(homeDirectory, snapshotPath, file.Name()))
		}
		return
	}
	snapshotDate := lastSnapshotDate.AddDate(0, 0, 1*durationDays)
	for snapshotDate.Before(yesterday) || snapshotDate.Equal(yesterday) {
		snapshotsToGenerate = append(snapshotsToGenerate, snapshotDate)
		snapshotDate = lastSnapshotDate.AddDate(0, 0, 1*durationDays)
	}
	totalCount := len(previousSnapshots) + len(snapshotsToGenerate)
	toRemoveIdx := 0
	for toRemoveIdx < len(previousSnapshots) && totalCount-toRemoveIdx > count {
		snapshotsToRemove = append(snapshotsToRemove, filepath.Join(homeDirectory, snapshotPath, previousSnapshots[toRemoveIdx].Name()))
		toRemoveIdx += 1
	}
	return
}

func computeRequirementsFromEmpty(homeDirectory string, count int, durationDays int) (snapshotsToRemove []string, snapshotsToGenerate []time.Time, err error) {
	yesterday := time.Now().Add(-time.Hour * 24).Format(DateFormat)
	// ensure snapshot start at yesterday 00:00
	snapshotDate, err := time.Parse(DateFormat, yesterday)
	if err != nil {
		return snapshotsToRemove, snapshotsToGenerate, err
	}
	for i := 0; i < count; i++ {
		snapshotsToGenerate = append(snapshotsToGenerate, snapshotDate)
		snapshotDate = snapshotDate.AddDate(0, 0, -1*durationDays)
	}
	return snapshotsToRemove, snapshotsToGenerate, nil
}

// compute number of snapshot need to be generated and number of snapshots to remove from give directory.
func computeRequirements(homeDirectory string, snapshotPath string, count int, durationDays int) (snapshotsToRemove []string, snapshotsToGenerate []time.Time, err error) {
	snapshotDirectory := filepath.Join(homeDirectory, snapshotPath)
	files, _ := os.ReadDir(snapshotDirectory)
	if len(files) == 0 {
		return computeRequirementsFromEmpty(homeDirectory, count, durationDays)
	}
	// sort files by name
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})
	// filter for snapshots file only
	var prevSnapshotFiles []os.DirEntry
	for _, file := range files {
		if strings.HasSuffix(file.Name(), SnapshotDirPostFix) {
			prevSnapshotFiles = append(prevSnapshotFiles, file)
		}
	}
	return computeRequirementsFromDirectory(prevSnapshotFiles, homeDirectory, snapshotPath, count, durationDays)
}

func setupLevelDb(levelDbPath string) (store *filer_leveldb.LevelDBStore, err error) {
	err = os.RemoveAll(levelDbPath)
	if err != nil {
		return &filer_leveldb.LevelDBStore{}, err
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
	snapshotsToRemove, snapshotsToGenerate, err := computeRequirements(homeDirname, *snapshotPath, *count, *intervalDays)
	if err != nil {
		return err
	}
	levelDbPath := filepath.Join(homeDirname, LevelDbPath)
	store, err := setupLevelDb(levelDbPath)
	if err != nil {
		return err
	}
	unfinishedSnapshotCnt := len(snapshotsToGenerate) - 1
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
			unfinishedSnapshotCnt, err = processMetaDataEvents(store, logEntry.Data, unfinishedSnapshotCnt, snapshotsToGenerate, homeDirname, *snapshotPath)
			if err != nil {
				return err
			}
		}
		return err
	}

	err = filer_pb.ReadDirAllEntries(commandEnv, util.FullPath(changeLogPath), "", processEntry)
	if err != nil {
		return err
	}
	// edge case
	// there might be unfinished snapshot left over in the duration gaps.
	// process meta event only triggers snapshots when there are event after the snapshot time
	for unfinishedSnapshotCnt >= 0 {
		generatePath := filepath.Join(homeDirname, *snapshotPath, snapshotsToGenerate[unfinishedSnapshotCnt].Format(DateFormat))
		err = createIfNotExists(generatePath, 0755)
		if err != nil {
			return err
		}
		err = generateSnapshots(levelDbPath, generatePath)
		if err != nil {
			return err
		}
		unfinishedSnapshotCnt--
	}
	// remove previous snapshot if needed.
	for _, snapshot := range snapshotsToRemove {
		err = os.RemoveAll(snapshot)
		if err != nil {
			return err
		}
	}
	return nil
}
