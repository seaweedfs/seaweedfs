package command

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	metaBackup FilerMetaBackupOptions
)

type FilerMetaBackupOptions struct {
	grpcDialOption    grpc.DialOption
	filerAddress      *string
	filerDirectory    *string
	excludePaths      *string
	excludePathsList  []string // parsed and cached list
	restart           *bool
	backupFilerConfig *string

	store       filer.FilerStore
	clientId    int32
	clientEpoch int32
}

func init() {
	cmdFilerMetaBackup.Run = runFilerMetaBackup // break init cycle
	metaBackup.filerAddress = cmdFilerMetaBackup.Flag.String("filer", "localhost:8888", "filer hostname:port")
	metaBackup.filerDirectory = cmdFilerMetaBackup.Flag.String("filerDir", "/", "a folder on the filer")
	metaBackup.excludePaths = cmdFilerMetaBackup.Flag.String("excludePaths", "", "comma-separated path prefixes to exclude from backup")
	metaBackup.restart = cmdFilerMetaBackup.Flag.Bool("restart", false, "copy the full metadata before async incremental backup")
	metaBackup.backupFilerConfig = cmdFilerMetaBackup.Flag.String("config", "", "path to filer.toml specifying backup filer store")
	metaBackup.clientId = util.RandomInt32()
}

var cmdFilerMetaBackup = &Command{
	UsageLine: "filer.meta.backup [-filer=localhost:8888] [-filerDir=/] [-excludePaths=/path1,/path2] [-restart] -config=/path/to/backup_filer.toml",
	Short:     "continuously backup filer meta data changes to anther filer store specified in a backup_filer.toml",
	Long: `continuously backup filer meta data changes.
The backup writes to another filer store specified in a backup_filer.toml.

	weed filer.meta.backup -config=/path/to/backup_filer.toml -filer="localhost:8888"
	weed filer.meta.backup -config=/path/to/backup_filer.toml -filer="localhost:8888" -restart

The -excludePaths flag accepts a comma-separated list of path prefixes to exclude from backup.
Paths must be absolute (start with '/'). Matching is performed at directory boundaries,
so excluding '/buckets/legacy1' will exclude that directory and all entries underneath it,
but will not exclude '/buckets/legacy1_backup'.

	weed filer.meta.backup -config=/path/to/backup_filer.toml -filerDir=/buckets \
	  -excludePaths=/buckets/legacy1,/buckets/legacy2

  `,
}

func runFilerMetaBackup(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()
	metaBackup.grpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")

	// load backup_filer.toml
	v := viper.New()
	v.SetConfigFile(*metaBackup.backupFilerConfig)

	if err := v.ReadInConfig(); err != nil { // Handle errors reading the config file
		glog.Fatalf("Failed to load %s file: %v\nPlease use this command to generate the a %s.toml file\n"+
			"    weed scaffold -config=%s -output=.\n\n\n",
			*metaBackup.backupFilerConfig, err, "backup_filer", "filer")
	}

	if err := metaBackup.initStore(v); err != nil {
		glog.V(0).Infof("init backup filer store: %v", err)
		return true
	}

	// Parse and validate exclude paths
	if *metaBackup.excludePaths != "" {
		for _, p := range strings.Split(*metaBackup.excludePaths, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if !strings.HasPrefix(p, "/") {
				glog.Warningf("exclude path %q does not start with '/', will never match", p)
				continue
			}
			if p == "/" {
				glog.Warningf("exclude path %q will exclude all paths from backup", p)
			}
			// Ensure trailing slash for directory boundary matching
			if !strings.HasSuffix(p, "/") {
				p = p + "/"
			}
			metaBackup.excludePathsList = append(metaBackup.excludePathsList, p)
		}
		if len(metaBackup.excludePathsList) > 0 {
			glog.V(0).Infof("excluding paths: %v", metaBackup.excludePathsList)
		}
	}

	missingPreviousBackup := false
	_, err := metaBackup.getOffset()
	if err != nil {
		missingPreviousBackup = true
	}

	if *metaBackup.restart || missingPreviousBackup {
		glog.V(0).Infof("traversing metadata tree...")
		startTime := time.Now()
		if err := metaBackup.traverseMetadata(); err != nil {
			glog.Errorf("traverse meta data: %v", err)
			return true
		}
		glog.V(0).Infof("metadata copied up to %v", startTime)
		if err := metaBackup.setOffset(startTime); err != nil {
			startTime = time.Now()
		}
	}

	for {
		err := metaBackup.streamMetadataBackup()
		if err != nil {
			glog.Errorf("filer meta backup from %s: %v", *metaBackup.filerAddress, err)
			time.Sleep(1747 * time.Millisecond)
		}
	}
	// Unreachable: satisfies bool return type signature for daemon function
	return false
}

func (metaBackup *FilerMetaBackupOptions) initStore(v *viper.Viper) error {
	// load configuration for default filer store
	hasDefaultStoreConfigured := false
	for _, store := range filer.Stores {
		if v.GetBool(store.GetName() + ".enabled") {
			store = reflect.New(reflect.ValueOf(store).Elem().Type()).Interface().(filer.FilerStore)
			if err := store.Initialize(v, store.GetName()+"."); err != nil {
				glog.Fatalf("failed to initialize store for %s: %+v", store.GetName(), err)
			}
			glog.V(0).Infof("configured filer store to %s", store.GetName())
			hasDefaultStoreConfigured = true
			metaBackup.store = filer.NewFilerStoreWrapper(store)
			break
		}
	}
	if !hasDefaultStoreConfigured {
		return fmt.Errorf("no filer store enabled in %s", v.ConfigFileUsed())
	}

	return nil
}

// shouldExclude checks if the given path should be excluded from backup
// based on the configured exclude path prefixes.
func (metaBackup *FilerMetaBackupOptions) shouldExclude(fullpath string) bool {
	if len(metaBackup.excludePathsList) == 0 {
		return false
	}
	// Normalize for directory boundary matching
	checkPath := fullpath
	if !strings.HasSuffix(checkPath, "/") {
		checkPath = checkPath + "/"
	}
	for _, excludePrefix := range metaBackup.excludePathsList {
		if strings.HasPrefix(checkPath, excludePrefix) {
			return true
		}
	}
	return false
}

func (metaBackup *FilerMetaBackupOptions) traverseMetadata() (err error) {
	var saveErr error

	traverseErr := filer_pb.TraverseBfs(metaBackup, util.FullPath(*metaBackup.filerDirectory), func(parentPath util.FullPath, entry *filer_pb.Entry) {
		fullpath := string(parentPath.Child(entry.Name))
		if metaBackup.shouldExclude(fullpath) {
			return
		}

		println("+", fullpath)
		if err := metaBackup.store.InsertEntry(context.Background(), filer.FromPbEntry(string(parentPath), entry)); err != nil {
			saveErr = fmt.Errorf("insert entry error: %w\n", err)
			return
		}

	})

	if traverseErr != nil {
		return fmt.Errorf("traverse: %w", traverseErr)
	}
	return saveErr
}

var (
	MetaBackupKey = []byte("metaBackup")
)

func (metaBackup *FilerMetaBackupOptions) streamMetadataBackup() error {

	startTime, err := metaBackup.getOffset()
	if err != nil {
		startTime = time.Now()
	}
	glog.V(0).Infof("streaming from %v", startTime)

	store := metaBackup.store

	eachEntryFunc := func(resp *filer_pb.SubscribeMetadataResponse) error {

		ctx := context.Background()
		message := resp.EventNotification

		if filer_pb.IsEmpty(resp) {
			return nil
		}

		// Compute exclusion for both old and new paths
		var oldPathExcluded, newPathExcluded bool
		var oldPath, newPath string
		if message.OldEntry != nil {
			oldPath = string(util.FullPath(resp.Directory).Child(message.OldEntry.Name))
			oldPathExcluded = metaBackup.shouldExclude(oldPath)
		}
		if message.NewEntry != nil {
			newPath = string(util.FullPath(message.NewParentPath).Child(message.NewEntry.Name))
			newPathExcluded = metaBackup.shouldExclude(newPath)
		}

		if filer_pb.IsCreate(resp) {
			if newPathExcluded {
				return nil
			}
			println("+", newPath)
			entry := filer.FromPbEntry(message.NewParentPath, message.NewEntry)
			return store.InsertEntry(ctx, entry)
		} else if filer_pb.IsDelete(resp) {
			if oldPathExcluded {
				return nil
			}
			println("-", oldPath)
			return store.DeleteEntry(ctx, util.FullPath(resp.Directory).Child(message.OldEntry.Name))
		} else if filer_pb.IsUpdate(resp) {
			if newPathExcluded {
				return nil
			}
			println("~", newPath)
			entry := filer.FromPbEntry(message.NewParentPath, message.NewEntry)
			return store.UpdateEntry(ctx, entry)
		} else {
			// renaming - handle all four combinations
			if !oldPathExcluded {
				println("-", oldPath)
				if err := store.DeleteEntry(ctx, util.FullPath(resp.Directory).Child(message.OldEntry.Name)); err != nil {
					return err
				}
			}
			if !newPathExcluded {
				println("+", newPath)
				return store.InsertEntry(ctx, filer.FromPbEntry(message.NewParentPath, message.NewEntry))
			}
			return nil
		}
	}

	processEventFnWithOffset := pb.AddOffsetFunc(eachEntryFunc, 3*time.Second, func(counter int64, lastTsNs int64) error {
		lastTime := time.Unix(0, lastTsNs)
		glog.V(0).Infof("meta backup %s progressed to %v %0.2f/sec", *metaBackup.filerAddress, lastTime, float64(counter)/float64(3))
		return metaBackup.setOffset(lastTime)
	})

	metaBackup.clientEpoch++

	prefix := *metaBackup.filerDirectory
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             "meta_backup",
		ClientId:               metaBackup.clientId,
		ClientEpoch:            metaBackup.clientEpoch,
		SelfSignature:          0,
		PathPrefix:             prefix,
		AdditionalPathPrefixes: nil,
		DirectoriesToWatch:     nil,
		StartTsNs:              startTime.UnixNano(),
		StopTsNs:               0,
		EventErrorType:         pb.RetryForeverOnError,
	}

	return pb.FollowMetadata(pb.ServerAddress(*metaBackup.filerAddress), metaBackup.grpcDialOption, metadataFollowOption, processEventFnWithOffset)

}

func (metaBackup *FilerMetaBackupOptions) getOffset() (lastWriteTime time.Time, err error) {
	value, err := metaBackup.store.KvGet(context.Background(), MetaBackupKey)
	if err != nil {
		return
	}
	tsNs := util.BytesToUint64(value)

	return time.Unix(0, int64(tsNs)), nil
}

func (metaBackup *FilerMetaBackupOptions) setOffset(lastWriteTime time.Time) error {
	valueBuf := make([]byte, 8)
	util.Uint64toBytes(valueBuf, uint64(lastWriteTime.UnixNano()))

	if err := metaBackup.store.KvPut(context.Background(), MetaBackupKey, valueBuf); err != nil {
		return err
	}
	return nil
}

var _ = filer_pb.FilerClient(&FilerMetaBackupOptions{})

func (metaBackup *FilerMetaBackupOptions) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithFilerClient(streamingMode, metaBackup.clientId, pb.ServerAddress(*metaBackup.filerAddress), metaBackup.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return fn(client)
	})

}

func (metaBackup *FilerMetaBackupOptions) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (metaBackup *FilerMetaBackupOptions) GetDataCenter() string {
	return ""
}
