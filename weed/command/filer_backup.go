package command

import (
	"context"
	"errors"
	"fmt"
	nethttp "net/http"
	"regexp"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/repl_util"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	"google.golang.org/grpc"
)

type FilerBackupOptions struct {
	isActivePassive     *bool
	filer               *string
	path                *string
	excludePaths        *string
	excludeFileName     *string // deprecated: use excludeFileNames
	excludeFileNames    *string
	excludePathPatterns *string
	debug               *bool
	proxyByFiler        *bool
	doDeleteFiles       *bool
	disableErrorRetry   *bool
	ignore404Error      *bool
	timeAgo             *time.Duration
	retentionDays       *int
	initialSnapshot     *bool
}

var (
	filerBackupOptions    FilerBackupOptions
	ignorable404ErrString = fmt.Sprintf("%d %s: %s", nethttp.StatusNotFound, nethttp.StatusText(nethttp.StatusNotFound), http.ErrNotFound.Error())
)

func init() {
	cmdFilerBackup.Run = runFilerBackup // break init cycle
	filerBackupOptions.filer = cmdFilerBackup.Flag.String("filer", "localhost:8888", "filer of one SeaweedFS cluster")
	filerBackupOptions.path = cmdFilerBackup.Flag.String("filerPath", "/", "directory to sync on filer")
	filerBackupOptions.excludePaths = cmdFilerBackup.Flag.String("filerExcludePaths", "", "exclude directories to sync on filer")
	filerBackupOptions.excludeFileName = cmdFilerBackup.Flag.String("filerExcludeFileName", "", "[DEPRECATED: use -filerExcludeFileNames] exclude file names that match the regexp")
	filerBackupOptions.excludeFileNames = cmdFilerBackup.Flag.String("filerExcludeFileNames", "", "comma-separated wildcard patterns to exclude file names, e.g., \"*.tmp,._*\"")
	filerBackupOptions.excludePathPatterns = cmdFilerBackup.Flag.String("filerExcludePathPatterns", "", "comma-separated wildcard patterns to exclude paths where any component matches, e.g., \".snapshot,temp*\"")
	filerBackupOptions.proxyByFiler = cmdFilerBackup.Flag.Bool("filerProxy", false, "read and write file chunks by filer instead of volume servers")
	filerBackupOptions.doDeleteFiles = cmdFilerBackup.Flag.Bool("doDeleteFiles", false, "delete files on the destination")
	filerBackupOptions.debug = cmdFilerBackup.Flag.Bool("debug", false, "debug mode to print out received files")
	filerBackupOptions.timeAgo = cmdFilerBackup.Flag.Duration("timeAgo", 0, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	filerBackupOptions.retentionDays = cmdFilerBackup.Flag.Int("retentionDays", 0, "incremental backup retention days")
	filerBackupOptions.disableErrorRetry = cmdFilerBackup.Flag.Bool("disableErrorRetry", false, "disables errors retry, only logs will print")
	filerBackupOptions.ignore404Error = cmdFilerBackup.Flag.Bool("ignore404Error", true, "ignore 404 errors from filer")
	filerBackupOptions.initialSnapshot = cmdFilerBackup.Flag.Bool("initialSnapshot", false, "before subscribing to metadata updates, walk the live filer tree under -filerPath and seed the destination. Only runs on a fresh sync (no prior checkpoint and -timeAgo is 0). After the walk, subscription starts from the walk-start timestamp so concurrent changes are still captured.")
}

var cmdFilerBackup = &Command{
	UsageLine: "filer.backup -filer=<filerHost>:<filerPort> ",
	Short:     "resume-able continuously replicate files from a SeaweedFS cluster to another location defined in replication.toml",
	Long: `resume-able continuously replicate files from a SeaweedFS cluster to another location defined in replication.toml

	filer.backup listens on filer notifications. If any file is updated, it will fetch the updated content,
	and write to the destination. This is to replace filer.replicate command since additional message queue is not needed.

	If restarted and "-timeAgo" is not set, the synchronization will resume from the previous checkpoints, persisted every minute.
	A fresh sync will start from the earliest metadata logs. To reset the checkpoints, just set "-timeAgo" to a high value.

	On a fresh sync the metadata event log only re-materializes files that still exist on the source; entries that were
	created and later deleted are replayed as a create-then-delete pair and therefore never appear on the destination.
	Pass "-initialSnapshot" to walk the live filer tree first and seed the destination with the current tree, then
	subscribe from the walk-start timestamp. The walk only runs when there is no prior checkpoint.

`,
}

func runFilerBackup(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()
	util.LoadConfiguration("replication", true)

	// Compile exclude patterns once before the retry loop — these are
	// configuration errors and must not be retried.
	reExcludeFileName, err := compileExcludePattern(*filerBackupOptions.excludeFileName, "exclude file name")
	if err != nil {
		glog.Fatalf("invalid -filerExcludeFileName: %v", err)
	}
	excludeFileNames := wildcard.CompileWildcardMatchers(*filerBackupOptions.excludeFileNames)
	excludePathPatterns := wildcard.CompileWildcardMatchers(*filerBackupOptions.excludePathPatterns)

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	clientId := util.RandomInt32()
	var clientEpoch int32

	for {
		clientEpoch++
		err := doFilerBackup(grpcDialOption, &filerBackupOptions, reExcludeFileName, excludeFileNames, excludePathPatterns, clientId, clientEpoch)
		if err != nil {
			glog.Errorf("backup from %s: %v", *filerBackupOptions.filer, err)
			time.Sleep(1747 * time.Millisecond)
		}
	}
}

const (
	BackupKeyPrefix = "backup."
)

func doFilerBackup(grpcDialOption grpc.DialOption, backupOption *FilerBackupOptions, reExcludeFileName *regexp.Regexp, excludeFileNames []*wildcard.WildcardMatcher, excludePathPatterns []*wildcard.WildcardMatcher, clientId int32, clientEpoch int32) error {

	// find data sink
	dataSink := findSink(util.GetViper())
	if dataSink == nil {
		return fmt.Errorf("no data sink configured in replication.toml")
	}

	sourceFiler := pb.ServerAddress(*backupOption.filer)
	sourcePath := *backupOption.path
	excludePaths := util.StringSplit(*backupOption.excludePaths, ",")
	timeAgo := *backupOption.timeAgo
	targetPath := dataSink.GetSinkToDirectory()
	debug := *backupOption.debug

	// get start time for the data sink
	startFrom := time.Unix(0, 0)
	isFreshSync := true
	sinkId := util.HashStringToLong(dataSink.GetName() + dataSink.GetSinkToDirectory())
	if timeAgo.Milliseconds() == 0 {
		lastOffsetTsNs, err := getOffset(grpcDialOption, sourceFiler, BackupKeyPrefix, int32(sinkId))
		if err != nil {
			glog.V(0).Infof("starting from %v", startFrom)
		} else if lastOffsetTsNs > 0 {
			startFrom = time.Unix(0, lastOffsetTsNs)
			isFreshSync = false
			glog.V(0).Infof("resuming from %v", startFrom)
		} else {
			glog.V(0).Infof("starting from %v (no prior checkpoint)", startFrom)
		}
	} else {
		startFrom = time.Now().Add(-timeAgo)
		isFreshSync = false
		glog.V(0).Infof("start time is set to %v", startFrom)
	}

	// create filer sink
	filerSource := &source.FilerSource{}
	filerSource.DoInitialize(
		sourceFiler.ToHttpAddress(),
		sourceFiler.ToGrpcAddress(),
		sourcePath,
		*backupOption.proxyByFiler)

	if err := repl_util.InitializeSSEForReplication(filerSource); err != nil {
		return fmt.Errorf("SSE initialization failed: %v", err)
	}
	dataSink.SetSourceFiler(filerSource)

	// When the destination has no prior checkpoint and the user opted in to an
	// initial snapshot, walk the live filer tree first and seed the destination
	// with the current entries. This avoids the "only new files appear" pitfall
	// of replaying the metadata event log: entries created-then-deleted before
	// the walk leave no trace, so a re-backup after wiping the destination
	// reflects what is actually live on the source instead of an empty tree.
	if *backupOption.initialSnapshot && isFreshSync {
		snapshotTsNs, err := runInitialSnapshot(sourceFiler.ToGrpcAddress(), filerSource, sourcePath, targetPath, excludePaths, reExcludeFileName, excludeFileNames, excludePathPatterns, dataSink)
		if err != nil {
			return fmt.Errorf("initial snapshot: %w", err)
		}
		if err := setOffset(grpcDialOption, sourceFiler, BackupKeyPrefix, int32(sinkId), snapshotTsNs); err != nil {
			return fmt.Errorf("persist initial snapshot offset: %w", err)
		}
		startFrom = time.Unix(0, snapshotTsNs)
		glog.V(0).Infof("initialSnapshot done; subscribing from %v", startFrom)
	}

	var processEventFn func(*filer_pb.SubscribeMetadataResponse) error
	if *backupOption.ignore404Error {
		processEventFnGenerated := genProcessFunction(sourcePath, targetPath, excludePaths, reExcludeFileName, excludeFileNames, excludePathPatterns, dataSink, *backupOption.doDeleteFiles, debug)
		processEventFn = func(resp *filer_pb.SubscribeMetadataResponse) error {
			err := processEventFnGenerated(resp)
			if err == nil {
				return nil
			}
			if isIgnorable404(err) {
				glog.V(0).Infof("got 404 error for %s, ignore it: %s", getSourceKey(resp), err.Error())
				return nil
			}
			return err
		}
	} else {
		processEventFn = genProcessFunction(sourcePath, targetPath, excludePaths, reExcludeFileName, excludeFileNames, excludePathPatterns, dataSink, *backupOption.doDeleteFiles, debug)
	}

	processEventFnWithOffset := pb.AddOffsetFunc(processEventFn, 3*time.Second, func(counter int64, lastTsNs int64) error {
		glog.V(0).Infof("backup %s progressed to %v %0.2f/sec", sourceFiler, time.Unix(0, lastTsNs), float64(counter)/float64(3))
		return setOffset(grpcDialOption, sourceFiler, BackupKeyPrefix, int32(sinkId), lastTsNs)
	})

	if dataSink.IsIncremental() && *filerBackupOptions.retentionDays > 0 {
		go func() {
			for {
				now := time.Now()
				time.Sleep(time.Hour * 24)
				key := util.Join(targetPath, now.Add(-1*time.Hour*24*time.Duration(*filerBackupOptions.retentionDays)).Format("2006-01-02"))
				_ = dataSink.DeleteEntry(util.Join(targetPath, key), true, true, nil)
				glog.V(0).Infof("incremental backup delete directory:%s", key)
			}
		}()
	}

	prefix := sourcePath
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	eventErrorType := pb.RetryForeverOnError
	if *backupOption.disableErrorRetry {
		eventErrorType = pb.TrivialOnError
	}

	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             "backup_" + dataSink.GetName(),
		ClientId:               clientId,
		ClientEpoch:            clientEpoch,
		SelfSignature:          0,
		PathPrefix:             prefix,
		AdditionalPathPrefixes: nil,
		DirectoriesToWatch:     nil,
		StartTsNs:              startFrom.UnixNano(),
		StopTsNs:               0,
		EventErrorType:         eventErrorType,
	}

	return pb.FollowMetadata(sourceFiler, grpcDialOption, metadataFollowOption, processEventFnWithOffset)

}

func getSourceKey(resp *filer_pb.SubscribeMetadataResponse) string {
	if resp == nil || resp.EventNotification == nil {
		return ""
	}
	message := resp.EventNotification
	if message.NewEntry != nil {
		return string(util.FullPath(message.NewParentPath).Child(message.NewEntry.Name))
	}
	if message.OldEntry != nil {
		return string(util.FullPath(resp.Directory).Child(message.OldEntry.Name))
	}
	return ""
}

// isIgnorable404 returns true if the error represents a 404/not-found condition
// that should be silently ignored during backup. This covers:
//   - errors wrapping http.ErrNotFound (direct volume server 404 via non-S3 sinks)
//   - errors containing the "404 Not Found: not found" status string (S3 sink path
//     where AWS SDK breaks the errors.Is unwrap chain)
//   - LookupFileId or volume-id-not-found errors from the volume id map
func isIgnorable404(err error) bool {
	if errors.Is(err, http.ErrNotFound) {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, ignorable404ErrString) ||
		strings.Contains(errStr, "LookupFileId") ||
		(strings.Contains(errStr, "volume id") && strings.Contains(errStr, "not found"))
}

// runInitialSnapshot walks the live filer tree under sourcePath and seeds the
// destination via the sink. The snapshot timestamp is captured before the walk
// starts so any create/update/delete that races with the walk is still caught
// by the subscription that runs afterward (sink CreateEntry is idempotent for
// all builtin sinks, so replaying a concurrent create from the subscription
// over an entry already written by the walk is safe).
func runInitialSnapshot(
	grpcAddress string,
	filerSource *source.FilerSource,
	sourcePath string,
	targetPath string,
	excludePaths []string,
	reExcludeFileName *regexp.Regexp,
	excludeFileNames []*wildcard.WildcardMatcher,
	excludePathPatterns []*wildcard.WildcardMatcher,
	dataSink sink.ReplicationSink,
) (int64, error) {
	snapshotTsNs := time.Now().UnixNano()
	glog.V(0).Infof("initialSnapshot: walking %s on %s -> %s (snapshotTsNs=%d)", sourcePath, grpcAddress, targetPath, snapshotTsNs)

	var entryCount, byteCount int64
	start := time.Now()
	lastLog := start

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := filer_pb.TraverseBfs(ctx, filerSource, util.FullPath(sourcePath), func(parentPath util.FullPath, entry *filer_pb.Entry) error {
		parent := string(parentPath)
		if parent == filer.SystemLogDir || strings.HasPrefix(parent, filer.SystemLogDir+"/") {
			return nil
		}
		if matchesExcludePath(parent, excludePaths) {
			return nil
		}
		if isEntryExcluded(parent, entry, reExcludeFileName, excludeFileNames, excludePathPatterns) {
			return nil
		}

		sourceKey := parentPath.Child(entry.Name)
		if !util.IsEqualOrUnder(string(sourceKey), sourcePath) {
			return nil
		}

		targetKey := initialSnapshotTargetKey(dataSink, targetPath, sourcePath, sourceKey, entry)
		if err := dataSink.CreateEntry(targetKey, entry, nil); err != nil {
			return fmt.Errorf("seed %s: %w", targetKey, err)
		}

		entryCount++
		if entry.Attributes != nil {
			byteCount += int64(entry.Attributes.FileSize)
		}
		if now := time.Now(); now.Sub(lastLog) >= 5*time.Second {
			lastLog = now
			elapsed := now.Sub(start).Seconds()
			if elapsed == 0 {
				elapsed = 1
			}
			glog.V(0).Infof("initialSnapshot: %d entries / %d bytes seeded (%.1f/sec)", entryCount, byteCount, float64(entryCount)/elapsed)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	glog.V(0).Infof("initialSnapshot: done — %d entries / %d bytes in %v", entryCount, byteCount, time.Since(start))
	return snapshotTsNs, nil
}

// initialSnapshotTargetKey mirrors buildKey from filer_sync.go but works from a
// raw (sourceKey, entry) pair instead of an EventNotification. Kept close to
// runInitialSnapshot so the mapping from source path to destination key stays
// in one place for the seed path.
func initialSnapshotTargetKey(dataSink sink.ReplicationSink, targetPath, sourcePath string, sourceKey util.FullPath, entry *filer_pb.Entry) string {
	relative := string(sourceKey)[len(sourcePath):]
	if !dataSink.IsIncremental() {
		return escapeKey(util.Join(targetPath, relative))
	}
	var mTime int64
	if entry.Attributes != nil {
		mTime = entry.Attributes.Mtime
	}
	dateKey := time.Unix(mTime, 0).Format("2006-01-02")
	return escapeKey(util.Join(targetPath, dateKey, relative))
}
