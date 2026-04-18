package command

import (
	"context"
	"errors"
	"fmt"
	nethttp "net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
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
			// A KV read failure is ambiguous — a checkpoint may well exist but the
			// source filer is temporarily unreachable. Don't treat that as a fresh
			// sync; otherwise runFilerBackup's retry loop would redo the full
			// -initialSnapshot walk on every transient error.
			isFreshSync = false
			glog.V(0).Infof("starting from %v (offset read failed: %v)", startFrom, err)
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
		snapshotTsNs, err := runInitialSnapshot(sourceFiler.ToGrpcAddress(), filerSource, sourcePath, targetPath, excludePaths, reExcludeFileName, excludeFileNames, excludePathPatterns, dataSink, *backupOption.ignore404Error)
		if err != nil {
			return fmt.Errorf("initial snapshot: %w", err)
		}
		// The walk can take hours on large trees; retry the tiny KV write a
		// handful of times before giving up so a flaky filer KV doesn't force
		// the whole walk to repeat on the next retry loop iteration.
		if err := persistSnapshotOffset(grpcDialOption, sourceFiler, int32(sinkId), snapshotTsNs); err != nil {
			glog.Errorf("initialSnapshot: FAILED to persist offset %d for sinkId %d after retries: %v — the next retry will redo the full walk", snapshotTsNs, sinkId, err)
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

// persistSnapshotOffset writes the snapshot high-water mark to the source
// filer's KV, retrying a few times with exponential backoff on transient
// errors. Losing this write forces a full re-walk on the next retry loop
// iteration, so a small retry budget here is far cheaper than paying the
// walk cost again on a large tree.
func persistSnapshotOffset(grpcDialOption grpc.DialOption, sourceFiler pb.ServerAddress, sinkId int32, tsNs int64) error {
	const attempts = 4
	backoff := 500 * time.Millisecond
	var lastErr error
	for i := 1; i <= attempts; i++ {
		if err := setOffset(grpcDialOption, sourceFiler, BackupKeyPrefix, sinkId, tsNs); err == nil {
			return nil
		} else {
			lastErr = err
			if i == attempts {
				break
			}
			glog.V(0).Infof("initialSnapshot: setOffset attempt %d/%d failed: %v (retrying in %v)", i, attempts, err, backoff)
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return lastErr
}

// runInitialSnapshot walks the live filer tree under sourcePath and seeds the
// destination via the sink. The snapshot timestamp is captured before the walk
// starts so any create/update/delete that races with the walk is still caught
// by the subscription that runs afterward (sink CreateEntry is idempotent for
// all builtin sinks, so replaying a concurrent create from the subscription
// over an entry already written by the walk is safe).
//
// Note on excludes: TraverseBfs enumerates every directory and enqueues its
// children unconditionally before the callback runs, so the exclude filters
// below only prevent *processing* of excluded entries — they do not prune the
// listing RPCs for excluded subtrees. For small system excludes (SystemLogDir)
// that is fine; for large user-supplied excludes (say an archive directory
// with millions of files), the listing cost can dominate the snapshot. A
// proper prune signal through TraverseBfs is a separate change.
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
	ignore404Error bool,
) (int64, error) {
	// Metadata events are stamped server-side, so the backup host clock may be
	// ahead of the filer's. Take `now - 1min` as the subscription watermark so
	// a fast client clock can't skip events that fire during/right-after the
	// walk. meta_aggregator.go uses the same 1-minute margin on initial peer
	// traversal; see the note there on why duplicate replay is harmless.
	snapshotTsNs := time.Now().Add(-time.Minute).UnixNano()
	glog.V(0).Infof("initialSnapshot: walking %s on %s -> %s (snapshotTsNs=%d, -1m skew margin applied)", sourcePath, grpcAddress, targetPath, snapshotTsNs)

	// TraverseBfs fans the callback out across 5 worker goroutines, so counter
	// updates and the progress-log gate need to be safe under concurrent access.
	var entryCount, byteCount atomic.Int64
	start := time.Now()
	var logMu sync.Mutex
	lastLog := start

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := filer_pb.TraverseBfs(ctx, filerSource, util.FullPath(sourcePath), func(parentPath util.FullPath, entry *filer_pb.Entry) error {
		parent := string(parentPath)
		sourceKey := parentPath.Child(entry.Name)
		source := string(sourceKey)
		// Check exclusion on the entry's own path too — otherwise a walked
		// directory whose full path is SystemLogDir or a user-excluded path
		// still gets seeded (only its children would be skipped by the parent
		// check).
		if parent == filer.SystemLogDir || strings.HasPrefix(parent, filer.SystemLogDir+"/") ||
			source == filer.SystemLogDir || strings.HasPrefix(source, filer.SystemLogDir+"/") {
			return nil
		}
		if matchesExcludePath(parent, excludePaths) || matchesExcludePath(source, excludePaths) {
			return nil
		}
		if isEntryExcluded(parent, entry, reExcludeFileName, excludeFileNames, excludePathPatterns) {
			return nil
		}

		if !util.IsEqualOrUnder(source, sourcePath) {
			return nil
		}

		targetKey := initialSnapshotTargetKey(dataSink, targetPath, sourcePath, sourceKey, entry)
		if err := dataSink.CreateEntry(targetKey, entry, nil); err != nil {
			// A file can be listed by TraverseBfs and then deleted before
			// CreateEntry reads its chunks. The follow phase ignores 404s when
			// -ignore404Error is set; apply the same policy here so the walk
			// doesn't abort (and trigger a full re-walk on retry) just because
			// a single entry disappeared mid-snapshot.
			if ignore404Error && isIgnorable404(err) {
				glog.V(0).Infof("initialSnapshot: source entry %s disappeared, ignore it: %s", sourceKey, err.Error())
				return nil
			}
			return fmt.Errorf("seed %s: %w", targetKey, err)
		}

		curEntries := entryCount.Add(1)
		var curBytes int64
		if entry.Attributes != nil {
			curBytes = byteCount.Add(int64(entry.Attributes.FileSize))
		} else {
			curBytes = byteCount.Load()
		}

		now := time.Now()
		logMu.Lock()
		shouldLog := now.Sub(lastLog) >= 5*time.Second
		if shouldLog {
			lastLog = now
		}
		logMu.Unlock()
		if shouldLog {
			elapsed := now.Sub(start).Seconds()
			if elapsed == 0 {
				elapsed = 1
			}
			glog.V(0).Infof("initialSnapshot: %d entries / %d bytes seeded (%.1f/sec)", curEntries, curBytes, float64(curEntries)/elapsed)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	glog.V(0).Infof("initialSnapshot: done — %d entries / %d bytes in %v", entryCount.Load(), byteCount.Load(), time.Since(start))
	return snapshotTsNs, nil
}

// initialSnapshotTargetKey pulls the entry mtime and delegates to the shared
// destKey helper in filer_sync.go so the walk and the event-log path produce
// the same destination key for the same source entry.
func initialSnapshotTargetKey(dataSink sink.ReplicationSink, targetPath, sourcePath string, sourceKey util.FullPath, entry *filer_pb.Entry) string {
	var mTime int64
	if entry.Attributes != nil {
		mTime = entry.Attributes.Mtime
	}
	return destKey(dataSink, targetPath, sourcePath, sourceKey, mTime)
}
