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
	"github.com/seaweedfs/seaweedfs/weed/replication/sink/filersink"
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
	concurrency         *int
	s3FlattenVersions   *bool
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
	filerBackupOptions.initialSnapshot = cmdFilerBackup.Flag.Bool("initialSnapshot", false, "before subscribing to metadata updates, walk the live filer tree under -filerPath and seed the destination, then subscribe from the walk-start timestamp so concurrent changes are still captured. Runs on every start when -timeAgo is 0 and overwrites the saved checkpoint, so it also re-seeds a reinitialized destination. Remove it once the backup is caught up, otherwise it re-walks the whole tree on each restart.")
	filerBackupOptions.concurrency = cmdFilerBackup.Flag.Int("concurrency", DefaultConcurrencyLimit, "the maximum number of files that will be backed up concurrently.")
	filerBackupOptions.s3FlattenVersions = cmdFilerBackup.Flag.Bool("s3.flattenVersions", false, "mirror the current S3 view of versioned buckets: under each <key>.versions directory only the latest version is written to <key>, a latest delete marker or the removal of the last version deletes <key> when -doDeleteFiles is set, and version files are not mirrored individually")
}

var cmdFilerBackup = &Command{
	UsageLine: "filer.backup -filer=<filerHost>:<filerPort> ",
	Short:     "resume-able continuously replicate files from a SeaweedFS cluster to another location defined in replication.toml",
	Long: `resume-able continuously replicate files from a SeaweedFS cluster to another location defined in replication.toml

	filer.backup listens on filer notifications. If any file is updated, it will fetch the updated content,
	and write to the destination. This is to replace filer.replicate command since additional message queue is not needed.

	If restarted and "-timeAgo" is not set, the synchronization will resume from the previous checkpoints, persisted every minute.
	A fresh sync will start from the earliest metadata logs.

	On a fresh sync the metadata event log only re-materializes files that still exist on the source; entries that were
	created and later deleted are replayed as a create-then-delete pair and therefore never appear on the destination.
	Pass "-initialSnapshot" to walk the live filer tree first and seed the destination with the current tree, then
	subscribe from the walk-start timestamp. This also re-seeds a reinitialized destination: it overwrites the saved
	checkpoint and re-walks on every start, so remove it once the backup is caught up.

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
	var resumeTsNs int64

	for {
		clientEpoch++
		var err error
		resumeTsNs, err = doFilerBackup(grpcDialOption, &filerBackupOptions, reExcludeFileName, excludeFileNames, excludePathPatterns, clientId, clientEpoch, resumeTsNs)
		if err != nil {
			glog.Errorf("backup from %s: %v", *filerBackupOptions.filer, err)
			time.Sleep(1747 * time.Millisecond)
		}
	}
}

const (
	BackupKeyPrefix = "backup."
)

func doFilerBackup(grpcDialOption grpc.DialOption, backupOption *FilerBackupOptions, reExcludeFileName *regexp.Regexp, excludeFileNames []*wildcard.WildcardMatcher, excludePathPatterns []*wildcard.WildcardMatcher, clientId int32, clientEpoch int32, resumeTsNs int64) (int64, error) {

	// find data sink
	dataSink := findSink(util.GetViper())
	if dataSink == nil {
		return 0, fmt.Errorf("no data sink configured in replication.toml")
	}

	sourceFiler := pb.ServerAddress(*backupOption.filer)
	sourcePath := *backupOption.path
	excludePaths := util.StringSplit(*backupOption.excludePaths, ",")
	timeAgo := *backupOption.timeAgo
	targetPath := dataSink.GetSinkToDirectory()
	debug := *backupOption.debug

	// get start time for the data sink
	startFrom := time.Unix(0, 0)
	sinkId, legacySinkId := backupCheckpointIds(sourcePath, dataSink, *backupOption.s3FlattenVersions)
	runSnapshot := *backupOption.initialSnapshot && timeAgo == 0
	if resumeTsNs > 0 {
		// The previous attempt pinned where it stopped — a failed event's
		// timestamp or the processed watermark — which can sit behind both the
		// persisted checkpoint and a recomputed -timeAgo window.
		startFrom = time.Unix(0, resumeTsNs)
		glog.V(0).Infof("resuming previous attempt at %v", startFrom)
	} else if timeAgo == 0 {
		if runSnapshot {
			// snapshot below sets the start point; no checkpoint read needed
			glog.V(0).Infof("initialSnapshot requested — walking live tree before subscribing")
		} else {
			lastOffsetTsNs, err := getOffsetWithFallback(grpcDialOption, sourceFiler, BackupKeyPrefix, sinkId, BackupKeyPrefix, legacySinkId)
			if err != nil {
				return 0, fmt.Errorf("read backup offset: %w", err)
			} else if lastOffsetTsNs > 0 {
				startFrom = time.Unix(0, lastOffsetTsNs)
				glog.V(0).Infof("resuming from %v", startFrom)
			} else {
				glog.V(0).Infof("starting from %v (no prior checkpoint)", startFrom)
			}
		}
	} else {
		startFrom = time.Now().Add(-timeAgo)
		glog.V(0).Infof("start time is set to %v", startFrom)
	}

	// create filer sink
	filerSource := &source.FilerSource{}
	if err := filerSource.DoInitialize(
		sourceFiler.ToHttpAddress(),
		sourceFiler.ToGrpcAddress(),
		sourcePath,
		*backupOption.proxyByFiler); err != nil {
		return resumeTsNs, fmt.Errorf("filersource initialization failed: %v", err)
	}

	if err := repl_util.InitializeSSEForReplication(filerSource); err != nil {
		return resumeTsNs, fmt.Errorf("SSE initialization failed: %v", err)
	}
	dataSink.SetSourceFiler(filerSource)

	var flattener *s3VersionFlattener
	if *backupOption.s3FlattenVersions {
		flattener = &s3VersionFlattener{
			filerSource:         filerSource,
			dataSink:            dataSink,
			sourcePath:          sourcePath,
			targetPath:          targetPath,
			doDeleteFiles:       *backupOption.doDeleteFiles,
			excludePaths:        excludePaths,
			reExcludeFileName:   reExcludeFileName,
			excludeFileNames:    excludeFileNames,
			excludePathPatterns: excludePathPatterns,
		}
	}

	// Walk and seed the live tree, then subscribe from the walk-start watermark:
	// replaying the event log alone misses entries created-then-deleted before
	// the walk. The watermark overwrites any stale checkpoint, re-seeding a wiped
	// destination.
	if runSnapshot {
		snapshotTsNs, err := runInitialSnapshot(sourceFiler.ToGrpcAddress(), filerSource, sourcePath, targetPath, excludePaths, reExcludeFileName, excludeFileNames, excludePathPatterns, dataSink, *backupOption.ignore404Error, flattener)
		if err != nil {
			return 0, fmt.Errorf("initial snapshot: %w", err)
		}
		// The walk can take hours on large trees; retry the tiny KV write a
		// handful of times before giving up so a flaky filer KV doesn't force
		// the whole walk to repeat on the next retry loop iteration.
		if err := persistSnapshotOffset(grpcDialOption, sourceFiler, sinkId, snapshotTsNs); err != nil {
			glog.Errorf("initialSnapshot: FAILED to persist offset %d for sinkId %d after retries: %v — the next retry will redo the full walk", snapshotTsNs, sinkId, err)
			return 0, fmt.Errorf("persist initial snapshot offset: %w", err)
		}
		startFrom = time.Unix(0, snapshotTsNs)
		// walk once per process; retries resume from the persisted checkpoint
		*backupOption.initialSnapshot = false
		glog.V(0).Infof("initialSnapshot done; subscribing from %v", startFrom)
	}

	generated := genProcessFunction(sourcePath, targetPath, excludePaths, reExcludeFileName, excludeFileNames, excludePathPatterns, dataSink, *backupOption.doDeleteFiles, debug)
	if flattener != nil {
		inner := generated
		generated = func(resp *filer_pb.SubscribeMetadataResponse) error {
			defer flattener.lockFor(resp)()
			if handled, err := flattener.process(resp); handled {
				return err
			}
			return inner(resp)
		}
	}

	var processEventFn func(*filer_pb.SubscribeMetadataResponse) error
	if *backupOption.ignore404Error {
		processEventFn = func(resp *filer_pb.SubscribeMetadataResponse) error {
			err := generated(resp)
			if err == nil {
				return nil
			}
			if isIgnorable404(err) {
				glog.V(0).Infof("got 404 error for %s, ignore it: %s", getSourceKey(resp), err.Error())
				return nil
			}
			if isSourceLookupError(err) && eventSourceSuperseded(filerSource, resp) {
				glog.V(0).Infof("source superseded %s during lookup failure, skip it: %s", getSourceKey(resp), err.Error())
				return nil
			}
			return err
		}
	} else {
		processEventFn = generated
	}

	if *backupOption.disableErrorRetry {
		// TrivialOnError semantics: log the failure and advance. Left to the
		// processor a permanent failure would pin the checkpoint and replay
		// forever, which is the opposite of what this flag asks for.
		inner := processEventFn
		processEventFn = func(resp *filer_pb.SubscribeMetadataResponse) error {
			if err := inner(resp); err != nil {
				glog.Errorf("process %s -> %s ts_ns=%d: %v", filer_pb.MetadataEventSourceFullPath(resp), filer_pb.MetadataEventTargetFullPath(resp), resp.TsNs, err)
			}
			return nil
		}
	}

	// Serial event handling caps throughput at one fetch+write at a time, so a
	// busy source keeps outrunning the backup. The metadata processor fans
	// events out while serializing same-path and subtree-barrier conflicts,
	// and the checkpoint only advances on the contiguous processed watermark.
	concurrency := *backupOption.concurrency
	if concurrency < 1 || concurrency > 1024 {
		glog.Warningf("invalid concurrency value %d, using default: %d", concurrency, DefaultConcurrencyLimit)
		concurrency = DefaultConcurrencyLimit
	}
	processor := NewMetadataProcessor(processEventFn, concurrency, startFrom.UnixNano())

	// AddOffsetFunc only checkpoints while events arrive; persist the
	// completed watermark on a ticker too so work finishing on a quiet
	// stream does not sit uncheckpointed until the next event.
	var lastPersistedTsNs atomic.Int64
	lastPersistedTsNs.Store(-1)
	stopPersist := make(chan struct{})
	persistDone := make(chan struct{})
	go func() {
		defer close(persistDone)
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		persist := func() {
			if ts := resumeTsNsFor(processor); ts != lastPersistedTsNs.Load() {
				if err := setOffset(grpcDialOption, sourceFiler, BackupKeyPrefix, sinkId, ts); err == nil {
					lastPersistedTsNs.Store(ts)
				} else {
					glog.Warningf("persist backup offset: %v", err)
				}
			}
		}
		for {
			select {
			case <-stopPersist:
				persist()
				return
			case <-ticker.C:
				persist()
			}
		}
	}()

	var lastLogTsNs = time.Now().UnixNano()
	processEventFnWithOffset := pb.AddOffsetFunc(func(resp *filer_pb.SubscribeMetadataResponse) error {
		processor.AddSyncJob(resp)
		return nil
	}, 3*time.Second, func(counter int64, lastTsNs int64) error {
		offsetTsNs := processor.processedTsWatermark.Load()
		if offsetTsNs == 0 {
			return nil
		}
		now := time.Now().UnixNano()
		glog.V(0).Infof("backup %s received %0.2f events/sec, processed through %v", sourceFiler, float64(counter)/(float64(now-lastLogTsNs)/1e9), time.Unix(0, offsetTsNs))
		lastLogTsNs = now
		return nil
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
		GetResumeTsNs: func() int64 {
			return resumeTsNsFor(processor)
		},
		Resubscribe: processor.ResubscribeCh(),
	}

	err := pb.FollowMetadata(sourceFiler, grpcDialOption, metadataFollowOption, processEventFnWithOffset)
	// Jobs admitted before the stream ended keep running; drain them so the
	// retry loop's replay cannot race an older write past a newer one.
	processor.Drain()
	close(stopPersist)
	// setOffset has no context, so a stalled checkpoint RPC must not hold the
	// retry loop: bound the wait and let a late write land in the background.
	select {
	case <-persistDone:
	case <-time.After(30 * time.Second):
		glog.Warningf("backup %s offset checkpoint did not finish in 30s; reconnecting anyway", sourceFiler)
	}
	// Resume the next attempt at the floored watermark — never recompute a
	// position that skips events. The floor sits one tick below the earliest
	// failure and stays there even when a redelivery clears the pin, so
	// events dropped while the processor was stopped still replay.
	return resumeTsNsFor(processor), err

}

// backupCheckpointIds derives the checkpoint key for this backup and the
// historical key kept for fallback reads. The checkpoint covers everything
// that defines one backup stream: what is backed up (the source path) and
// exactly where it lands (the sink's destination identity). The historical
// key hashed only sink name + directory, so two backups to different buckets
// or endpoints sharing a directory layout advanced one checkpoint and could
// silently skip each other's changes. Writes go only to the new key; the
// historical key is read once when the new key has no value yet, so an
// existing backup resumes where it left off after an upgrade.
//
// NUL joins the fields because it cannot occur in a path or a configuration
// value, so distinct field tuples cannot concatenate to the same hash input.
// The flattening mode is part of the identity too: resuming a checkpoint taken
// in the other mode would skip the flattened re-seed and leave mirrored
// version files behind, so a mode change must restart the snapshot.
func backupCheckpointIds(sourcePath string, dataSink sink.ReplicationSink, flattenVersions bool) (sinkId, legacySinkId int32) {
	mode := ""
	if flattenVersions {
		mode = "\x00flatten"
	}
	sinkId = int32(util.HashStringToLong(sourcePath + "\x00" + dataSink.GetName() + "\x00" + dataSink.GetDestinationIdentity() + mode))
	legacySinkId = int32(util.HashStringToLong(dataSink.GetName() + dataSink.GetSinkToDirectory() + mode))
	return
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

// isIgnorable404 returns true only for a genuine source-side 404, where skipping
// the event is lossless: errors wrapping http.ErrNotFound, or carrying the S3
// "404 Not Found: not found" status string (the AWS SDK breaks the unwrap chain).
// It deliberately does not match "LookupFileId" / "volume id ... not found" —
// usually transient lookup races whose skip drops live files; those are resolved
// against the live source instead (isSourceLookupError + eventSourceSuperseded).
func isIgnorable404(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, http.ErrNotFound) {
		return true
	}
	return strings.Contains(err.Error(), ignorable404ErrString)
}

// isSourceLookupError reports whether err is a source volume-lookup failure.
// The same strings cover a transient race and a permanently gone volume, so
// the caller must consult the live source to pick skip or retry.
func isSourceLookupError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "LookupFileId") ||
		(strings.Contains(errStr, "volume id") && strings.Contains(errStr, "not found"))
}

// eventSourceSuperseded reports whether the live source has moved past the
// version carried by this event (entry gone or strictly newer), making a skip
// lossless. Without a NewEntry it reports false so the error propagates.
func eventSourceSuperseded(filerSource *source.FilerSource, resp *filer_pb.SubscribeMetadataResponse) bool {
	sourcePath, mtimeNs, ok := eventSupersessionProbe(resp)
	if !ok {
		return false
	}
	return filersink.SourceSupersedes(context.Background(), filerSource, sourcePath, mtimeNs)
}

// eventSupersessionProbe derives the source path and replayed mtime of the
// event's landing entry. Legacy events can carry an empty NewParentPath, so the
// path comes from MetadataEventTargetFullPath (falls back to resp.Directory) —
// a wrong path here would read as "gone" and skip a live file.
func eventSupersessionProbe(resp *filer_pb.SubscribeMetadataResponse) (util.FullPath, int64, bool) {
	if resp == nil || resp.EventNotification == nil || resp.EventNotification.NewEntry == nil {
		return "", 0, false
	}
	return util.FullPath(filer_pb.MetadataEventTargetFullPath(resp)),
		filersink.EntryMtimeNs(resp.EventNotification.NewEntry), true
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
	flattener *s3VersionFlattener,
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

		if flattener != nil {
			if insideVersionsDir(parent) && strings.HasPrefix(entry.Name, "v_") {
				return nil
			}
			if entry.IsDirectory && isVersionsDirName(entry.Name) {
				// Seed only the version the pointer names; every other version
				// file is skipped by the check above.
				err := flattener.applyLatest(parentPath.Child(entry.Name), entry, true, nil)
				if err != nil && ignore404Error {
					if isIgnorable404(err) {
						glog.V(0).Infof("initialSnapshot: versions dir %s moved on, ignore it: %s", sourceKey, err.Error())
						return nil
					}
					if isSourceLookupError(err) && filersink.SourceSupersedes(ctx, filerSource, sourceKey, filersink.EntryMtimeNs(entry)) {
						glog.V(0).Infof("initialSnapshot: source superseded %s mid-walk, skip it: %s", sourceKey, err.Error())
						return nil
					}
				}
				return err
			}
		}

		targetKey := initialSnapshotTargetKey(dataSink, targetPath, sourcePath, sourceKey, entry)
		if err := dataSink.CreateEntry(targetKey, entry, nil); err != nil {
			// A file can be listed by TraverseBfs and then deleted before
			// CreateEntry reads its chunks. The follow phase ignores 404s when
			// -ignore404Error is set; apply the same policy here so the walk
			// doesn't abort (and trigger a full re-walk on retry) just because
			// a single entry disappeared mid-snapshot.
			if ignore404Error {
				if isIgnorable404(err) {
					glog.V(0).Infof("initialSnapshot: source entry %s disappeared, ignore it: %s", sourceKey, err.Error())
					return nil
				}
				// Same decision as the follow phase; lossless because the
				// post-walk subscription replays the newer change.
				if isSourceLookupError(err) && filersink.SourceSupersedes(ctx, filerSource, sourceKey, filersink.EntryMtimeNs(entry)) {
					glog.V(0).Infof("initialSnapshot: source superseded %s mid-walk, skip it: %s", sourceKey, err.Error())
					return nil
				}
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
