package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink/filersink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/security"
	statsCollect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"google.golang.org/grpc"
)

type SyncOptions struct {
	isActivePassive *bool
	filerA          *string
	filerB          *string
	aPath           *string
	aExcludePaths   *string
	bPath           *string
	bExcludePaths   *string
	aReplication    *string
	bReplication    *string
	aCollection     *string
	bCollection     *string
	aTtlSec         *int
	bTtlSec         *int
	aDiskType       *string
	bDiskType       *string
	aDebug          *bool
	bDebug          *bool
	aFromTsMs       *int64
	bFromTsMs       *int64
	aProxyByFiler   *bool
	bProxyByFiler   *bool
	metricsHttpIp   *string
	metricsHttpPort *int
	concurrency     *int
	aDoDeleteFiles  *bool
	bDoDeleteFiles  *bool
	clientId        int32
	clientEpoch     atomic.Int32
}

const (
	SyncKeyPrefix           = "sync."
	DefaultConcurrencyLimit = 32
)

var (
	syncOptions    SyncOptions
	syncCpuProfile *string
	syncMemProfile *string
)

func init() {
	cmdFilerSynchronize.Run = runFilerSynchronize // break init cycle
	syncOptions.isActivePassive = cmdFilerSynchronize.Flag.Bool("isActivePassive", false, "one directional follow from A to B if true")
	syncOptions.filerA = cmdFilerSynchronize.Flag.String("a", "", "filer A in one SeaweedFS cluster")
	syncOptions.filerB = cmdFilerSynchronize.Flag.String("b", "", "filer B in the other SeaweedFS cluster")
	syncOptions.aPath = cmdFilerSynchronize.Flag.String("a.path", "/", "directory to sync on filer A")
	syncOptions.aExcludePaths = cmdFilerSynchronize.Flag.String("a.excludePaths", "", "exclude directories to sync on filer A")
	syncOptions.bPath = cmdFilerSynchronize.Flag.String("b.path", "/", "directory to sync on filer B")
	syncOptions.bExcludePaths = cmdFilerSynchronize.Flag.String("b.excludePaths", "", "exclude directories to sync on filer B")
	syncOptions.aReplication = cmdFilerSynchronize.Flag.String("a.replication", "", "replication on filer A")
	syncOptions.bReplication = cmdFilerSynchronize.Flag.String("b.replication", "", "replication on filer B")
	syncOptions.aCollection = cmdFilerSynchronize.Flag.String("a.collection", "", "collection on filer A")
	syncOptions.bCollection = cmdFilerSynchronize.Flag.String("b.collection", "", "collection on filer B")
	syncOptions.aTtlSec = cmdFilerSynchronize.Flag.Int("a.ttlSec", 0, "ttl in seconds on filer A")
	syncOptions.bTtlSec = cmdFilerSynchronize.Flag.Int("b.ttlSec", 0, "ttl in seconds on filer B")
	syncOptions.aDiskType = cmdFilerSynchronize.Flag.String("a.disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag on filer A")
	syncOptions.bDiskType = cmdFilerSynchronize.Flag.String("b.disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag on filer B")
	syncOptions.aProxyByFiler = cmdFilerSynchronize.Flag.Bool("a.filerProxy", false, "read and write file chunks by filer A instead of volume servers")
	syncOptions.bProxyByFiler = cmdFilerSynchronize.Flag.Bool("b.filerProxy", false, "read and write file chunks by filer B instead of volume servers")
	syncOptions.aDebug = cmdFilerSynchronize.Flag.Bool("a.debug", false, "debug mode to print out filer A received files")
	syncOptions.bDebug = cmdFilerSynchronize.Flag.Bool("b.debug", false, "debug mode to print out filer B received files")
	syncOptions.aFromTsMs = cmdFilerSynchronize.Flag.Int64("a.fromTsMs", 0, "synchronization from timestamp on filer A. The unit is millisecond")
	syncOptions.bFromTsMs = cmdFilerSynchronize.Flag.Int64("b.fromTsMs", 0, "synchronization from timestamp on filer B. The unit is millisecond")
	syncOptions.concurrency = cmdFilerSynchronize.Flag.Int("concurrency", DefaultConcurrencyLimit, "The maximum number of files that will be synced concurrently.")
	syncCpuProfile = cmdFilerSynchronize.Flag.String("cpuprofile", "", "cpu profile output file")
	syncMemProfile = cmdFilerSynchronize.Flag.String("memprofile", "", "memory profile output file")
	syncOptions.metricsHttpIp = cmdFilerSynchronize.Flag.String("metricsIp", "", "metrics listen ip")
	syncOptions.metricsHttpPort = cmdFilerSynchronize.Flag.Int("metricsPort", 0, "metrics listen port")
	syncOptions.aDoDeleteFiles = cmdFilerSynchronize.Flag.Bool("a.doDeleteFiles", true, "delete and update files when synchronizing on filer A")
	syncOptions.bDoDeleteFiles = cmdFilerSynchronize.Flag.Bool("b.doDeleteFiles", true, "delete and update files when synchronizing on filer B")
	syncOptions.clientId = util.RandomInt32()
}

var cmdFilerSynchronize = &Command{
	UsageLine: "filer.sync -a=<oneFilerHost>:<oneFilerPort> -b=<otherFilerHost>:<otherFilerPort>",
	Short:     "resumable continuous synchronization between two active-active or active-passive SeaweedFS clusters",
	Long: `resumable continuous synchronization for file changes between two active-active or active-passive filers

	filer.sync listens on filer notifications. If any file is updated, it will fetch the updated content,
	and write to the other destination. Different from filer.replicate:

	* filer.sync only works between two filers.
	* filer.sync does not need any special message queue setup.
	* filer.sync supports both active-active and active-passive modes.
	
	If restarted, the synchronization will resume from the previous checkpoints, persisted every minute.
	A fresh sync will start from the earliest metadata logs.

`,
}

func runFilerSynchronize(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	grace.SetupProfiling(*syncCpuProfile, *syncMemProfile)

	filerA := pb.ServerAddress(*syncOptions.filerA)
	filerB := pb.ServerAddress(*syncOptions.filerB)

	// start filer.sync metrics server
	go statsCollect.StartMetricsServer(*syncOptions.metricsHttpIp, *syncOptions.metricsHttpPort)

	// read a filer signature
	aFilerSignature, aFilerErr := replication.ReadFilerSignature(grpcDialOption, filerA)
	if aFilerErr != nil {
		glog.Errorf("get filer 'a' signature %d error from %s to %s: %v", aFilerSignature, *syncOptions.filerA, *syncOptions.filerB, aFilerErr)
		return true
	}
	// read b filer signature
	bFilerSignature, bFilerErr := replication.ReadFilerSignature(grpcDialOption, filerB)
	if bFilerErr != nil {
		glog.Errorf("get filer 'b' signature %d error from %s to %s: %v", bFilerSignature, *syncOptions.filerA, *syncOptions.filerB, bFilerErr)
		return true
	}

	go func() {
		// a->b
		// set synchronization start timestamp to offset
		initOffsetError := initOffsetFromTsMs(grpcDialOption, filerB, aFilerSignature, *syncOptions.bFromTsMs, getSignaturePrefixByPath(*syncOptions.aPath))
		if initOffsetError != nil {
			glog.Errorf("init offset from timestamp %d error from %s to %s: %v", *syncOptions.bFromTsMs, *syncOptions.filerA, *syncOptions.filerB, initOffsetError)
			os.Exit(2)
		}
		for {
			syncOptions.clientEpoch.Add(1)
			err := doSubscribeFilerMetaChanges(
				syncOptions.clientId,
				syncOptions.clientEpoch.Load(),
				grpcDialOption,
				filerA,
				*syncOptions.aPath,
				util.StringSplit(*syncOptions.aExcludePaths, ","),
				*syncOptions.aProxyByFiler,
				filerB,
				*syncOptions.bPath,
				*syncOptions.bReplication,
				*syncOptions.bCollection,
				*syncOptions.bTtlSec,
				*syncOptions.bProxyByFiler,
				*syncOptions.bDiskType,
				*syncOptions.bDebug,
				*syncOptions.concurrency,
				*syncOptions.bDoDeleteFiles,
				aFilerSignature,
				bFilerSignature)
			if err != nil {
				glog.Errorf("sync from %s to %s: %v", *syncOptions.filerA, *syncOptions.filerB, err)
				time.Sleep(1747 * time.Millisecond)
			}
		}
	}()

	if !*syncOptions.isActivePassive {
		// b->a
		// set synchronization start timestamp to offset
		initOffsetError := initOffsetFromTsMs(grpcDialOption, filerA, bFilerSignature, *syncOptions.aFromTsMs, getSignaturePrefixByPath(*syncOptions.bPath))
		if initOffsetError != nil {
			glog.Errorf("init offset from timestamp %d error from %s to %s: %v", *syncOptions.aFromTsMs, *syncOptions.filerB, *syncOptions.filerA, initOffsetError)
			os.Exit(2)
		}
		go func() {
			for {
				syncOptions.clientEpoch.Add(1)
				err := doSubscribeFilerMetaChanges(
					syncOptions.clientId,
					syncOptions.clientEpoch.Load(),
					grpcDialOption,
					filerB,
					*syncOptions.bPath,
					util.StringSplit(*syncOptions.bExcludePaths, ","),
					*syncOptions.bProxyByFiler,
					filerA,
					*syncOptions.aPath,
					*syncOptions.aReplication,
					*syncOptions.aCollection,
					*syncOptions.aTtlSec,
					*syncOptions.aProxyByFiler,
					*syncOptions.aDiskType,
					*syncOptions.aDebug,
					*syncOptions.concurrency,
					*syncOptions.aDoDeleteFiles,
					bFilerSignature,
					aFilerSignature)
				if err != nil {
					glog.Errorf("sync from %s to %s: %v", *syncOptions.filerB, *syncOptions.filerA, err)
					time.Sleep(2147 * time.Millisecond)
				}
			}
		}()
	}

	select {}

	return true
}

// initOffsetFromTsMs Initialize offset
func initOffsetFromTsMs(grpcDialOption grpc.DialOption, targetFiler pb.ServerAddress, sourceFilerSignature int32, fromTsMs int64, signaturePrefix string) error {
	if fromTsMs <= 0 {
		return nil
	}
	// convert to nanosecond
	fromTsNs := fromTsMs * 1000_000
	// If not successful, exit the program.
	setOffsetErr := setOffset(grpcDialOption, targetFiler, signaturePrefix, sourceFilerSignature, fromTsNs)
	if setOffsetErr != nil {
		return setOffsetErr
	}
	glog.Infof("setOffset from timestamp ms success! start offset: %d from %s to %s", fromTsNs, *syncOptions.filerA, *syncOptions.filerB)
	return nil
}

func doSubscribeFilerMetaChanges(clientId int32, clientEpoch int32, grpcDialOption grpc.DialOption, sourceFiler pb.ServerAddress, sourcePath string, sourceExcludePaths []string, sourceReadChunkFromFiler bool, targetFiler pb.ServerAddress, targetPath string,
	replicationStr, collection string, ttlSec int, sinkWriteChunkByFiler bool, diskType string, debug bool, concurrency int, doDeleteFiles bool, sourceFilerSignature int32, targetFilerSignature int32) error {

	// if first time, start from now
	// if has previously synced, resume from that point of time
	sourceFilerOffsetTsNs, err := getOffset(grpcDialOption, targetFiler, getSignaturePrefixByPath(sourcePath), sourceFilerSignature)
	if err != nil {
		return err
	}

	glog.V(0).Infof("start sync %s(%d) => %s(%d) from %v(%d)", sourceFiler, sourceFilerSignature, targetFiler, targetFilerSignature, time.Unix(0, sourceFilerOffsetTsNs), sourceFilerOffsetTsNs)

	// create filer sink
	filerSource := &source.FilerSource{}
	filerSource.DoInitialize(sourceFiler.ToHttpAddress(), sourceFiler.ToGrpcAddress(), sourcePath, sourceReadChunkFromFiler)
	filerSink := &filersink.FilerSink{}
	filerSink.DoInitialize(targetFiler.ToHttpAddress(), targetFiler.ToGrpcAddress(), targetPath, replicationStr, collection, ttlSec, diskType, grpcDialOption, sinkWriteChunkByFiler)
	filerSink.SetSourceFiler(filerSource)

	persistEventFn := genProcessFunction(sourcePath, targetPath, sourceExcludePaths, nil, filerSink, doDeleteFiles, debug)

	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		for _, sig := range message.Signatures {
			if sig == targetFilerSignature && targetFilerSignature != 0 {
				fmt.Printf("%s skipping %s change to %v\n", targetFiler, sourceFiler, message)
				return nil
			}
		}
		return persistEventFn(resp)
	}

	if concurrency < 0 || concurrency > 1024 {
		glog.Warningf("invalid concurrency value, using default: %d", DefaultConcurrencyLimit)
		concurrency = DefaultConcurrencyLimit
	}
	processor := NewMetadataProcessor(processEventFn, concurrency, sourceFilerOffsetTsNs)

	var lastLogTsNs = time.Now().UnixNano()
	var clientName = fmt.Sprintf("syncFrom_%s_To_%s", string(sourceFiler), string(targetFiler))
	processEventFnWithOffset := pb.AddOffsetFunc(func(resp *filer_pb.SubscribeMetadataResponse) error {
		processor.AddSyncJob(resp)
		return nil
	}, 3*time.Second, func(counter int64, lastTsNs int64) error {
		offsetTsNs := processor.processedTsWatermark.Load()
		if offsetTsNs == 0 {
			return nil
		}
		// use processor.processedTsWatermark instead of the lastTsNs from the most recent job
		now := time.Now().UnixNano()
		glog.V(0).Infof("sync %s to %s progressed to %v %0.2f/sec", sourceFiler, targetFiler, time.Unix(0, offsetTsNs), float64(counter)/(float64(now-lastLogTsNs)/1e9))
		lastLogTsNs = now
		// collect synchronous offset
		statsCollect.FilerSyncOffsetGauge.WithLabelValues(sourceFiler.String(), targetFiler.String(), clientName, sourcePath).Set(float64(offsetTsNs))
		return setOffset(grpcDialOption, targetFiler, getSignaturePrefixByPath(sourcePath), sourceFilerSignature, offsetTsNs)
	})

	prefix := sourcePath
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             clientName,
		ClientId:               clientId,
		ClientEpoch:            clientEpoch,
		SelfSignature:          targetFilerSignature,
		PathPrefix:             prefix,
		AdditionalPathPrefixes: nil,
		DirectoriesToWatch:     nil,
		StartTsNs:              sourceFilerOffsetTsNs,
		StopTsNs:               0,
		EventErrorType:         pb.RetryForeverOnError,
	}

	return pb.FollowMetadata(sourceFiler, grpcDialOption, metadataFollowOption, processEventFnWithOffset)

}

// When each business is distinguished according to path, and offsets need to be maintained separately.
func getSignaturePrefixByPath(path string) string {
	// compatible historical version
	if path == "/" {
		return SyncKeyPrefix
	} else {
		return SyncKeyPrefix + path
	}
}

func getOffset(grpcDialOption grpc.DialOption, filer pb.ServerAddress, signaturePrefix string, signature int32) (lastOffsetTsNs int64, readErr error) {

	readErr = pb.WithFilerClient(false, signature, filer, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		syncKey := []byte(signaturePrefix + "____")
		util.Uint32toBytes(syncKey[len(signaturePrefix):len(signaturePrefix)+4], uint32(signature))

		resp, err := client.KvGet(context.Background(), &filer_pb.KvGetRequest{Key: syncKey})
		if err != nil {
			return err
		}

		if len(resp.Error) != 0 {
			return errors.New(resp.Error)
		}
		if len(resp.Value) < 8 {
			return nil
		}

		lastOffsetTsNs = int64(util.BytesToUint64(resp.Value))

		return nil
	})

	return

}

func setOffset(grpcDialOption grpc.DialOption, filer pb.ServerAddress, signaturePrefix string, signature int32, offsetTsNs int64) error {
	return pb.WithFilerClient(false, signature, filer, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

		syncKey := []byte(signaturePrefix + "____")
		util.Uint32toBytes(syncKey[len(signaturePrefix):len(signaturePrefix)+4], uint32(signature))

		valueBuf := make([]byte, 8)
		util.Uint64toBytes(valueBuf, uint64(offsetTsNs))

		resp, err := client.KvPut(context.Background(), &filer_pb.KvPutRequest{
			Key:   syncKey,
			Value: valueBuf,
		})
		if err != nil {
			return err
		}

		if len(resp.Error) != 0 {
			return errors.New(resp.Error)
		}

		return nil

	})

}

func genProcessFunction(sourcePath string, targetPath string, excludePaths []string, reExcludeFileName *regexp.Regexp, dataSink sink.ReplicationSink, doDeleteFiles bool, debug bool) func(resp *filer_pb.SubscribeMetadataResponse) error {
	// process function
	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification

		var sourceOldKey, sourceNewKey util.FullPath
		if message.OldEntry != nil {
			sourceOldKey = util.FullPath(resp.Directory).Child(message.OldEntry.Name)
		}
		if message.NewEntry != nil {
			sourceNewKey = util.FullPath(message.NewParentPath).Child(message.NewEntry.Name)
		}

		if debug {
			glog.V(0).Infof("received %v", resp)
		}

		if isMultipartUploadDir(resp.Directory + "/") {
			return nil
		}

		if !strings.HasPrefix(resp.Directory+"/", sourcePath) {
			return nil
		}
		for _, excludePath := range excludePaths {
			if strings.HasPrefix(resp.Directory+"/", excludePath) {
				return nil
			}
		}
		if reExcludeFileName != nil && reExcludeFileName.MatchString(message.NewEntry.Name) {
			return nil
		}
		if dataSink.IsIncremental() {
			doDeleteFiles = false
		}
		// handle deletions
		if filer_pb.IsDelete(resp) {
			if !doDeleteFiles {
				return nil
			}
			if !strings.HasPrefix(string(sourceOldKey), sourcePath) {
				return nil
			}
			key := buildKey(dataSink, message, targetPath, sourceOldKey, sourcePath)
			return dataSink.DeleteEntry(key, message.OldEntry.IsDirectory, message.DeleteChunks, message.Signatures)
		}

		// handle new entries
		if filer_pb.IsCreate(resp) {
			if !strings.HasPrefix(string(sourceNewKey), sourcePath) {
				return nil
			}
			key := buildKey(dataSink, message, targetPath, sourceNewKey, sourcePath)
			if err := dataSink.CreateEntry(key, message.NewEntry, message.Signatures); err != nil {
				return fmt.Errorf("create entry1 : %w", err)
			} else {
				return nil
			}
		}

		// this is something special?
		if filer_pb.IsEmpty(resp) {
			return nil
		}

		// handle updates
		if strings.HasPrefix(string(sourceOldKey), sourcePath) {
			// old key is in the watched directory
			if strings.HasPrefix(string(sourceNewKey), sourcePath) {
				// new key is also in the watched directory
				if doDeleteFiles {
					oldKey := util.Join(targetPath, string(sourceOldKey)[len(sourcePath):])
					if strings.HasSuffix(sourcePath, "/") {
						message.NewParentPath = util.Join(targetPath, message.NewParentPath[len(sourcePath)-1:])
					} else {
						message.NewParentPath = util.Join(targetPath, message.NewParentPath[len(sourcePath):])
					}
					foundExisting, err := dataSink.UpdateEntry(string(oldKey), message.OldEntry, message.NewParentPath, message.NewEntry, message.DeleteChunks, message.Signatures)
					if foundExisting {
						return err
					}

					// not able to find old entry
					if err = dataSink.DeleteEntry(string(oldKey), message.OldEntry.IsDirectory, false, message.Signatures); err != nil {
						return fmt.Errorf("delete old entry %v: %w", oldKey, err)
					}
				}
				// create the new entry
				newKey := buildKey(dataSink, message, targetPath, sourceNewKey, sourcePath)
				if err := dataSink.CreateEntry(newKey, message.NewEntry, message.Signatures); err != nil {
					return fmt.Errorf("create entry2 : %w", err)
				} else {
					return nil
				}

			} else {
				// new key is outside the watched directory
				if doDeleteFiles {
					key := buildKey(dataSink, message, targetPath, sourceOldKey, sourcePath)
					return dataSink.DeleteEntry(key, message.OldEntry.IsDirectory, message.DeleteChunks, message.Signatures)
				}
			}
		} else {
			// old key is outside the watched directory
			if strings.HasPrefix(string(sourceNewKey), sourcePath) {
				// new key is in the watched directory
				key := buildKey(dataSink, message, targetPath, sourceNewKey, sourcePath)
				if err := dataSink.CreateEntry(key, message.NewEntry, message.Signatures); err != nil {
					return fmt.Errorf("create entry3 : %w", err)
				} else {
					return nil
				}
			} else {
				// new key is also outside the watched directory
				// skip
			}
		}

		return nil
	}
	return processEventFn
}

func buildKey(dataSink sink.ReplicationSink, message *filer_pb.EventNotification, targetPath string, sourceKey util.FullPath, sourcePath string) (key string) {
	if !dataSink.IsIncremental() {
		key = util.Join(targetPath, string(sourceKey)[len(sourcePath):])
	} else {
		var mTime int64
		if message.NewEntry != nil {
			mTime = message.NewEntry.Attributes.Mtime
		} else if message.OldEntry != nil {
			mTime = message.OldEntry.Attributes.Mtime
		}
		dateKey := time.Unix(mTime, 0).Format("2006-01-02")
		key = util.Join(targetPath, dateKey, string(sourceKey)[len(sourcePath):])
	}

	return escapeKey(key)
}
