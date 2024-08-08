package command

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"regexp"
	"strings"
	"time"
)

type FilerBackupOptions struct {
	isActivePassive *bool
	filer           *string
	path            *string
	excludePaths    *string
	excludeFileName *string
	debug           *bool
	proxyByFiler    *bool
	doDeleteFiles   *bool
	timeAgo         *time.Duration
	retentionDays   *int
}

var (
	filerBackupOptions FilerBackupOptions
)

func init() {
	cmdFilerBackup.Run = runFilerBackup // break init cycle
	filerBackupOptions.filer = cmdFilerBackup.Flag.String("filer", "localhost:8888", "filer of one SeaweedFS cluster")
	filerBackupOptions.path = cmdFilerBackup.Flag.String("filerPath", "/", "directory to sync on filer")
	filerBackupOptions.excludePaths = cmdFilerBackup.Flag.String("filerExcludePaths", "", "exclude directories to sync on filer")
	filerBackupOptions.excludeFileName = cmdFilerBackup.Flag.String("filerExcludeFileName", "", "exclude file names that match the regexp to sync on filer")
	filerBackupOptions.proxyByFiler = cmdFilerBackup.Flag.Bool("filerProxy", false, "read and write file chunks by filer instead of volume servers")
	filerBackupOptions.doDeleteFiles = cmdFilerBackup.Flag.Bool("doDeleteFiles", false, "delete files on the destination")
	filerBackupOptions.debug = cmdFilerBackup.Flag.Bool("debug", false, "debug mode to print out received files")
	filerBackupOptions.timeAgo = cmdFilerBackup.Flag.Duration("timeAgo", 0, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	filerBackupOptions.retentionDays = cmdFilerBackup.Flag.Int("retentionDays", 0, "incremental backup retention days")
}

var cmdFilerBackup = &Command{
	UsageLine: "filer.backup -filer=<filerHost>:<filerPort> ",
	Short:     "resume-able continuously replicate files from a SeaweedFS cluster to another location defined in replication.toml",
	Long: `resume-able continuously replicate files from a SeaweedFS cluster to another location defined in replication.toml

	filer.backup listens on filer notifications. If any file is updated, it will fetch the updated content,
	and write to the destination. This is to replace filer.replicate command since additional message queue is not needed.

	If restarted and "-timeAgo" is not set, the synchronization will resume from the previous checkpoints, persisted every minute.
	A fresh sync will start from the earliest metadata logs. To reset the checkpoints, just set "-timeAgo" to a high value.

`,
}

func runFilerBackup(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()
	util.LoadConfiguration("replication", true)

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	clientId := util.RandomInt32()
	var clientEpoch int32

	for {
		clientEpoch++
		err := doFilerBackup(grpcDialOption, &filerBackupOptions, clientId, clientEpoch)
		if err != nil {
			glog.Errorf("backup from %s: %v", *filerBackupOptions.filer, err)
			time.Sleep(1747 * time.Millisecond)
		}
	}

	return true
}

const (
	BackupKeyPrefix = "backup."
)

func doFilerBackup(grpcDialOption grpc.DialOption, backupOption *FilerBackupOptions, clientId int32, clientEpoch int32) error {

	// find data sink
	dataSink := findSink(util.GetViper())
	if dataSink == nil {
		return fmt.Errorf("no data sink configured in replication.toml")
	}

	sourceFiler := pb.ServerAddress(*backupOption.filer)
	sourcePath := *backupOption.path
	excludePaths := util.StringSplit(*backupOption.excludePaths, ",")
	var reExcludeFileName *regexp.Regexp
	if *backupOption.excludeFileName != "" {
		var err error
		if reExcludeFileName, err = regexp.Compile(*backupOption.excludeFileName); err != nil {
			return fmt.Errorf("error compile regexp %v for exclude file name: %+v", *backupOption.excludeFileName, err)
		}
	}
	timeAgo := *backupOption.timeAgo
	targetPath := dataSink.GetSinkToDirectory()
	debug := *backupOption.debug

	// get start time for the data sink
	startFrom := time.Unix(0, 0)
	sinkId := util.HashStringToLong(dataSink.GetName() + dataSink.GetSinkToDirectory())
	if timeAgo.Milliseconds() == 0 {
		lastOffsetTsNs, err := getOffset(grpcDialOption, sourceFiler, BackupKeyPrefix, int32(sinkId))
		if err != nil {
			glog.V(0).Infof("starting from %v", startFrom)
		} else {
			startFrom = time.Unix(0, lastOffsetTsNs)
			glog.V(0).Infof("resuming from %v", startFrom)
		}
	} else {
		startFrom = time.Now().Add(-timeAgo)
		glog.V(0).Infof("start time is set to %v", startFrom)
	}

	// create filer sink
	filerSource := &source.FilerSource{}
	filerSource.DoInitialize(
		sourceFiler.ToHttpAddress(),
		sourceFiler.ToGrpcAddress(),
		sourcePath,
		*backupOption.proxyByFiler)
	dataSink.SetSourceFiler(filerSource)

	processEventFn := genProcessFunction(sourcePath, targetPath, excludePaths, reExcludeFileName, dataSink, *backupOption.doDeleteFiles, debug)

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
		EventErrorType:         pb.RetryForeverOnError,
	}

	return pb.FollowMetadata(sourceFiler, grpcDialOption, metadataFollowOption, processEventFnWithOffset)

}
