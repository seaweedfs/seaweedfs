package command

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"io"
	"time"
)

type FilerBackupOptions struct {
	isActivePassive *bool
	filer           *string
	path            *string
	debug           *bool
	proxyByFiler    *bool
	timeAgo         *time.Duration
}

var (
	filerBackupOptions FilerBackupOptions
)

func init() {
	cmdFilerBackup.Run = runFilerBackup // break init cycle
	filerBackupOptions.filer = cmdFilerBackup.Flag.String("filer", "localhost:8888", "filer of one SeaweedFS cluster")
	filerBackupOptions.path = cmdFilerBackup.Flag.String("filerPath", "/", "directory to sync on filer")
	filerBackupOptions.proxyByFiler = cmdFilerBackup.Flag.Bool("filerProxy", false, "read and write file chunks by filer instead of volume servers")
	filerBackupOptions.debug = cmdFilerBackup.Flag.Bool("debug", false, "debug mode to print out received files")
	filerBackupOptions.timeAgo = cmdFilerBackup.Flag.Duration("timeAgo", 0, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
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

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	util.LoadConfiguration("security", false)
	util.LoadConfiguration("replication", true)

	for {
		err := doFilerBackup(grpcDialOption, &filerBackupOptions)
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

func doFilerBackup(grpcDialOption grpc.DialOption, backupOption *FilerBackupOptions) error {

	// find data sink
	config := util.GetViper()
	dataSink := findSink(config)
	if dataSink == nil {
		return fmt.Errorf("no data sink configured in replication.toml")
	}

	sourceFiler := *backupOption.filer
	sourcePath := *backupOption.path
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
	filerSource.DoInitialize(sourceFiler, pb.ServerToGrpcAddress(sourceFiler), sourcePath, *backupOption.proxyByFiler)
	dataSink.SetSourceFiler(filerSource)

	processEventFn := genProcessFunction(sourcePath, targetPath, dataSink, debug)

	return pb.WithFilerClient(sourceFiler, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, err := client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
			ClientName: "backup_" + dataSink.GetName(),
			PathPrefix: sourcePath,
			SinceNs:    startFrom.UnixNano(),
		})
		if err != nil {
			return fmt.Errorf("listen: %v", err)
		}

		var counter int64
		var lastWriteTime time.Time
		for {
			resp, listenErr := stream.Recv()

			if listenErr == io.EOF {
				return nil
			}
			if listenErr != nil {
				return listenErr
			}

			if err := processEventFn(resp); err != nil {
				return fmt.Errorf("processEventFn: %v", err)
			}

			counter++
			if lastWriteTime.Add(3 * time.Second).Before(time.Now()) {
				glog.V(0).Infof("backup %s progressed to %v %0.2f/sec", sourceFiler, time.Unix(0, resp.TsNs), float64(counter)/float64(3))
				counter = 0
				lastWriteTime = time.Now()
				if err := setOffset(grpcDialOption, sourceFiler, BackupKeyPrefix, int32(sinkId), resp.TsNs); err != nil {
					return fmt.Errorf("setOffset: %v", err)
				}
			}

		}

	})

}
