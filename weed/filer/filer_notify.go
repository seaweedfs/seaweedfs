package filer

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"io"
	"regexp"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (f *Filer) NotifyUpdateEvent(ctx context.Context, oldEntry, newEntry *Entry, deleteChunks, isFromOtherCluster bool, signatures []int32) {
	var fullpath string
	if oldEntry != nil {
		fullpath = string(oldEntry.FullPath)
	} else if newEntry != nil {
		fullpath = string(newEntry.FullPath)
	} else {
		return
	}

	// println("fullpath:", fullpath)

	if strings.HasPrefix(fullpath, SystemLogDir) {
		return
	}
	foundSelf := false
	for _, sig := range signatures {
		if sig == f.Signature {
			foundSelf = true
		}
	}
	if !foundSelf {
		signatures = append(signatures, f.Signature)
	}

	newParentPath := ""
	if newEntry != nil {
		newParentPath, _ = newEntry.FullPath.DirAndName()
	}
	eventNotification := &filer_pb.EventNotification{
		OldEntry:           oldEntry.ToProtoEntry(),
		NewEntry:           newEntry.ToProtoEntry(),
		DeleteChunks:       deleteChunks,
		NewParentPath:      newParentPath,
		IsFromOtherCluster: isFromOtherCluster,
		Signatures:         signatures,
	}

	if notification.Queue != nil {
		glog.V(3).Infof("notifying entry update %v", fullpath)
		if err := notification.Queue.SendMessage(fullpath, eventNotification); err != nil {
			// throw message
			glog.Error(err)
		}
	}

	f.logMetaEvent(ctx, fullpath, eventNotification)

}

func (f *Filer) logMetaEvent(ctx context.Context, fullpath string, eventNotification *filer_pb.EventNotification) {

	dir, _ := util.FullPath(fullpath).DirAndName()

	event := &filer_pb.SubscribeMetadataResponse{
		Directory:         dir,
		EventNotification: eventNotification,
		TsNs:              time.Now().UnixNano(),
	}
	data, err := proto.Marshal(event)
	if err != nil {
		glog.Errorf("failed to marshal filer_pb.SubscribeMetadataResponse %+v: %v", event, err)
		return
	}

	f.LocalMetaLogBuffer.AddDataToBuffer([]byte(dir), data, event.TsNs)

}

func (f *Filer) logFlushFunc(logBuffer *log_buffer.LogBuffer, startTime, stopTime time.Time, buf []byte) {

	if len(buf) == 0 {
		return
	}

	startTime, stopTime = startTime.UTC(), stopTime.UTC()

	targetFile := fmt.Sprintf("%s/%04d-%02d-%02d/%02d-%02d.%08x", SystemLogDir,
		startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), startTime.Minute(), f.UniqueFilerId,
		// startTime.Second(), startTime.Nanosecond(),
	)

	for {
		if err := f.appendToFile(targetFile, buf); err != nil {
			glog.V(0).Infof("metadata log write failed %s: %v", targetFile, err)
			time.Sleep(737 * time.Millisecond)
		} else {
			break
		}
	}
}

var (
	VolumeNotFoundPattern = regexp.MustCompile(`volume \d+? not found`)
)

func (f *Filer) ReadPersistedLogBuffer(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (lastTsNs int64, isDone bool, err error) {

	visitor, visitErr := f.collectPersistedLogBuffer(startPosition, stopTsNs)
	if visitErr != nil {
		if visitErr == io.EOF {
			return
		}
		err = fmt.Errorf("reading from persisted logs: %v", visitErr)
		return
	}
	var logEntry *filer_pb.LogEntry
	for {
		logEntry, visitErr = visitor.GetNext()
		if visitErr != nil {
			if visitErr == io.EOF {
				break
			}
			err = fmt.Errorf("read next from persisted logs: %v", visitErr)
			return
		}
		isDone, visitErr = eachLogEntryFn(logEntry)
		if visitErr != nil {
			err = fmt.Errorf("process persisted log entry: %v", visitErr)
			return
		}
		lastTsNs = logEntry.TsNs
		if isDone {
			return
		}
	}

	return
}
