package filer

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"

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

	// Trigger empty folder cleanup for local events
	// Remote events are handled via MetaAggregator.onMetadataChangeEvent
	f.triggerLocalEmptyFolderCleanup(oldEntry, newEntry)

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

	if err := f.LocalMetaLogBuffer.AddDataToBuffer([]byte(dir), data, event.TsNs); err != nil {
		glog.Errorf("failed to add data to log buffer for %s: %v", dir, err)
	}

}

// triggerLocalEmptyFolderCleanup triggers empty folder cleanup for local events
// This is needed because onMetadataChangeEvent is only called for remote peer events
func (f *Filer) triggerLocalEmptyFolderCleanup(oldEntry, newEntry *Entry) {
	if f.EmptyFolderCleaner == nil || !f.EmptyFolderCleaner.IsEnabled() {
		return
	}

	eventTime := time.Now()

	// Handle delete events (oldEntry exists, newEntry is nil)
	if oldEntry != nil && newEntry == nil {
		dir, name := oldEntry.FullPath.DirAndName()
		f.EmptyFolderCleaner.OnDeleteEvent(dir, name, oldEntry.IsDirectory(), eventTime)
	}

	// Handle create events (oldEntry is nil, newEntry exists)
	if oldEntry == nil && newEntry != nil {
		dir, name := newEntry.FullPath.DirAndName()
		f.EmptyFolderCleaner.OnCreateEvent(dir, name, newEntry.IsDirectory())
	}

	// Handle rename/move events (both exist but paths differ)
	if oldEntry != nil && newEntry != nil {
		oldDir, oldName := oldEntry.FullPath.DirAndName()
		newDir, newName := newEntry.FullPath.DirAndName()

		if oldDir != newDir || oldName != newName {
			// Treat old location as delete
			f.EmptyFolderCleaner.OnDeleteEvent(oldDir, oldName, oldEntry.IsDirectory(), eventTime)
			// Treat new location as create
			f.EmptyFolderCleaner.OnCreateEvent(newDir, newName, newEntry.IsDirectory())
		}
	}
}

func (f *Filer) logFlushFunc(logBuffer *log_buffer.LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {

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
		err = fmt.Errorf("reading from persisted logs: %w", visitErr)
		return
	}
	var logEntry *filer_pb.LogEntry
	for {
		logEntry, visitErr = visitor.GetNext()
		if visitErr != nil {
			if visitErr == io.EOF {
				break
			}
			err = fmt.Errorf("read next from persisted logs: %w", visitErr)
			return
		}
		isDone, visitErr = eachLogEntryFn(logEntry)
		if visitErr != nil {
			err = fmt.Errorf("process persisted log entry: %w", visitErr)
			return
		}
		lastTsNs = logEntry.TsNs
		if isDone {
			return
		}
	}

	return
}
