package filer

import (
	"context"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"regexp"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func (f *Filer) NotifyUpdateEvent(ctx context.Context, oldEntry, newEntry *Entry, deleteChunks, isFromOtherCluster bool, signatures []int32) {
	f.notifyUpdateEvent(ctx, oldEntry, newEntry, deleteChunks, isFromOtherCluster, signatures)
}

func (f *Filer) notifyUpdateEvent(ctx context.Context, oldEntry, newEntry *Entry, deleteChunks, isFromOtherCluster bool, signatures []int32) *filer_pb.SubscribeMetadataResponse {
	if metadataEventsSuppressed(ctx) {
		return nil
	}

	var fullpath string
	if oldEntry != nil {
		fullpath = string(oldEntry.FullPath)
	} else if newEntry != nil {
		fullpath = string(newEntry.FullPath)
	} else {
		return nil
	}

	// println("fullpath:", fullpath)

	if strings.HasPrefix(fullpath, SystemLogDir) {
		return nil
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

	event := f.newMetadataEvent(oldEntry, newEntry, deleteChunks, isFromOtherCluster, signatures)
	eventNotification := event.EventNotification

	if notification.Queue != nil {
		glog.V(3).Infof("notifying entry update %v", fullpath)
		if err := notification.Queue.SendMessage(fullpath, eventNotification); err != nil {
			// throw message
			glog.Error(err)
		}
	}

	f.logMetaEvent(ctx, event)
	if sink := metadataEventSinkFromContext(ctx); sink != nil {
		sink.Record(event)
	}

	// Trigger empty folder cleanup for local events
	// Remote events are handled via MetaAggregator.onMetadataChangeEvent
	f.triggerLocalEmptyFolderCleanup(oldEntry, newEntry)

	return event
}

func (f *Filer) newMetadataEvent(oldEntry, newEntry *Entry, deleteChunks, isFromOtherCluster bool, signatures []int32) *filer_pb.SubscribeMetadataResponse {
	if oldEntry == nil && newEntry == nil {
		return nil
	}
	var fullpath util.FullPath
	if oldEntry != nil {
		fullpath = oldEntry.FullPath
	}
	if fullpath == "" && newEntry != nil {
		fullpath = newEntry.FullPath
	}
	dir, _ := fullpath.DirAndName()
	newParentPath := ""
	if newEntry != nil {
		newParentPath, _ = newEntry.FullPath.DirAndName()
	}
	return &filer_pb.SubscribeMetadataResponse{
		Directory: dir,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:           oldEntry.ToProtoEntry(),
			NewEntry:           newEntry.ToProtoEntry(),
			DeleteChunks:       deleteChunks,
			NewParentPath:      newParentPath,
			IsFromOtherCluster: isFromOtherCluster,
			Signatures:         signatures,
		},
		TsNs: time.Now().UnixNano(),
	}
}

func (f *Filer) logMetaEvent(ctx context.Context, event *filer_pb.SubscribeMetadataResponse) {
	data, err := proto.Marshal(event)
	if err != nil {
		glog.Errorf("failed to marshal filer_pb.SubscribeMetadataResponse %+v: %v", event, err)
		return
	}

	if err := f.LocalMetaLogBuffer.AddDataToBuffer([]byte(event.Directory), data, event.TsNs); err != nil {
		glog.Errorf("failed to add data to log buffer for %s: %v", event.Directory, err)
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
	volumeNotFoundPattern = regexp.MustCompile(`volume \d+? not found`)
	chunkNotFoundPattern  = regexp.MustCompile(`(urls not found|File Not Found)`)
	httpNotFoundPattern   = regexp.MustCompile(`404 Not Found: not found`)
)

// isChunkNotFoundError checks if the error indicates that a volume or chunk
// has been deleted and is no longer available. These errors can be skipped
// when reading persisted log files since the data is unrecoverable.
func isChunkNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, util_http.ErrNotFound) || errors.Is(err, nethttp.ErrMissingFile) {
		return true
	}
	errMsg := err.Error()
	return volumeNotFoundPattern.MatchString(errMsg) ||
		chunkNotFoundPattern.MatchString(errMsg) ||
		httpNotFoundPattern.MatchString(errMsg)
}

func (f *Filer) ReadPersistedLogBuffer(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (lastTsNs int64, isDone bool, err error) {

	visitor, visitErr := f.collectPersistedLogBuffer(startPosition, stopTsNs)
	if visitErr != nil {
		if visitErr == io.EOF {
			return
		}
		err = fmt.Errorf("reading from persisted logs: %w", visitErr)
		return
	}

	// Readahead: run the visitor in a background goroutine so volume server I/O
	// for the next log file overlaps with event processing and gRPC delivery.
	const readaheadSize = 1024
	type entryOrErr struct {
		entry *filer_pb.LogEntry
		err   error
	}
	ch := make(chan entryOrErr, readaheadSize)
	stopReadahead := make(chan struct{})
	go func() {
		defer close(ch)
		for {
			entry, readErr := visitor.GetNext()
			if readErr != nil {
				if readErr != io.EOF {
					select {
					case ch <- entryOrErr{err: fmt.Errorf("read next from persisted logs: %w", readErr)}:
					case <-stopReadahead:
					}
				}
				return
			}
			select {
			case ch <- entryOrErr{entry: entry}:
			case <-stopReadahead:
				return
			}
		}
	}()
	defer close(stopReadahead)

	for item := range ch {
		if item.err != nil {
			err = item.err
			return
		}
		var processErr error
		isDone, processErr = eachLogEntryFn(item.entry)
		if processErr != nil {
			err = fmt.Errorf("process persisted log entry: %w", processErr)
			return
		}
		lastTsNs = item.entry.TsNs
		if isDone {
			return
		}
	}

	return
}

