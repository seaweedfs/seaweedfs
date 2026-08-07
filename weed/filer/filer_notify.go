package filer

import (
	"context"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"regexp"
	"strconv"
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

	f.onMetadataChangeEvent(event)

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

// metadataLogUploadLimit is the piece size a metadata log flush starts with. A
// volume server refuses anything over its -fileSizeLimitMB (256 MB by default),
// and a single oversized event — a CreateEntry carrying a large inline Content,
// say — grows the log buffer well past that, leaving a blob that can never be
// written and blocks every later flush behind it. BufferSize is what an
// ordinary flush already produces, so it is a size the volume server accepts
// under any configuration that works at all; a cluster running below it says so
// in the rejection and volumeFileSizeLimit picks the real limit up from there.
const metadataLogUploadLimit = log_buffer.BufferSize

var fileSizeLimitPattern = regexp.MustCompile(`file over the limited (\d+) bytes`)

// volumeFileSizeLimit reads the byte limit back out of a volume server's size
// rejection, and returns 0 for any other error.
func volumeFileSizeLimit(err error) int {
	match := fileSizeLimitPattern.FindStringSubmatch(err.Error())
	if match == nil {
		return 0
	}
	limit, convErr := strconv.Atoi(match[1])
	if convErr != nil {
		return 0
	}
	return limit
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

	// One piece at a time, each retried on its own so a partial success is not
	// replayed, and the piece size follows the limit the volume servers report.
	limit := metadataLogUploadLimit
	for len(buf) > 0 {
		piece := nextLogPiece(buf, limit)
		if err := f.appendToFile(targetFile, piece); err != nil {
			glog.V(0).Infof("metadata log write failed %s: %v", targetFile, err)
			if reported := volumeFileSizeLimit(err); reported > 0 && reported < limit {
				glog.V(0).Infof("metadata log upload limit lowered to %d bytes", reported)
				limit = reported
				continue
			}
			time.Sleep(737 * time.Millisecond)
			continue
		}
		buf = buf[len(piece):]
	}
}

// nextLogPiece returns the leading piece of a flushed log buffer, at most
// maxSize bytes and ending on a record boundary where it can so the piece still
// decodes on its own. A record longer than maxSize is cut by size instead; the
// readers fall back to streaming the whole file when a chunk does not decode
// standalone, so a record may cross a chunk boundary.
func nextLogPiece(buf []byte, maxSize int) []byte {
	if len(buf) <= maxSize {
		return buf
	}

	pos := 0
	for pos+4 <= len(buf) {
		size := int(util.BytesToUint32(buf[pos : pos+4]))
		end := pos + 4 + size
		if size <= 0 || end > len(buf) || end > maxSize {
			break
		}
		pos = end
	}
	if pos == 0 {
		// Either the leading record alone is over the limit, or buf starts
		// mid-record because the piece before it was cut by size.
		return buf[:maxSize]
	}
	return buf[:pos]
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

// persistedLogReplayLimit caps concurrent legacy replays; decodes are shared
// through the persisted-log cache, so this only bounds the listing fan-out.
const persistedLogReplayLimit = 64

var persistedLogReplaySem = make(chan struct{}, persistedLogReplayLimit)

func (f *Filer) ReadPersistedLogBuffer(ctx context.Context, startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (lastTsNs int64, isDone bool, err error) {

	// Cap concurrent replays; bail if the stream is already gone so cancelled
	// clients do not park on the semaphore.
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	select {
	case persistedLogReplaySem <- struct{}{}:
		defer func() { <-persistedLogReplaySem }()
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}

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
	readaheadDone := make(chan struct{})
	go func() {
		defer close(ch)
		defer close(readaheadDone)
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
	// Stop the readahead goroutine, wait for it to exit, then release any log
	// file readers it left open (e.g. on early return or cancellation).
	defer func() {
		close(stopReadahead)
		<-readaheadDone
		visitor.Close()
	}()

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
