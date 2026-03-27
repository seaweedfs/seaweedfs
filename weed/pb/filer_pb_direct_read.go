package pb

import (
	"fmt"
	"io"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// LogFileReaderFn creates an io.ReadCloser for a set of file chunks.
// This is typically filer.NewChunkStreamReader or similar, passed in by
// the caller to avoid a circular dependency on the filer package.
type LogFileReaderFn func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error)

// ReadLogFileRefs reads log file data directly from volume servers using the
// chunk references, parses the entries, applies path filtering, and invokes
// processEventFn for each matching event.
//
// This replaces the server-side ReadPersistedLogBuffer path — the client does
// the work that the server previously did, but can read from multiple volume
// servers in parallel.
func ReadLogFileRefs(
	refs []*filer_pb.LogFileChunkRef,
	newReader LogFileReaderFn,
	startTsNs, stopTsNs int64,
	pathPrefix string,
	processEventFn ProcessMetadataFunc,
) (lastTsNs int64, err error) {

	for _, ref := range refs {
		if len(ref.Chunks) == 0 {
			continue
		}

		entries, readErr := readLogFileEntries(newReader, ref.Chunks, startTsNs, stopTsNs)
		if readErr != nil {
			glog.V(0).Infof("read log file filer=%s ts=%d: %v", ref.FilerId, ref.FileTsNs, readErr)
			continue // skip unreadable files, similar to server-side isChunkNotFoundError handling
		}

		for _, logEntry := range entries {
			event := &filer_pb.SubscribeMetadataResponse{}
			if unmarshalErr := proto.Unmarshal(logEntry.Data, event); unmarshalErr != nil {
				glog.Errorf("unmarshal log entry: %v", unmarshalErr)
				continue
			}

			if !matchesPathPrefix(event, pathPrefix) {
				continue
			}

			if err = processEventFn(event); err != nil {
				return lastTsNs, fmt.Errorf("process event: %w", err)
			}
			lastTsNs = event.TsNs
		}
	}
	return
}

// readLogFileEntries reads a single log file and parses the
// [4-byte size | protobuf LogEntry] records within.
func readLogFileEntries(newReader LogFileReaderFn, chunks []*filer_pb.FileChunk, startTsNs, stopTsNs int64) ([]*filer_pb.LogEntry, error) {
	reader, err := newReader(chunks)
	if err != nil {
		return nil, fmt.Errorf("create reader: %w", err)
	}
	defer reader.Close()

	sizeBuf := make([]byte, 4)
	var entries []*filer_pb.LogEntry

	for {
		n, err := reader.Read(sizeBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return entries, err
		}
		if n != 4 {
			return entries, fmt.Errorf("size %d bytes, expected 4", n)
		}

		size := util.BytesToUint32(sizeBuf)
		entryData := make([]byte, size)
		n, err = reader.Read(entryData)
		if err != nil {
			return entries, err
		}
		if n != int(size) {
			return entries, fmt.Errorf("entry data %d bytes, expected %d", n, size)
		}

		logEntry := &filer_pb.LogEntry{}
		if err = proto.Unmarshal(entryData, logEntry); err != nil {
			return entries, err
		}

		if logEntry.TsNs <= startTsNs {
			continue
		}
		if stopTsNs != 0 && logEntry.TsNs > stopTsNs {
			break
		}

		entries = append(entries, logEntry)
	}
	return entries, nil
}

// matchesPathPrefix checks if a subscription event matches the given path prefix.
func matchesPathPrefix(resp *filer_pb.SubscribeMetadataResponse, pathPrefix string) bool {
	if pathPrefix == "" || pathPrefix == "/" {
		return true
	}

	var entryName string
	if resp.EventNotification != nil {
		if resp.EventNotification.OldEntry != nil {
			entryName = resp.EventNotification.OldEntry.Name
		} else if resp.EventNotification.NewEntry != nil {
			entryName = resp.EventNotification.NewEntry.Name
		}
	}

	fullpath := resp.Directory + "/" + entryName
	if strings.HasPrefix(fullpath, pathPrefix) {
		return true
	}

	if resp.EventNotification != nil && resp.EventNotification.NewParentPath != "" {
		newFullPath := resp.EventNotification.NewParentPath + "/" + entryName
		if strings.HasPrefix(newFullPath, pathPrefix) {
			return true
		}
	}

	return false
}
