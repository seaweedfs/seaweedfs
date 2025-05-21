package sftpd

import (
	"fmt"
	"io"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/utils"
)

type SeaweedFileReaderAt struct {
	fs         *SftpServer
	entry      *filer_pb.Entry
	reader     io.ReadSeeker
	mu         sync.Mutex
	bufferSize int
	cache      *utils.LruCache
	fileSize   int64
}

func NewSeaweedFileReaderAt(fs *SftpServer, entry *filer_pb.Entry) *SeaweedFileReaderAt {
	return &SeaweedFileReaderAt{
		fs:         fs,
		entry:      entry,
		bufferSize: 5 * 1024 * 1024,       // 5MB
		cache:      utils.NewLRUCache(10), // Max 10 chunks = ~50MB
		fileSize:   int64(entry.Attributes.FileSize),
	}
}

func (ra *SeaweedFileReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	ra.mu.Lock()
	defer ra.mu.Unlock()

	if off >= ra.fileSize {
		return 0, io.EOF
	}

	remaining := len(p)
	readOffset := off
	totalRead := 0

	for remaining > 0 && readOffset < ra.fileSize {
		bufferKey := (readOffset / int64(ra.bufferSize)) * int64(ra.bufferSize)
		bufferOffset := int(readOffset - bufferKey)

		buffer, ok := ra.cache.Get(bufferKey)
		if !ok {
			readSize := ra.bufferSize
			if bufferKey+int64(readSize) > ra.fileSize {
				readSize = int(ra.fileSize - bufferKey)
			}

			if ra.reader == nil {
				r := filer.NewFileReader(ra.fs, ra.entry)
				if rs, ok := r.(io.ReadSeeker); ok {
					ra.reader = rs
				} else {
					return 0, fmt.Errorf("reader is not seekable")
				}
			}

			if _, err := ra.reader.Seek(bufferKey, io.SeekStart); err != nil {
				return 0, fmt.Errorf("seek error: %v", err)
			}

			buffer = make([]byte, readSize)
			readBytes, err := io.ReadFull(ra.reader, buffer)
			if err != nil && err != io.ErrUnexpectedEOF {
				return 0, fmt.Errorf("read error: %v", err)
			}
			buffer = buffer[:readBytes]
			ra.cache.Put(bufferKey, buffer)
		}

		toCopy := len(buffer) - bufferOffset
		if toCopy > remaining {
			toCopy = remaining
		}
		if toCopy <= 0 {
			break
		}

		copy(p[totalRead:], buffer[bufferOffset:bufferOffset+toCopy])
		totalRead += toCopy
		readOffset += int64(toCopy)
		remaining -= toCopy
	}

	if totalRead == 0 {
		return 0, io.EOF
	}
	if totalRead < len(p) {
		return totalRead, io.EOF
	}
	return totalRead, nil
}
