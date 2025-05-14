package sftpd

import (
	"fmt"
	"io"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type SeaweedFileReaderAt struct {
	fs         *SftpServer
	entry      *filer_pb.Entry
	reader     io.Reader // Use the correct type
	mu         sync.Mutex
	bufferSize int
	cache      map[int64][]byte
	fileSize   int64
}

func NewSeaweedFileReaderAt(fs *SftpServer, entry *filer_pb.Entry) *SeaweedFileReaderAt {
	return &SeaweedFileReaderAt{
		fs:         fs,
		entry:      entry,
		reader:     nil,             // Will be initialized on first read
		bufferSize: 5 * 1024 * 1024, // 1MB buffer size, adjust as needed
		cache:      make(map[int64][]byte),
		fileSize:   int64(entry.Attributes.FileSize),
	}
}

func (ra *SeaweedFileReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	ra.mu.Lock()
	defer ra.mu.Unlock()

	// Check if we're reading past the end of the file
	if off >= ra.fileSize {
		return 0, io.EOF
	}

	// Calculate the buffer key (aligned to buffer size)
	bufferKey := (off / int64(ra.bufferSize)) * int64(ra.bufferSize)

	// Check if we have this buffer in cache
	buffer, exists := ra.cache[bufferKey]

	// If not in cache, fetch the buffer
	if !exists {
		// Create reader if not exists
		if ra.reader == nil {
			ra.reader = filer.NewFileReader(ra.fs, ra.entry)
			if ra.reader == nil {
				return 0, fmt.Errorf("failed to create file reader")
			}
		}

		// Calculate how much to read
		readSize := ra.bufferSize
		if bufferKey+int64(readSize) > ra.fileSize {
			readSize = int(ra.fileSize - bufferKey)
		}

		// Seek to the buffer start position
		if seeker, ok := ra.reader.(io.Seeker); ok {
			_, err = seeker.Seek(bufferKey, io.SeekStart)
			if err != nil {
				return 0, fmt.Errorf("seek error: %v", err)
			}
		} else {
			// If we can't seek, create a new reader positioned at the right offset
			ra.reader = filer.NewFileReader(ra.fs, ra.entry)
			if ra.reader == nil {
				return 0, fmt.Errorf("failed to create file reader")
			}

			// Skip to the position
			toSkip := bufferKey
			skipBuf := make([]byte, 8192)
			for toSkip > 0 {
				skipSize := int64(len(skipBuf))
				if skipSize > toSkip {
					skipSize = toSkip
				}
				read, err := ra.reader.Read(skipBuf[:skipSize])
				if err != nil {
					return 0, fmt.Errorf("skip error: %v", err)
				}
				if read == 0 {
					return 0, fmt.Errorf("unable to skip to offset %d", bufferKey)
				}
				toSkip -= int64(read)
			}
		}

		// Read the buffer
		buffer = make([]byte, readSize)
		bytesRead, err := io.ReadFull(ra.reader, buffer)
		if err != nil && err != io.ErrUnexpectedEOF {
			return 0, fmt.Errorf("buffer read error: %v", err)
		}

		// Store in cache (only if we read something)
		if bytesRead > 0 {
			buffer = buffer[:bytesRead]
			ra.cache[bufferKey] = buffer
		} else {
			return 0, io.EOF
		}
	}

	// Calculate the offset within the buffer
	bufferOffset := int(off - bufferKey)

	// Calculate how much we can copy from this buffer
	copySize := len(buffer) - bufferOffset
	if copySize > len(p) {
		copySize = len(p)
	}

	// Copy from buffer to output
	if copySize <= 0 {
		return 0, io.EOF
	}

	copy(p, buffer[bufferOffset:bufferOffset+copySize])

	// If we didn't fill the entire output buffer and we're not at EOF
	if copySize < len(p) && bufferKey+int64(len(buffer)) < ra.fileSize {
		// Recursively read the next chunk
		nextOff := bufferKey + int64(len(buffer))

		// Release lock during recursive call to prevent deadlock
		ra.mu.Unlock()
		nextRead, err := ra.ReadAt(p[copySize:], nextOff)
		ra.mu.Lock() // Reacquire lock

		if err != nil && err != io.EOF {
			return copySize, err
		}
		return copySize + nextRead, nil
	}

	// Check if we're at EOF
	if bufferOffset+copySize >= len(buffer) && bufferKey+int64(len(buffer)) >= ra.fileSize {
		return copySize, io.EOF
	}

	return copySize, nil
}
