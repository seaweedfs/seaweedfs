package operation

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

// ChunkedUploadResult contains the result of a chunked upload
type ChunkedUploadResult struct {
	FileChunks   []*filer_pb.FileChunk
	Md5Hash      hash.Hash
	TotalSize    int64
	SmallContent []byte // For files smaller than threshold
}

// ChunkedUploadOption contains options for chunked uploads
type ChunkedUploadOption struct {
	ChunkSize       int32
	SmallFileLimit  int64
	Collection      string
	Replication     string
	DataCenter      string
	SaveSmallInline bool
	Jwt             security.EncodedJwt
	MimeType        string
	AssignFunc      func(ctx context.Context, count int) (*VolumeAssignRequest, *AssignResult, error)
	UploadFunc      func(ctx context.Context, data []byte, option *UploadOption) (*UploadResult, error) // Optional: for testing
}

var chunkBufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// UploadReaderInChunks reads from reader and uploads in chunks to volume servers
// This prevents OOM by processing the stream in fixed-size chunks
// Returns file chunks, MD5 hash, total size, and any small content stored inline
func UploadReaderInChunks(ctx context.Context, reader io.Reader, opt *ChunkedUploadOption) (*ChunkedUploadResult, error) {

	md5Hash := md5.New()
	var partReader = io.TeeReader(reader, md5Hash)

	var fileChunks []*filer_pb.FileChunk
	var fileChunksLock sync.Mutex
	var uploadErr error
	var uploadErrLock sync.Mutex
	var chunkOffset int64 = 0

	var wg sync.WaitGroup
	const bytesBufferCounter = 4
	bytesBufferLimitChan := make(chan struct{}, bytesBufferCounter)

uploadLoop:
	for {
		// Throttle buffer usage
		bytesBufferLimitChan <- struct{}{}

		// Check for errors from parallel uploads
		uploadErrLock.Lock()
		if uploadErr != nil {
			<-bytesBufferLimitChan
			uploadErrLock.Unlock()
			break
		}
		uploadErrLock.Unlock()

		// Check for context cancellation
		select {
		case <-ctx.Done():
			<-bytesBufferLimitChan
			uploadErrLock.Lock()
			if uploadErr == nil {
				uploadErr = ctx.Err()
			}
			uploadErrLock.Unlock()
			break uploadLoop
		default:
		}

		// Get buffer from pool
		bytesBuffer := chunkBufferPool.Get().(*bytes.Buffer)
		limitedReader := io.LimitReader(partReader, int64(opt.ChunkSize))
		bytesBuffer.Reset()

		// Read one chunk
		dataSize, err := bytesBuffer.ReadFrom(limitedReader)
		if err != nil {
			glog.V(2).Infof("UploadReaderInChunks: read error at offset %d: %v", chunkOffset, err)
			chunkBufferPool.Put(bytesBuffer)
			<-bytesBufferLimitChan
			uploadErrLock.Lock()
			if uploadErr == nil {
				uploadErr = err
			}
			uploadErrLock.Unlock()
			break
		}
		// If no data was read, we've reached EOF
		// Only break if we've already read some data (chunkOffset > 0) or if this is truly EOF
		if dataSize == 0 {
			if chunkOffset == 0 {
				glog.Warningf("UploadReaderInChunks: received 0 bytes on first read - creating empty file")
			}
			chunkBufferPool.Put(bytesBuffer)
			<-bytesBufferLimitChan
			// If we've already read some chunks, this is normal EOF
			// If we haven't read anything yet (chunkOffset == 0), this could be an empty file
			// which is valid (e.g., touch command creates 0-byte files)
			break
		}

		// For small files at offset 0, store inline instead of uploading
		if chunkOffset == 0 && opt.SaveSmallInline && dataSize < opt.SmallFileLimit {
			smallContent := make([]byte, dataSize)
			n, readErr := io.ReadFull(bytesBuffer, smallContent)
			chunkBufferPool.Put(bytesBuffer)
			<-bytesBufferLimitChan

			if readErr != nil {
				return nil, fmt.Errorf("failed to read small content: read %d of %d bytes: %w", n, dataSize, readErr)
			}

			return &ChunkedUploadResult{
				FileChunks:   nil,
				Md5Hash:      md5Hash,
				TotalSize:    dataSize,
				SmallContent: smallContent,
			}, nil
		}

		// Upload chunk in parallel goroutine
		wg.Add(1)
		go func(offset int64, buf *bytes.Buffer) {
			defer func() {
				chunkBufferPool.Put(buf)
				<-bytesBufferLimitChan
				wg.Done()
			}()

			// Assign volume for this chunk
			_, assignResult, assignErr := opt.AssignFunc(ctx, 1)
			if assignErr != nil {
				uploadErrLock.Lock()
				if uploadErr == nil {
					uploadErr = fmt.Errorf("assign volume: %w", assignErr)
				}
				uploadErrLock.Unlock()
				return
			}

			// Upload chunk data
			uploadUrl := fmt.Sprintf("http://%s/%s", assignResult.Url, assignResult.Fid)

			// Use per-assignment JWT if present, otherwise fall back to the original JWT
			// This is critical for secured clusters where each volume assignment has its own JWT
			jwt := opt.Jwt
			if assignResult.Auth != "" {
				jwt = assignResult.Auth
			}

			uploadOption := &UploadOption{
				UploadUrl:         uploadUrl,
				Cipher:            false,
				IsInputCompressed: false,
				MimeType:          opt.MimeType,
				PairMap:           nil,
				Jwt:               jwt,
			}

			var uploadResult *UploadResult
			var uploadResultErr error

			// Use mock upload function if provided (for testing), otherwise use real uploader
			if opt.UploadFunc != nil {
				uploadResult, uploadResultErr = opt.UploadFunc(ctx, buf.Bytes(), uploadOption)
			} else {
				uploader, uploaderErr := NewUploader()
				if uploaderErr != nil {
					uploadErrLock.Lock()
					if uploadErr == nil {
						uploadErr = fmt.Errorf("create uploader: %w", uploaderErr)
					}
					uploadErrLock.Unlock()
					return
				}
				uploadResult, uploadResultErr = uploader.UploadData(ctx, buf.Bytes(), uploadOption)
			}

			if uploadResultErr != nil {
				uploadErrLock.Lock()
				if uploadErr == nil {
					uploadErr = fmt.Errorf("upload chunk: %w", uploadResultErr)
				}
				uploadErrLock.Unlock()
				return
			}

			// Create chunk entry
			// Set ModifiedTsNs to current time (nanoseconds) to track when upload completed
			// This is critical for multipart uploads where the same part may be uploaded multiple times
			// The part with the latest ModifiedTsNs is selected as the authoritative version
			fid, _ := filer_pb.ToFileIdObject(assignResult.Fid)
			chunk := &filer_pb.FileChunk{
				FileId:       assignResult.Fid,
				Offset:       offset,
				Size:         uint64(uploadResult.Size),
				ModifiedTsNs: time.Now().UnixNano(),
				ETag:         uploadResult.ContentMd5,
				Fid:          fid,
				CipherKey:    uploadResult.CipherKey,
			}

			fileChunksLock.Lock()
			fileChunks = append(fileChunks, chunk)
			glog.V(4).Infof("uploaded chunk %d to %s [%d,%d)", len(fileChunks), chunk.FileId, offset, offset+int64(chunk.Size))
			fileChunksLock.Unlock()

		}(chunkOffset, bytesBuffer)

		// Update offset for next chunk
		chunkOffset += dataSize

		// If this was a partial chunk, we're done
		if dataSize < int64(opt.ChunkSize) {
			break
		}
	}

	// Wait for all uploads to complete
	wg.Wait()

	// Sort chunks by offset (do this even if there's an error, for cleanup purposes)
	sort.Slice(fileChunks, func(i, j int) bool {
		return fileChunks[i].Offset < fileChunks[j].Offset
	})

	// Check for errors - return partial results for cleanup
	if uploadErr != nil {
		glog.Errorf("chunked upload failed: %v (returning %d partial chunks for cleanup)", uploadErr, len(fileChunks))
		// IMPORTANT: Return partial results even on error so caller can cleanup orphaned chunks
		return &ChunkedUploadResult{
			FileChunks:   fileChunks,
			Md5Hash:      md5Hash,
			TotalSize:    chunkOffset,
			SmallContent: nil,
		}, uploadErr
	}

	return &ChunkedUploadResult{
		FileChunks:   fileChunks,
		Md5Hash:      md5Hash,
		TotalSize:    chunkOffset,
		SmallContent: nil,
	}, nil
}
