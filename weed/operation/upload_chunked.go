package operation

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
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
	ChunkSize         int32
	SmallFileLimit    int64
	Collection        string
	Replication       string
	DataCenter        string
	SaveSmallInline   bool
	Jwt               security.EncodedJwt
	MimeType          string
	AssignFunc        func(ctx context.Context, count int) (*VolumeAssignRequest, *AssignResult, error)
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
		
		// Get buffer from pool
		bytesBuffer := chunkBufferPool.Get().(*bytes.Buffer)
		limitedReader := io.LimitReader(partReader, int64(opt.ChunkSize))
		bytesBuffer.Reset()
		
		// Read one chunk
		dataSize, err := bytesBuffer.ReadFrom(limitedReader)
		if err != nil || dataSize == 0 {
			chunkBufferPool.Put(bytesBuffer)
			<-bytesBufferLimitChan
			if err != nil {
				uploadErrLock.Lock()
				if uploadErr == nil {
					uploadErr = err
				}
				uploadErrLock.Unlock()
			}
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
			
			uploader, uploaderErr := NewUploader()
			if uploaderErr != nil {
				uploadErrLock.Lock()
				if uploadErr == nil {
					uploadErr = fmt.Errorf("create uploader: %w", uploaderErr)
				}
				uploadErrLock.Unlock()
				return
			}
			
			uploadResult, uploadResultErr := uploader.UploadData(ctx, buf.Bytes(), uploadOption)
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
	
	// Check for errors
	if uploadErr != nil {
		glog.Errorf("chunked upload failed: %v", uploadErr)
		return nil, uploadErr
	}
	
	// Sort chunks by offset
	// Note: We could use slices.SortFunc here, but keeping it simple for Go 1.20 compatibility
	for i := 0; i < len(fileChunks); i++ {
		for j := i + 1; j < len(fileChunks); j++ {
			if fileChunks[i].Offset > fileChunks[j].Offset {
				fileChunks[i], fileChunks[j] = fileChunks[j], fileChunks[i]
			}
		}
	}
	
	return &ChunkedUploadResult{
		FileChunks:   fileChunks,
		Md5Hash:      md5Hash,
		TotalSize:    chunkOffset,
		SmallContent: nil,
	}, nil
}

