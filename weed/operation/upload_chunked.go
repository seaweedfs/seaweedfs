package operation

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
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
	Cipher          bool // encrypt data on volume servers
	AssignFunc      func(ctx context.Context, count int, expectedDataSize uint64) (*VolumeAssignRequest, *AssignResult, error)
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
			// Attach offset + bytes-read to distinguish client disconnect
			// before any data (offset=0,got=0) from mid-stream truncation.
			// A bare io.ErrUnexpectedEOF is not actionable on its own (see #9149).
			wrapped := fmt.Errorf("read chunk at offset %d (got %d bytes): %w", chunkOffset, dataSize, err)
			glog.V(2).Infof("UploadReaderInChunks: %v", wrapped)
			chunkBufferPool.Put(bytesBuffer)
			<-bytesBufferLimitChan
			uploadErrLock.Lock()
			if uploadErr == nil {
				uploadErr = wrapped
			}
			uploadErrLock.Unlock()
			break
		}
		// If no data was read, we've reached EOF
		// Only break if we've already read some data (chunkOffset > 0) or if this is truly EOF
		if dataSize == 0 {
			if chunkOffset == 0 {
				// Empty objects are valid for S3/HTTP uploads (e.g. zero-byte files).
				// Keep this at verbose level to avoid warning noise in normal operation.
				glog.V(4).Infof("UploadReaderInChunks: received 0 bytes on first read - creating empty file")
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
		go func(offset int64, buf *bytes.Buffer, size int64) {
			defer func() {
				chunkBufferPool.Put(buf)
				<-bytesBufferLimitChan
				wg.Done()
			}()

			// Assign volume for this chunk
			_, assignResult, assignErr := opt.AssignFunc(ctx, 1, uint64(size))
			if assignErr != nil {
				uploadErrLock.Lock()
				if uploadErr == nil {
					uploadErr = fmt.Errorf("assign volume: %w", assignErr)
				}
				uploadErrLock.Unlock()
				return
			}

			// Use per-assignment JWT if present, otherwise fall back to the original JWT
			// This is critical for secured clusters where each volume assignment has its own JWT
			jwt := opt.Jwt
			if assignResult.Auth != "" {
				jwt = assignResult.Auth
			}

			// Calculate MD5 for the chunk
			chunkMd5 := md5.Sum(buf.Bytes())
			chunkMd5B64 := base64.StdEncoding.EncodeToString(chunkMd5[:])

			var uploadResult *UploadResult
			var uploadResultErr error

			holders := chunkHolders(assignResult)
			// Gateway fan-out (PR #10078) writes every holder with type=replicate.
			// Rust volume servers replicate on a normal primary write instead; fan-out
			// causes cookie mismatch / invalid fid paths under cross-DC replication.
			if opt.UploadFunc == nil && !opt.Cipher && chunkUploadReplicaFanoutEnabled(holders) {
				uploadResult, uploadResultErr = uploadChunkToHolders(ctx, holders, assignResult.Fid, buf.Bytes(), jwt, chunkMd5B64, opt)
			} else {
				uploadOption := &UploadOption{
					UploadUrl:         fmt.Sprintf("http://%s/%s", assignResult.Url, assignResult.Fid),
					Cipher:            opt.Cipher,
					IsInputCompressed: false,
					MimeType:          opt.MimeType,
					PairMap:           nil,
					Jwt:               jwt,
					Md5:               chunkMd5B64,
				}
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
				IsCompressed: uploadResult.Gzip > 0,
			}
			fileChunksLock.Lock()
			fileChunks = append(fileChunks, chunk)
			fileChunksLock.Unlock()

		}(chunkOffset, bytesBuffer, dataSize)

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

// chunkHolders returns the assigned volume plus its replica holders.
func chunkHolders(assignResult *AssignResult) []string {
	hosts := []string{assignResult.Url}
	for _, replica := range assignResult.Replicas {
		if replica.Url != "" && replica.Url != assignResult.Url {
			hosts = append(hosts, replica.Url)
		}
	}
	return hosts
}

// uploadChunkToHolders writes the chunk to every holder concurrently (each with
// type=replicate). Used only when every holder is a Go volume server (PR #10078).
// On the first failure it cancels the remaining uploads and
// deletes any copies that already landed, so a partial fan-out leaves no
// orphaned needle the caller cannot see.
func uploadChunkToHolders(ctx context.Context, hosts []string, fid string, data []byte, jwt security.EncodedJwt, md5b64 string, opt *ChunkedUploadOption) (*UploadResult, error) {
	uploader, err := NewUploader()
	if err != nil {
		return nil, fmt.Errorf("create uploader: %w", err)
	}
	glog.V(4).Infof("replica fan-out: writing chunk %s to %d holders %v", fid, len(hosts), hosts)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type outcome struct {
		host   string
		result *UploadResult
		err    error
	}
	outcomes := make(chan outcome, len(hosts))
	for _, host := range hosts {
		go func(host string) {
			uploadOption := &UploadOption{
				UploadUrl:         fmt.Sprintf("http://%s/%s?type=replicate", host, fid),
				Cipher:            false,
				IsInputCompressed: false,
				MimeType:          opt.MimeType,
				PairMap:           nil,
				Jwt:               jwt,
				Md5:               md5b64,
			}
			r, e := uploader.UploadData(ctx, data, uploadOption)
			outcomes <- outcome{host, r, e}
		}(host)
	}
	var first *UploadResult
	var firstErr error
	var succeeded []string
	for range hosts {
		o := <-outcomes
		if o.err != nil {
			if firstErr == nil {
				firstErr = o.err
				cancel()
			}
		} else {
			succeeded = append(succeeded, o.host)
			if first == nil {
				first = o.result
			}
		}
	}
	if firstErr != nil {
		// A failed fan-out records no chunk, so roll back the copies that landed
		// before the cancel rather than leaking them as orphans.
		deleteChunkFromHolders(succeeded, fid, jwt)
		return nil, firstErr
	}
	return first, nil
}

// deleteChunkFromHolders best-effort removes a needle from each holder it landed
// on, using type=replicate so the volume drops only its local copy. A failed
// delete falls back to vacuum reclaiming the orphan.
func deleteChunkFromHolders(hosts []string, fid string, jwt security.EncodedJwt) {
	for _, host := range hosts {
		if err := util_http.Delete(fmt.Sprintf("http://%s/%s?type=replicate", host, fid), string(jwt)); err != nil {
			glog.Warningf("replica fan-out cleanup: delete %s from %s: %v", fid, host, err)
		}
	}
}
