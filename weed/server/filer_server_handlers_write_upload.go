package weed_server

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (fs *FilerServer) uploadRequestToChunks(w http.ResponseWriter, r *http.Request, reader io.Reader, chunkSize int32, fileName, contentType string, contentLength int64, so *operation.StorageOption) (fileChunks []*filer_pb.FileChunk, md5Hash hash.Hash, chunkOffset int64, uploadErr error, smallContent []byte) {
	query := r.URL.Query()

	isAppend := isAppend(r)
	if query.Has("offset") {
		offset := query.Get("offset")
		offsetInt, err := strconv.ParseInt(offset, 10, 64)
		if err != nil || offsetInt < 0 {
			err = fmt.Errorf("invalid 'offset': '%s'", offset)
			return nil, nil, 0, err, nil
		}
		if isAppend && offsetInt > 0 {
			err = fmt.Errorf("cannot set offset when op=append")
			return nil, nil, 0, err, nil
		}
		chunkOffset = offsetInt
	}

	return fs.uploadReaderToChunks(reader, chunkOffset, chunkSize, fileName, contentType, isAppend, so)
}

func (fs *FilerServer) uploadReaderToChunks(reader io.Reader, startOffset int64, chunkSize int32, fileName, contentType string, isAppend bool, so *operation.StorageOption) (fileChunks []*filer_pb.FileChunk, md5Hash hash.Hash, chunkOffset int64, uploadErr error, smallContent []byte) {

	md5Hash = md5.New()
	chunkOffset = startOffset
	var partReader = io.NopCloser(io.TeeReader(reader, md5Hash))

	var wg sync.WaitGroup
	var bytesBufferCounter int64 = 4
	bytesBufferLimitChan := make(chan struct{}, bytesBufferCounter)
	var fileChunksLock sync.Mutex
	var uploadErrLock sync.Mutex
	for {

		// need to throttle used byte buffer
		bytesBufferLimitChan <- struct{}{}

		bytesBuffer := bufPool.Get().(*bytes.Buffer)

		limitedReader := io.LimitReader(partReader, int64(chunkSize))

		bytesBuffer.Reset()

		dataSize, err := bytesBuffer.ReadFrom(limitedReader)

		// data, err := io.ReadAll(limitedReader)
		if err != nil || dataSize == 0 {
			bufPool.Put(bytesBuffer)
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
		if chunkOffset == 0 && !isAppend {
			if dataSize < fs.option.SaveToFilerLimit {
				chunkOffset += dataSize
				smallContent = make([]byte, dataSize)
				bytesBuffer.Read(smallContent)
				bufPool.Put(bytesBuffer)
				<-bytesBufferLimitChan
				stats.FilerHandlerCounter.WithLabelValues(stats.ContentSaveToFiler).Inc()
				break
			}
		} else {
			stats.FilerHandlerCounter.WithLabelValues(stats.AutoChunk).Inc()
		}

		wg.Add(1)
		go func(offset int64, buf *bytes.Buffer) {
			defer func() {
				bufPool.Put(buf)
				<-bytesBufferLimitChan
				wg.Done()
			}()

			chunks, toChunkErr := fs.dataToChunk(fileName, contentType, buf.Bytes(), offset, so)
			if toChunkErr != nil {
				uploadErrLock.Lock()
				if uploadErr == nil {
					uploadErr = toChunkErr
				}
				uploadErrLock.Unlock()
			}
			if chunks != nil {
				fileChunksLock.Lock()
				fileChunksSize := len(fileChunks) + len(chunks)
				for _, chunk := range chunks {
					fileChunks = append(fileChunks, chunk)
					glog.V(4).Infof("uploaded %s chunk %d to %s [%d,%d)", fileName, fileChunksSize, chunk.FileId, offset, offset+int64(chunk.Size))
				}
				fileChunksLock.Unlock()
			}
		}(chunkOffset, bytesBuffer)

		// reset variables for the next chunk
		chunkOffset = chunkOffset + dataSize

		// if last chunk was not at full chunk size, but already exhausted the reader
		if dataSize < int64(chunkSize) {
			break
		}
	}

	wg.Wait()

	if uploadErr != nil {
		glog.V(0).Infof("upload file %s error: %v", fileName, uploadErr)
		for _, chunk := range fileChunks {
			glog.V(4).Infof("purging failed uploaded %s chunk %s [%d,%d)", fileName, chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}
		fs.filer.DeleteUncommittedChunks(fileChunks)
		return nil, md5Hash, 0, uploadErr, nil
	}
	slices.SortFunc(fileChunks, func(a, b *filer_pb.FileChunk) int {
		return int(a.Offset - b.Offset)
	})
	return fileChunks, md5Hash, chunkOffset, nil, smallContent
}

func (fs *FilerServer) doUpload(urlLocation string, limitedReader io.Reader, fileName string, contentType string, pairMap map[string]string, auth security.EncodedJwt) (*operation.UploadResult, error, []byte) {

	stats.FilerHandlerCounter.WithLabelValues(stats.ChunkUpload).Inc()
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues(stats.ChunkUpload).Observe(time.Since(start).Seconds())
	}()

	uploadOption := &operation.UploadOption{
		UploadUrl:         urlLocation,
		Filename:          fileName,
		Cipher:            fs.option.Cipher,
		IsInputCompressed: false,
		MimeType:          contentType,
		PairMap:           pairMap,
		Jwt:               auth,
	}

	uploader, err := operation.NewUploader()
	if err != nil {
		return nil, err, []byte{}
	}

	uploadResult, err, data := uploader.Upload(limitedReader, uploadOption)
	if uploadResult != nil && uploadResult.RetryCount > 0 {
		stats.FilerHandlerCounter.WithLabelValues(stats.ChunkUploadRetry).Add(float64(uploadResult.RetryCount))
	}
	return uploadResult, err, data
}

func (fs *FilerServer) dataToChunk(fileName, contentType string, data []byte, chunkOffset int64, so *operation.StorageOption) ([]*filer_pb.FileChunk, error) {
	dataReader := util.NewBytesReader(data)

	// retry to assign a different file id
	var fileId, urlLocation string
	var auth security.EncodedJwt
	var uploadErr error
	var uploadResult *operation.UploadResult
	var failedFileChunks []*filer_pb.FileChunk

	err := util.Retry("filerDataToChunk", func() error {
		// assign one file id for one chunk
		fileId, urlLocation, auth, uploadErr = fs.assignNewFileInfo(so)
		if uploadErr != nil {
			glog.V(4).Infof("retry later due to assign error: %v", uploadErr)
			stats.FilerHandlerCounter.WithLabelValues(stats.ChunkAssignRetry).Inc()
			return uploadErr
		}
		// upload the chunk to the volume server
		uploadResult, uploadErr, _ = fs.doUpload(urlLocation, dataReader, fileName, contentType, nil, auth)
		if uploadErr != nil {
			glog.V(4).Infof("retry later due to upload error: %v", uploadErr)
			stats.FilerHandlerCounter.WithLabelValues(stats.ChunkDoUploadRetry).Inc()
			fid, _ := filer_pb.ToFileIdObject(fileId)
			fileChunk := filer_pb.FileChunk{
				FileId: fileId,
				Offset: chunkOffset,
				Fid:    fid,
			}
			failedFileChunks = append(failedFileChunks, &fileChunk)
			return uploadErr
		}
		return nil
	})
	if err != nil {
		glog.Errorf("upload error: %v", err)
		return failedFileChunks, err
	}

	// if last chunk exhausted the reader exactly at the border
	if uploadResult.Size == 0 {
		return nil, nil
	}
	return []*filer_pb.FileChunk{uploadResult.ToPbFileChunk(fileId, chunkOffset, time.Now().UnixNano())}, nil
}
