package weed_server

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (fs *FilerServer) uploadReaderToChunks(w http.ResponseWriter, r *http.Request, reader io.Reader, chunkSize int32, fileName, contentType string, contentLength int64, so *operation.StorageOption) (fileChunks []*filer_pb.FileChunk, md5Hash hash.Hash, chunkOffset int64, uploadErr error, smallContent []byte) {
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

	md5Hash = md5.New()
	var partReader = io.NopCloser(io.TeeReader(reader, md5Hash))

	var wg sync.WaitGroup
	var bytesBufferCounter int64
	bytesBufferLimitCond := sync.NewCond(new(sync.Mutex))
	var fileChunksLock sync.Mutex
	for {

		// need to throttle used byte buffer
		bytesBufferLimitCond.L.Lock()
		for atomic.LoadInt64(&bytesBufferCounter) >= 4 {
			glog.V(4).Infof("waiting for byte buffer %d", bytesBufferCounter)
			bytesBufferLimitCond.Wait()
		}
		atomic.AddInt64(&bytesBufferCounter, 1)
		bytesBufferLimitCond.L.Unlock()

		bytesBuffer := bufPool.Get().(*bytes.Buffer)
		glog.V(4).Infof("received byte buffer %d", bytesBufferCounter)

		limitedReader := io.LimitReader(partReader, int64(chunkSize))

		bytesBuffer.Reset()

		dataSize, err := bytesBuffer.ReadFrom(limitedReader)

		// data, err := io.ReadAll(limitedReader)
		if err != nil || dataSize == 0 {
			bufPool.Put(bytesBuffer)
			atomic.AddInt64(&bytesBufferCounter, -1)
			bytesBufferLimitCond.Signal()
			uploadErr = err
			break
		}
		if chunkOffset == 0 && !isAppend {
			if dataSize < fs.option.SaveToFilerLimit || strings.HasPrefix(r.URL.Path, filer.DirectoryEtcRoot) {
				chunkOffset += dataSize
				smallContent = make([]byte, dataSize)
				bytesBuffer.Read(smallContent)
				bufPool.Put(bytesBuffer)
				atomic.AddInt64(&bytesBufferCounter, -1)
				bytesBufferLimitCond.Signal()
				break
			}
		}

		wg.Add(1)
		go func(offset int64) {
			defer func() {
				bufPool.Put(bytesBuffer)
				atomic.AddInt64(&bytesBufferCounter, -1)
				bytesBufferLimitCond.Signal()
				wg.Done()
			}()

			chunk, toChunkErr := fs.dataToChunk(fileName, contentType, bytesBuffer.Bytes(), offset, so)
			if toChunkErr != nil {
				uploadErr = toChunkErr
			}
			if chunk != nil {
				fileChunksLock.Lock()
				fileChunks = append(fileChunks, chunk)
				fileChunksLock.Unlock()
				glog.V(4).Infof("uploaded %s chunk %d to %s [%d,%d)", fileName, len(fileChunks), chunk.FileId, offset, offset+int64(chunk.Size))
			}
		}(chunkOffset)

		// reset variables for the next chunk
		chunkOffset = chunkOffset + dataSize

		// if last chunk was not at full chunk size, but already exhausted the reader
		if dataSize < int64(chunkSize) {
			break
		}
	}

	wg.Wait()

	if uploadErr != nil {
		fs.filer.DeleteChunks(fileChunks)
		return nil, md5Hash, 0, uploadErr, nil
	}

	sort.Slice(fileChunks, func(i, j int) bool {
		return fileChunks[i].Offset < fileChunks[j].Offset
	})

	return fileChunks, md5Hash, chunkOffset, nil, smallContent
}

func (fs *FilerServer) doUpload(urlLocation string, limitedReader io.Reader, fileName string, contentType string, pairMap map[string]string, auth security.EncodedJwt) (*operation.UploadResult, error, []byte) {

	stats.FilerRequestCounter.WithLabelValues("chunkUpload").Inc()
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues("chunkUpload").Observe(time.Since(start).Seconds())
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
	uploadResult, err, data := operation.Upload(limitedReader, uploadOption)
	if uploadResult != nil && uploadResult.RetryCount > 0 {
		stats.FilerRequestCounter.WithLabelValues("chunkUploadRetry").Add(float64(uploadResult.RetryCount))
	}
	return uploadResult, err, data
}

func (fs *FilerServer) dataToChunk(fileName, contentType string, data []byte, chunkOffset int64, so *operation.StorageOption) (*filer_pb.FileChunk, error) {
	dataReader := util.NewBytesReader(data)

	// retry to assign a different file id
	var fileId, urlLocation string
	var auth security.EncodedJwt
	var uploadErr error
	var uploadResult *operation.UploadResult
	for i := 0; i < 3; i++ {
		// assign one file id for one chunk
		fileId, urlLocation, auth, uploadErr = fs.assignNewFileInfo(so)
		if uploadErr != nil {
			glog.V(4).Infof("retry later due to assign error: %v", uploadErr)
			time.Sleep(time.Duration(i+1) * 251 * time.Millisecond)
			continue
		}

		// upload the chunk to the volume server
		uploadResult, uploadErr, _ = fs.doUpload(urlLocation, dataReader, fileName, contentType, nil, auth)
		if uploadErr != nil {
			glog.V(4).Infof("retry later due to upload error: %v", uploadErr)
			time.Sleep(time.Duration(i+1) * 251 * time.Millisecond)
			continue
		}
		break
	}
	if uploadErr != nil {
		glog.Errorf("upload error: %v", uploadErr)
		return nil, uploadErr
	}

	// if last chunk exhausted the reader exactly at the border
	if uploadResult.Size == 0 {
		return nil, nil
	}

	return uploadResult.ToPbFileChunk(fileId, chunkOffset), nil
}
