package weed_server

import (
	"crypto/md5"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	limitedUploadProcessor = util.NewLimitedOutOfOrderProcessor(int32(runtime.NumCPU()))
)

func (fs *FilerServer) uploadReaderToChunks(w http.ResponseWriter, r *http.Request, reader io.Reader, chunkSize int32, fileName, contentType string, contentLength int64, so *operation.StorageOption) (fileChunks []*filer_pb.FileChunk, md5Hash hash.Hash, dataSize int64, err error, smallContent []byte) {

	md5Hash = md5.New()
	var partReader = ioutil.NopCloser(io.TeeReader(reader, md5Hash))

	// save small content directly
	if !isAppend(r) && ((0 < contentLength && contentLength < fs.option.SaveToFilerLimit) || strings.HasPrefix(r.URL.Path, filer.DirectoryEtcRoot) && contentLength < 4*1024) {
		smallContent, err = ioutil.ReadAll(partReader)
		dataSize = int64(len(smallContent))
		return
	}

	resultsChan := make(chan *ChunkCreationResult, operation.ConcurrentUploadLimit)

	var waitForAllData sync.WaitGroup
	waitForAllData.Add(1)
	go func() {
		// process upload results
		defer waitForAllData.Done()
		for result := range resultsChan {
			if result.err != nil {
				err = result.err
				continue
			}

			// Save to chunk manifest structure
			fileChunks = append(fileChunks, result.chunk)
		}
	}()

	var lock sync.Mutex
	readOffset := int64(0)
	var wg sync.WaitGroup
	var readErr error

	for readErr == nil {

		wg.Add(1)
		limitedUploadProcessor.Execute(func() {
			defer wg.Done()

			var localOffset int64
			var data []byte
			// read from the input
			lock.Lock()
			localOffset = readOffset
			limitedReader := io.LimitReader(partReader, int64(chunkSize))
			data, readErr = ioutil.ReadAll(limitedReader)
			readOffset += int64(len(data))
			lock.Unlock()
			// handle read errors
			if readErr != nil {
				if readErr != io.EOF {
					resultsChan <- &ChunkCreationResult{
						err: readErr,
					}
				}
				return
			}
			if len(data) == 0 {
				readErr = io.EOF
				return
			}

			// upload
			dataReader := util.NewBytesReader(data)
			fileId, uploadResult, uploadErr := fs.doCreateChunk(w, r, so, dataReader, fileName, contentType)
			if uploadErr != nil {
				resultsChan <- &ChunkCreationResult{
					err: uploadErr,
				}
				return
			}

			glog.V(4).Infof("uploaded %s to %s [%d,%d)", fileName, fileId, localOffset, localOffset+int64(uploadResult.Size))

			// send back uploaded file chunk
			resultsChan <- &ChunkCreationResult{
				chunk: uploadResult.ToPbFileChunk(fileId, localOffset),
			}

		})
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	waitForAllData.Wait()

	return fileChunks, md5Hash, readOffset, err, nil
}

type ChunkCreationResult struct {
	chunk *filer_pb.FileChunk
	err   error
}

func (fs *FilerServer) doCreateChunk(w http.ResponseWriter, r *http.Request, so *operation.StorageOption, dataReader *util.BytesReader, fileName string, contentType string) (string, *operation.UploadResult, error) {
	// retry to assign a different file id
	var fileId, urlLocation string
	var auth security.EncodedJwt
	var assignErr, uploadErr error
	var uploadResult *operation.UploadResult
	for i := 0; i < 3; i++ {
		// assign one file id for one chunk
		fileId, urlLocation, auth, assignErr = fs.assignNewFileInfo(so)
		if assignErr != nil {
			return "", nil, assignErr
		}

		// upload the chunk to the volume server
		uploadResult, uploadErr, _ = fs.doUpload(urlLocation, w, r, dataReader, fileName, contentType, nil, auth)
		if uploadErr != nil {
			time.Sleep(251 * time.Millisecond)
			continue
		}
		break
	}
	return fileId, uploadResult, uploadErr
}

func (fs *FilerServer) doUpload(urlLocation string, w http.ResponseWriter, r *http.Request, limitedReader io.Reader, fileName string, contentType string, pairMap map[string]string, auth security.EncodedJwt) (*operation.UploadResult, error, []byte) {

	stats.FilerRequestCounter.WithLabelValues("chunkUpload").Inc()
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues("chunkUpload").Observe(time.Since(start).Seconds())
	}()

	uploadResult, err, data := operation.Upload(urlLocation, fileName, fs.option.Cipher, limitedReader, false, contentType, pairMap, auth)
	if uploadResult != nil && uploadResult.RetryCount > 0 {
		stats.FilerRequestCounter.WithLabelValues("chunkUploadRetry").Add(float64(uploadResult.RetryCount))
	}
	return uploadResult, err, data
}
