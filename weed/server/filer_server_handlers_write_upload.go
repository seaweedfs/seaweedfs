package weed_server

import (
	"bytes"
	"crypto/md5"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerServer) uploadReaderToChunks(w http.ResponseWriter, r *http.Request, reader io.Reader, chunkSize int32, fileName, contentType string, contentLength int64, so *operation.StorageOption) ([]*filer_pb.FileChunk, hash.Hash, int64, error, []byte) {
	var fileChunks []*filer_pb.FileChunk

	md5Hash := md5.New()
	var partReader = ioutil.NopCloser(io.TeeReader(reader, md5Hash))

	chunkOffset := int64(0)
	var smallContent []byte

	for {
		limitedReader := io.LimitReader(partReader, int64(chunkSize))

		data, err := ioutil.ReadAll(limitedReader)
		if err != nil {
			return nil, nil, 0, err, nil
		}
		if chunkOffset == 0 && !isAppend(r) {
			if len(data) < int(fs.option.SaveToFilerLimit) || strings.HasPrefix(r.URL.Path, filer.DirectoryEtcRoot) && len(data) < 4*1024 {
				smallContent = data
				chunkOffset += int64(len(data))
				break
			}
		}
		dataReader := util.NewBytesReader(data)

		// retry to assign a different file id
		var fileId, urlLocation string
		var auth security.EncodedJwt
		var assignErr, uploadErr error
		var uploadResult *operation.UploadResult
		for i := 0; i < 3; i++ {
			// assign one file id for one chunk
			fileId, urlLocation, auth, assignErr = fs.assignNewFileInfo(so)
			if assignErr != nil {
				return nil, nil, 0, assignErr, nil
			}

			// upload the chunk to the volume server
			uploadResult, uploadErr, _ = fs.doUpload(urlLocation, w, r, dataReader, fileName, contentType, nil, auth)
			if uploadErr != nil {
				time.Sleep(251 * time.Millisecond)
				continue
			}
			break
		}
		if uploadErr != nil {
			return nil, nil, 0, uploadErr, nil
		}

		// if last chunk exhausted the reader exactly at the border
		if uploadResult.Size == 0 {
			break
		}
		if chunkOffset == 0 {
			uploadedMd5 := util.Base64Md5ToBytes(uploadResult.ContentMd5)
			readedMd5 := md5Hash.Sum(nil)
			if !bytes.Equal(uploadedMd5, readedMd5) {
				glog.Errorf("md5 %x does not match %x uploaded chunk %s to the volume server", readedMd5, uploadedMd5, uploadResult.Name)
			}
		}

		// Save to chunk manifest structure
		fileChunks = append(fileChunks, uploadResult.ToPbFileChunk(fileId, chunkOffset))

		glog.V(4).Infof("uploaded %s chunk %d to %s [%d,%d)", fileName, len(fileChunks), fileId, chunkOffset, chunkOffset+int64(uploadResult.Size))

		// reset variables for the next chunk
		chunkOffset = chunkOffset + int64(uploadResult.Size)

		// if last chunk was not at full chunk size, but already exhausted the reader
		if int64(uploadResult.Size) < int64(chunkSize) {
			break
		}
	}

	return fileChunks, md5Hash, chunkOffset, nil, smallContent
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
