package weed_server

import (
	"context"
	"crypto/md5"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerServer) autoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request,
	replication string, collection string, dataCenter string, ttlSec int32, ttlString string, fsync bool) bool {
	if r.Method != "POST" {
		glog.V(4).Infoln("AutoChunking not supported for method", r.Method)
		return false
	}

	// autoChunking can be set at the command-line level or as a query param. Query param overrides command-line
	query := r.URL.Query()

	parsedMaxMB, _ := strconv.ParseInt(query.Get("maxMB"), 10, 32)
	maxMB := int32(parsedMaxMB)
	if maxMB <= 0 && fs.option.MaxMB > 0 {
		maxMB = int32(fs.option.MaxMB)
	}
	if maxMB <= 0 {
		glog.V(4).Infoln("AutoChunking not enabled")
		return false
	}
	glog.V(4).Infoln("AutoChunking level set to", maxMB, "(MB)")

	chunkSize := 1024 * 1024 * maxMB

	contentLength := int64(0)
	if contentLengthHeader := r.Header["Content-Length"]; len(contentLengthHeader) == 1 {
		contentLength, _ = strconv.ParseInt(contentLengthHeader[0], 10, 64)
		if contentLength <= int64(chunkSize) {
			glog.V(4).Infoln("Content-Length of", contentLength, "is less than the chunk size of", chunkSize, "so autoChunking will be skipped.")
			return false
		}
	}

	if contentLength <= 0 {
		glog.V(4).Infoln("Content-Length value is missing or unexpected so autoChunking will be skipped.")
		return false
	}

	reply, err := fs.doAutoChunk(ctx, w, r, contentLength, chunkSize, replication, collection, dataCenter, ttlSec, ttlString, fsync)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
	} else if reply != nil {
		writeJsonQuiet(w, r, http.StatusCreated, reply)
	}
	return true
}

func (fs *FilerServer) doAutoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request,
	contentLength int64, chunkSize int32, replication string, collection string, dataCenter string, ttlSec int32, ttlString string, fsync bool) (filerResult *FilerPostResult, replyerr error) {

	stats.FilerRequestCounter.WithLabelValues("postAutoChunk").Inc()
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues("postAutoChunk").Observe(time.Since(start).Seconds())
	}()

	multipartReader, multipartReaderErr := r.MultipartReader()
	if multipartReaderErr != nil {
		return nil, multipartReaderErr
	}

	part1, part1Err := multipartReader.NextPart()
	if part1Err != nil {
		return nil, part1Err
	}

	fileName := part1.FileName()
	if fileName != "" {
		fileName = path.Base(fileName)
	}
	contentType := part1.Header.Get("Content-Type")

	var fileChunks []*filer_pb.FileChunk

	md5Hash := md5.New()
	var partReader = ioutil.NopCloser(io.TeeReader(part1, md5Hash))

	chunkOffset := int64(0)

	for chunkOffset < contentLength {
		limitedReader := io.LimitReader(partReader, int64(chunkSize))

		// assign one file id for one chunk
		fileId, urlLocation, auth, assignErr := fs.assignNewFileInfo(w, r, replication, collection, dataCenter, ttlString, fsync)
		if assignErr != nil {
			return nil, assignErr
		}

		// upload the chunk to the volume server
		uploadResult, uploadErr := fs.doUpload(urlLocation, w, r, limitedReader, fileName, contentType, nil, auth)
		if uploadErr != nil {
			return nil, uploadErr
		}

		// if last chunk exhausted the reader exactly at the border
		if uploadResult.Size == 0 {
			break
		}

		// Save to chunk manifest structure
		fileChunks = append(fileChunks, uploadResult.ToPbFileChunk(fileId, chunkOffset))

		glog.V(4).Infof("uploaded %s chunk %d to %s [%d,%d) of %d", fileName, len(fileChunks), fileId, chunkOffset, chunkOffset+int64(uploadResult.Size), contentLength)

		// reset variables for the next chunk
		chunkOffset = chunkOffset + int64(uploadResult.Size)

		// if last chunk was not at full chunk size, but already exhausted the reader
		if int64(uploadResult.Size) < int64(chunkSize) {
			break
		}
	}

	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		if fileName != "" {
			path += fileName
		}
	}

	glog.V(4).Infoln("saving", path)
	entry := &filer2.Entry{
		FullPath: util.FullPath(path),
		Attr: filer2.Attr{
			Mtime:       time.Now(),
			Crtime:      time.Now(),
			Mode:        0660,
			Uid:         OS_UID,
			Gid:         OS_GID,
			Replication: replication,
			Collection:  collection,
			TtlSec:      ttlSec,
			Mime:        contentType,
			Md5:         md5Hash.Sum(nil),
		},
		Chunks: fileChunks,
	}

	filerResult = &FilerPostResult{
		Name: fileName,
		Size: chunkOffset,
	}

	if dbErr := fs.filer.CreateEntry(ctx, entry, false); dbErr != nil {
		fs.filer.DeleteChunks(entry.Chunks)
		replyerr = dbErr
		filerResult.Error = dbErr.Error()
		glog.V(0).Infof("failing to write %s to filer server : %v", path, dbErr)
		return
	}

	return
}

func (fs *FilerServer) doUpload(urlLocation string, w http.ResponseWriter, r *http.Request, limitedReader io.Reader, fileName string, contentType string, pairMap map[string]string, auth security.EncodedJwt) (*operation.UploadResult, error) {

	stats.FilerRequestCounter.WithLabelValues("postAutoChunkUpload").Inc()
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues("postAutoChunkUpload").Observe(time.Since(start).Seconds())
	}()

	uploadResult, err, _ := operation.Upload(urlLocation, fileName, fs.option.Cipher, limitedReader, false, contentType, pairMap, auth)
	return uploadResult, err
}
