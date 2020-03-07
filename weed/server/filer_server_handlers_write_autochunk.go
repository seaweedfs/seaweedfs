package weed_server

import (
	"context"
	"io"
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
	replication string, collection string, dataCenter string) bool {
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

	reply, err := fs.doAutoChunk(ctx, w, r, contentLength, chunkSize, replication, collection, dataCenter)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
	} else if reply != nil {
		writeJsonQuiet(w, r, http.StatusCreated, reply)
	}
	return true
}

func (fs *FilerServer) doAutoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request,
	contentLength int64, chunkSize int32, replication string, collection string, dataCenter string) (filerResult *FilerPostResult, replyerr error) {

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

	var fileChunks []*filer_pb.FileChunk

	chunkOffset := int64(0)

	for chunkOffset < contentLength {
		limitedReader := io.LimitReader(part1, int64(chunkSize))

		// assign one file id for one chunk
		fileId, urlLocation, auth, assignErr := fs.assignNewFileInfo(w, r, replication, collection, dataCenter)
		if assignErr != nil {
			return nil, assignErr
		}

		// upload the chunk to the volume server
		chunkName := fileName + "_chunk_" + strconv.FormatInt(int64(len(fileChunks)+1), 10)
		uploadResult, uploadErr := fs.doUpload(urlLocation, w, r, limitedReader, chunkName, "", fileId, auth)
		if uploadErr != nil {
			return nil, uploadErr
		}

		// if last chunk exhausted the reader exactly at the border
		if uploadResult.Size == 0 {
			break
		}

		// Save to chunk manifest structure
		fileChunks = append(fileChunks,
			&filer_pb.FileChunk{
				FileId:    fileId,
				Offset:    chunkOffset,
				Size:      uint64(uploadResult.Size),
				Mtime:     time.Now().UnixNano(),
				ETag:      uploadResult.ETag,
				CipherKey: uploadResult.CipherKey,
			},
		)

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
		FullPath: filer2.FullPath(path),
		Attr: filer2.Attr{
			Mtime:       time.Now(),
			Crtime:      time.Now(),
			Mode:        0660,
			Uid:         OS_UID,
			Gid:         OS_GID,
			Replication: replication,
			Collection:  collection,
			TtlSec:      int32(util.ParseInt(r.URL.Query().Get("ttl"), 0)),
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

func (fs *FilerServer) doUpload(urlLocation string, w http.ResponseWriter, r *http.Request,
	limitedReader io.Reader, fileName string, contentType string, fileId string, auth security.EncodedJwt) (*operation.UploadResult, error) {

	stats.FilerRequestCounter.WithLabelValues("postAutoChunkUpload").Inc()
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues("postAutoChunkUpload").Observe(time.Since(start).Seconds())
	}()

	return operation.Upload(urlLocation, fileName, fs.option.Cipher, limitedReader, false, contentType, nil, auth)
}
