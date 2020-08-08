package weed_server

import (
	"context"
	"crypto/md5"
	"hash"
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

	stats.FilerRequestCounter.WithLabelValues("postAutoChunk").Inc()
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues("postAutoChunk").Observe(time.Since(start).Seconds())
	}()

	var reply *FilerPostResult
	var err error
	if r.Method == "POST" {
		reply, err = fs.doPostAutoChunk(ctx, w, r, contentLength, chunkSize, replication, collection, dataCenter, ttlSec, ttlString, fsync)
	} else {
		reply, err = fs.doPutAutoChunk(ctx, w, r, contentLength, chunkSize, replication, collection, dataCenter, ttlSec, ttlString, fsync)
	}
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
	} else if reply != nil {
		writeJsonQuiet(w, r, http.StatusCreated, reply)
	}
	return true
}

func (fs *FilerServer) doPostAutoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request,
	contentLength int64, chunkSize int32, replication string, collection string, dataCenter string, ttlSec int32, ttlString string, fsync bool) (filerResult *FilerPostResult, replyerr error) {

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
	if contentType == "application/octet-stream" {
		contentType = ""
	}

	fileChunks, md5Hash, chunkOffset, err := fs.uploadReaderToChunks(w, r, part1, contentLength, chunkSize, replication, collection, dataCenter, ttlString, fileName, contentType, fsync)
	if err != nil {
		return nil, err
	}

	fileChunks, replyerr = filer2.MaybeManifestize(fs.saveAsChunk(replication, collection, dataCenter, ttlString, fsync), fileChunks)
	if replyerr != nil {
		glog.V(0).Infof("manifestize %s: %v", r.RequestURI, replyerr)
		return
	}

	filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, replication, collection, ttlSec, contentType, md5Hash, fileChunks, chunkOffset)

	return
}


func (fs *FilerServer) doPutAutoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request,
	contentLength int64, chunkSize int32, replication string, collection string, dataCenter string, ttlSec int32, ttlString string, fsync bool) (filerResult *FilerPostResult, replyerr error) {

	fileName := ""
	contentType := ""

	fileChunks, md5Hash, chunkOffset, err := fs.uploadReaderToChunks(w, r, r.Body, contentLength, chunkSize, replication, collection, dataCenter, ttlString, fileName, contentType, fsync)
	if err != nil {
		return nil, err
	}

	fileChunks, replyerr = filer2.MaybeManifestize(fs.saveAsChunk(replication, collection, dataCenter, ttlString, fsync), fileChunks)
	if replyerr != nil {
		glog.V(0).Infof("manifestize %s: %v", r.RequestURI, replyerr)
		return
	}

	filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, replication, collection, ttlSec, contentType, md5Hash, fileChunks, chunkOffset)

	return
}

func (fs *FilerServer) saveMetaData(ctx context.Context, r *http.Request, fileName string, replication string, collection string, ttlSec int32, contentType string, md5Hash hash.Hash, fileChunks []*filer_pb.FileChunk, chunkOffset int64) (filerResult *FilerPostResult, replyerr error) {
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

	if dbErr := fs.filer.CreateEntry(ctx, entry, false, false); dbErr != nil {
		fs.filer.DeleteChunks(entry.Chunks)
		replyerr = dbErr
		filerResult.Error = dbErr.Error()
		glog.V(0).Infof("failing to write %s to filer server : %v", path, dbErr)
	}
	return filerResult, replyerr
}

func (fs *FilerServer) uploadReaderToChunks(w http.ResponseWriter, r *http.Request, reader io.Reader, contentLength int64, chunkSize int32, replication string, collection string, dataCenter string, ttlString string, fileName string, contentType string, fsync bool) ([]*filer_pb.FileChunk, hash.Hash, int64, error) {
	var fileChunks []*filer_pb.FileChunk

	md5Hash := md5.New()
	var partReader = ioutil.NopCloser(io.TeeReader(reader, md5Hash))

	chunkOffset := int64(0)

	for chunkOffset < contentLength {
		limitedReader := io.LimitReader(partReader, int64(chunkSize))

		// assign one file id for one chunk
		fileId, urlLocation, auth, assignErr := fs.assignNewFileInfo(replication, collection, dataCenter, ttlString, fsync)
		if assignErr != nil {
			return nil, nil, 0, assignErr
		}

		// upload the chunk to the volume server
		uploadResult, uploadErr := fs.doUpload(urlLocation, w, r, limitedReader, fileName, contentType, nil, auth)
		if uploadErr != nil {
			return nil, nil, 0, uploadErr
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
	return fileChunks, md5Hash, chunkOffset, nil
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

func (fs *FilerServer) saveAsChunk(replication string, collection string, dataCenter string, ttlString string, fsync bool) filer2.SaveDataAsChunkFunctionType {

	return func(reader io.Reader, name string, offset int64) (*filer_pb.FileChunk, string, string, error) {
		// assign one file id for one chunk
		fileId, urlLocation, auth, assignErr := fs.assignNewFileInfo(replication, collection, dataCenter, ttlString, fsync)
		if assignErr != nil {
			return nil, "", "", assignErr
		}

		// upload the chunk to the volume server
		uploadResult, uploadErr, _ := operation.Upload(urlLocation, name, fs.option.Cipher, reader, false, "", nil, auth)
		if uploadErr != nil {
			return nil, "", "", uploadErr
		}

		return uploadResult.ToPbFileChunk(fileId, offset), collection, replication, nil
	}
}

