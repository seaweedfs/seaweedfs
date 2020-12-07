package weed_server

import (
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	xhttp "github.com/chrislusf/seaweedfs/weed/s3api/http"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerServer) autoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, so *operation.StorageOption) {

	// autoChunking can be set at the command-line level or as a query param. Query param overrides command-line
	query := r.URL.Query()

	parsedMaxMB, _ := strconv.ParseInt(query.Get("maxMB"), 10, 32)
	maxMB := int32(parsedMaxMB)
	if maxMB <= 0 && fs.option.MaxMB > 0 {
		maxMB = int32(fs.option.MaxMB)
	}

	chunkSize := 1024 * 1024 * maxMB

	stats.FilerRequestCounter.WithLabelValues("postAutoChunk").Inc()
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues("postAutoChunk").Observe(time.Since(start).Seconds())
	}()

	var reply *FilerPostResult
	var err error
	var md5bytes []byte
	if r.Method == "POST" {
		if r.Header.Get("Content-Type") == "" && strings.HasSuffix(r.URL.Path, "/") {
			reply, err = fs.mkdir(ctx, w, r)
		} else {
			reply, md5bytes, err = fs.doPostAutoChunk(ctx, w, r, chunkSize, so)
		}
	} else {
		reply, md5bytes, err = fs.doPutAutoChunk(ctx, w, r, chunkSize, so)
	}
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
	} else if reply != nil {
		if len(md5bytes) > 0 {
			w.Header().Set("Content-MD5", util.Base64Encode(md5bytes))
		}
		writeJsonQuiet(w, r, http.StatusCreated, reply)
	}
}

func (fs *FilerServer) doPostAutoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, chunkSize int32, so *operation.StorageOption) (filerResult *FilerPostResult, md5bytes []byte, replyerr error) {

	multipartReader, multipartReaderErr := r.MultipartReader()
	if multipartReaderErr != nil {
		return nil, nil, multipartReaderErr
	}

	part1, part1Err := multipartReader.NextPart()
	if part1Err != nil {
		return nil, nil, part1Err
	}

	fileName := part1.FileName()
	if fileName != "" {
		fileName = path.Base(fileName)
	}
	contentType := part1.Header.Get("Content-Type")
	if contentType == "application/octet-stream" {
		contentType = ""
	}

	fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadReaderToChunks(w, r, part1, chunkSize, fileName, contentType, so)
	if err != nil {
		return nil, nil, err
	}

	fileChunks, replyerr = filer.MaybeManifestize(fs.saveAsChunk(so), fileChunks)
	if replyerr != nil {
		glog.V(0).Infof("manifestize %s: %v", r.RequestURI, replyerr)
		return
	}

	md5bytes = md5Hash.Sum(nil)
	filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, contentType, so, md5bytes, fileChunks, chunkOffset, smallContent)

	return
}

func (fs *FilerServer) doPutAutoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, chunkSize int32, so *operation.StorageOption) (filerResult *FilerPostResult, md5bytes []byte, replyerr error) {

	fileName := ""
	contentType := ""

	fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadReaderToChunks(w, r, r.Body, chunkSize, fileName, contentType, so)
	if err != nil {
		return nil, nil, err
	}

	fileChunks, replyerr = filer.MaybeManifestize(fs.saveAsChunk(so), fileChunks)
	if replyerr != nil {
		glog.V(0).Infof("manifestize %s: %v", r.RequestURI, replyerr)
		return
	}

	md5bytes = md5Hash.Sum(nil)
	filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, contentType, so, md5bytes, fileChunks, chunkOffset, smallContent)

	return
}

func (fs *FilerServer) saveMetaData(ctx context.Context, r *http.Request, fileName string, contentType string, so *operation.StorageOption, md5bytes []byte, fileChunks []*filer_pb.FileChunk, chunkOffset int64, content []byte) (filerResult *FilerPostResult, replyerr error) {

	// detect file mode
	modeStr := r.URL.Query().Get("mode")
	if modeStr == "" {
		modeStr = "0660"
	}
	mode, err := strconv.ParseUint(modeStr, 8, 32)
	if err != nil {
		glog.Errorf("Invalid mode format: %s, use 0660 by default", modeStr)
		mode = 0660
	}

	// fix the path
	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		if fileName != "" {
			path += fileName
		}
	}

	glog.V(4).Infoln("saving", path)
	entry := &filer.Entry{
		FullPath: util.FullPath(path),
		Attr: filer.Attr{
			Mtime:       time.Now(),
			Crtime:      time.Now(),
			Mode:        os.FileMode(mode),
			Uid:         OS_UID,
			Gid:         OS_GID,
			Replication: so.Replication,
			Collection:  so.Collection,
			TtlSec:      so.TtlSeconds,
			Mime:        contentType,
			Md5:         md5bytes,
			FileSize:    uint64(chunkOffset),
		},
		Chunks:  fileChunks,
		Content: content,
	}

	filerResult = &FilerPostResult{
		Name: fileName,
		Size: chunkOffset,
	}

	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}

	fs.saveAmzMetaData(r, entry)

	for k, v := range r.Header {
		if len(v) > 0 && strings.HasPrefix(k, needle.PairNamePrefix) {
			entry.Extended[k] = []byte(v[0])
		}
	}

	if dbErr := fs.filer.CreateEntry(ctx, entry, false, false, nil); dbErr != nil {
		fs.filer.DeleteChunks(entry.Chunks)
		replyerr = dbErr
		filerResult.Error = dbErr.Error()
		glog.V(0).Infof("failing to write %s to filer server : %v", path, dbErr)
	}
	return filerResult, replyerr
}

func (fs *FilerServer) uploadReaderToChunks(w http.ResponseWriter, r *http.Request, reader io.Reader, chunkSize int32, fileName, contentType string, so *operation.StorageOption) ([]*filer_pb.FileChunk, hash.Hash, int64, error, []byte) {
	var fileChunks []*filer_pb.FileChunk

	md5Hash := md5.New()
	var partReader = ioutil.NopCloser(io.TeeReader(reader, md5Hash))

	chunkOffset := int64(0)
	var smallContent, content []byte

	for {
		limitedReader := io.LimitReader(partReader, int64(chunkSize))

		// assign one file id for one chunk
		fileId, urlLocation, auth, assignErr := fs.assignNewFileInfo(so)
		if assignErr != nil {
			return nil, nil, 0, assignErr, nil
		}

		// upload the chunk to the volume server
		uploadResult, uploadErr, data := fs.doUpload(urlLocation, w, r, limitedReader, fileName, contentType, nil, auth)
		if uploadErr != nil {
			return nil, nil, 0, uploadErr, nil
		}
		content = data

		// if last chunk exhausted the reader exactly at the border
		if uploadResult.Size == 0 {
			break
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

	if chunkOffset < fs.option.CacheToFilerLimit || strings.HasPrefix(r.URL.Path, filer.DirectoryEtc) && chunkOffset < 4*1024 {
		smallContent = content
	}
	return fileChunks, md5Hash, chunkOffset, nil, smallContent
}

func (fs *FilerServer) doUpload(urlLocation string, w http.ResponseWriter, r *http.Request, limitedReader io.Reader, fileName string, contentType string, pairMap map[string]string, auth security.EncodedJwt) (*operation.UploadResult, error, []byte) {

	stats.FilerRequestCounter.WithLabelValues("postAutoChunkUpload").Inc()
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues("postAutoChunkUpload").Observe(time.Since(start).Seconds())
	}()

	uploadResult, err, data := operation.Upload(urlLocation, fileName, fs.option.Cipher, limitedReader, false, contentType, pairMap, auth)
	return uploadResult, err, data
}

func (fs *FilerServer) saveAsChunk(so *operation.StorageOption) filer.SaveDataAsChunkFunctionType {

	return func(reader io.Reader, name string, offset int64) (*filer_pb.FileChunk, string, string, error) {
		// assign one file id for one chunk
		fileId, urlLocation, auth, assignErr := fs.assignNewFileInfo(so)
		if assignErr != nil {
			return nil, "", "", assignErr
		}

		// upload the chunk to the volume server
		uploadResult, uploadErr, _ := operation.Upload(urlLocation, name, fs.option.Cipher, reader, false, "", nil, auth)
		if uploadErr != nil {
			return nil, "", "", uploadErr
		}

		return uploadResult.ToPbFileChunk(fileId, offset), so.Collection, so.Replication, nil
	}
}

func (fs *FilerServer) mkdir(ctx context.Context, w http.ResponseWriter, r *http.Request) (filerResult *FilerPostResult, replyerr error) {

	// detect file mode
	modeStr := r.URL.Query().Get("mode")
	if modeStr == "" {
		modeStr = "0660"
	}
	mode, err := strconv.ParseUint(modeStr, 8, 32)
	if err != nil {
		glog.Errorf("Invalid mode format: %s, use 0660 by default", modeStr)
		mode = 0660
	}

	// fix the path
	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}

	existingEntry, err := fs.filer.FindEntry(ctx, util.FullPath(path))
	if err == nil && existingEntry != nil {
		replyerr = fmt.Errorf("dir %s already exists", path)
		return
	}

	glog.V(4).Infoln("mkdir", path)
	entry := &filer.Entry{
		FullPath: util.FullPath(path),
		Attr: filer.Attr{
			Mtime:  time.Now(),
			Crtime: time.Now(),
			Mode:   os.FileMode(mode) | os.ModeDir,
			Uid:    OS_UID,
			Gid:    OS_GID,
		},
	}

	filerResult = &FilerPostResult{
		Name: util.FullPath(path).Name(),
	}

	if dbErr := fs.filer.CreateEntry(ctx, entry, false, false, nil); dbErr != nil {
		replyerr = dbErr
		filerResult.Error = dbErr.Error()
		glog.V(0).Infof("failing to create dir %s on filer server : %v", path, dbErr)
	}
	return filerResult, replyerr
}

func (fs *FilerServer) saveAmzMetaData(r *http.Request, entry *filer.Entry) {

	if sc := r.Header.Get(xhttp.AmzStorageClass); sc != "" {
		entry.Extended[xhttp.AmzStorageClass] = []byte(sc)
	}

	if tags := r.Header.Get(xhttp.AmzObjectTagging); tags != "" {
		for _, v := range strings.Split(tags, "&") {
			tag := strings.Split(v, "=")
			if len(tag) == 2 {
				entry.Extended[xhttp.AmzObjectTagging+"-"+tag[0]] = []byte(tag[1])
			}
		}
	}

	for header, values := range r.Header {
		if strings.HasPrefix(header, xhttp.AmzUserMetaPrefix) {
			for _, value := range values {
				entry.Extended[header] = []byte(value)
			}
		}
	}
}
