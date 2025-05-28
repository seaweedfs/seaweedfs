package weed_server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (fs *FilerServer) autoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, contentLength int64, so *operation.StorageOption) {

	// autoChunking can be set at the command-line level or as a query param. Query param overrides command-line
	query := r.URL.Query()

	parsedMaxMB, _ := strconv.ParseInt(query.Get("maxMB"), 10, 32)
	maxMB := int32(parsedMaxMB)
	if maxMB <= 0 && fs.option.MaxMB > 0 {
		maxMB = int32(fs.option.MaxMB)
	}

	chunkSize := 1024 * 1024 * maxMB

	var reply *FilerPostResult
	var err error
	var md5bytes []byte
	if r.Method == http.MethodPost {
		if r.Header.Get("Content-Type") == "" && strings.HasSuffix(r.URL.Path, "/") {
			reply, err = fs.mkdir(ctx, w, r, so)
		} else {
			reply, md5bytes, err = fs.doPostAutoChunk(ctx, w, r, chunkSize, contentLength, so)
		}
	} else {
		reply, md5bytes, err = fs.doPutAutoChunk(ctx, w, r, chunkSize, contentLength, so)
	}
	if err != nil {
		if err.Error() == "operation not permitted" {
			writeJsonError(w, r, http.StatusForbidden, err)
		} else if strings.HasPrefix(err.Error(), "read input:") || err.Error() == io.ErrUnexpectedEOF.Error() {
			writeJsonError(w, r, util.HttpStatusCancelled, err)
		} else if strings.HasSuffix(err.Error(), "is a file") || strings.HasSuffix(err.Error(), "already exists") {
			writeJsonError(w, r, http.StatusConflict, err)
		} else {
			writeJsonError(w, r, http.StatusInternalServerError, err)
		}
	} else if reply != nil {
		if len(md5bytes) > 0 {
			md5InBase64 := util.Base64Encode(md5bytes)
			w.Header().Set("Content-MD5", md5InBase64)
		}
		writeJsonQuiet(w, r, http.StatusCreated, reply)
	}
}

func (fs *FilerServer) doPostAutoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, chunkSize int32, contentLength int64, so *operation.StorageOption) (filerResult *FilerPostResult, md5bytes []byte, replyerr error) {
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

	if err := fs.checkPermissions(ctx, r, fileName); err != nil {
		return nil, nil, err
	}

	if so.SaveInside {
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()
		buf.ReadFrom(part1)
		filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, contentType, so, nil, nil, 0, buf.Bytes())
		bufPool.Put(buf)
		return
	}

	fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadRequestToChunks(ctx, w, r, part1, chunkSize, fileName, contentType, contentLength, so)
	if err != nil {
		return nil, nil, err
	}

	md5bytes = md5Hash.Sum(nil)
	headerMd5 := r.Header.Get("Content-Md5")
	if headerMd5 != "" && !(util.Base64Encode(md5bytes) == headerMd5 || fmt.Sprintf("%x", md5bytes) == headerMd5) {
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
		return nil, nil, errors.New("The Content-Md5 you specified did not match what we received.")
	}
	filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, contentType, so, md5bytes, fileChunks, chunkOffset, smallContent)
	if replyerr != nil {
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
	}

	return
}

func (fs *FilerServer) doPutAutoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, chunkSize int32, contentLength int64, so *operation.StorageOption) (filerResult *FilerPostResult, md5bytes []byte, replyerr error) {

	fileName := path.Base(r.URL.Path)
	contentType := r.Header.Get("Content-Type")
	if contentType == "application/octet-stream" {
		contentType = ""
	}

	if err := fs.checkPermissions(ctx, r, fileName); err != nil {
		return nil, nil, err
	}

	fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadRequestToChunks(ctx, w, r, r.Body, chunkSize, fileName, contentType, contentLength, so)

	if err != nil {
		return nil, nil, err
	}

	md5bytes = md5Hash.Sum(nil)
	headerMd5 := r.Header.Get("Content-Md5")
	if headerMd5 != "" && !(util.Base64Encode(md5bytes) == headerMd5 || fmt.Sprintf("%x", md5bytes) == headerMd5) {
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
		return nil, nil, errors.New("The Content-Md5 you specified did not match what we received.")
	}
	filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, contentType, so, md5bytes, fileChunks, chunkOffset, smallContent)
	if replyerr != nil {
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
	}

	return
}

func isAppend(r *http.Request) bool {
	return r.URL.Query().Get("op") == "append"
}

func skipCheckParentDirEntry(r *http.Request) bool {
	return r.URL.Query().Get("skipCheckParentDir") == "true"
}

func isS3Request(r *http.Request) bool {
	return r.Header.Get(s3_constants.AmzAuthType) != "" || r.Header.Get("X-Amz-Date") != ""
}

func (fs *FilerServer) checkPermissions(ctx context.Context, r *http.Request, fileName string) error {
	fullPath := fs.fixFilePath(ctx, r, fileName)
	enforced, err := fs.wormEnforcedForEntry(ctx, fullPath)
	if err != nil {
		return err
	} else if enforced {
		// you cannot change a worm file
		return errors.New("operation not permitted")
	}

	return nil
}

func (fs *FilerServer) wormEnforcedForEntry(ctx context.Context, fullPath string) (bool, error) {
	rule := fs.filer.FilerConf.MatchStorageRule(fullPath)
	if !rule.Worm {
		return false, nil
	}

	entry, err := fs.filer.FindEntry(ctx, util.FullPath(fullPath))
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return false, nil
		}

		return false, err
	}

	// worm is not enforced
	if entry.WORMEnforcedAtTsNs == 0 {
		return false, nil
	}

	// worm will never expire
	if rule.WormRetentionTimeSeconds == 0 {
		return true, nil
	}

	enforcedAt := time.Unix(0, entry.WORMEnforcedAtTsNs)

	// worm is expired
	if time.Now().Sub(enforcedAt).Seconds() >= float64(rule.WormRetentionTimeSeconds) {
		return false, nil
	}

	return true, nil
}

func (fs *FilerServer) fixFilePath(ctx context.Context, r *http.Request, fileName string) string {
	// fix the path
	fullPath := r.URL.Path
	if strings.HasSuffix(fullPath, "/") {
		if fileName != "" {
			fullPath += fileName
		}
	} else {
		if fileName != "" {
			if possibleDirEntry, findDirErr := fs.filer.FindEntry(ctx, util.FullPath(fullPath)); findDirErr == nil {
				if possibleDirEntry.IsDirectory() {
					fullPath += "/" + fileName
				}
			}
		}
	}

	return fullPath
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
	path := fs.fixFilePath(ctx, r, fileName)

	var entry *filer.Entry
	var newChunks []*filer_pb.FileChunk
	var mergedChunks []*filer_pb.FileChunk

	isAppend := isAppend(r)
	isOffsetWrite := len(fileChunks) > 0 && fileChunks[0].Offset > 0
	// when it is an append
	if isAppend || isOffsetWrite {
		existingEntry, findErr := fs.filer.FindEntry(ctx, util.FullPath(path))
		if findErr != nil && findErr != filer_pb.ErrNotFound {
			glog.V(0).Infof("failing to find %s: %v", path, findErr)
		}
		entry = existingEntry
	}
	if entry != nil {
		entry.Mtime = time.Now()
		entry.Md5 = nil
		// adjust chunk offsets
		if isAppend {
			for _, chunk := range fileChunks {
				chunk.Offset += int64(entry.FileSize)
			}
			entry.FileSize += uint64(chunkOffset)
		}
		newChunks = append(entry.GetChunks(), fileChunks...)

		// TODO
		if len(entry.Content) > 0 {
			replyerr = fmt.Errorf("append to small file is not supported yet")
			return
		}

	} else {
		glog.V(4).Infoln("saving", path)
		newChunks = fileChunks
		entry = &filer.Entry{
			FullPath: util.FullPath(path),
			Attr: filer.Attr{
				Mtime:    time.Now(),
				Crtime:   time.Now(),
				Mode:     os.FileMode(mode),
				Uid:      OS_UID,
				Gid:      OS_GID,
				TtlSec:   so.TtlSeconds,
				Mime:     contentType,
				Md5:      md5bytes,
				FileSize: uint64(chunkOffset),
			},
			Content: content,
		}
	}

	// maybe concatenate small chunks into one whole chunk
	mergedChunks, replyerr = fs.maybeMergeChunks(ctx, so, newChunks)
	if replyerr != nil {
		glog.V(0).Infof("merge chunks %s: %v", r.RequestURI, replyerr)
		mergedChunks = newChunks
	}

	// maybe compact entry chunks
	mergedChunks, replyerr = filer.MaybeManifestize(fs.saveAsChunk(ctx, so), mergedChunks)
	if replyerr != nil {
		glog.V(0).Infof("manifestize %s: %v", r.RequestURI, replyerr)
		return
	}
	entry.Chunks = mergedChunks
	if isOffsetWrite {
		entry.Md5 = nil
		entry.FileSize = entry.Size()
	}

	filerResult = &FilerPostResult{
		Name: fileName,
		Size: int64(entry.FileSize),
	}

	entry.Extended = SaveAmzMetaData(r, entry.Extended, false)

	for k, v := range r.Header {
		if len(v) > 0 && len(v[0]) > 0 {
			if strings.HasPrefix(k, needle.PairNamePrefix) || k == "Cache-Control" || k == "Expires" || k == "Content-Disposition" {
				entry.Extended[k] = []byte(v[0])
			}
			if k == "Response-Content-Disposition" {
				entry.Extended["Content-Disposition"] = []byte(v[0])
			}
		}
	}

	dbErr := fs.filer.CreateEntry(ctx, entry, false, false, nil, skipCheckParentDirEntry(r), so.MaxFileNameLength)
	// In test_bucket_listv2_delimiter_basic, the valid object key is the parent folder
	if dbErr != nil && strings.HasSuffix(dbErr.Error(), " is a file") && isS3Request(r) {
		dbErr = fs.filer.CreateEntry(ctx, entry, false, false, nil, true, so.MaxFileNameLength)
	}
	if dbErr != nil {
		replyerr = dbErr
		filerResult.Error = dbErr.Error()
		glog.V(0).Infof("failing to write %s to filer server : %v", path, dbErr)
	}
	return filerResult, replyerr
}

func (fs *FilerServer) saveAsChunk(ctx context.Context, so *operation.StorageOption) filer.SaveDataAsChunkFunctionType {

	return func(reader io.Reader, name string, offset int64, tsNs int64) (*filer_pb.FileChunk, error) {
		var fileId string
		var uploadResult *operation.UploadResult

		err := util.Retry("saveAsChunk", func() error {
			// assign one file id for one chunk
			assignedFileId, urlLocation, auth, assignErr := fs.assignNewFileInfo(ctx, so)
			if assignErr != nil {
				return assignErr
			}

			fileId = assignedFileId

			// upload the chunk to the volume server
			uploadOption := &operation.UploadOption{
				UploadUrl:         urlLocation,
				Filename:          name,
				Cipher:            fs.option.Cipher,
				IsInputCompressed: false,
				MimeType:          "",
				PairMap:           nil,
				Jwt:               auth,
			}

			uploader, uploaderErr := operation.NewUploader()
			if uploaderErr != nil {
				return uploaderErr
			}

			var uploadErr error
			uploadResult, uploadErr, _ = uploader.Upload(ctx, reader, uploadOption)
			if uploadErr != nil {
				return uploadErr
			}
			return nil
		})
		if err != nil {
			return nil, err
		}

		return uploadResult.ToPbFileChunk(fileId, offset, tsNs), nil
	}
}

func (fs *FilerServer) mkdir(ctx context.Context, w http.ResponseWriter, r *http.Request, so *operation.StorageOption) (filerResult *FilerPostResult, replyerr error) {

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
			TtlSec: so.TtlSeconds,
		},
	}

	filerResult = &FilerPostResult{
		Name: util.FullPath(path).Name(),
	}

	if dbErr := fs.filer.CreateEntry(ctx, entry, false, false, nil, false, so.MaxFileNameLength); dbErr != nil {
		replyerr = dbErr
		filerResult.Error = dbErr.Error()
		glog.V(0).Infof("failing to create dir %s on filer server : %v", path, dbErr)
	}
	return filerResult, replyerr
}

func SaveAmzMetaData(r *http.Request, existing map[string][]byte, isReplace bool) (metadata map[string][]byte) {

	metadata = make(map[string][]byte)
	if !isReplace {
		for k, v := range existing {
			metadata[k] = v
		}
	}

	if sc := r.Header.Get(s3_constants.AmzStorageClass); sc != "" {
		metadata[s3_constants.AmzStorageClass] = []byte(sc)
	}

	if ce := r.Header.Get("Content-Encoding"); ce != "" {
		metadata["Content-Encoding"] = []byte(ce)
	}

	if tags := r.Header.Get(s3_constants.AmzObjectTagging); tags != "" {
		for _, v := range strings.Split(tags, "&") {
			tag := strings.Split(v, "=")
			if len(tag) == 2 {
				metadata[s3_constants.AmzObjectTagging+"-"+tag[0]] = []byte(tag[1])
			} else if len(tag) == 1 {
				metadata[s3_constants.AmzObjectTagging+"-"+tag[0]] = nil
			}
		}
	}

	for header, values := range r.Header {
		if strings.HasPrefix(header, s3_constants.AmzUserMetaPrefix) {
			for _, value := range values {
				metadata[header] = []byte(value)
			}
		}
	}

	//acp-owner
	acpOwner := r.Header.Get(s3_constants.ExtAmzOwnerKey)
	if len(acpOwner) > 0 {
		metadata[s3_constants.ExtAmzOwnerKey] = []byte(acpOwner)
	}

	//acp-grants
	acpGrants := r.Header.Get(s3_constants.ExtAmzAclKey)
	if len(acpOwner) > 0 {
		metadata[s3_constants.ExtAmzAclKey] = []byte(acpGrants)
	}

	return

}
