package weed_server

import (
	"bytes"
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
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerServer) autoChunk(w http.ResponseWriter, r *http.Request, replication string, collection string, dataCenter string) bool {
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

	reply, err := fs.doAutoChunk(w, r, contentLength, chunkSize, replication, collection, dataCenter)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
	} else if reply != nil {
		writeJsonQuiet(w, r, http.StatusCreated, reply)
	}
	return true
}

func (fs *FilerServer) doAutoChunk(w http.ResponseWriter, r *http.Request, contentLength int64, chunkSize int32, replication string, collection string, dataCenter string) (filerResult *FilerPostResult, replyerr error) {

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

	totalBytesRead := int64(0)
	tmpBufferSize := int32(1024 * 1024)
	tmpBuffer := bytes.NewBuffer(make([]byte, 0, tmpBufferSize))
	chunkBuf := make([]byte, chunkSize+tmpBufferSize, chunkSize+tmpBufferSize) // chunk size plus a little overflow
	chunkBufOffset := int32(0)
	chunkOffset := int64(0)
	writtenChunks := 0

	filerResult = &FilerPostResult{
		Name: fileName,
	}

	for totalBytesRead < contentLength {
		tmpBuffer.Reset()
		bytesRead, readErr := io.CopyN(tmpBuffer, part1, int64(tmpBufferSize))
		readFully := readErr != nil && readErr == io.EOF
		tmpBuf := tmpBuffer.Bytes()
		bytesToCopy := tmpBuf[0:int(bytesRead)]

		copy(chunkBuf[chunkBufOffset:chunkBufOffset+int32(bytesRead)], bytesToCopy)
		chunkBufOffset = chunkBufOffset + int32(bytesRead)

		if chunkBufOffset >= chunkSize || readFully || (chunkBufOffset > 0 && bytesRead == 0) {
			writtenChunks = writtenChunks + 1
			fileId, urlLocation, auth, assignErr := fs.assignNewFileInfo(w, r, replication, collection, dataCenter)
			if assignErr != nil {
				return nil, assignErr
			}

			// upload the chunk to the volume server
			chunkName := fileName + "_chunk_" + strconv.FormatInt(int64(len(fileChunks)+1), 10)
			uploadErr := fs.doUpload(urlLocation, w, r, chunkBuf[0:chunkBufOffset], chunkName, "application/octet-stream", fileId, auth)
			if uploadErr != nil {
				return nil, uploadErr
			}

			// Save to chunk manifest structure
			fileChunks = append(fileChunks,
				&filer_pb.FileChunk{
					FileId: fileId,
					Offset: chunkOffset,
					Size:   uint64(chunkBufOffset),
					Mtime:  time.Now().UnixNano(),
				},
			)

			// reset variables for the next chunk
			chunkBufOffset = 0
			chunkOffset = totalBytesRead + int64(bytesRead)
		}

		totalBytesRead = totalBytesRead + int64(bytesRead)

		if bytesRead == 0 || readFully {
			break
		}

		if readErr != nil {
			return nil, readErr
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
	if db_err := fs.filer.CreateEntry(entry); db_err != nil {
		replyerr = db_err
		filerResult.Error = db_err.Error()
		glog.V(0).Infof("failing to write %s to filer server : %v", path, db_err)
		return
	}

	return
}

func (fs *FilerServer) doUpload(urlLocation string, w http.ResponseWriter, r *http.Request, chunkBuf []byte, fileName string, contentType string, fileId string, auth security.EncodedJwt) (err error) {
	err = nil

	ioReader := ioutil.NopCloser(bytes.NewBuffer(chunkBuf))
	uploadResult, uploadError := operation.Upload(urlLocation, fileName, ioReader, false, contentType, nil, auth)
	if uploadResult != nil {
		glog.V(0).Infoln("Chunk upload result. Name:", uploadResult.Name, "Fid:", fileId, "Size:", uploadResult.Size)
	}
	if uploadError != nil {
		err = uploadError
	}
	return
}
