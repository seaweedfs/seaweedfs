package weed_server

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

type FilerPostResult struct {
	Name  string `json:"name,omitempty"`
	Size  uint32 `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
	Fid   string `json:"fid,omitempty"`
	Url   string `json:"url,omitempty"`
}

var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

func escapeQuotes(s string) string {
	return quoteEscaper.Replace(s)
}

func createFormFile(writer *multipart.Writer, fieldname, filename, mime string) (io.Writer, error) {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition",
		fmt.Sprintf(`form-data; name="%s"; filename="%s"`,
			escapeQuotes(fieldname), escapeQuotes(filename)))
	if len(mime) == 0 {
		mime = "application/octet-stream"
	}
	h.Set("Content-Type", mime)
	return writer.CreatePart(h)
}

func makeFormData(filename, mimeType string, content io.Reader) (formData io.Reader, contentType string, err error) {
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)
	defer writer.Close()

	part, err := createFormFile(writer, "file", filename, mimeType)
	if err != nil {
		glog.V(0).Infoln(err)
		return
	}
	_, err = io.Copy(part, content)
	if err != nil {
		glog.V(0).Infoln(err)
		return
	}

	formData = buf
	contentType = writer.FormDataContentType()

	return
}

func (fs *FilerServer) queryFileInfoByPath(w http.ResponseWriter, r *http.Request, path string) (fileId, urlLocation string, err error) {
	var found bool
	var entry *filer2.Entry
	if found, entry, err = fs.filer.FindEntry(filer2.FullPath(path)); err != nil {
		glog.V(0).Infoln("failing to find path in filer store", path, err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
	} else if found {
		fileId = entry.Chunks[0].FileId
		urlLocation, err = operation.LookupFileId(fs.getMasterNode(), fileId)
		if err != nil {
			glog.V(1).Infoln("operation LookupFileId %s failed, err is %s", fileId, err.Error())
			w.WriteHeader(http.StatusNotFound)
		}
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
	return
}

func (fs *FilerServer) assignNewFileInfo(w http.ResponseWriter, r *http.Request, replication, collection string) (fileId, urlLocation string, err error) {
	ar := &operation.VolumeAssignRequest{
		Count:       1,
		Replication: replication,
		Collection:  collection,
		Ttl:         r.URL.Query().Get("ttl"),
	}
	assignResult, ae := operation.Assign(fs.getMasterNode(), ar)
	if ae != nil {
		glog.V(0).Infoln("failing to assign a file id", ae.Error())
		writeJsonError(w, r, http.StatusInternalServerError, ae)
		err = ae
		return
	}
	fileId = assignResult.Fid
	urlLocation = "http://" + assignResult.Url + "/" + assignResult.Fid
	return
}

func (fs *FilerServer) multipartUploadAnalyzer(w http.ResponseWriter, r *http.Request, replication, collection string) (fileId, urlLocation string, err error) {
	//Default handle way for http multipart
	if r.Method == "PUT" {
		buf, _ := ioutil.ReadAll(r.Body)
		r.Body = ioutil.NopCloser(bytes.NewBuffer(buf))
		fileName, _, _, _, _, _, _, _, pe := storage.ParseUpload(r)
		if pe != nil {
			glog.V(0).Infoln("failing to parse post body", pe.Error())
			writeJsonError(w, r, http.StatusInternalServerError, pe)
			err = pe
			return
		}
		//reconstruct http request body for following new request to volume server
		r.Body = ioutil.NopCloser(bytes.NewBuffer(buf))

		path := r.URL.Path
		if strings.HasSuffix(path, "/") {
			if fileName != "" {
				path += fileName
			}
		}
		fileId, urlLocation, err = fs.queryFileInfoByPath(w, r, path)
	} else {
		fileId, urlLocation, err = fs.assignNewFileInfo(w, r, replication, collection)
	}
	return
}

func multipartHttpBodyBuilder(w http.ResponseWriter, r *http.Request, fileName string) (err error) {
	body, contentType, te := makeFormData(fileName, r.Header.Get("Content-Type"), r.Body)
	if te != nil {
		glog.V(0).Infoln("S3 protocol to raw seaweed protocol failed", te.Error())
		writeJsonError(w, r, http.StatusInternalServerError, te)
		err = te
		return
	}

	if body != nil {
		switch v := body.(type) {
		case *bytes.Buffer:
			r.ContentLength = int64(v.Len())
		case *bytes.Reader:
			r.ContentLength = int64(v.Len())
		case *strings.Reader:
			r.ContentLength = int64(v.Len())
		}
	}

	r.Header.Set("Content-Type", contentType)
	rc, ok := body.(io.ReadCloser)
	if !ok && body != nil {
		rc = ioutil.NopCloser(body)
	}
	r.Body = rc
	return
}

func checkContentMD5(w http.ResponseWriter, r *http.Request) (err error) {
	if contentMD5 := r.Header.Get("Content-MD5"); contentMD5 != "" {
		buf, _ := ioutil.ReadAll(r.Body)
		//checkMD5
		sum := md5.Sum(buf)
		fileDataMD5 := base64.StdEncoding.EncodeToString(sum[0:len(sum)])
		if strings.ToLower(fileDataMD5) != strings.ToLower(contentMD5) {
			glog.V(0).Infof("fileDataMD5 [%s] is not equal to Content-MD5 [%s]", fileDataMD5, contentMD5)
			err = fmt.Errorf("MD5 check failed")
			writeJsonError(w, r, http.StatusNotAcceptable, err)
			return
		}
		//reconstruct http request body for following new request to volume server
		r.Body = ioutil.NopCloser(bytes.NewBuffer(buf))
	}
	return
}

func (fs *FilerServer) monolithicUploadAnalyzer(w http.ResponseWriter, r *http.Request, replication, collection string) (fileId, urlLocation string, err error) {
	/*
		Amazon S3 ref link:[http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html]
		There is a long way to provide a completely compatibility against all Amazon S3 API, I just made
		a simple data stream adapter between S3 PUT API and seaweedfs's volume storage Write API
		1. The request url format should be http://$host:$port/$bucketName/$objectName
		2. bucketName will be mapped to seaweedfs's collection name
		3. You could customize and make your enhancement.
	*/
	lastPos := strings.LastIndex(r.URL.Path, "/")
	if lastPos == -1 || lastPos == 0 || lastPos == len(r.URL.Path)-1 {
		glog.V(0).Infoln("URL Path [%s] is invalid, could not retrieve file name", r.URL.Path)
		err = fmt.Errorf("URL Path is invalid")
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	if err = checkContentMD5(w, r); err != nil {
		return
	}

	fileName := r.URL.Path[lastPos+1:]
	if err = multipartHttpBodyBuilder(w, r, fileName); err != nil {
		return
	}

	secondPos := strings.Index(r.URL.Path[1:], "/") + 1
	collection = r.URL.Path[1:secondPos]
	path := r.URL.Path

	if fileId, urlLocation, err = fs.queryFileInfoByPath(w, r, path); err == nil && fileId == "" {
		fileId, urlLocation, err = fs.assignNewFileInfo(w, r, replication, collection)
	}
	return
}

func (fs *FilerServer) PostHandler(w http.ResponseWriter, r *http.Request) {

	query := r.URL.Query()
	replication := query.Get("replication")
	if replication == "" {
		replication = fs.defaultReplication
	}
	collection := query.Get("collection")
	if collection == "" {
		collection = fs.collection
	}

	if autoChunked := fs.autoChunk(w, r, replication, collection); autoChunked {
		return
	}

	var fileId, urlLocation string
	var err error

	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data; boundary=") {
		fileId, urlLocation, err = fs.multipartUploadAnalyzer(w, r, replication, collection)
		if err != nil {
			return
		}
	} else {
		fileId, urlLocation, err = fs.monolithicUploadAnalyzer(w, r, replication, collection)
		if err != nil {
			return
		}
	}

	u, _ := url.Parse(urlLocation)

	// This allows a client to generate a chunk manifest and submit it to the filer -- it is a little off
	// because they need to provide FIDs instead of file paths...
	cm, _ := strconv.ParseBool(query.Get("cm"))
	if cm {
		q := u.Query()
		q.Set("cm", "true")
		u.RawQuery = q.Encode()
	}
	glog.V(4).Infoln("post to", u)

	request := &http.Request{
		Method:        r.Method,
		URL:           u,
		Proto:         r.Proto,
		ProtoMajor:    r.ProtoMajor,
		ProtoMinor:    r.ProtoMinor,
		Header:        r.Header,
		Body:          r.Body,
		Host:          r.Host,
		ContentLength: r.ContentLength,
	}
	resp, do_err := util.Do(request)
	if do_err != nil {
		glog.V(0).Infoln("failing to connect to volume server", r.RequestURI, do_err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, do_err)
		return
	}
	defer resp.Body.Close()
	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		glog.V(0).Infoln("failing to upload to volume server", r.RequestURI, ra_err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, ra_err)
		return
	}
	glog.V(4).Infoln("post result", string(resp_body))
	var ret operation.UploadResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.V(0).Infoln("failing to read upload resonse", r.RequestURI, string(resp_body))
		writeJsonError(w, r, http.StatusInternalServerError, unmarshal_err)
		return
	}
	if ret.Error != "" {
		glog.V(0).Infoln("failing to post to volume server", r.RequestURI, ret.Error)
		writeJsonError(w, r, http.StatusInternalServerError, errors.New(ret.Error))
		return
	}
	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		if ret.Name != "" {
			path += ret.Name
		} else {
			operation.DeleteFile(fs.getMasterNode(), fileId, fs.jwt(fileId)) //clean up
			glog.V(0).Infoln("Can not to write to folder", path, "without a file name!")
			writeJsonError(w, r, http.StatusInternalServerError,
				errors.New("Can not to write to folder "+path+" without a file name"))
			return
		}
	}

	// also delete the old fid unless PUT operation
	if r.Method != "PUT" {
		if found, entry, err := fs.filer.FindEntry(filer2.FullPath(path)); err == nil && found {
			oldFid := entry.Chunks[0].FileId
			operation.DeleteFile(fs.getMasterNode(), oldFid, fs.jwt(oldFid))
		} else if err != nil && err != filer.ErrNotFound {
			glog.V(0).Infof("error %v occur when finding %s in filer store", err, path)
		}
	}

	glog.V(4).Infoln("saving", path, "=>", fileId)
	entry := &filer2.Entry{
		FullPath: filer2.FullPath(path),
		Attr: filer2.Attr{
			Mode: 0660,
		},
		Chunks: []*filer_pb.FileChunk{{
			FileId: fileId,
			Size:   uint64(r.ContentLength),
		}},
	}
	if db_err := fs.filer.CreateEntry(entry); db_err != nil {
		operation.DeleteFile(fs.getMasterNode(), fileId, fs.jwt(fileId)) //clean up
		glog.V(0).Infof("failing to write %s to filer server : %v", path, db_err)
		writeJsonError(w, r, http.StatusInternalServerError, db_err)
		return
	}

	reply := FilerPostResult{
		Name:  ret.Name,
		Size:  ret.Size,
		Error: ret.Error,
		Fid:   fileId,
		Url:   urlLocation,
	}
	writeJsonQuiet(w, r, http.StatusCreated, reply)
}

func (fs *FilerServer) autoChunk(w http.ResponseWriter, r *http.Request, replication string, collection string) bool {
	if r.Method != "POST" {
		glog.V(4).Infoln("AutoChunking not supported for method", r.Method)
		return false
	}

	// autoChunking can be set at the command-line level or as a query param. Query param overrides command-line
	query := r.URL.Query()

	parsedMaxMB, _ := strconv.ParseInt(query.Get("maxMB"), 10, 32)
	maxMB := int32(parsedMaxMB)
	if maxMB <= 0 && fs.maxMB > 0 {
		maxMB = int32(fs.maxMB)
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

	reply, err := fs.doAutoChunk(w, r, contentLength, chunkSize, replication, collection)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
	} else if reply != nil {
		writeJsonQuiet(w, r, http.StatusCreated, reply)
	}
	return true
}

func (fs *FilerServer) doAutoChunk(w http.ResponseWriter, r *http.Request, contentLength int64, chunkSize int32, replication string, collection string) (filerResult *FilerPostResult, replyerr error) {

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
			fileId, urlLocation, assignErr := fs.assignNewFileInfo(w, r, replication, collection)
			if assignErr != nil {
				return nil, assignErr
			}

			// upload the chunk to the volume server
			chunkName := fileName + "_chunk_" + strconv.FormatInt(int64(len(fileChunks)+1), 10)
			uploadErr := fs.doUpload(urlLocation, w, r, chunkBuf[0:chunkBufOffset], chunkName, "application/octet-stream", fileId)
			if uploadErr != nil {
				return nil, uploadErr
			}

			// Save to chunk manifest structure
			fileChunks = append(fileChunks,
				&filer_pb.FileChunk{
					FileId: fileId,
					Offset: chunkOffset,
					Size:   uint64(chunkBufOffset),
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
	// also delete the old fid unless PUT operation
	if r.Method != "PUT" {
		if found, entry, err := fs.filer.FindEntry(filer2.FullPath(path)); found && err == nil {
			for _, chunk := range entry.Chunks {
				oldFid := chunk.FileId
				operation.DeleteFile(fs.getMasterNode(), oldFid, fs.jwt(oldFid))
			}
		} else if err != nil {
			glog.V(0).Infof("error %v occur when finding %s in filer store", err, path)
		}
	}

	glog.V(4).Infoln("saving", path)
	entry := &filer2.Entry{
		FullPath: filer2.FullPath(path),
		Attr: filer2.Attr{
			Mode: 0660,
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

func (fs *FilerServer) doUpload(urlLocation string, w http.ResponseWriter, r *http.Request, chunkBuf []byte, fileName string, contentType string, fileId string) (err error) {
	err = nil

	ioReader := ioutil.NopCloser(bytes.NewBuffer(chunkBuf))
	uploadResult, uploadError := operation.Upload(urlLocation, fileName, ioReader, false, contentType, nil, fs.jwt(fileId))
	if uploadResult != nil {
		glog.V(0).Infoln("Chunk upload result. Name:", uploadResult.Name, "Fid:", fileId, "Size:", uploadResult.Size)
	}
	if uploadError != nil {
		err = uploadError
	}
	return
}

// curl -X DELETE http://localhost:8888/path/to
func (fs *FilerServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {

	entry, err := fs.filer.DeleteEntry(filer2.FullPath(r.URL.Path))
	if err != nil {
		glog.V(4).Infoln("deleting", r.URL.Path, ":", err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	if entry != nil && !entry.IsDirectory() {
		for _, chunk := range entry.Chunks {
			oldFid := chunk.FileId
			operation.DeleteFile(fs.getMasterNode(), oldFid, fs.jwt(oldFid))
		}
	}

	writeJsonQuiet(w, r, http.StatusAccepted, map[string]string{"error": ""})
}
