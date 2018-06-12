package weed_server

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

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
		glog.V(0).Infof("URL Path [%s] is invalid, could not retrieve file name", r.URL.Path)
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
