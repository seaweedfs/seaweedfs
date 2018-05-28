package weed_server

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
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
