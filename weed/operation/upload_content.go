package operation

import (
	"bufio"
	"compress/flate"
	"compress/gzip"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path/filepath"
	"strings"

	"github.com/valyala/fasthttp"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type UploadResult struct {
	Name  string `json:"name,omitempty"`
	Size  uint32 `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
	ETag  string `json:"eTag,omitempty"`
}

var (
	client *http.Client
)

func init() {
	client = &http.Client{Transport: &http.Transport{
		MaxIdleConnsPerHost: 1024,
	}}
}

var fileNameEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

// Upload sends a POST request to a volume server to upload the content with adjustable compression level
func UploadWithLocalCompressionLevel(uploadUrl string, filename string, reader io.Reader, isGzipped bool, mtype string, pairMap map[string]string, jwt security.EncodedJwt, compressionLevel int) (*UploadResult, error) {
	if compressionLevel < 1 {
		compressionLevel = 1
	}
	if compressionLevel > 9 {
		compressionLevel = 9
	}
	return doUpload(uploadUrl, filename, reader, isGzipped, mtype, pairMap, compressionLevel, jwt)
}

// Upload sends a POST request to a volume server to upload the content with fast compression
func Upload(uploadUrl string, filename string, reader io.Reader, isGzipped bool, mtype string, pairMap map[string]string, jwt security.EncodedJwt) (*UploadResult, error) {
	return doUpload(uploadUrl, filename, reader, isGzipped, mtype, pairMap, flate.BestSpeed, jwt)
}

func doUpload(uploadUrl string, filename string, reader io.Reader, isGzipped bool, mtype string, pairMap map[string]string, compression int, jwt security.EncodedJwt) (*UploadResult, error) {
	contentIsGzipped := isGzipped
	shouldGzipNow := false
	if !isGzipped {
		if shouldBeZipped, iAmSure := util.IsGzippableFileType(filepath.Base(filename), mtype); iAmSure && shouldBeZipped {
			shouldGzipNow = true
			contentIsGzipped = true
		}
	}
	return upload_content(uploadUrl, func(w io.Writer) (err error) {
		if shouldGzipNow {
			gzWriter, _ := gzip.NewWriterLevel(w, compression)
			_, err = io.Copy(gzWriter, reader)
			gzWriter.Close()
		} else {
			_, err = io.Copy(w, reader)
		}
		return
	}, filename, contentIsGzipped, mtype, pairMap, jwt)
}

func randomBoundary() string {
	var buf [30]byte
	_, err := io.ReadFull(rand.Reader, buf[:])
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", buf[:])
}

func upload_content(uploadUrl string, fillBufferFunction func(w io.Writer) error, filename string, isGzipped bool, mtype string, pairMap map[string]string, jwt security.EncodedJwt) (*UploadResult, error) {

	if mtype == "" {
		mtype = mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	}
	boundary := randomBoundary()
	contentType := "multipart/form-data; boundary=" + boundary

	var ret UploadResult
	var etag string
	var writeErr error
	err := util.PostContent(uploadUrl, func(w *bufio.Writer) {
		h := make(textproto.MIMEHeader)
		h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, fileNameEscaper.Replace(filename)))
		if mtype != "" {
			h.Set("Content-Type", mtype)
		}
		if isGzipped {
			h.Set("Content-Encoding", "gzip")
		}

		body_writer := multipart.NewWriter(w)
		body_writer.SetBoundary(boundary)
		file_writer, cp_err := body_writer.CreatePart(h)
		if cp_err != nil {
			glog.V(0).Infoln("error creating form file", cp_err.Error())
			writeErr = cp_err
			return
		}
		if err := fillBufferFunction(file_writer); err != nil {
			glog.V(0).Infoln("error copying data", err)
			writeErr = err
			return
		}
		if err := body_writer.Close(); err != nil {
			glog.V(0).Infoln("error closing body", err)
			writeErr = err
			return
		}
		w.Flush()

	}, func(header *fasthttp.RequestHeader) {
		header.Set("Content-Type", contentType)
		for k, v := range pairMap {
			header.Set(k, v)
		}
		if jwt != "" {
			header.Set("Authorization", "BEARER "+string(jwt))
		}
	}, func(resp *fasthttp.Response) error {
		etagBytes := resp.Header.Peek("ETag")
		lenEtagBytes := len(etagBytes)
		if lenEtagBytes > 2 && etagBytes[0] == '"' && etagBytes[lenEtagBytes-1] == '"' {
			etag = string(etagBytes[1 : len(etagBytes)-1])
		}

		unmarshal_err := json.Unmarshal(resp.Body(), &ret)
		if unmarshal_err != nil {
			glog.V(0).Infoln("failing to read upload response", uploadUrl, string(resp.Body()))
			return unmarshal_err
		}
		if ret.Error != "" {
			return errors.New(ret.Error)
		}
		return nil
	})

	if writeErr != nil {
		return nil, writeErr
	}

	ret.ETag = etag
	if err != nil {
		return nil, err
	}
	return &ret, nil
}
