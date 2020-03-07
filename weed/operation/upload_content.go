package operation

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path/filepath"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type UploadResult struct {
	Name      string `json:"name,omitempty"`
	Size      uint32 `json:"size,omitempty"`
	Error     string `json:"error,omitempty"`
	ETag      string `json:"eTag,omitempty"`
	CipherKey []byte `json:"cipherKey,omitempty"`
	Mime      string `json:"mime,omitempty"`
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
func UploadWithLocalCompressionLevel(uploadUrl string, filename string, cipher bool, reader io.Reader, isGzipped bool, mtype string, pairMap map[string]string, jwt security.EncodedJwt, compressionLevel int) (*UploadResult, error) {
	if compressionLevel < 1 {
		compressionLevel = 1
	}
	if compressionLevel > 9 {
		compressionLevel = 9
	}
	return doUpload(uploadUrl, filename, cipher, reader, isGzipped, mtype, pairMap, compressionLevel, jwt)
}

// Upload sends a POST request to a volume server to upload the content with fast compression
func Upload(uploadUrl string, filename string, cipher bool, reader io.Reader, isGzipped bool, mtype string, pairMap map[string]string, jwt security.EncodedJwt) (*UploadResult, error) {
	return doUpload(uploadUrl, filename, cipher, reader, isGzipped, mtype, pairMap, flate.BestSpeed, jwt)
}

func doUpload(uploadUrl string, filename string, cipher bool, reader io.Reader, isGzipped bool, mtype string, pairMap map[string]string, compression int, jwt security.EncodedJwt) (*UploadResult, error) {
	contentIsGzipped := isGzipped
	shouldGzipNow := false
	if !isGzipped {
		if shouldBeZipped, iAmSure := util.IsGzippableFileType(filepath.Base(filename), mtype); iAmSure && shouldBeZipped {
			shouldGzipNow = true
			contentIsGzipped = true
		}
	}
	// encrypt data
	var cipherKey util.CipherKey
	var clearDataLen int
	var err error
	if cipher {
		cipherKey, reader, clearDataLen, _, err = util.EncryptReader(reader)
		if err != nil {
			return nil, err
		}
	}

	// upload data
	uploadResult, err := upload_content(uploadUrl, func(w io.Writer) (err error) {
		if shouldGzipNow {
			gzWriter, _ := gzip.NewWriterLevel(w, compression)
			_, err = io.Copy(gzWriter, reader)
			gzWriter.Close()
		} else {
			_, err = io.Copy(w, reader)
		}
		return
	}, filename, contentIsGzipped, mtype, pairMap, jwt)

	// remember cipher key
	if uploadResult != nil && cipherKey != nil {
		uploadResult.CipherKey = cipherKey
		uploadResult.Size = uint32(clearDataLen)
	}

	return uploadResult, err
}

func upload_content(uploadUrl string, fillBufferFunction func(w io.Writer) error, filename string, isGzipped bool, mtype string, pairMap map[string]string, jwt security.EncodedJwt) (*UploadResult, error) {
	body_buf := bytes.NewBufferString("")
	body_writer := multipart.NewWriter(body_buf)
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, fileNameEscaper.Replace(filename)))
	if mtype == "" {
		mtype = mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	}
	if mtype != "" {
		h.Set("Content-Type", mtype)
	}
	if isGzipped {
		h.Set("Content-Encoding", "gzip")
	}

	file_writer, cp_err := body_writer.CreatePart(h)
	if cp_err != nil {
		glog.V(0).Infoln("error creating form file", cp_err.Error())
		return nil, cp_err
	}
	if err := fillBufferFunction(file_writer); err != nil {
		glog.V(0).Infoln("error copying data", err)
		return nil, err
	}
	content_type := body_writer.FormDataContentType()
	if err := body_writer.Close(); err != nil {
		glog.V(0).Infoln("error closing body", err)
		return nil, err
	}

	req, postErr := http.NewRequest("POST", uploadUrl, body_buf)
	if postErr != nil {
		glog.V(0).Infoln("failing to upload to", uploadUrl, postErr.Error())
		return nil, postErr
	}
	req.Header.Set("Content-Type", content_type)
	for k, v := range pairMap {
		req.Header.Set(k, v)
	}
	if jwt != "" {
		req.Header.Set("Authorization", "BEARER "+string(jwt))
	}
	resp, post_err := client.Do(req)
	if post_err != nil {
		glog.V(0).Infoln("failing to upload to", uploadUrl, post_err.Error())
		return nil, post_err
	}
	defer resp.Body.Close()
	etag := getEtag(resp)
	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		return nil, ra_err
	}
	var ret UploadResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.V(0).Infoln("failing to read upload response", uploadUrl, string(resp_body))
		return nil, unmarshal_err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	ret.ETag = etag
	return &ret, nil
}

func getEtag(r *http.Response) (etag string) {
	etag = r.Header.Get("ETag")
	if strings.HasPrefix(etag, "\"") && strings.HasSuffix(etag, "\"") {
		etag = etag[1 : len(etag)-1]
	}
	return
}
