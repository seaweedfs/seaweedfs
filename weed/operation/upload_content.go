package operation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	util_http_client "github.com/seaweedfs/seaweedfs/weed/util/http/client"
)

type UploadOption struct {
	UploadUrl         string
	Filename          string
	Cipher            bool
	IsInputCompressed bool
	MimeType          string
	PairMap           map[string]string
	Jwt               security.EncodedJwt
	RetryForever      bool
	Md5               string
	BytesBuffer       *bytes.Buffer
}

type UploadResult struct {
	Name       string `json:"name,omitempty"`
	Size       uint32 `json:"size,omitempty"`
	Error      string `json:"error,omitempty"`
	ETag       string `json:"eTag,omitempty"`
	CipherKey  []byte `json:"cipherKey,omitempty"`
	Mime       string `json:"mime,omitempty"`
	Gzip       uint32 `json:"gzip,omitempty"`
	ContentMd5 string `json:"contentMd5,omitempty"`
	RetryCount int    `json:"-"`
}

func (uploadResult *UploadResult) ToPbFileChunk(fileId string, offset int64, tsNs int64) *filer_pb.FileChunk {
	fid, _ := filer_pb.ToFileIdObject(fileId)
	return &filer_pb.FileChunk{
		FileId:       fileId,
		Offset:       offset,
		Size:         uint64(uploadResult.Size),
		ModifiedTsNs: tsNs,
		ETag:         uploadResult.ContentMd5,
		CipherKey:    uploadResult.CipherKey,
		IsCompressed: uploadResult.Gzip > 0,
		Fid:          fid,
	}
}

var (
	fileNameEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", "")
	uploader        *Uploader
	uploaderErr     error
	once            sync.Once
)

// HTTPClient interface for testing
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Uploader
type Uploader struct {
	httpClient HTTPClient
}

func NewUploader() (*Uploader, error) {
	once.Do(func() {
		// With Dial context
		var httpClient *util_http_client.HTTPClient
		httpClient, uploaderErr = util_http.NewGlobalHttpClient(util_http_client.AddDialContext)
		if uploaderErr != nil {
			uploaderErr = fmt.Errorf("error initializing the loader: %s", uploaderErr)
		}
		if httpClient != nil {
			uploader = newUploader(httpClient)
		}
	})
	return uploader, uploaderErr
}

func newUploader(httpClient HTTPClient) *Uploader {
	return &Uploader{
		httpClient: httpClient,
	}
}

// UploadWithRetry will retry both assigning volume request and uploading content
// The option parameter does not need to specify UploadUrl and Jwt, which will come from assigning volume.
func (uploader *Uploader) UploadWithRetry(filerClient filer_pb.FilerClient, assignRequest *filer_pb.AssignVolumeRequest, uploadOption *UploadOption, genFileUrlFn func(host, fileId string) string, reader io.Reader) (fileId string, uploadResult *UploadResult, err error, data []byte) {
	doUploadFunc := func() error {

		var host string
		var auth security.EncodedJwt

		// grpc assign volume
		if grpcAssignErr := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			resp, assignErr := client.AssignVolume(context.Background(), assignRequest)
			if assignErr != nil {
				glog.V(0).Infof("assign volume failure %v: %v", assignRequest, assignErr)
				return assignErr
			}
			if resp.Error != "" {
				return fmt.Errorf("assign volume failure %v: %v", assignRequest, resp.Error)
			}

			fileId, auth = resp.FileId, security.EncodedJwt(resp.Auth)
			loc := resp.Location
			host = filerClient.AdjustedUrl(loc)

			return nil
		}); grpcAssignErr != nil {
			return fmt.Errorf("filerGrpcAddress assign volume: %v", grpcAssignErr)
		}

		uploadOption.UploadUrl = genFileUrlFn(host, fileId)
		uploadOption.Jwt = auth

		var uploadErr error
		uploadResult, uploadErr, data = uploader.doUpload(reader, uploadOption)
		return uploadErr
	}
	if uploadOption.RetryForever {
		util.RetryUntil("uploadWithRetryForever", doUploadFunc, func(err error) (shouldContinue bool) {
			glog.V(0).Infof("upload content: %v", err)
			return true
		})
	} else {
		uploadErrList := []string{"transport", "is read only"}
		err = util.MultiRetry("uploadWithRetry", uploadErrList, doUploadFunc)
	}

	return
}

// Upload sends a POST request to a volume server to upload the content with adjustable compression level
func (uploader *Uploader) UploadData(data []byte, option *UploadOption) (uploadResult *UploadResult, err error) {
	uploadResult, err = uploader.retriedUploadData(data, option)
	return
}

// Upload sends a POST request to a volume server to upload the content with fast compression
func (uploader *Uploader) Upload(reader io.Reader, option *UploadOption) (uploadResult *UploadResult, err error, data []byte) {
	uploadResult, err, data = uploader.doUpload(reader, option)
	return
}

func (uploader *Uploader) doUpload(reader io.Reader, option *UploadOption) (uploadResult *UploadResult, err error, data []byte) {
	bytesReader, ok := reader.(*util.BytesReader)
	if ok {
		data = bytesReader.Bytes
	} else {
		data, err = io.ReadAll(reader)
		if err != nil {
			err = fmt.Errorf("read input: %v", err)
			return
		}
	}
	uploadResult, uploadErr := uploader.retriedUploadData(data, option)
	return uploadResult, uploadErr, data
}

func (uploader *Uploader) retriedUploadData(data []byte, option *UploadOption) (uploadResult *UploadResult, err error) {
	for i := 0; i < 3; i++ {
		if i > 0 {
			time.Sleep(time.Millisecond * time.Duration(237*(i+1)))
		}
		uploadResult, err = uploader.doUploadData(data, option)
		if err == nil {
			uploadResult.RetryCount = i
			return
		}
		glog.Warningf("uploading %d to %s: %v", i, option.UploadUrl, err)
	}
	return
}

func (uploader *Uploader) doUploadData(data []byte, option *UploadOption) (uploadResult *UploadResult, err error) {
	contentIsGzipped := option.IsInputCompressed
	shouldGzipNow := false
	if !option.IsInputCompressed {
		if option.MimeType == "" {
			option.MimeType = http.DetectContentType(data)
			// println("detect1 mimetype to", MimeType)
			if option.MimeType == "application/octet-stream" {
				option.MimeType = ""
			}
		}
		if shouldBeCompressed, iAmSure := util.IsCompressableFileType(filepath.Base(option.Filename), option.MimeType); iAmSure && shouldBeCompressed {
			shouldGzipNow = true
		} else if !iAmSure && option.MimeType == "" && len(data) > 16*1024 {
			var compressed []byte
			compressed, err = util.GzipData(data[0:128])
			if err != nil {
				return
			}
			shouldGzipNow = len(compressed)*10 < 128*9 // can not compress to less than 90%
		}
	}

	var clearDataLen int

	// gzip if possible
	// this could be double copying
	clearDataLen = len(data)
	clearData := data
	if shouldGzipNow {
		compressed, compressErr := util.GzipData(data)
		// fmt.Printf("data is compressed from %d ==> %d\n", len(data), len(compressed))
		if compressErr == nil {
			data = compressed
			contentIsGzipped = true
		}
	} else if option.IsInputCompressed {
		// just to get the clear data length
		clearData, err = util.DecompressData(data)
		if err == nil {
			clearDataLen = len(clearData)
		}
	}

	if option.Cipher {
		// encrypt(gzip(data))

		// encrypt
		cipherKey := util.GenCipherKey()
		encryptedData, encryptionErr := util.Encrypt(data, cipherKey)
		if encryptionErr != nil {
			err = fmt.Errorf("encrypt input: %v", encryptionErr)
			return
		}

		// upload data
		uploadResult, err = uploader.upload_content(func(w io.Writer) (err error) {
			_, err = w.Write(encryptedData)
			return
		}, len(encryptedData), &UploadOption{
			UploadUrl:         option.UploadUrl,
			Filename:          "",
			Cipher:            false,
			IsInputCompressed: false,
			MimeType:          "",
			PairMap:           nil,
			Jwt:               option.Jwt,
		})
		if uploadResult == nil {
			return
		}
		uploadResult.Name = option.Filename
		uploadResult.Mime = option.MimeType
		uploadResult.CipherKey = cipherKey
		uploadResult.Size = uint32(clearDataLen)
		if contentIsGzipped {
			uploadResult.Gzip = 1
		}
	} else {
		// upload data
		uploadResult, err = uploader.upload_content(func(w io.Writer) (err error) {
			_, err = w.Write(data)
			return
		}, len(data), &UploadOption{
			UploadUrl:         option.UploadUrl,
			Filename:          option.Filename,
			Cipher:            false,
			IsInputCompressed: contentIsGzipped,
			MimeType:          option.MimeType,
			PairMap:           option.PairMap,
			Jwt:               option.Jwt,
			Md5:               option.Md5,
			BytesBuffer:       option.BytesBuffer,
		})
		if uploadResult == nil {
			return
		}
		uploadResult.Size = uint32(clearDataLen)
		if contentIsGzipped {
			uploadResult.Gzip = 1
		}
	}

	return uploadResult, err
}

func (uploader *Uploader) upload_content(fillBufferFunction func(w io.Writer) error, originalDataSize int, option *UploadOption) (*UploadResult, error) {
	var body_writer *multipart.Writer
	var reqReader *bytes.Reader
	var buf *bytebufferpool.ByteBuffer
	if option.BytesBuffer == nil {
		buf = GetBuffer()
		defer PutBuffer(buf)
		body_writer = multipart.NewWriter(buf)
	} else {
		option.BytesBuffer.Reset()
		body_writer = multipart.NewWriter(option.BytesBuffer)
	}
	h := make(textproto.MIMEHeader)
	filename := fileNameEscaper.Replace(option.Filename)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, filename))
	h.Set("Idempotency-Key", option.UploadUrl)
	if option.MimeType == "" {
		option.MimeType = mime.TypeByExtension(strings.ToLower(filepath.Ext(option.Filename)))
	}
	if option.MimeType != "" {
		h.Set("Content-Type", option.MimeType)
	}
	if option.IsInputCompressed {
		h.Set("Content-Encoding", "gzip")
	}
	if option.Md5 != "" {
		h.Set("Content-MD5", option.Md5)
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
	if option.BytesBuffer == nil {
		reqReader = bytes.NewReader(buf.Bytes())
	} else {
		reqReader = bytes.NewReader(option.BytesBuffer.Bytes())
	}
	req, postErr := http.NewRequest(http.MethodPost, option.UploadUrl, reqReader)
	if postErr != nil {
		glog.V(1).Infof("create upload request %s: %v", option.UploadUrl, postErr)
		return nil, fmt.Errorf("create upload request %s: %v", option.UploadUrl, postErr)
	}
	req.Header.Set("Content-Type", content_type)
	for k, v := range option.PairMap {
		req.Header.Set(k, v)
	}
	if option.Jwt != "" {
		req.Header.Set("Authorization", "BEARER "+string(option.Jwt))
	}
	// print("+")
	resp, post_err := uploader.httpClient.Do(req)
	defer util_http.CloseResponse(resp)
	if post_err != nil {
		if strings.Contains(post_err.Error(), "connection reset by peer") ||
			strings.Contains(post_err.Error(), "use of closed network connection") {
			glog.V(1).Infof("repeat error upload request %s: %v", option.UploadUrl, postErr)
			stats.FilerHandlerCounter.WithLabelValues(stats.RepeatErrorUploadContent).Inc()
			resp, post_err = uploader.httpClient.Do(req)
			defer util_http.CloseResponse(resp)
		}
	}
	if post_err != nil {
		return nil, fmt.Errorf("upload %s %d bytes to %v: %v", option.Filename, originalDataSize, option.UploadUrl, post_err)
	}
	// print("-")

	var ret UploadResult
	etag := getEtag(resp)
	if resp.StatusCode == http.StatusNoContent {
		ret.ETag = etag
		return &ret, nil
	}

	resp_body, ra_err := io.ReadAll(resp.Body)
	if ra_err != nil {
		return nil, fmt.Errorf("read response body %v: %v", option.UploadUrl, ra_err)
	}

	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.Errorf("unmarshal %s: %v", option.UploadUrl, string(resp_body))
		return nil, fmt.Errorf("unmarshal %v: %v", option.UploadUrl, unmarshal_err)
	}
	if ret.Error != "" {
		return nil, fmt.Errorf("unmarshalled error %v: %v", option.UploadUrl, ret.Error)
	}
	ret.ETag = etag
	ret.ContentMd5 = resp.Header.Get("Content-MD5")
	return &ret, nil
}

func getEtag(r *http.Response) (etag string) {
	etag = r.Header.Get("ETag")
	if strings.HasPrefix(etag, "\"") && strings.HasSuffix(etag, "\"") {
		etag = etag[1 : len(etag)-1]
	}
	return
}
