package operation

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
	"github.com/valyala/bytebufferpool"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	util_http_client "github.com/seaweedfs/seaweedfs/weed/util/http/client"
)

// GenUploadUrlProxy returns a function that builds a chunk upload URL via the
// filer proxy.  Identical to the inline logic used by weed mount -filerProxy
// and weed filer gateway:
//
//   - without filerProxy: "http://{host}/{fileId}"
//   - with filerProxy:     "http://{filerAddress}/?proxyChunkId={fileId}"
func GenUploadUrlProxy(filerAddress string) func(host, fileId string) string {
	return func(host, fileId string) string {
		if filerAddress == "" {
			return fmt.Sprintf("http://%s/%s", host, fileId)
		}
		return util_http.ProxyChunkUrl(filerAddress, fileId)
	}
}

type UploadOption struct {
	UploadUrl         string
	Filename          string
	Cipher            bool
	IsInputCompressed bool
	IsReplication     bool // preserve the source needle's compression state
	MimeType          string
	PairMap           map[string]string
	Jwt               security.EncodedJwt
	RetryForever      bool
	Md5               string
	WantMd5           bool // compute Content-MD5 from the data when Md5 is unset and the upload is not ciphered
	BytesBuffer       *bytes.Buffer
	SourceUrl         string                           // optional: for logging when reading from a remote source
	MaxAttempts       int                              // <=0 uses the default
	GenUploadUrl      func(host, fileId string) string // if nil → fallback "http://{host}/{fileId}"
	FilerJwt          security.FilerJwtProvider        // credential for proxy chunk URLs; nil → process-wide jwt.filer_signing

	// stallWatchdogDisarmed turns the watchdog off for this upload's later
	// attempts once it fired: a re-arm could burn every retry.
	stallWatchdogDisarmed bool
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

// ToPbFileChunkWithSSE creates a FileChunk with SSE metadata
func (uploadResult *UploadResult) ToPbFileChunkWithSSE(fileId string, offset int64, tsNs int64, sseType filer_pb.SSEType, sseMetadata []byte) *filer_pb.FileChunk {
	fid, _ := filer_pb.ToFileIdObject(fileId)
	chunk := &filer_pb.FileChunk{
		FileId:       fileId,
		Offset:       offset,
		Size:         uint64(uploadResult.Size),
		ModifiedTsNs: tsNs,
		ETag:         uploadResult.ContentMd5,
		CipherKey:    uploadResult.CipherKey,
		IsCompressed: uploadResult.Gzip > 0,
		Fid:          fid,
	}

	// Add SSE metadata if provided
	chunk.SseType = sseType
	if len(sseMetadata) > 0 {
		chunk.SseMetadata = sseMetadata
	}

	return chunk
}

var (
	uploader    *Uploader
	uploaderErr error
	once        sync.Once
)

// uploadStatusError carries the volume-server HTTP status of a failed upload so
// the retry gate can decide on the status code instead of the message text.
// StatusCode 0 means the request never got a response — a transport failure.
type uploadStatusError struct {
	StatusCode int
	stalled    bool // the stall watchdog cancelled this attempt's request
	err        error
}

func (e *uploadStatusError) Error() string { return e.err.Error() }
func (e *uploadStatusError) Unwrap() error { return e.err }

// ShouldReassignUpload reports whether an upload error means the client should
// ask for a fresh volume assignment and retry on another volume.
//
// On the write path a volume server only 5xxs on a ReplicatedWrite failure
// (local disk, replica peer down, or under-replication) — all of which a
// different volume dodges — so any 5xx is reassignable. A status of 0 means the
// assigned target never answered (down/unreachable), also reassignable. A 4xx
// is a genuine client error and is surfaced. Errors without a status come from
// the AssignVolume RPC or request setup; retry only transient transport ones.
func ShouldReassignUpload(err error) bool {
	if err == nil {
		return false
	}
	var se *uploadStatusError
	if errors.As(err, &se) {
		return se.StatusCode == 0 || se.StatusCode >= 500
	}
	return strings.Contains(err.Error(), "transport")
}

// assignVolumeTimeout bounds a single AssignVolume RPC so an overwhelmed filer
// can't block the caller forever. Overridable in tests.
var assignVolumeTimeout = 30 * time.Second

// HTTPClient interface for testing
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Uploader
type Uploader struct {
	httpClient HTTPClient
	// fastUploadHosts records the last time an upload to a host ran at a
	// healthy rate, so a later upload stalling far below it can be blamed on
	// that one connection rather than the link and get cancelled for a retry
	// on a fresh conn.
	fastUploadHosts sync.Map
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

// NewUploaderWithHttpClient creates an Uploader that uses the provided HTTP
// client instead of the global one. This is used by filer.sync to upload to
// remote clusters that use different TLS certificates.
func NewUploaderWithHttpClient(httpClient HTTPClient) *Uploader {
	return &Uploader{
		httpClient: httpClient,
	}
}

func (uploader *Uploader) uploadWithRetryData(assignFn func() (fileId string, host string, auth security.EncodedJwt, fsync bool, err error), uploadOption *UploadOption, data []byte) (fileId string, uploadResult *UploadResult, err error) {
	doUploadFunc := func() error {
		var host string
		var auth security.EncodedJwt
		var fsync bool
		fileId, host, auth, fsync, err = assignFn()
		if err != nil {
			return err
		}

		genUrl := uploadOption.GenUploadUrl
		if genUrl == nil {
			genUrl = func(host, fileId string) string { return fmt.Sprintf("http://%s/%s", host, fileId) }
		}
		uploadOption.UploadUrl = genUrl(host, fileId)
		if fsync {
			uploadOption.UploadUrl = util_http.AppendQueryParameter(uploadOption.UploadUrl, "fsync", "true")
		}
		uploadOption.Jwt = auth
		if util_http.IsProxyChunkUrl(uploadOption.UploadUrl) {
			// The request addresses the filer, which authorizes it and mints the
			// volume credential itself. The AssignVolume token is not a filer
			// credential and gets the caller nowhere here.
			if uploadOption.FilerJwt != nil {
				uploadOption.Jwt = uploadOption.FilerJwt(true)
			} else {
				uploadOption.Jwt = security.EncodedJwt(util_http.JwtForFilerServer(true))
			}
		}

		uploadResult, err = uploader.retriedUploadData(context.Background(), data, uploadOption)
		return err
	}

	if uploadOption.RetryForever {
		util.RetryUntil("uploadWithRetryForever", doUploadFunc, func(err error) (shouldContinue bool) {
			glog.V(0).Infof("upload content: %v", err)
			return true
		})
	} else {
		err = util.RetryOnError("uploadWithRetry", ShouldReassignUpload, doUploadFunc)
	}

	return
}

// UploadWithRetry will retry both assigning volume request and uploading content
// The option parameter does not need to specify UploadUrl and Jwt, which will come from assigning volume.
func (uploader *Uploader) UploadWithRetry(filerClient filer_pb.FilerClient, assignRequest *filer_pb.AssignVolumeRequest, uploadOption *UploadOption, reader io.Reader) (fileId string, uploadResult *UploadResult, err error, data []byte) {
	bytesReader, ok := reader.(*util.BytesReader)
	if ok {
		data = bytesReader.Bytes
	} else {
		data, err = io.ReadAll(reader)
		if err != nil {
			glog.V(0).Infof("upload read input %s: %v", uploadOption.SourceUrl, err)
			err = fmt.Errorf("read input: %w", err)
			return
		}
		glog.V(4).Infof("upload read %d bytes from %s", len(data), uploadOption.SourceUrl)
	}

	// Tell the master the real chunk size so its effectiveSize accounting
	// doesn't fall back to the 1 MB DefaultNeedleSizeEstimate per fid.
	if assignRequest.ExpectedDataSize == 0 {
		assignRequest.ExpectedDataSize = uint64(len(data))
	}

	// Hash the buffer we already hold so the server echoes Content-MD5 back as
	// the chunk ETag (std-base64 of the raw digest, the form ParseUpload
	// verifies). Under cipher the server never sees the header, but the digest
	// still becomes the chunk ETag.
	if uploadOption.WantMd5 && uploadOption.Md5 == "" {
		digest := md5.Sum(data)
		uploadOption.Md5 = base64.StdEncoding.EncodeToString(digest[:])
	}

	fileId, uploadResult, err = uploader.uploadWithRetryData(func() (fileId string, host string, auth security.EncodedJwt, fsync bool, err error) {
		// grpc assign volume
		if grpcAssignErr := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			assignCtx, assignCancel := context.WithTimeout(context.Background(), assignVolumeTimeout)
			defer assignCancel()
			resp, assignErr := client.AssignVolume(assignCtx, assignRequest)
			if assignErr != nil {
				glog.V(0).Infof("assign volume failure %v: %v", assignRequest, assignErr)
				return assignErr
			}
			if resp.Error != "" {
				return fmt.Errorf("assign volume failure %v: %v", assignRequest, resp.Error)
			}

			fileId, auth = resp.FileId, security.EncodedJwt(resp.Auth)
			fsync = resp.Fsync
			loc := resp.Location
			host = filerClient.AdjustedUrl(loc)

			return nil
		}); grpcAssignErr != nil {
			err = fmt.Errorf("filerGrpcAddress assign volume: %w", grpcAssignErr)
		}
		return
	}, uploadOption, data)
	return
}

// Upload sends a POST request to a volume server to upload the content with adjustable compression level
func (uploader *Uploader) UploadData(ctx context.Context, data []byte, option *UploadOption) (uploadResult *UploadResult, err error) {
	uploadResult, err = uploader.retriedUploadData(ctx, data, option)
	return
}

// Upload sends a POST request to a volume server to upload the content with fast compression
func (uploader *Uploader) Upload(ctx context.Context, reader io.Reader, option *UploadOption) (uploadResult *UploadResult, err error, data []byte) {
	uploadResult, err, data = uploader.doUpload(ctx, reader, option)
	return
}

func (uploader *Uploader) doUpload(ctx context.Context, reader io.Reader, option *UploadOption) (uploadResult *UploadResult, err error, data []byte) {
	bytesReader, ok := reader.(*util.BytesReader)
	if ok {
		data = bytesReader.Bytes
	} else {
		data, err = io.ReadAll(reader)
		if err != nil {
			err = fmt.Errorf("read input: %w", err)
			return
		}
	}
	uploadResult, uploadErr := uploader.retriedUploadData(ctx, data, option)
	return uploadResult, uploadErr, data
}

func (uploader *Uploader) retriedUploadData(ctx context.Context, data []byte, option *UploadOption) (uploadResult *UploadResult, err error) {
	// A stall disarms the watchdog for this upload's remaining attempts only;
	// keep that on a per-call option so a caller reusing UploadOption starts
	// the next upload armed.
	localOption := *option
	option = &localOption
	maxAttempts := option.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	for i := 0; i < maxAttempts; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Millisecond * time.Duration(237*(i+1))):
			}
		}
		uploadResult, err = uploader.doUploadData(ctx, data, option)
		if err == nil {
			uploadResult.RetryCount = i
			return
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var statusErr *uploadStatusError
		if errors.As(err, &statusErr) && statusErr.stalled {
			option.stallWatchdogDisarmed = true
		}
		glog.WarningfCtx(ctx, "uploading %d to %s: %v", i, option.UploadUrl, err)
	}
	return
}

func (uploader *Uploader) doUploadData(ctx context.Context, data []byte, option *UploadOption) (uploadResult *UploadResult, err error) {
	contentIsGzipped := option.IsInputCompressed
	shouldGzipNow := false
	if !option.IsInputCompressed && !option.IsReplication {
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
			if len(compressed) < len(data) {
				data = compressed
				contentIsGzipped = true
			}
		}
	} else if option.IsInputCompressed && !option.IsReplication {
		// decompress only to report the clear data length; replication discards the result, so skip it
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
			err = fmt.Errorf("encrypt input: %w", encryptionErr)
			return
		}

		// upload data
		uploadResult, err = uploader.upload_content(ctx, func(w io.Writer) (err error) {
			_, err = w.Write(encryptedData)
			return
		}, len(encryptedData), &UploadOption{
			UploadUrl:             option.UploadUrl,
			Filename:              "",
			Cipher:                false,
			IsInputCompressed:     false,
			MimeType:              "",
			PairMap:               nil,
			Jwt:                   option.Jwt,
			BytesBuffer:           option.BytesBuffer,
			stallWatchdogDisarmed: option.stallWatchdogDisarmed,
		})
		if uploadResult == nil {
			return
		}
		uploadResult.Name = option.Filename
		uploadResult.Mime = option.MimeType
		uploadResult.CipherKey = cipherKey
		// The volume server only ever hashes the ciphertext it stored, so the
		// chunk ETag has to come from the caller's plaintext digest.
		uploadResult.ContentMd5 = option.Md5
		uploadResult.Size = uint32(clearDataLen)
		if contentIsGzipped {
			uploadResult.Gzip = 1
		}
	} else {
		// upload data
		uploadResult, err = uploader.upload_content(ctx, func(w io.Writer) (err error) {
			_, err = w.Write(data)
			return
		}, len(data), &UploadOption{
			UploadUrl:             option.UploadUrl,
			Filename:              option.Filename,
			Cipher:                false,
			IsInputCompressed:     contentIsGzipped,
			MimeType:              option.MimeType,
			PairMap:               option.PairMap,
			Jwt:                   option.Jwt,
			Md5:                   option.Md5,
			BytesBuffer:           option.BytesBuffer,
			stallWatchdogDisarmed: option.stallWatchdogDisarmed,
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

func (uploader *Uploader) upload_content(ctx context.Context, fillBufferFunction func(w io.Writer) error, originalDataSize int, option *UploadOption) (*UploadResult, error) {
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
	// Use mime.FormatMediaType for RFC 6266 compliant Content-Disposition,
	// properly handling non-ASCII characters and special characters
	h.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": "file", "filename": option.Filename}))
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
		glog.V(0).InfolnCtx(ctx, "error creating form file", cp_err.Error())
		return nil, cp_err
	}
	if err := fillBufferFunction(file_writer); err != nil {
		glog.V(0).InfolnCtx(ctx, "error copying data", err)
		return nil, err
	}
	content_type := body_writer.FormDataContentType()
	if err := body_writer.Close(); err != nil {
		glog.V(0).InfolnCtx(ctx, "error closing body", err)
		return nil, err
	}
	if option.BytesBuffer == nil {
		reqReader = bytes.NewReader(buf.Bytes())
	} else {
		reqReader = bytes.NewReader(option.BytesBuffer.Bytes())
	}
	reqCtx, cancelUpload := context.WithCancel(ctx)
	defer cancelUpload()

	req, postErr := http.NewRequestWithContext(reqCtx, http.MethodPost, option.UploadUrl, reqReader)
	if postErr != nil {
		glog.V(1).InfofCtx(ctx, "create upload request %s: %v", option.UploadUrl, postErr)
		return nil, fmt.Errorf("create upload request %s: %v", option.UploadUrl, postErr)
	}
	req.Header.Set("Content-Type", content_type)
	for k, v := range option.PairMap {
		req.Header.Set(k, v)
	}
	if option.Jwt != "" {
		req.Header.Set("Authorization", security.BearerPrefix+string(option.Jwt))
	}

	request_id.InjectToRequest(ctx, req)

	var uploadDone atomic.Bool
	defer uploadDone.Store(true)
	// bodySentAt marks when the transport finished draining the request body;
	// past it the wait is server-side commit, which the transport's response
	// header timeout already bounds — the stall watchdog only blames a conn
	// that is still trickling the body.
	var curBodySentAt atomic.Pointer[atomic.Int64]
	bodySentAt := &atomic.Int64{}
	curBodySentAt.Store(bodySentAt)
	req.Body = &watchdogBody{ReadCloser: req.Body, sentAt: bodySentAt}
	last, hostWasFast := uploader.fastUploadHosts.Load(req.URL.Host)
	hostWasFast = hostWasFast && !option.stallWatchdogDisarmed && time.Since(time.Unix(0, last.(int64))) < fastUploadHostMemory
	stallFired := func() {
		if !uploadDone.CompareAndSwap(false, true) {
			return
		}
		// The host was moving chunks at full rate a moment ago, so an
		// upload still trickling past its floor rides a wedged pooled
		// conn; cancel drops the conn so the retry dials a fresh one.
		// CompareAndDelete leaves a newer mark a concurrent success stored.
		uploader.fastUploadHosts.CompareAndDelete(req.URL.Host, last)
		glog.WarningfCtx(ctx, "upload %d bytes to %s stalled on a pooled connection, retrying on a fresh one", originalDataSize, option.UploadUrl)
		cancelUpload()
	}
	var stallTimer *time.Timer
	if hostWasFast {
		defer func() {
			if stallTimer != nil {
				stallTimer.Stop()
			}
		}()
		stallTimer = time.AfterFunc(uploadStallTimeout(int64(originalDataSize)), func() {
			if curBodySentAt.Load().Load() == 0 {
				stallFired()
			}
		})
	}

	// print("+")
	started := time.Now()
	resp, post_err := uploader.httpClient.Do(req)
	defer util_http.CloseResponse(resp)
	if post_err != nil {
		if strings.Contains(post_err.Error(), "connection reset by peer") ||
			strings.Contains(post_err.Error(), "use of closed network connection") {
			glog.V(1).InfofCtx(ctx, "repeat error upload request %s: %v", option.UploadUrl, post_err)
			stats.FilerHandlerCounter.WithLabelValues(stats.RepeatErrorUploadContent).Inc()
			// The first attempt already consumed (or partially consumed) the
			// body, so retrying with the same *http.Request would send 0 bytes
			// and Go's transport would surface "ContentLength=N with Body
			// length 0". http.NewRequestWithContext sets GetBody for
			// *bytes.Reader bodies; use it to attach a fresh body for retry.
			// If we can't rewind, skip the inner retry and let the outer
			// retriedUploadData loop reissue the request with a fresh body —
			// retrying here with a consumed body would mask the original
			// "connection reset" error with a misleading "Body length 0".
			if req.GetBody != nil {
				if newBody, gbErr := req.GetBody(); gbErr == nil {
					bodySentAt = &atomic.Int64{}
					curBodySentAt.Store(bodySentAt)
					started = time.Now()
					if stallTimer != nil && !uploadDone.Load() {
						stallTimer.Reset(uploadStallTimeout(int64(originalDataSize)))
					}
					req.Body = &watchdogBody{ReadCloser: newBody, sentAt: bodySentAt}
					resp, post_err = uploader.httpClient.Do(req)
					defer util_http.CloseResponse(resp)
				} else {
					glog.V(1).InfofCtx(ctx, "skip inner retry for %s: GetBody returned %v", option.UploadUrl, gbErr)
				}
			} else {
				glog.V(1).InfofCtx(ctx, "skip inner retry for %s: req.GetBody is nil", option.UploadUrl)
			}
		}
	}
	if post_err != nil {
		stats.UploadErrorCounter.WithLabelValues("0").Inc()
		return nil, &uploadStatusError{StatusCode: 0, stalled: reqCtx.Err() != nil && ctx.Err() == nil, err: fmt.Errorf("upload %s %d bytes to %v: %v", option.Filename, originalDataSize, option.UploadUrl, post_err)}
	}
	// The wire time proves the host, but the mark only lands on a verified
	// upload: a fast rejection says nothing about how the conn drains.
	movedIn := time.Since(started)
	if sentNs := curBodySentAt.Load().Load(); sentNs > 0 {
		movedIn = time.Duration(sentNs - started.UnixNano())
	}

	// print("-")

	var ret UploadResult
	etag := getEtag(resp)
	if resp.StatusCode == http.StatusNoContent {
		if uploadMovedHealthy(int64(originalDataSize), movedIn) {
			uploader.fastUploadHosts.Store(req.URL.Host, time.Now().UnixNano())
		}
		ret.ETag = etag
		ret.ContentMd5 = resp.Header.Get("Content-MD5")
		return &ret, nil
	}

	if stallTimer != nil {
		// The header timeout does not cover the response body: keep the
		// watchdog armed for the read so a conn that wedges mid-reply gets
		// cancelled for a fresh retry.
		respStallTimer := time.AfterFunc(uploadStallMinWait, stallFired)
		defer respStallTimer.Stop()
	}
	resp_body, ra_err := io.ReadAll(resp.Body)
	uploadDone.Store(true)
	if ra_err != nil {
		stats.UploadErrorCounter.WithLabelValues(strconv.Itoa(resp.StatusCode)).Inc()
		return nil, &uploadStatusError{StatusCode: resp.StatusCode, stalled: reqCtx.Err() != nil && ctx.Err() == nil, err: fmt.Errorf("read response body %v: %w", option.UploadUrl, ra_err)}
	}

	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		stats.UploadErrorCounter.WithLabelValues(strconv.Itoa(resp.StatusCode)).Inc()
		glog.ErrorfCtx(ctx, "unmarshal %s: %v", option.UploadUrl, string(resp_body))
		return nil, &uploadStatusError{StatusCode: resp.StatusCode, err: fmt.Errorf("unmarshal %v: %w", option.UploadUrl, unmarshal_err)}
	}
	if ret.Error != "" {
		stats.UploadErrorCounter.WithLabelValues(strconv.Itoa(resp.StatusCode)).Inc()
		return nil, &uploadStatusError{StatusCode: resp.StatusCode, err: fmt.Errorf("unmarshalled error %v: %v", option.UploadUrl, ret.Error)}
	}
	if uploadMovedHealthy(int64(originalDataSize), movedIn) {
		uploader.fastUploadHosts.Store(req.URL.Host, time.Now().UnixNano())
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

const uploadHealthyBps = int64(1 << 20) // marks a host as able to drain chunks quickly

var (
	uploadStallBps       = int64(256 << 10)
	uploadStallMinWait   = 10 * time.Second
	fastUploadHostMemory = time.Minute
)

// uploadStallTimeout is the most an upload to a recently-fast host may take
// before its connection is presumed wedged; a healthy pooled conn moves the
// body orders of magnitude faster.
func uploadStallTimeout(size int64) time.Duration {
	if d := time.Duration(size/uploadStallBps) * time.Second; d > uploadStallMinWait {
		return d
	}
	return uploadStallMinWait
}

// uploadMovedHealthy keeps tiny uploads and genuinely slow links from marking a
// host fast: the mark is what lets a later stall blame the conn, not the link.
func uploadMovedHealthy(size int64, elapsed time.Duration) bool {
	return size >= 256*1024 && elapsed > 0 && float64(size)/elapsed.Seconds() >= float64(uploadHealthyBps)
}

// watchdogBody records when the transport drains the request body: before that
// a stuck send blames the conn; after it the wait is the server's.
type watchdogBody struct {
	io.ReadCloser
	sentAt *atomic.Int64
}

func (b *watchdogBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.sentAt.Store(time.Now().UnixNano())
	}
	return n, err
}

func (b *watchdogBody) Close() error {
	b.sentAt.Store(time.Now().UnixNano())
	return b.ReadCloser.Close()
}
