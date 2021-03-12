package util

import (
	"bytes"
	"fmt"
	"github.com/valyala/fasthttp"
	"sync"
	"time"
)

var (
	fastClient = &fasthttp.Client{
		NoDefaultUserAgentHeader:      true, // Don't send: User-Agent: fasthttp
		MaxConnsPerHost:               1024,
		ReadBufferSize:                4096,      // Make sure to set this big enough that your whole request can be read at once.
		WriteBufferSize:               64 * 1024, // Same but for your response.
		ReadTimeout:                   time.Second,
		WriteTimeout:                  time.Second,
		MaxIdleConnDuration:           time.Minute,
		DisableHeaderNamesNormalizing: true, // If you set the case on your headers correctly you can enable this.
		DialDualStack:                 true,
	}

	// Put everything in pools to prevent garbage.
	bytesPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, 0)
			return &b
		},
	}

	responsePool = sync.Pool{
		New: func() interface{} {
			return make(chan *fasthttp.Response)
		},
	}
)

func FastGet(url string) ([]byte, bool, error) {

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(res)

	req.SetRequestURIBytes([]byte(url))
	req.Header.Add("Accept-Encoding", "gzip")

	err := fastClient.Do(req, res)
	if err != nil {
		return nil, true, err
	}

	var data []byte
	contentEncoding := res.Header.Peek("Content-Encoding")
	if bytes.Compare(contentEncoding, []byte("gzip")) == 0 {
		data, err = res.BodyGunzip()
	} else {
		data = res.Body()
	}

	out := make([]byte, len(data))
	copy(out, data)

	if res.StatusCode() >= 400 {
		retryable := res.StatusCode() >= 500
		return nil, retryable, fmt.Errorf("%s: %d", url, res.StatusCode())
	}
	if err != nil {
		return nil, false, err
	}
	return out, false, nil
}

func FastReadUrlAsStream(fileUrl string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (retryable bool, err error) {

	if cipherKey != nil {
		return readEncryptedUrl(fileUrl, cipherKey, isContentGzipped, isFullChunk, offset, size, fn)
	}

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(res)

	req.SetRequestURIBytes([]byte(fileUrl))

	if isFullChunk {
		req.Header.Add("Accept-Encoding", "gzip")
	} else {
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+int64(size)-1))
	}

	if err = fastClient.Do(req, res); err != nil {
		return true, err
	}

	if res.StatusCode() >= 400 {
		retryable = res.StatusCode() >= 500
		return retryable, fmt.Errorf("%s: %d", fileUrl, res.StatusCode())
	}

	contentEncoding := res.Header.Peek("Content-Encoding")
	if bytes.Compare(contentEncoding, []byte("gzip")) == 0 {
		bodyData, err := res.BodyGunzip()
		if err != nil {
			return false, err
		}
		fn(bodyData)
	} else {
		fn(res.Body())
	}

	return false, nil

}
