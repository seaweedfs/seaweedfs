package http

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	util "github.com/seaweedfs/seaweedfs/weed/util"
)

func Post(clientCfg *ClientCfg, url string, values url.Values) ([]byte, error) {
	url, err := clientCfg.FixHttpScheme(url)
	if err != nil {
		return nil, err
	}
	r, err := clientCfg.Client.PostForm(url, values)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	if r.StatusCode >= 400 {
		if err != nil {
			return nil, fmt.Errorf("%s: %d - %s", url, r.StatusCode, string(b))
		} else {
			return nil, fmt.Errorf("%s: %s", url, r.Status)
		}
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

// github.com/seaweedfs/seaweedfs/unmaintained/repeated_vacuum/repeated_vacuum.go
// may need increasing http.Client.Timeout
func Get(clientCfg *ClientCfg, url string) ([]byte, bool, error) {
	return GetAuthenticated(clientCfg, url, "")
}

func GetAuthenticated(clientCfg *ClientCfg, url, jwt string) ([]byte, bool, error) {
	url, err := clientCfg.FixHttpScheme(url)
	if err != nil {
		return nil, true, err
	}
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, true, err
	}
	maybeAddAuth(request, jwt)
	request.Header.Add("Accept-Encoding", "gzip")

	response, err := clientCfg.Client.Do(request)
	if err != nil {
		return nil, true, err
	}
	defer CloseResponse(response)

	var reader io.ReadCloser
	switch response.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(response.Body)
		if err != nil {
			return nil, true, err
		}
		defer reader.Close()
	default:
		reader = response.Body
	}

	b, err := io.ReadAll(reader)
	if response.StatusCode >= 400 {
		retryable := response.StatusCode >= 500
		return nil, retryable, fmt.Errorf("%s: %s", url, response.Status)
	}
	if err != nil {
		return nil, false, err
	}
	return b, false, nil
}

func Head(clientCfg *ClientCfg, url string) (http.Header, error) {
	url, err := clientCfg.FixHttpScheme(url)
	if err != nil {
		return nil, err
	}
	r, err := clientCfg.Client.Head(url)
	if err != nil {
		return nil, err
	}
	defer CloseResponse(r)
	if r.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	return r.Header, nil
}

func maybeAddAuth(req *http.Request, jwt string) {
	if jwt != "" {
		req.Header.Set("Authorization", "BEARER "+string(jwt))
	}
}

func Delete(clientCfg *ClientCfg, url string, jwt string) error {
	url, err := clientCfg.FixHttpScheme(url)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("DELETE", url, nil)
	maybeAddAuth(req, jwt)
	if err != nil {
		return err
	}
	resp, e := clientCfg.Client.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusAccepted, http.StatusOK:
		return nil
	}
	m := make(map[string]interface{})
	if e := json.Unmarshal(body, &m); e == nil {
		if s, ok := m["error"].(string); ok {
			return errors.New(s)
		}
	}
	return errors.New(string(body))
}

func DeleteProxied(clientCfg *ClientCfg, url string, jwt string) (body []byte, httpStatus int, err error) {
	url, err = clientCfg.FixHttpScheme(url)
	if err != nil {
		return
	}
	req, err := http.NewRequest("DELETE", url, nil)
	maybeAddAuth(req, jwt)
	if err != nil {
		return
	}
	resp, err := clientCfg.Client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	httpStatus = resp.StatusCode
	return
}

func GetBufferStream(clientCfg *ClientCfg, url string, values url.Values, allocatedBytes []byte, eachBuffer func([]byte)) error {
	url, err := clientCfg.FixHttpScheme(url)
	if err != nil {
		return err
	}
	r, err := clientCfg.Client.PostForm(url, values)
	if err != nil {
		return err
	}
	defer CloseResponse(r)
	if r.StatusCode != 200 {
		return fmt.Errorf("%s: %s", url, r.Status)
	}
	for {
		n, err := r.Body.Read(allocatedBytes)
		if n > 0 {
			eachBuffer(allocatedBytes[:n])
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func GetUrlStream(clientCfg *ClientCfg, url string, values url.Values, readFn func(io.Reader) error) error {
	url, err := clientCfg.FixHttpScheme(url)
	if err != nil {
		return err
	}
	r, err := clientCfg.Client.PostForm(url, values)
	if err != nil {
		return err
	}
	defer CloseResponse(r)
	if r.StatusCode != 200 {
		return fmt.Errorf("%s: %s", url, r.Status)
	}
	return readFn(r.Body)
}

func DownloadFile(clientCfg *ClientCfg, fileUrl string, jwt string) (filename string, header http.Header, resp *http.Response, e error) {
	fileUrl, err := clientCfg.FixHttpScheme(fileUrl)
	if err != nil {
		return "", nil, nil, err
	}
	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return "", nil, nil, err
	}

	maybeAddAuth(req, jwt)

	response, err := clientCfg.Client.Do(req)
	if err != nil {
		return "", nil, nil, err
	}
	header = response.Header
	contentDisposition := response.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		idx := strings.Index(contentDisposition[0], "filename=")
		if idx != -1 {
			filename = contentDisposition[0][idx+len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}
	resp = response
	return
}

func Do(clientCfg *ClientCfg, req *http.Request) (resp *http.Response, err error) {
	req.URL.Scheme = clientCfg.GetHttpScheme()
	return clientCfg.Client.Do(req)
}

func NormalizeUrl(clientCfg *ClientCfg, url string) (string, error) {
	return clientCfg.FixHttpScheme(url)
}

func ReadUrl(clientCfg *ClientCfg, fileUrl string, cipherKey []byte, isContentCompressed bool, isFullChunk bool, offset int64, size int, buf []byte) (int64, error) {
	fileUrl, err := clientCfg.FixHttpScheme(fileUrl)
	if err != nil {
		return 0, err
	}

	if cipherKey != nil {
		var n int
		_, err := readEncryptedUrl(clientCfg, fileUrl, "", cipherKey, isContentCompressed, isFullChunk, offset, size, func(data []byte) {
			n = copy(buf, data)
		})
		return int64(n), err
	}

	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return 0, err
	}
	if !isFullChunk {
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+int64(size)-1))
	} else {
		req.Header.Set("Accept-Encoding", "gzip")
	}

	r, err := clientCfg.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer CloseResponse(r)

	if r.StatusCode >= 400 {
		return 0, fmt.Errorf("%s: %s", fileUrl, r.Status)
	}

	var reader io.ReadCloser
	contentEncoding := r.Header.Get("Content-Encoding")
	switch contentEncoding {
	case "gzip":
		reader, err = gzip.NewReader(r.Body)
		if err != nil {
			return 0, err
		}
		defer reader.Close()
	default:
		reader = r.Body
	}

	var (
		i, m int
		n    int64
	)

	// refers to https://github.com/golang/go/blob/master/src/bytes/buffer.go#L199
	// commit id c170b14c2c1cfb2fd853a37add92a82fd6eb4318
	for {
		m, err = reader.Read(buf[i:])
		i += m
		n += int64(m)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		if n == int64(len(buf)) {
			break
		}
	}
	// drains the response body to avoid memory leak
	data, _ := io.ReadAll(reader)
	if len(data) != 0 {
		glog.V(1).Infof("%s reader has remaining %d bytes", contentEncoding, len(data))
	}
	return n, err
}

func ReadUrlAsStream(clientCfg *ClientCfg, fileUrl string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (retryable bool, err error) {
	return ReadUrlAsStreamAuthenticated(clientCfg, fileUrl, "", cipherKey, isContentGzipped, isFullChunk, offset, size, fn)
}

func ReadUrlAsStreamAuthenticated(clientCfg *ClientCfg, fileUrl, jwt string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (retryable bool, err error) {
	fileUrl, err = clientCfg.FixHttpScheme(fileUrl)
	if err != nil {
		return false, err
	}

	if cipherKey != nil {
		return readEncryptedUrl(clientCfg, fileUrl, jwt, cipherKey, isContentGzipped, isFullChunk, offset, size, fn)
	}

	req, err := http.NewRequest("GET", fileUrl, nil)
	maybeAddAuth(req, jwt)
	if err != nil {
		return false, err
	}

	if isFullChunk {
		req.Header.Add("Accept-Encoding", "gzip")
	} else {
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+int64(size)-1))
	}

	r, err := clientCfg.Client.Do(req)
	if err != nil {
		return true, err
	}
	defer CloseResponse(r)
	if r.StatusCode >= 400 {
		retryable = r.StatusCode == http.StatusNotFound || r.StatusCode >= 499
		return retryable, fmt.Errorf("%s: %s", fileUrl, r.Status)
	}

	var reader io.ReadCloser
	contentEncoding := r.Header.Get("Content-Encoding")
	switch contentEncoding {
	case "gzip":
		reader, err = gzip.NewReader(r.Body)
		defer reader.Close()
	default:
		reader = r.Body
	}

	var (
		m int
	)
	buf := mem.Allocate(64 * 1024)
	defer mem.Free(buf)

	for {
		m, err = reader.Read(buf)
		if m > 0 {
			fn(buf[:m])
		}
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return true, err
		}
	}

}

func readEncryptedUrl(clientCfg *ClientCfg, fileUrl, jwt string, cipherKey []byte, isContentCompressed bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (bool, error) {
	fileUrl, err := clientCfg.FixHttpScheme(fileUrl)
	if err != nil {
		return false, err
	}

	encryptedData, retryable, err := GetAuthenticated(clientCfg, fileUrl, jwt)
	if err != nil {
		return retryable, fmt.Errorf("fetch %s: %v", fileUrl, err)
	}
	decryptedData, err := util.Decrypt(encryptedData, util.CipherKey(cipherKey))
	if err != nil {
		return false, fmt.Errorf("decrypt %s: %v", fileUrl, err)
	}
	if isContentCompressed {
		decryptedData, err = util.DecompressData(decryptedData)
		if err != nil {
			glog.V(0).Infof("unzip decrypt %s: %v", fileUrl, err)
		}
	}
	if len(decryptedData) < int(offset)+size {
		return false, fmt.Errorf("read decrypted %s size %d [%d, %d)", fileUrl, len(decryptedData), offset, int(offset)+size)
	}
	if isFullChunk {
		fn(decryptedData)
	} else {
		fn(decryptedData[int(offset) : int(offset)+size])
	}
	return false, nil
}

func ReadUrlAsReaderCloser(clientCfg *ClientCfg, fileUrl string, jwt string, rangeHeader string) (*http.Response, io.ReadCloser, error) {
	fileUrl, err := clientCfg.FixHttpScheme(fileUrl)
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return nil, nil, err
	}
	if rangeHeader != "" {
		req.Header.Add("Range", rangeHeader)
	} else {
		req.Header.Add("Accept-Encoding", "gzip")
	}

	maybeAddAuth(req, jwt)

	r, err := clientCfg.Client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if r.StatusCode >= 400 {
		CloseResponse(r)
		return nil, nil, fmt.Errorf("%s: %s", fileUrl, r.Status)
	}

	var reader io.ReadCloser
	contentEncoding := r.Header.Get("Content-Encoding")
	switch contentEncoding {
	case "gzip":
		reader, err = gzip.NewReader(r.Body)
		if err != nil {
			return nil, nil, err
		}
	default:
		reader = r.Body
	}

	return r, reader, nil
}

func CloseResponse(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	reader := &CountingReader{reader: resp.Body}
	io.Copy(io.Discard, reader)
	resp.Body.Close()
	if reader.BytesRead > 0 {
		glog.V(1).Infof("response leftover %d bytes", reader.BytesRead)
	}
}

func CloseRequest(req *http.Request) {
	reader := &CountingReader{reader: req.Body}
	io.Copy(io.Discard, reader)
	req.Body.Close()
	if reader.BytesRead > 0 {
		glog.V(1).Infof("request leftover %d bytes", reader.BytesRead)
	}
}

type CountingReader struct {
	reader    io.Reader
	BytesRead int
}

func (r *CountingReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.BytesRead += n
	return n, err
}

func RetriedFetchChunkData(clientCfg *ClientCfg, buffer []byte, urlStrings []string, cipherKey []byte, isGzipped bool, isFullChunk bool, offset int64) (n int, err error) {

	var shouldRetry bool

	for waitTime := time.Second; waitTime < util.RetryWaitTime; waitTime += waitTime / 2 {
		for _, urlString := range urlStrings {
			n = 0
			if strings.Contains(urlString, "%") {
				urlString = url.PathEscape(urlString)
			}
			shouldRetry, err = ReadUrlAsStream(clientCfg, urlString+"?readDeleted=true", cipherKey, isGzipped, isFullChunk, offset, len(buffer), func(data []byte) {
				if n < len(buffer) {
					x := copy(buffer[n:], data)
					n += x
				}
			})
			if !shouldRetry {
				break
			}
			if err != nil {
				glog.V(0).Infof("read %s failed, err: %v", urlString, err)
			} else {
				break
			}
		}
		if err != nil && shouldRetry {
			glog.V(0).Infof("retry reading in %v", waitTime)
			time.Sleep(waitTime)
		} else {
			break
		}
	}

	return n, err

}
