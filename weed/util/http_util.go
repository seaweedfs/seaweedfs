package util

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util/mem"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

var (
	client    *http.Client
	Transport *http.Transport
)

func init() {
	Transport = &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
	}
	client = &http.Client{
		Transport: Transport,
	}
}

func Post(url string, values url.Values) ([]byte, error) {
	r, err := client.PostForm(url, values)
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

//	github.com/chrislusf/seaweedfs/unmaintained/repeated_vacuum/repeated_vacuum.go
//	may need increasing http.Client.Timeout
func Get(url string) ([]byte, bool, error) {

	request, err := http.NewRequest("GET", url, nil)
	request.Header.Add("Accept-Encoding", "gzip")

	response, err := client.Do(request)
	if err != nil {
		return nil, true, err
	}
	defer response.Body.Close()

	var reader io.ReadCloser
	switch response.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(response.Body)
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

func Head(url string) (http.Header, error) {
	r, err := client.Head(url)
	if err != nil {
		return nil, err
	}
	defer CloseResponse(r)
	if r.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	return r.Header, nil
}

func Delete(url string, jwt string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if jwt != "" {
		req.Header.Set("Authorization", "BEARER "+string(jwt))
	}
	if err != nil {
		return err
	}
	resp, e := client.Do(req)
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

func DeleteProxied(url string, jwt string) (body []byte, httpStatus int, err error) {
	req, err := http.NewRequest("DELETE", url, nil)
	if jwt != "" {
		req.Header.Set("Authorization", "BEARER "+string(jwt))
	}
	if err != nil {
		return
	}
	resp, err := client.Do(req)
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

func GetBufferStream(url string, values url.Values, allocatedBytes []byte, eachBuffer func([]byte)) error {
	r, err := client.PostForm(url, values)
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

func GetUrlStream(url string, values url.Values, readFn func(io.Reader) error) error {
	r, err := client.PostForm(url, values)
	if err != nil {
		return err
	}
	defer CloseResponse(r)
	if r.StatusCode != 200 {
		return fmt.Errorf("%s: %s", url, r.Status)
	}
	return readFn(r.Body)
}

func DownloadFile(fileUrl string, jwt string) (filename string, header http.Header, resp *http.Response, e error) {
	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return "", nil, nil, err
	}

	if len(jwt) > 0 {
		req.Header.Set("Authorization", "BEARER "+jwt)
	}

	response, err := client.Do(req)
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

func Do(req *http.Request) (resp *http.Response, err error) {
	return client.Do(req)
}

func NormalizeUrl(url string) string {
	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		return url
	}
	return "http://" + url
}

func ReadUrl(fileUrl string, cipherKey []byte, isContentCompressed bool, isFullChunk bool, offset int64, size int, buf []byte) (int64, error) {

	if cipherKey != nil {
		var n int
		_, err := readEncryptedUrl(fileUrl, cipherKey, isContentCompressed, isFullChunk, offset, size, func(data []byte) {
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

	r, err := client.Do(req)
	if err != nil {
		return 0, err
	}

	defer r.Body.Close()
	if r.StatusCode >= 400 {
		return 0, fmt.Errorf("%s: %s", fileUrl, r.Status)
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

func ReadUrlAsStream(fileUrl string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (retryable bool, err error) {

	if cipherKey != nil {
		return readEncryptedUrl(fileUrl, cipherKey, isContentGzipped, isFullChunk, offset, size, fn)
	}

	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return false, err
	}

	if isFullChunk {
		req.Header.Add("Accept-Encoding", "gzip")
	} else {
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+int64(size)-1))
	}

	r, err := client.Do(req)
	if err != nil {
		return true, err
	}
	defer CloseResponse(r)
	if r.StatusCode >= 400 {
		retryable = r.StatusCode >= 500
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
		fn(buf[:m])
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return true, err
		}
	}

}

func readEncryptedUrl(fileUrl string, cipherKey []byte, isContentCompressed bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (bool, error) {
	encryptedData, retryable, err := Get(fileUrl)
	if err != nil {
		return retryable, fmt.Errorf("fetch %s: %v", fileUrl, err)
	}
	decryptedData, err := Decrypt(encryptedData, CipherKey(cipherKey))
	if err != nil {
		return false, fmt.Errorf("decrypt %s: %v", fileUrl, err)
	}
	if isContentCompressed {
		decryptedData, err = DecompressData(decryptedData)
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

func ReadUrlAsReaderCloser(fileUrl string, jwt string, rangeHeader string) (io.ReadCloser, error) {

	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return nil, err
	}
	if rangeHeader != "" {
		req.Header.Add("Range", rangeHeader)
	} else {
		req.Header.Add("Accept-Encoding", "gzip")
	}

	if len(jwt) > 0 {
		req.Header.Set("Authorization", "BEARER "+jwt)
	}

	r, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if r.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: %s", fileUrl, r.Status)
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

	return reader, nil
}

func CloseResponse(resp *http.Response) {
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
