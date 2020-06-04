package util

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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
		MaxIdleConnsPerHost: 1024,
	}
	client = &http.Client{
		Transport: Transport,
	}
}

func PostBytes(url string, body []byte) ([]byte, error) {
	r, err := client.Post(url, "", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("Post to %s: %v", url, err)
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("Read response body: %v", err)
	}
	if r.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	return b, nil
}

func Post(url string, values url.Values) ([]byte, error) {
	r, err := client.PostForm(url, values)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
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
func Get(url string) ([]byte, error) {
	r, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if r.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	if err != nil {
		return nil, err
	}
	return b, nil
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
	body, err := ioutil.ReadAll(resp.Body)
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

func DownloadFile(fileUrl string) (filename string, header http.Header, rc io.ReadCloser, e error) {
	response, err := client.Get(fileUrl)
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
	rc = response.Body
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

func ReadUrl(fileUrl string, cipherKey []byte, isGzipped bool, isFullChunk bool, offset int64, size int, buf []byte) (int64, error) {

	if cipherKey != nil {
		var n int
		err := readEncryptedUrl(fileUrl, cipherKey, isGzipped, isFullChunk, offset, size, func(data []byte) {
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
	data, _ := ioutil.ReadAll(reader)
	if len(data) != 0 {
		glog.V(1).Infof("%s reader has remaining %d bytes", contentEncoding, len(data))
	}
	return n, err
}

func ReadUrlAsStream(fileUrl string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) error {

	if cipherKey != nil {
		return readEncryptedUrl(fileUrl, cipherKey, isContentGzipped, isFullChunk, offset, size, fn)
	}

	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return err
	}

	if !isFullChunk {
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+int64(size)-1))
	}

	r, err := client.Do(req)
	if err != nil {
		return err
	}
	defer CloseResponse(r)
	if r.StatusCode >= 400 {
		return fmt.Errorf("%s: %s", fileUrl, r.Status)
	}

	var (
		m int
	)
	buf := make([]byte, 64*1024)

	for {
		m, err = r.Body.Read(buf)
		fn(buf[:m])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}

}

func readEncryptedUrl(fileUrl string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) error {
	encryptedData, err := Get(fileUrl)
	if err != nil {
		return fmt.Errorf("fetch %s: %v", fileUrl, err)
	}
	decryptedData, err := Decrypt(encryptedData, CipherKey(cipherKey))
	if err != nil {
		return fmt.Errorf("decrypt %s: %v", fileUrl, err)
	}
	if isContentGzipped {
		decryptedData, err = UnGzipData(decryptedData)
		if err != nil {
			return fmt.Errorf("unzip decrypt %s: %v", fileUrl, err)
		}
	}
	if len(decryptedData) < int(offset)+size {
		return fmt.Errorf("read decrypted %s size %d [%d, %d)", fileUrl, len(decryptedData), offset, int(offset)+size)
	}
	if isFullChunk {
		fn(decryptedData)
	} else {
		fn(decryptedData[int(offset) : int(offset)+size])
	}
	return nil
}

func ReadUrlAsReaderCloser(fileUrl string, rangeHeader string) (io.ReadCloser, error) {

	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return nil, err
	}
	if rangeHeader != "" {
		req.Header.Add("Range", rangeHeader)
	}

	r, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if r.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: %s", fileUrl, r.Status)
	}

	return r.Body, nil
}

func CloseResponse(resp *http.Response) {
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
}
