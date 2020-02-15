package util

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/golang/protobuf/proto"

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
	if e := json.Unmarshal(body, m); e == nil {
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

func ReadUrl(fileUrl string, offset int64, size int, buf []byte, isReadRange bool) (int64, error) {

	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return 0, err
	}
	if isReadRange {
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

func ReadUrlAsStream(fileUrl string, offset int64, size int, fn func(data []byte)) (int64, error) {

	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+int64(size)-1))

	r, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer CloseResponse(r)
	if r.StatusCode >= 400 {
		return 0, fmt.Errorf("%s: %s", fileUrl, r.Status)
	}

	var (
		m int
		n int64
	)
	buf := make([]byte, 64*1024)

	for {
		m, err = r.Body.Read(buf)
		fn(buf[:m])
		n += int64(m)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
	}

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

func WriteMessage(conn net.Conn, message proto.Message) error {
	data, err := proto.Marshal(message)
	if err != nil {
		glog.Fatalf("marshal: %v", err)
	}
	messageSizeBytes := make([]byte, 4)
	Uint32toBytes(messageSizeBytes, uint32(len(data)))
	_, err = conn.Write(messageSizeBytes)
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}
func WriteMessageEOF(conn net.Conn) error {
	messageSizeBytes := make([]byte, 4)
	Uint32toBytes(messageSizeBytes, math.MaxUint32)
	_, err := conn.Write(messageSizeBytes)
	return err
}
func ReadMessage(conn net.Conn, message proto.Message) error {
	messageSizeBuffer := make([]byte, 4)
	n, err := conn.Read(messageSizeBuffer)
	if err != nil {
		if err == io.EOF {
			// println("unexpected eof")
			return err
		}
		return fmt.Errorf("read message size byte length: %d %v", n, err)
	}
	if n != 4 {
		return fmt.Errorf("unexpected message size byte length: %d", n)
	}
	messageSize := BytesToUint32(messageSizeBuffer)
	if messageSize == math.MaxUint32 {
		// println("marked eof")
		return io.EOF
	}

	messageBytes := make([]byte, messageSize)

	readMessageLength, err := conn.Read(messageBytes)
	if readMessageLength != int(messageSize) {
		return fmt.Errorf("message size:%d, expected:%d", readMessageLength, messageSize)
	}

	if err := proto.Unmarshal(messageBytes, message); err != nil {
		return err
	}
	return nil
}
