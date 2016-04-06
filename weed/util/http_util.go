package util

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/golang/protobuf/proto"
	"github.com/pierrec/lz4"
	"strconv"
)

var (
	client    *http.Client
	Transport *http.Transport
)

func init() {
	Transport = &http.Transport{
		MaxIdleConnsPerHost: 1024,
	}
	client = &http.Client{Transport: Transport}
}

func MkUrl(host, path string, args url.Values) string {
	u := url.URL{
		Scheme: "http",
		Host:   host,
		Path:   path,
	}
	if args != nil {
		u.RawQuery = args.Encode()
	}
	return u.String()
}

func PostBytes(url string, body []byte) ([]byte, error) {
	r, err := client.Post(url, "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("Post to %s: %v", url, err)
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("Read response body: %v", err)
	}
	return b, nil
}

func PostPbMsg(url string, msg proto.Message, ret proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/protobuf")
	req.Header.Set("Accept", "application/protobuf")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Post to %s: %v", url, err)
	}
	defer resp.Body.Close()
	retBlob, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Read response body: %v", err)
	}

	if err := proto.Unmarshal(retBlob, ret); err != nil {
		glog.V(0).Infof("Failed to umarshal pb %s with response: %s", url, string(retBlob))
		return err
	}
	return nil
}

func PostEx(host, path string, values url.Values) (content []byte, statusCode int, e error) {
	url := MkUrl(host, path, nil)
	glog.V(4).Infoln("Post", url+"?"+values.Encode())
	r, err := client.PostForm(url, values)
	if err != nil {
		return nil, 0, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, r.StatusCode, err
	}
	return b, r.StatusCode, nil
}

func Post(host, path string, values url.Values) (content []byte, e error) {
	content, _, e = PostEx(host, path, values)
	return
}

type RApiError struct {
	E string
}

func (e *RApiError) Error() string {
	return e.E
}

func IsRemoteApiError(e error) bool {
	switch e.(type) {
	case *RApiError:
		return true
	}
	return false
}

func RemoteApiCall(host, path string, values url.Values) (result map[string]interface{}, e error) {
	jsonBlob, code, e := PostEx(host, path, values)
	if e != nil {
		return nil, e
	}
	result = make(map[string]interface{})
	if e := json.Unmarshal(jsonBlob, &result); e != nil {
		return nil, e
	}
	if err, ok := result["error"]; ok && err.(string) != "" {
		return nil, &RApiError{E: err.(string)}
	}
	if code != http.StatusOK && code != http.StatusAccepted {
		return nil, fmt.Errorf("RemoteApiCall %s/%s return %d", host, path, code)
	}
	return result, nil
}

func Get(host, path string, values url.Values) ([]byte, error) {
	url := MkUrl(host, path, values)
	r, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if r.StatusCode != 200 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func Delete(url string, jwt security.EncodedJwt) error {
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
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("%s: %s", url, r.Status)
	}
	bufferSize := len(allocatedBytes)
	for {
		n, err := r.Body.Read(allocatedBytes)
		if n == bufferSize {
			eachBuffer(allocatedBytes)
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
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("%s: %s", url, r.Status)
	}
	return readFn(r.Body)
}

func DownloadUrl(fileUrl string) (filename string, rc io.ReadCloser, e error) {
	response, err := client.Get(fileUrl)
	if err != nil {
		return "", nil, err
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return "", nil, fmt.Errorf("%s: %s", fileUrl, response.Status)
	}
	contentDisposition := response.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		if strings.HasPrefix(contentDisposition[0], "filename=") {
			filename = contentDisposition[0][len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}
	rc = response.Body
	return
}

func DownloadToFile(fileUrl, savePath string) (e error) {
	response, err := client.Get(fileUrl)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", fileUrl, response.Status)
	}
	var r io.Reader
	content_encoding := strings.ToLower(response.Header.Get("Content-Encoding"))
	size := response.ContentLength
	if n, e := strconv.ParseInt(response.Header.Get("X-Content-Length"), 10, 64); e == nil {
		size = n
	}
	switch content_encoding {
	case "lz4":
		r = lz4.NewReader(response.Body)
	default:
		r = response.Body
	}
	var f *os.File
	if f, e = os.OpenFile(savePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); e != nil {
		return
	}
	if size >= 0 {
		_, e = io.CopyN(f, r, size)
	} else {
		_, e = io.Copy(f, r)
	}

	f.Close()
	return
}

func HttpDo(req *http.Request) (resp *http.Response, err error) {
	return client.Do(req)
}

func NormalizeUrl(url string) string {
	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		return url
	}
	return "http://" + url
}
