package util

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/chrislusf/seaweedfs/go/security"
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

func Post(url string, values url.Values) ([]byte, error) {
	r, err := client.PostForm(url, values)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func Get(url string) ([]byte, error) {
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
	if _, err := ioutil.ReadAll(resp.Body); err != nil {
		return err
	}
	return nil
}

func DownloadUrl(fileUrl string) (filename string, content []byte, e error) {
	response, err := client.Get(fileUrl)
	if err != nil {
		return "", nil, err
	}
	defer response.Body.Close()
	contentDisposition := response.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		if strings.HasPrefix(contentDisposition[0], "filename=") {
			filename = contentDisposition[0][len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}
	content, e = ioutil.ReadAll(response.Body)
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
