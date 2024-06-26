package unknown

import (
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"io"
	"net/http"
	"net/url"
)

func Post(url string, values url.Values) ([]byte, error) {
	return util_http.Post(GetClientCfg(), url, values)
}

func Get(url string) ([]byte, bool, error) {
	return util_http.Get(GetClientCfg(), url)
}

func GetAuthenticated(url, jwt string) ([]byte, bool, error) {
	return util_http.GetAuthenticated(GetClientCfg(), url, jwt)
}

func Head(url string) (http.Header, error) {
	return util_http.Head(GetClientCfg(), url)
}

func Delete(url string, jwt string) error {
	return util_http.Delete(GetClientCfg(), url, jwt)
}

func DeleteProxied(url string, jwt string) (body []byte, httpStatus int, err error) {
	return util_http.DeleteProxied(GetClientCfg(), url, jwt)
}

func GetBufferStream(url string, values url.Values, allocatedBytes []byte, eachBuffer func([]byte)) error {
	return util_http.GetBufferStream(GetClientCfg(), url, values, allocatedBytes, eachBuffer)
}

func GetUrlStream(url string, values url.Values, readFn func(io.Reader) error) error {
	return util_http.GetUrlStream(GetClientCfg(), url, values, readFn)
}

func DownloadFile(fileUrl string, jwt string) (filename string, header http.Header, resp *http.Response, e error) {
	return util_http.DownloadFile(GetClientCfg(), fileUrl, jwt)
}

func Do(req *http.Request) (resp *http.Response, err error) {
	return util_http.Do(GetClientCfg(), req)
}

func NormalizeUrl(url string) (string, error) {
	return util_http.NormalizeUrl(GetClientCfg(), url)
}

func ReadUrl(fileUrl string, cipherKey []byte, isContentCompressed bool, isFullChunk bool, offset int64, size int, buf []byte) (int64, error) {
	return util_http.ReadUrl(GetClientCfg(), fileUrl, cipherKey, isContentCompressed, isFullChunk, offset, size, buf)
}

func ReadUrlAsStream(fileUrl string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (retryable bool, err error) {
	return util_http.ReadUrlAsStream(GetClientCfg(), fileUrl, cipherKey, isContentGzipped, isFullChunk, offset, size, fn)
}

func ReadUrlAsStreamAuthenticated(fileUrl, jwt string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (retryable bool, err error) {
	return util_http.ReadUrlAsStreamAuthenticated(GetClientCfg(), fileUrl, jwt, cipherKey, isContentGzipped, isFullChunk, offset, size, fn)
}

func ReadUrlAsReaderCloser(fileUrl string, jwt string, rangeHeader string) (*http.Response, io.ReadCloser, error) {
	return util_http.ReadUrlAsReaderCloser(GetClientCfg(), fileUrl, jwt, rangeHeader)
}

func RetriedFetchChunkData(buffer []byte, urlStrings []string, cipherKey []byte, isGzipped bool, isFullChunk bool, offset int64) (n int, err error) {
	return util_http.RetriedFetchChunkData(GetClientCfg(), buffer, urlStrings, cipherKey, isGzipped, isFullChunk, offset)
}
