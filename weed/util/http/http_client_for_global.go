package http

import (
	"io"
	"net/http"
	"net/url"
)


func NewGlobalHttpClient(opt ...HttpClientOpt)(*GlobalHttpClient, error) {
	httpClient, err := NewHttpClient(Global, opt...)
	if err != nil {
		return nil, err
	}
	return &GlobalHttpClient{*httpClient}, nil
}

type GlobalHttpClient struct {
	HttpClient
}

func (httpClient *GlobalHttpClient)Post(url string, values url.Values) ([]byte, error) {
	return Post(&httpClient.HttpClient, url, values)
}

func (httpClient *GlobalHttpClient)Get(url string) ([]byte, bool, error) {
	return Get(&httpClient.HttpClient, url)
}

func (httpClient *GlobalHttpClient)GetAuthenticated(url, jwt string) ([]byte, bool, error) {
	return GetAuthenticated(&httpClient.HttpClient, url, jwt)
}

func (httpClient *GlobalHttpClient)Head(url string) (http.Header, error) {
	return Head(&httpClient.HttpClient, url)
}

func (httpClient *GlobalHttpClient)Delete(url string, jwt string) error {
	return Delete(&httpClient.HttpClient, url, jwt)
}

func (httpClient *GlobalHttpClient)DeleteProxied(url string, jwt string) (body []byte, httpStatus int, err error) {
	return DeleteProxied(&httpClient.HttpClient, url, jwt)
}

func (httpClient *GlobalHttpClient)GetBufferStream(url string, values url.Values, allocatedBytes []byte, eachBuffer func([]byte)) error {
	return GetBufferStream(&httpClient.HttpClient, url, values, allocatedBytes, eachBuffer)
}

func (httpClient *GlobalHttpClient)GetUrlStream(url string, values url.Values, readFn func(io.Reader) error) error {
	return GetUrlStream(&httpClient.HttpClient, url, values, readFn)
}

func (httpClient *GlobalHttpClient)DownloadFile(fileUrl string, jwt string) (filename string, header http.Header, resp *http.Response, e error) {
	return DownloadFile(&httpClient.HttpClient, fileUrl, jwt)
}

func (httpClient *GlobalHttpClient)Do(req *http.Request) (resp *http.Response, err error) {
	return Do(&httpClient.HttpClient, req)
}

func (httpClient *GlobalHttpClient)NormalizeUrl(url string) (string, error) {
	return NormalizeUrl(&httpClient.HttpClient, url)
}

func (httpClient *GlobalHttpClient)ReadUrl(fileUrl string, cipherKey []byte, isContentCompressed bool, isFullChunk bool, offset int64, size int, buf []byte) (int64, error) {
	return ReadUrl(&httpClient.HttpClient, fileUrl, cipherKey, isContentCompressed, isFullChunk, offset, size, buf)
}

func (httpClient *GlobalHttpClient)ReadUrlAsStream(fileUrl string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (retryable bool, err error) {
	return ReadUrlAsStream(&httpClient.HttpClient, fileUrl, cipherKey, isContentGzipped, isFullChunk, offset, size, fn)
}

func (httpClient *GlobalHttpClient)ReadUrlAsStreamAuthenticated(fileUrl, jwt string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (retryable bool, err error) {
	return ReadUrlAsStreamAuthenticated(&httpClient.HttpClient, fileUrl, jwt, cipherKey, isContentGzipped, isFullChunk, offset, size, fn)
}

func (httpClient *GlobalHttpClient)ReadUrlAsReaderCloser(fileUrl string, jwt string, rangeHeader string) (*http.Response, io.ReadCloser, error) {
	return ReadUrlAsReaderCloser(&httpClient.HttpClient, fileUrl, jwt, rangeHeader)
}

func (httpClient *GlobalHttpClient)RetriedFetchChunkData(buffer []byte, urlStrings []string, cipherKey []byte, isGzipped bool, isFullChunk bool, offset int64) (n int, err error) {
	return RetriedFetchChunkData(&httpClient.HttpClient, buffer, urlStrings, cipherKey, isGzipped, isFullChunk, offset)
}
