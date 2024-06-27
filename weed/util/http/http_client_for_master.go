package http

import (
	"io"
	"net/http"
	"net/url"
)


func NewMasterHttpClient(opt ...HttpClientOpt)(*MasterHttpClient, error) {
	httpClient, err := NewHttpClient(Master, opt...)
	if err != nil {
		return nil, err
	}
	return &MasterHttpClient{*httpClient}, nil
}

type MasterHttpClient struct {
	HttpClient
}

func (httpClient *MasterHttpClient)Post(url string, values url.Values) ([]byte, error) {
	return Post(&httpClient.HttpClient, url, values)
}

func (httpClient *MasterHttpClient)Get(url string) ([]byte, bool, error) {
	return Get(&httpClient.HttpClient, url)
}

func (httpClient *MasterHttpClient)GetAuthenticated(url, jwt string) ([]byte, bool, error) {
	return GetAuthenticated(&httpClient.HttpClient, url, jwt)
}

func (httpClient *MasterHttpClient)Head(url string) (http.Header, error) {
	return Head(&httpClient.HttpClient, url)
}

func (httpClient *MasterHttpClient)Delete(url string, jwt string) error {
	return Delete(&httpClient.HttpClient, url, jwt)
}

func (httpClient *MasterHttpClient)DeleteProxied(url string, jwt string) (body []byte, httpStatus int, err error) {
	return DeleteProxied(&httpClient.HttpClient, url, jwt)
}

func (httpClient *MasterHttpClient)GetBufferStream(url string, values url.Values, allocatedBytes []byte, eachBuffer func([]byte)) error {
	return GetBufferStream(&httpClient.HttpClient, url, values, allocatedBytes, eachBuffer)
}

func (httpClient *MasterHttpClient)GetUrlStream(url string, values url.Values, readFn func(io.Reader) error) error {
	return GetUrlStream(&httpClient.HttpClient, url, values, readFn)
}

func (httpClient *MasterHttpClient)DownloadFile(fileUrl string, jwt string) (filename string, header http.Header, resp *http.Response, e error) {
	return DownloadFile(&httpClient.HttpClient, fileUrl, jwt)
}

func (httpClient *MasterHttpClient)Do(req *http.Request) (resp *http.Response, err error) {
	return Do(&httpClient.HttpClient, req)
}

func (httpClient *MasterHttpClient)NormalizeUrl(url string) (string, error) {
	return NormalizeUrl(&httpClient.HttpClient, url)
}

func (httpClient *MasterHttpClient)ReadUrl(fileUrl string, cipherKey []byte, isContentCompressed bool, isFullChunk bool, offset int64, size int, buf []byte) (int64, error) {
	return ReadUrl(&httpClient.HttpClient, fileUrl, cipherKey, isContentCompressed, isFullChunk, offset, size, buf)
}

func (httpClient *MasterHttpClient)ReadUrlAsStream(fileUrl string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (retryable bool, err error) {
	return ReadUrlAsStream(&httpClient.HttpClient, fileUrl, cipherKey, isContentGzipped, isFullChunk, offset, size, fn)
}

func (httpClient *MasterHttpClient)ReadUrlAsStreamAuthenticated(fileUrl, jwt string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (retryable bool, err error) {
	return ReadUrlAsStreamAuthenticated(&httpClient.HttpClient, fileUrl, jwt, cipherKey, isContentGzipped, isFullChunk, offset, size, fn)
}

func (httpClient *MasterHttpClient)ReadUrlAsReaderCloser(fileUrl string, jwt string, rangeHeader string) (*http.Response, io.ReadCloser, error) {
	return ReadUrlAsReaderCloser(&httpClient.HttpClient, fileUrl, jwt, rangeHeader)
}

func (httpClient *MasterHttpClient)RetriedFetchChunkData(buffer []byte, urlStrings []string, cipherKey []byte, isGzipped bool, isFullChunk bool, offset int64) (n int, err error) {
	return RetriedFetchChunkData(&httpClient.HttpClient, buffer, urlStrings, cipherKey, isGzipped, isFullChunk, offset)
}
