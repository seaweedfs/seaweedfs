package s3api

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/gorilla/mux"
)

var (
	client *http.Client
)

func init() {
	client = &http.Client{Transport: &http.Transport{
		MaxIdleConnsPerHost: 1024,
	}}
}

func (s3a *S3ApiServer) PutObjectHandler(w http.ResponseWriter, r *http.Request) {

	// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := getObject(vars)

	_, err := validateContentMd5(r.Header)
	if err != nil {
		writeErrorResponse(w, ErrInvalidDigest, r.URL)
		return
	}

	rAuthType := getRequestAuthType(r)
	dataReader := r.Body
	if rAuthType == authTypeStreamingSigned {
		dataReader = newSignV4ChunkedReader(r)
	}

	uploadUrl := fmt.Sprintf("http://%s%s/%s%s?collection=%s",
		s3a.option.Filer, s3a.option.BucketsPath, bucket, object, bucket)

	etag, errCode := s3a.putToFiler(r, uploadUrl, dataReader)

	if errCode != ErrNone {
		writeErrorResponse(w, errCode, r.URL)
		return
	}

	setEtag(w, etag)

	writeSuccessResponseEmpty(w)
}

func (s3a *S3ApiServer) GetObjectHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := getObject(vars)

	if strings.HasSuffix(r.URL.Path, "/") {
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}

	destUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer, s3a.option.BucketsPath, bucket, object)

	s3a.proxyToFiler(w, r, destUrl, passThroughResponse)

}

func (s3a *S3ApiServer) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := getObject(vars)

	destUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer, s3a.option.BucketsPath, bucket, object)

	s3a.proxyToFiler(w, r, destUrl, passThroughResponse)

}

func (s3a *S3ApiServer) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := getObject(vars)

	destUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer, s3a.option.BucketsPath, bucket, object)

	s3a.proxyToFiler(w, r, destUrl, func(proxyResonse *http.Response, w http.ResponseWriter) {
		for k, v := range proxyResonse.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(http.StatusNoContent)
	})

}

// DeleteMultipleObjectsHandler - Delete multiple objects
func (s3a *S3ApiServer) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	// TODO
	writeErrorResponse(w, ErrNotImplemented, r.URL)
}

func (s3a *S3ApiServer) proxyToFiler(w http.ResponseWriter, r *http.Request, destUrl string, responseFn func(proxyResonse *http.Response, w http.ResponseWriter)) {

	glog.V(2).Infof("s3 proxying %s to %s", r.Method, destUrl)

	proxyReq, err := http.NewRequest(r.Method, destUrl, r.Body)

	if err != nil {
		glog.Errorf("NewRequest %s: %v", destUrl, err)
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	proxyReq.Header.Set("Host", s3a.option.Filer)
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	proxyReq.Header.Set("Etag-MD5", "True")

	for header, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(header, value)
		}
	}

	resp, postErr := client.Do(proxyReq)

	if postErr != nil {
		glog.Errorf("post to filer: %v", postErr)
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}
	defer resp.Body.Close()

	responseFn(resp, w)
}
func passThroughResponse(proxyResonse *http.Response, w http.ResponseWriter) {
	for k, v := range proxyResonse.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(proxyResonse.StatusCode)
	io.Copy(w, proxyResonse.Body)
}

func (s3a *S3ApiServer) putToFiler(r *http.Request, uploadUrl string, dataReader io.ReadCloser) (etag string, code ErrorCode) {

	hash := md5.New()
	var body io.Reader = io.TeeReader(dataReader, hash)

	proxyReq, err := http.NewRequest("PUT", uploadUrl, body)

	if err != nil {
		glog.Errorf("NewRequest %s: %v", uploadUrl, err)
		return "", ErrInternalError
	}

	proxyReq.Header.Set("Host", s3a.option.Filer)
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)

	for header, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(header, value)
		}
	}

	resp, postErr := client.Do(proxyReq)

	dataReader.Close()

	if postErr != nil {
		glog.Errorf("post to filer: %v", postErr)
		return "", ErrInternalError
	}
	defer resp.Body.Close()

	etag = fmt.Sprintf("%x", hash.Sum(nil))

	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		glog.Errorf("upload to filer response read: %v", ra_err)
		return etag, ErrInternalError
	}
	var ret weed_server.FilerPostResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.Errorf("failing to read upload to %s : %v", uploadUrl, string(resp_body))
		return "", ErrInternalError
	}
	if ret.Error != "" {
		glog.Errorf("upload to filer error: %v", ret.Error)
		return "", ErrInternalError
	}

	return etag, ErrNone
}

func setEtag(w http.ResponseWriter, etag string) {
	if etag != "" {
		if strings.HasPrefix(etag, "\"") {
			w.Header().Set("ETag", etag)
		} else {
			w.Header().Set("ETag", "\""+etag+"\"")
		}
	}
}

func getObject(vars map[string]string) string {
	object := vars["object"]
	if !strings.HasPrefix(object, "/") {
		object = "/" + object
	}
	return object
}
