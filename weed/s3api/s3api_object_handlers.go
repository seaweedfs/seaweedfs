package s3api

import (
	"encoding/json"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

var (
	client *http.Client
)

func init() {
	client = &http.Client{Transport: &http.Transport{
		MaxIdleConnsPerHost: 1024,
	}}
}

type UploadResult struct {
	Name  string `json:"name,omitempty"`
	Size  uint32 `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
}

func (s3a *S3ApiServer) PutObjectHandler(w http.ResponseWriter, r *http.Request) {

	// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

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

	uploadUrl := fmt.Sprintf("http://%s%s/%s/%s?collection=%s",
		s3a.option.Filer, s3a.option.BucketsPath, bucket, object, bucket)
	proxyReq, err := http.NewRequest("PUT", uploadUrl, dataReader)

	if err != nil {
		glog.Errorf("NewRequest %s: %v", uploadUrl, err)
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	proxyReq.Header.Set("Host", s3a.option.Filer)
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)

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

	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		glog.Errorf("upload to filer response read: %v", ra_err)
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}
	var ret UploadResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.Errorf("failing to read upload to %s : %v", uploadUrl, string(resp_body))
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}
	if ret.Error != "" {
		glog.Errorf("upload to filer error: %v", ret.Error)
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	writeSuccessResponseEmpty(w)
}

func (s3a *S3ApiServer) GetObjectHandler(w http.ResponseWriter, r *http.Request) {

	if strings.HasSuffix(r.URL.Path, "/") {
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}

	destUrl := fmt.Sprintf("http://%s%s%s",
		s3a.option.Filer, s3a.option.BucketsPath, r.RequestURI)

	s3a.proxyToFiler(w, r, destUrl, passThroghResponse)

}

func (s3a *S3ApiServer) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {

	destUrl := fmt.Sprintf("http://%s%s%s",
		s3a.option.Filer, s3a.option.BucketsPath, r.RequestURI)

	s3a.proxyToFiler(w, r, destUrl, passThroghResponse)

}

func (s3a *S3ApiServer) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {

	destUrl := fmt.Sprintf("http://%s%s%s",
		s3a.option.Filer, s3a.option.BucketsPath, r.RequestURI)

	s3a.proxyToFiler(w, r, destUrl, func(proxyResonse *http.Response, w http.ResponseWriter) {
		for k, v := range proxyResonse.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(http.StatusNoContent)
	})

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
func passThroghResponse(proxyResonse *http.Response, w http.ResponseWriter) {
	for k, v := range proxyResonse.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(proxyResonse.StatusCode)
	io.Copy(w, proxyResonse.Body)
}
