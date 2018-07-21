package s3api

import (
	"net/http"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"encoding/json"
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

	uploadUrl := fmt.Sprintf("http://%s%s/%s/%s?collection=%s",
		s3a.option.Filer, s3a.option.BucketsPath, bucket, object, bucket)
	proxyReq, err := http.NewRequest("PUT", uploadUrl, r.Body)

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
