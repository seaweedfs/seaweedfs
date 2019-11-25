package s3api

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/s3select"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/gorilla/mux"
	xhttp "github.com/minio/minio/cmd/http"
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
	var method string
	method = r.Method

	if r.URL.RawQuery == "select&select-type=2" {
		method = "GET"
	}
	glog.V(2).Infof("s3 proxying %s to %s", method, destUrl)

	proxyReq, err := http.NewRequest(method, destUrl, r.Body)

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

func (s3a *S3ApiServer) SelectObjectContent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := getObject(vars)

	s3Select, err := s3select.NewS3Select(r.Body)
	if err != nil {
		if serr, ok := err.(s3select.SelectError); ok {
			encodedErrorResponse := encodeResponse(cmd.APIErrorResponse{
				Code:       serr.ErrorCode(),
				Message:    serr.ErrorMessage(),
				BucketName: bucket,
				Key:        object,
				Resource:   r.URL.Path,
				RequestID:  w.Header().Get(xhttp.AmzRequestID),
				HostID:     "",
			})
			writeResponse(w, serr.HTTPStatusCode(), encodedErrorResponse, "application/xml")
		} else {
			writeErrorResponse(w, ErrInternalError, r.URL)
		}
		return
	}

	if strings.HasSuffix(r.URL.Path, "/") {
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}

	destUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer, s3a.option.BucketsPath, bucket, object)
	passThroughResponseSelectObjectContent := func(proxyResonse *http.Response,
		w http.ResponseWriter) {
		getObject := func(offset, length int64) (io.ReadCloser, error) {
			return proxyResonse.Body, nil
		}
		if err = s3Select.Open(getObject); err != nil {
			if serr, ok := err.(s3select.SelectError); ok {
				encodedErrorResponse := encodeResponse(cmd.APIErrorResponse{
					Code:       serr.ErrorCode(),
					Message:    serr.ErrorMessage(),
					BucketName: bucket,
					Key:        object,
					Resource:   r.URL.Path,
					RequestID:  w.Header().Get(xhttp.AmzRequestID),
					HostID:     "",
				})
				writeResponse(w, serr.HTTPStatusCode(), encodedErrorResponse, mimeXML)
			} else {
				writeResponse(w, http.StatusInternalServerError, encodeResponse("not s3select.SelectError"), mimeXML)
			}
			return
		}

		s3Select.Evaluate(w)
		s3Select.Close()
	}
	s3a.proxyToFiler(w, r, destUrl, passThroughResponseSelectObjectContent)
}
