package s3api

import (
	"crypto/md5"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/gorilla/mux"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/util"
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
	var s3ErrCode ErrorCode
	if rAuthType == authTypeStreamingSigned {
		dataReader, s3ErrCode = s3a.iam.newSignV4ChunkedReader(r)
	}
	if s3ErrCode != ErrNone {
		writeErrorResponse(w, s3ErrCode, r.URL)
		return
	}
	defer dataReader.Close()

	uploadUrl := fmt.Sprintf("http://%s%s/%s%s", s3a.option.Filer, s3a.option.BucketsPath, bucket, object)

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

/// ObjectIdentifier carries key name for the object to delete.
type ObjectIdentifier struct {
	ObjectName string `xml:"Key"`
}

// DeleteObjectsRequest - xml carrying the object key names which needs to be deleted.
type DeleteObjectsRequest struct {
	// Element to enable quiet mode for the request
	Quiet bool
	// List of objects to be deleted
	Objects []ObjectIdentifier `xml:"Object"`
}

// DeleteError structure.
type DeleteError struct {
	Code    string
	Message string
	Key     string
}

// DeleteObjectsResponse container for multiple object deletes.
type DeleteObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult" json:"-"`

	// Collection of all deleted objects
	DeletedObjects []ObjectIdentifier `xml:"Deleted,omitempty"`

	// Collection of errors deleting certain objects.
	Errors []DeleteError `xml:"Error,omitempty"`
}

// DeleteMultipleObjectsHandler - Delete multiple objects
func (s3a *S3ApiServer) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	deleteXMLBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	deleteObjects := &DeleteObjectsRequest{}
	if err := xml.Unmarshal(deleteXMLBytes, deleteObjects); err != nil {
		writeErrorResponse(w, ErrMalformedXML, r.URL)
		return
	}

	var deletedObjects []ObjectIdentifier
	var deleteErrors []DeleteError

	s3a.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		for _, object := range deleteObjects.Objects {
			lastSeparator := strings.LastIndex(object.ObjectName, "/")
			parentDirectoryPath, entryName, isDeleteData, isRecursive := "/", object.ObjectName, true, true
			if lastSeparator > 0 && lastSeparator+1 < len(object.ObjectName) {
				entryName = object.ObjectName[lastSeparator+1:]
				parentDirectoryPath = "/" + object.ObjectName[:lastSeparator]
			}
			parentDirectoryPath = fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, parentDirectoryPath)

			err := doDeleteEntry(client, parentDirectoryPath, entryName, isDeleteData, isRecursive)
			if err == nil {
				deletedObjects = append(deletedObjects, object)
			} else {
				deleteErrors = append(deleteErrors, DeleteError{
					Code:    "",
					Message: err.Error(),
					Key:     object.ObjectName,
				})
			}
		}
		return nil
	})

	deleteResp := DeleteObjectsResponse{}
	if !deleteObjects.Quiet {
		deleteResp.DeletedObjects = deletedObjects
	}
	deleteResp.Errors = deleteErrors

	writeSuccessResponseXML(w, encodeResponse(deleteResp))

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
	defer util.CloseResponse(resp)

	responseFn(resp, w)

}
func passThroughResponse(proxyResonse *http.Response, w http.ResponseWriter) {
	for k, v := range proxyResonse.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(proxyResonse.StatusCode)
	io.Copy(w, proxyResonse.Body)
}

func (s3a *S3ApiServer) putToFiler(r *http.Request, uploadUrl string, dataReader io.Reader) (etag string, code ErrorCode) {

	hash := md5.New()
	var body = io.TeeReader(dataReader, hash)

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
