package s3api

import (
	"crypto/md5"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/pquerna/cachecontrol/cacheobject"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"

	"github.com/gorilla/mux"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	weed_server "github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	client *http.Client
)

func init() {
	client = &http.Client{Transport: &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
	}}
}

func (s3a *S3ApiServer) PutObjectHandler(w http.ResponseWriter, r *http.Request) {

	// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html

	bucket, object := getBucketAndObject(r)

	_, err := validateContentMd5(r.Header)
	if err != nil {
		writeErrorResponse(w, s3err.ErrInvalidDigest, r.URL)
		return
	}

	if r.Header.Get("Cache-Control") != "" {
		if _, err = cacheobject.ParseRequestCacheControl(r.Header.Get("Cache-Control")); err != nil {
			writeErrorResponse(w, s3err.ErrInvalidDigest, r.URL)
			return
		}
	}

	if r.Header.Get("Expires") != "" {
		if _, err = time.Parse(http.TimeFormat, r.Header.Get("Expires")); err != nil {
			writeErrorResponse(w, s3err.ErrInvalidDigest, r.URL)
			return
		}
	}

	dataReader := r.Body
	if s3a.iam.isEnabled() {
		rAuthType := getRequestAuthType(r)
		var s3ErrCode s3err.ErrorCode
		switch rAuthType {
		case authTypeStreamingSigned:
			dataReader, s3ErrCode = s3a.iam.newSignV4ChunkedReader(r)
		case authTypeSignedV2, authTypePresignedV2:
			_, s3ErrCode = s3a.iam.isReqAuthenticatedV2(r)
		case authTypePresigned, authTypeSigned:
			_, s3ErrCode = s3a.iam.reqSignatureV4Verify(r)
		}
		if s3ErrCode != s3err.ErrNone {
			writeErrorResponse(w, s3ErrCode, r.URL)
			return
		}
	} else {
		rAuthType := getRequestAuthType(r)
		if authTypeAnonymous != rAuthType {
			writeErrorResponse(w, s3err.ErrAuthNotSetup, r.URL)
			return
		}
	}
	defer dataReader.Close()

	if strings.HasSuffix(object, "/") {
		if err := s3a.mkdir(s3a.option.BucketsPath, bucket+object, nil); err != nil {
			writeErrorResponse(w, s3err.ErrInternalError, r.URL)
			return
		}
	} else {
		uploadUrl := fmt.Sprintf("http://%s%s/%s%s", s3a.option.Filer, s3a.option.BucketsPath, bucket, urlPathEscape(object))

		etag, errCode := s3a.putToFiler(r, uploadUrl, dataReader)

		if errCode != s3err.ErrNone {
			writeErrorResponse(w, errCode, r.URL)
			return
		}

		setEtag(w, etag)
	}

	writeSuccessResponseEmpty(w)
}

func urlPathEscape(object string) string {
	var escapedParts []string
	for _, part := range strings.Split(object, "/") {
		escapedParts = append(escapedParts, url.PathEscape(part))
	}
	return strings.Join(escapedParts, "/")
}

func (s3a *S3ApiServer) GetObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := getBucketAndObject(r)

	if strings.HasSuffix(r.URL.Path, "/") {
		writeErrorResponse(w, s3err.ErrNotImplemented, r.URL)
		return
	}

	destUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer, s3a.option.BucketsPath, bucket, urlPathEscape(object))

	s3a.proxyToFiler(w, r, destUrl, passThroughResponse)

}

func (s3a *S3ApiServer) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := getBucketAndObject(r)

	destUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer, s3a.option.BucketsPath, bucket, urlPathEscape(object))

	s3a.proxyToFiler(w, r, destUrl, passThroughResponse)

}

func (s3a *S3ApiServer) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := getBucketAndObject(r)

	destUrl := fmt.Sprintf("http://%s%s/%s%s?recursive=true",
		s3a.option.Filer, s3a.option.BucketsPath, bucket, urlPathEscape(object))

	s3a.proxyToFiler(w, r, destUrl, func(proxyResponse *http.Response, w http.ResponseWriter) {
		for k, v := range proxyResponse.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

// / ObjectIdentifier carries key name for the object to delete.
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

	bucket, _ := getBucketAndObject(r)

	deleteXMLBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}

	deleteObjects := &DeleteObjectsRequest{}
	if err := xml.Unmarshal(deleteXMLBytes, deleteObjects); err != nil {
		writeErrorResponse(w, s3err.ErrMalformedXML, r.URL)
		return
	}

	var deletedObjects []ObjectIdentifier
	var deleteErrors []DeleteError

	directoriesWithDeletion := make(map[string]int)

	s3a.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		// delete file entries
		for _, object := range deleteObjects.Objects {

			lastSeparator := strings.LastIndex(object.ObjectName, "/")
			parentDirectoryPath, entryName, isDeleteData, isRecursive := "", object.ObjectName, true, false
			if lastSeparator > 0 && lastSeparator+1 < len(object.ObjectName) {
				entryName = object.ObjectName[lastSeparator+1:]
				parentDirectoryPath = "/" + object.ObjectName[:lastSeparator]
			}
			parentDirectoryPath = fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, parentDirectoryPath)

			err := doDeleteEntry(client, parentDirectoryPath, entryName, isDeleteData, isRecursive)
			if err == nil {
				directoriesWithDeletion[parentDirectoryPath]++
				deletedObjects = append(deletedObjects, object)
			} else if strings.Contains(err.Error(), filer.MsgFailDelNonEmptyFolder) {
				deletedObjects = append(deletedObjects, object)
			} else {
				delete(directoriesWithDeletion, parentDirectoryPath)
				deleteErrors = append(deleteErrors, DeleteError{
					Code:    "",
					Message: err.Error(),
					Key:     object.ObjectName,
				})
			}
		}

		// purge empty folders, only checking folders with deletions
		for len(directoriesWithDeletion) > 0 {
			directoriesWithDeletion = s3a.doDeleteEmptyDirectories(client, directoriesWithDeletion)
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

func (s3a *S3ApiServer) doDeleteEmptyDirectories(client filer_pb.SeaweedFilerClient, directoriesWithDeletion map[string]int) (newDirectoriesWithDeletion map[string]int) {
	var allDirs []string
	for dir, _ := range directoriesWithDeletion {
		allDirs = append(allDirs, dir)
	}
	sort.Slice(allDirs, func(i, j int) bool {
		return len(allDirs[i]) > len(allDirs[j])
	})
	newDirectoriesWithDeletion = make(map[string]int)
	for _, dir := range allDirs {
		parentDir, dirName := util.FullPath(dir).DirAndName()
		if parentDir == s3a.option.BucketsPath {
			continue
		}
		if err := doDeleteEntry(client, parentDir, dirName, false, false); err != nil {
			glog.V(4).Infof("directory %s has %d deletion but still not empty: %v", dir, directoriesWithDeletion[dir], err)
		} else {
			newDirectoriesWithDeletion[parentDir]++
		}
	}
	return
}

var passThroughHeaders = []string{
	"response-cache-control",
	"response-content-disposition",
	"response-content-encoding",
	"response-content-language",
	"response-content-type",
	"response-expires",
}

func (s3a *S3ApiServer) proxyToFiler(w http.ResponseWriter, r *http.Request, destUrl string, responseFn func(proxyResponse *http.Response, w http.ResponseWriter)) {

	glog.V(2).Infof("s3 proxying %s to %s", r.Method, destUrl)

	proxyReq, err := http.NewRequest(r.Method, destUrl, r.Body)

	if err != nil {
		glog.Errorf("NewRequest %s: %v", destUrl, err)
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}

	proxyReq.Header.Set("Host", s3a.option.Filer)
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)

	for header, values := range r.Header {
		// handle s3 related headers
		passed := false
		for _, h := range passThroughHeaders {
			if strings.ToLower(header) == h && len(values) > 0 {
				proxyReq.Header.Add(header[len("response-"):], values[0])
				passed = true
				break
			}
		}
		if passed {
			continue
		}
		// handle other headers
		for _, value := range values {
			proxyReq.Header.Add(header, value)
		}
	}

	resp, postErr := client.Do(proxyReq)

	if postErr != nil {
		glog.Errorf("post to filer: %v", postErr)
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}
	defer util.CloseResponse(resp)

	if resp.StatusCode == http.StatusPreconditionFailed {
		writeErrorResponse(w, s3err.ErrPreconditionFailed, r.URL)
		return
	}

	if (resp.ContentLength == -1 || resp.StatusCode == 404) && resp.StatusCode != 304 {
		if r.Method != "DELETE" {
			writeErrorResponse(w, s3err.ErrNoSuchKey, r.URL)
			return
		}
	}

	responseFn(resp, w)

}

func passThroughResponse(proxyResponse *http.Response, w http.ResponseWriter) {
	for k, v := range proxyResponse.Header {
		w.Header()[k] = v
	}
	if proxyResponse.Header.Get("Content-Range") != "" && proxyResponse.StatusCode == 200 {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(proxyResponse.StatusCode)
	}
	io.Copy(w, proxyResponse.Body)
}

func (s3a *S3ApiServer) putToFiler(r *http.Request, uploadUrl string, dataReader io.Reader) (etag string, code s3err.ErrorCode) {

	hash := md5.New()
	var body = io.TeeReader(dataReader, hash)

	proxyReq, err := http.NewRequest("PUT", uploadUrl, body)

	if err != nil {
		glog.Errorf("NewRequest %s: %v", uploadUrl, err)
		return "", s3err.ErrInternalError
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
		return "", s3err.ErrInternalError
	}
	defer resp.Body.Close()

	etag = fmt.Sprintf("%x", hash.Sum(nil))

	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		glog.Errorf("upload to filer response read %d: %v", resp.StatusCode, ra_err)
		return etag, s3err.ErrInternalError
	}
	var ret weed_server.FilerPostResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.Errorf("failing to read upload to %s : %v", uploadUrl, string(resp_body))
		return "", s3err.ErrInternalError
	}
	if ret.Error != "" {
		glog.Errorf("upload to filer error: %v", ret.Error)
		return "", filerErrorToS3Error(ret.Error)
	}

	return etag, s3err.ErrNone
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

func getBucketAndObject(r *http.Request) (bucket, object string) {
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]
	if !strings.HasPrefix(object, "/") {
		object = "/" + object
	}

	return
}

func filerErrorToS3Error(errString string) s3err.ErrorCode {
	if strings.HasPrefix(errString, "existing ") && strings.HasSuffix(errString, "is a directory") {
		return s3err.ErrExistingObjectIsDirectory
	}
	return s3err.ErrInternalError
}
