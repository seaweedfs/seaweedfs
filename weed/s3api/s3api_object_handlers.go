package s3api

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"golang.org/x/exp/slices"

	"github.com/pquerna/cachecontrol/cacheobject"
	"github.com/seaweedfs/seaweedfs/weed/filer"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	deleteMultipleObjectsLimit = 1000
)

func mimeDetect(r *http.Request, dataReader io.Reader) io.ReadCloser {
	mimeBuffer := make([]byte, 512)
	size, _ := dataReader.Read(mimeBuffer)
	if size > 0 {
		r.Header.Set("Content-Type", http.DetectContentType(mimeBuffer[:size]))
		return io.NopCloser(io.MultiReader(bytes.NewReader(mimeBuffer[:size]), dataReader))
	}
	return io.NopCloser(dataReader)
}

func (s3a *S3ApiServer) PutObjectHandler(w http.ResponseWriter, r *http.Request) {

	// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutObjectHandler %s %s", bucket, object)

	_, err := validateContentMd5(r.Header)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidDigest)
		return
	}

	if r.Header.Get("Cache-Control") != "" {
		if _, err = cacheobject.ParseRequestCacheControl(r.Header.Get("Cache-Control")); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidDigest)
			return
		}
	}

	if r.Header.Get("Expires") != "" {
		if _, err = time.Parse(http.TimeFormat, r.Header.Get("Expires")); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrMalformedDate)
			return
		}
	}

	dataReader := r.Body
	rAuthType := getRequestAuthType(r)
	if s3a.iam.isEnabled() {
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
			s3err.WriteErrorResponse(w, r, s3ErrCode)
			return
		}
	} else {
		if authTypeStreamingSigned == rAuthType {
			s3err.WriteErrorResponse(w, r, s3err.ErrAuthNotSetup)
			return
		}
	}
	defer dataReader.Close()

	objectContentType := r.Header.Get("Content-Type")
	uploadUrl := s3a.toFilerUrl(bucket, object)
	if objectContentType == "" {
		dataReader = mimeDetect(r, dataReader)
	}

	etag, errCode := s3a.putToFiler(r, uploadUrl, dataReader, "")

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	setEtag(w, etag)

	writeSuccessResponseEmpty(w, r)
}

func urlEscapeObject(object string) string {
	t := urlPathEscape(removeDuplicateSlashes(object))
	if !strings.HasPrefix(t, "/") {
		t = "/" + t
	}
	return t
}

func urlPathEscape(object string) string {
	var escapedParts []string
	for _, part := range strings.Split(object, "/") {
		escapedParts = append(escapedParts, url.PathEscape(part))
	}
	return strings.Join(escapedParts, "/")
}

func removeDuplicateSlashes(object string) string {
	result := strings.Builder{}
	result.Grow(len(object))

	isLastSlash := false
	for _, r := range object {
		switch r {
		case '/':
			if !isLastSlash {
				result.WriteRune(r)
			}
			isLastSlash = true
		default:
			result.WriteRune(r)
			isLastSlash = false
		}
	}

	r := result.String()
	if r != "/" && strings.HasSuffix(r, "/") {
		return r[:len(r)-1]
	}
	return r
}

func (s3a *S3ApiServer) toFilerUrl(bucket, object string) string {
	object = urlPathEscape(removeDuplicateSlashes(object))
	destUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer.ToHttpAddress(), s3a.option.BucketsPath, bucket, object)
	return destUrl
}

func (s3a *S3ApiServer) GetObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetObjectHandler %s %s", bucket, object)

	destUrl := s3a.toFilerUrl(bucket, object)

	s3a.proxyToFiler(w, r, destUrl, false, passThroughResponse)
}

func (s3a *S3ApiServer) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("HeadObjectHandler %s %s", bucket, object)

	destUrl := s3a.toFilerUrl(bucket, object)

	s3a.proxyToFiler(w, r, destUrl, false, passThroughResponse)
}

func (s3a *S3ApiServer) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("DeleteObjectHandler %s %s", bucket, object)

	destUrl := s3a.toFilerUrl(bucket, object)

	s3a.proxyToFiler(w, r, destUrl, true, func(proxyResponse *http.Response, w http.ResponseWriter) (statusCode int) {
		statusCode = http.StatusNoContent
		for k, v := range proxyResponse.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(statusCode)
		return statusCode
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

	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("DeleteMultipleObjectsHandler %s", bucket)

	deleteXMLBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	deleteObjects := &DeleteObjectsRequest{}
	if err := xml.Unmarshal(deleteXMLBytes, deleteObjects); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	if len(deleteObjects.Objects) > deleteMultipleObjectsLimit {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxDeleteObjects)
		return
	}

	var deletedObjects []ObjectIdentifier
	var deleteErrors []DeleteError
	var auditLog *s3err.AccessLog

	directoriesWithDeletion := make(map[string]int)

	if s3err.Logger != nil {
		auditLog = s3err.GetAccessLog(r, http.StatusNoContent, s3err.ErrNone)
	}
	s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		// delete file entries
		for _, object := range deleteObjects.Objects {
			if object.ObjectName == "" {
				continue
			}
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
			if auditLog != nil {
				auditLog.Key = entryName
				s3err.PostAccessLog(*auditLog)
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

	writeSuccessResponseXML(w, r, deleteResp)

}

func (s3a *S3ApiServer) doDeleteEmptyDirectories(client filer_pb.SeaweedFilerClient, directoriesWithDeletion map[string]int) (newDirectoriesWithDeletion map[string]int) {
	var allDirs []string
	for dir := range directoriesWithDeletion {
		allDirs = append(allDirs, dir)
	}
	slices.SortFunc(allDirs, func(a, b string) bool {
		return len(a) > len(b)
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

func (s3a *S3ApiServer) proxyToFiler(w http.ResponseWriter, r *http.Request, destUrl string, isWrite bool, responseFn func(proxyResponse *http.Response, w http.ResponseWriter) (statusCode int)) {

	glog.V(3).Infof("s3 proxying %s to %s", r.Method, destUrl)

	proxyReq, err := http.NewRequest(r.Method, destUrl, r.Body)

	if err != nil {
		glog.Errorf("NewRequest %s: %v", destUrl, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	for k, v := range r.URL.Query() {
		if _, ok := s3_constants.PassThroughHeaders[strings.ToLower(k)]; ok {
			proxyReq.Header[k] = v
		}
	}
	for header, values := range r.Header {
		proxyReq.Header[header] = values
	}

	// ensure that the Authorization header is overriding any previous
	// Authorization header which might be already present in proxyReq
	s3a.maybeAddFilerJwtAuthorization(proxyReq, isWrite)
	resp, postErr := s3a.client.Do(proxyReq)

	if postErr != nil {
		glog.Errorf("post to filer: %v", postErr)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	defer util.CloseResponse(resp)

	if resp.StatusCode == http.StatusPreconditionFailed {
		s3err.WriteErrorResponse(w, r, s3err.ErrPreconditionFailed)
		return
	}

	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
		return
	}

	if r.Method == "DELETE" {
		if resp.StatusCode == http.StatusNotFound {
			// this is normal
			responseStatusCode := responseFn(resp, w)
			s3err.PostLog(r, responseStatusCode, s3err.ErrNone)
			return
		}
	}

	if resp.StatusCode == http.StatusNotFound {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return
	}

	if resp.Header.Get(s3_constants.X_SeaweedFS_Header_Directory_Key) == "true" {
		responseStatusCode := responseFn(resp, w)
		s3err.PostLog(r, responseStatusCode, s3err.ErrNone)
		return
	}

	// when HEAD a directory, it should be reported as no such key
	// https://github.com/seaweedfs/seaweedfs/issues/3457
	if resp.ContentLength == -1 && resp.StatusCode != http.StatusNotModified {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return
	}

	setUserMetadataKeyToLowercase(resp)

	responseStatusCode := responseFn(resp, w)
	s3err.PostLog(r, responseStatusCode, s3err.ErrNone)
}

func setUserMetadataKeyToLowercase(resp *http.Response) {
	for key, value := range resp.Header {
		if strings.HasPrefix(key, s3_constants.AmzUserMetaPrefix) {
			resp.Header[strings.ToLower(key)] = value
			delete(resp.Header, key)
		}
	}
}

func passThroughResponse(proxyResponse *http.Response, w http.ResponseWriter) (statusCode int) {
	for k, v := range proxyResponse.Header {
		w.Header()[k] = v
	}
	if proxyResponse.Header.Get("Content-Range") != "" && proxyResponse.StatusCode == 200 {
		w.WriteHeader(http.StatusPartialContent)
		statusCode = http.StatusPartialContent
	} else {
		statusCode = proxyResponse.StatusCode
	}
	w.WriteHeader(statusCode)
	buf := mem.Allocate(128 * 1024)
	defer mem.Free(buf)
	if n, err := io.CopyBuffer(w, proxyResponse.Body, buf); err != nil {
		glog.V(1).Infof("passthrough response read %d bytes: %v", n, err)
	}
	return statusCode
}

func (s3a *S3ApiServer) putToFiler(r *http.Request, uploadUrl string, dataReader io.Reader, destination string) (etag string, code s3err.ErrorCode) {

	hash := md5.New()
	var body = io.TeeReader(dataReader, hash)

	proxyReq, err := http.NewRequest("PUT", uploadUrl, body)

	if err != nil {
		glog.Errorf("NewRequest %s: %v", uploadUrl, err)
		return "", s3err.ErrInternalError
	}

	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	if destination != "" {
		proxyReq.Header.Set(s3_constants.SeaweedStorageDestinationHeader, destination)
	}

	for header, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(header, value)
		}
	}
	// ensure that the Authorization header is overriding any previous
	// Authorization header which might be already present in proxyReq
	s3a.maybeAddFilerJwtAuthorization(proxyReq, true)
	resp, postErr := s3a.client.Do(proxyReq)

	if postErr != nil {
		glog.Errorf("post to filer: %v", postErr)
		return "", s3err.ErrInternalError
	}
	defer resp.Body.Close()

	etag = fmt.Sprintf("%x", hash.Sum(nil))

	resp_body, ra_err := io.ReadAll(resp.Body)
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
			w.Header()["ETag"] = []string{etag}
		} else {
			w.Header()["ETag"] = []string{"\"" + etag + "\""}
		}
	}
}

func filerErrorToS3Error(errString string) s3err.ErrorCode {
	switch {
	case strings.HasPrefix(errString, "existing ") && strings.HasSuffix(errString, "is a directory"):
		return s3err.ErrExistingObjectIsDirectory
	case strings.HasSuffix(errString, "is a file"):
		return s3err.ErrExistingObjectIsFile
	default:
		return s3err.ErrInternalError
	}
}

func (s3a *S3ApiServer) maybeAddFilerJwtAuthorization(r *http.Request, isWrite bool) {
	encodedJwt := s3a.maybeGetFilerJwtAuthorizationToken(isWrite)

	if encodedJwt == "" {
		return
	}

	r.Header.Set("Authorization", "BEARER "+string(encodedJwt))
}

func (s3a *S3ApiServer) maybeGetFilerJwtAuthorizationToken(isWrite bool) string {
	var encodedJwt security.EncodedJwt
	if isWrite {
		encodedJwt = security.GenJwtForFilerServer(s3a.filerGuard.SigningKey, s3a.filerGuard.ExpiresAfterSec)
	} else {
		encodedJwt = security.GenJwtForFilerServer(s3a.filerGuard.ReadSigningKey, s3a.filerGuard.ReadExpiresAfterSec)
	}
	return string(encodedJwt)
}
