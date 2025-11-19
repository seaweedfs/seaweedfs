package s3err

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/private/protocol/xml/xmlutil"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

type mimeType string

const (
	mimeNone mimeType = ""
	MimeXML  mimeType = "application/xml"
)

func WriteAwsXMLResponse(w http.ResponseWriter, r *http.Request, statusCode int, result interface{}) {
	var bytesBuffer bytes.Buffer
	err := xmlutil.BuildXML(result, xml.NewEncoder(&bytesBuffer))
	if err != nil {
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	WriteResponse(w, r, statusCode, bytesBuffer.Bytes(), MimeXML)
}

func WriteXMLResponse(w http.ResponseWriter, r *http.Request, statusCode int, response interface{}) {
	WriteResponse(w, r, statusCode, EncodeXMLResponse(response), MimeXML)
}

func WriteEmptyResponse(w http.ResponseWriter, r *http.Request, statusCode int) {
	WriteResponse(w, r, statusCode, []byte{}, mimeNone)
	PostLog(r, statusCode, ErrNone)
}

func WriteErrorResponse(w http.ResponseWriter, r *http.Request, errorCode ErrorCode) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]
	if strings.HasPrefix(object, "/") {
		object = object[1:]
	}

	apiError := GetAPIError(errorCode)
	errorResponse := getRESTErrorResponse(apiError, r.URL.Path, bucket, object)
	WriteXMLResponse(w, r, apiError.HTTPStatusCode, errorResponse)
	PostLog(r, apiError.HTTPStatusCode, errorCode)
}

func getRESTErrorResponse(err APIError, resource string, bucket, object string) RESTErrorResponse {
	return RESTErrorResponse{
		Code:       err.Code,
		BucketName: bucket,
		Key:        object,
		Message:    err.Description,
		Resource:   resource,
		RequestID:  fmt.Sprintf("%d", time.Now().UnixNano()),
	}
}

// Encodes the response headers into XML format.
func EncodeXMLResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	e := xml.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}

func setCommonHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("x-amz-request-id", fmt.Sprintf("%d", time.Now().UnixNano()))
	w.Header().Set("Accept-Ranges", "bytes")

	// Handle CORS headers for requests with Origin header
	if r.Header.Get("Origin") != "" {
		// Use mux.Vars to detect bucket-specific requests more reliably
		vars := mux.Vars(r)
		bucket := vars["bucket"]
		isBucketRequest := bucket != ""

		if !isBucketRequest {
			// Service-level request (like OPTIONS /) - apply static CORS if none set
			if w.Header().Get("Access-Control-Allow-Origin") == "" {
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Header().Set("Access-Control-Allow-Methods", "*")
				w.Header().Set("Access-Control-Allow-Headers", "*")
				w.Header().Set("Access-Control-Expose-Headers", "*")
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}
		} else {
			// Bucket-specific request - preserve existing CORS headers or set default
			// This handles cases where CORS middleware set headers but auth failed
			if w.Header().Get("Access-Control-Allow-Origin") == "" {
				// No CORS headers were set by middleware, so this request doesn't match any CORS rule
				// According to CORS spec, we should not set CORS headers for non-matching requests
				// However, if the bucket has CORS config but request doesn't match,
				// we still should not set headers here as it would be incorrect
			}
			// If CORS headers were already set by middleware, preserve them
		}
	}
}

func WriteResponse(w http.ResponseWriter, r *http.Request, statusCode int, response []byte, mType mimeType) {
	setCommonHeaders(w, r)
	if response != nil {
		w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	}
	if mType != mimeNone {
		w.Header().Set("Content-Type", string(mType))
	}
	w.WriteHeader(statusCode)
	if response != nil {
		glog.V(4).Infof("status %d %s: %s", statusCode, mType, string(response))
		_, err := w.Write(response)
		if err != nil {
			glog.V(1).Infof("write err: %v", err)
		}
		w.(http.Flusher).Flush()
	}
}

// If none of the http routes match respond with MethodNotAllowed
func NotFoundHandler(w http.ResponseWriter, r *http.Request) {
	glog.V(2).Infof("unsupported %s %s", r.Method, r.RequestURI)
	WriteErrorResponse(w, r, ErrMethodNotAllowed)
}
