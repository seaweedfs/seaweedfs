package errors

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"strconv"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// IAMErrorCode represents IAM-specific error codes
type IAMErrorCode string

const (
	ErrAccessDenied              IAMErrorCode = "AccessDenied"
	ErrEntityAlreadyExists       IAMErrorCode = "EntityAlreadyExists"
	ErrNoSuchEntity              IAMErrorCode = "NoSuchEntity"
	ErrValidationError           IAMErrorCode = "ValidationError"
	ErrDeleteConflict            IAMErrorCode = "DeleteConflict"
	ErrLimitExceeded             IAMErrorCode = "LimitExceeded"
	ErrServiceFailure            IAMErrorCode = "ServiceFailure"
	ErrMalformedPolicyDocument   IAMErrorCode = "MalformedPolicyDocument"
	ErrInvalidInput              IAMErrorCode = "InvalidInput"
	ErrInternalError             IAMErrorCode = "InternalError"
)

type IAMError struct {
	Code       IAMErrorCode
	Message    string
	HTTPStatus int
}

func (e *IAMError) Error() string {
	return e.Message
}

type mimeType string

const (
	mimeNone mimeType = ""
	MimeXML  mimeType = "application/xml"
)

func WriteXMLResponse(w http.ResponseWriter, r *http.Request, statusCode int, response interface{}) {
	WriteResponse(w, r, statusCode, EncodeXMLResponse(response), MimeXML)
}

func EncodeXMLResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	e := xml.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}

func WriteResponse(w http.ResponseWriter, r *http.Request, statusCode int, response []byte, mType mimeType) {
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

// NotFoundHandler for 404
func NotFoundHandler(w http.ResponseWriter, r *http.Request) {
    glog.V(2).Infof("unsupported %s %s", r.Method, r.RequestURI)
    http.Error(w, "404 page not found", http.StatusNotFound)
}
