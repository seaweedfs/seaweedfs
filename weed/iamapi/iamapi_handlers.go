package iamapi

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strconv"

	"net/http"
	"net/url"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"

	"github.com/aws/aws-sdk-go/service/iam"
)

type mimeType string

const (
	mimeNone mimeType = ""
	mimeXML  mimeType = "application/xml"
)

func setCommonHeaders(w http.ResponseWriter) {
	w.Header().Set("x-amz-request-id", fmt.Sprintf("%d", time.Now().UnixNano()))
	w.Header().Set("Accept-Ranges", "bytes")
}

// Encodes the response headers into XML format.
func encodeResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	e := xml.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}

// If none of the http routes match respond with MethodNotAllowed
func notFoundHandler(w http.ResponseWriter, r *http.Request) {
	glog.V(0).Infof("unsupported %s %s", r.Method, r.RequestURI)
	writeErrorResponse(w, s3err.ErrMethodNotAllowed, r.URL)
}

func writeErrorResponse(w http.ResponseWriter, errorCode s3err.ErrorCode, reqURL *url.URL) {
	apiError := s3err.GetAPIError(errorCode)
	errorResponse := getRESTErrorResponse(apiError, reqURL.Path)
	encodedErrorResponse := encodeResponse(errorResponse)
	writeResponse(w, apiError.HTTPStatusCode, encodedErrorResponse, mimeXML)
}

func writeIamErrorResponse(w http.ResponseWriter, err error, object string, value string, msg error) {
	errCode := err.Error()
	errorResp := ErrorResponse{}
	errorResp.Error.Type = "Sender"
	errorResp.Error.Code = &errCode
	if msg != nil {
		errMsg := msg.Error()
		errorResp.Error.Message = &errMsg
	}
	glog.Errorf("Response %+v", err)
	switch errCode {
	case iam.ErrCodeNoSuchEntityException:
		msg := fmt.Sprintf("The %s with name %s cannot be found.", object, value)
		errorResp.Error.Message = &msg
		writeResponse(w, http.StatusNotFound, encodeResponse(errorResp), mimeXML)
	case iam.ErrCodeServiceFailureException:
		writeResponse(w, http.StatusInternalServerError, encodeResponse(errorResp), mimeXML)
	default:
		writeResponse(w, http.StatusInternalServerError, encodeResponse(errorResp), mimeXML)
	}
}

func getRESTErrorResponse(err s3err.APIError, resource string) s3err.RESTErrorResponse {
	return s3err.RESTErrorResponse{
		Code:      err.Code,
		Message:   err.Description,
		Resource:  resource,
		RequestID: fmt.Sprintf("%d", time.Now().UnixNano()),
	}
}

func writeResponse(w http.ResponseWriter, statusCode int, response []byte, mType mimeType) {
	setCommonHeaders(w)
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
			glog.V(0).Infof("write err: %v", err)
		}
		w.(http.Flusher).Flush()
	}
}

func writeSuccessResponseXML(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, mimeXML)
}
