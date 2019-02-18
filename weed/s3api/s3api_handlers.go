package s3api

import (
	"bytes"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"net/http"
	"net/url"
	"time"
)

type mimeType string

const (
	mimeNone mimeType = ""
	mimeJSON mimeType = "application/json"
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

func (s3a *S3ApiServer) withFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {

	grpcConnection, err := util.GrpcDial(s3a.option.FilerGrpcAddress, s3a.option.GrpcDialOption)
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", s3a.option.FilerGrpcAddress, err)
	}
	defer grpcConnection.Close()

	client := filer_pb.NewSeaweedFilerClient(grpcConnection)

	return fn(client)
}

// If none of the http routes match respond with MethodNotAllowed
func notFoundHandler(w http.ResponseWriter, r *http.Request) {
	glog.V(0).Infof("unsupported %s %s", r.Method, r.RequestURI)
	writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
}

func writeErrorResponse(w http.ResponseWriter, errorCode ErrorCode, reqURL *url.URL) {
	apiError := getAPIError(errorCode)
	errorResponse := getRESTErrorResponse(apiError, reqURL.Path)
	encodedErrorResponse := encodeResponse(errorResponse)
	writeResponse(w, apiError.HTTPStatusCode, encodedErrorResponse, mimeXML)
}

func getRESTErrorResponse(err APIError, resource string) RESTErrorResponse {
	return RESTErrorResponse{
		Code:      err.Code,
		Message:   err.Description,
		Resource:  resource,
		RequestID: fmt.Sprintf("%d", time.Now().UnixNano()),
	}
}

func writeResponse(w http.ResponseWriter, statusCode int, response []byte, mType mimeType) {
	setCommonHeaders(w)
	if mType != mimeNone {
		w.Header().Set("Content-Type", string(mType))
	}
	w.WriteHeader(statusCode)
	if response != nil {
		glog.V(4).Infof("status %d %s: %s", statusCode, mType, string(response))
		w.Write(response)
		w.(http.Flusher).Flush()
	}
}

func writeSuccessResponseXML(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, mimeXML)
}

func writeSuccessResponseEmpty(w http.ResponseWriter) {
	writeResponse(w, http.StatusOK, nil, mimeNone)
}

func validateContentMd5(h http.Header) ([]byte, error) {
	md5B64, ok := h["Content-Md5"]
	if ok {
		if md5B64[0] == "" {
			return nil, fmt.Errorf("Content-Md5 header set to empty value")
		}
		return base64.StdEncoding.DecodeString(md5B64[0])
	}
	return []byte{}, nil
}
