package s3api

import (
	"net/http"
	"net/url"
	"fmt"
	"time"
	"github.com/gorilla/mux"
	"context"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func newContext(r *http.Request, api string) context.Context {
	vars := mux.Vars(r)
	return context.WithValue(context.Background(), "bucket", vars["bucket"])
}

func (s3a *S3ApiServer) withFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {

	grpcConnection, err := util.GrpcDial(s3a.option.FilerGrpcAddress)
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", s3a.option.FilerGrpcAddress, err)
	}
	defer grpcConnection.Close()

	client := filer_pb.NewSeaweedFilerClient(grpcConnection)

	return fn(client)
}

// If none of the http routes match respond with MethodNotAllowed
func notFoundHandler(w http.ResponseWriter, r *http.Request) {
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
		w.Write(response)
		w.(http.Flusher).Flush()
	}
}

func writeSuccessResponseXML(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, mimeXML)
}
