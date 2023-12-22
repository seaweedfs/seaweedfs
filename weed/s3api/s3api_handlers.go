package s3api

import (
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

var _ = filer_pb.FilerClient(&S3ApiServer{})

func (s3a *S3ApiServer) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithGrpcClient(streamingMode, s3a.randomClientId, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, s3a.option.Filer.ToGrpcAddress(), false, s3a.option.GrpcDialOption)

}

func (s3a *S3ApiServer) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (s3a *S3ApiServer) GetDataCenter() string {
	return s3a.option.DataCenter
}

func writeSuccessResponseXML(w http.ResponseWriter, r *http.Request, response interface{}) {
	s3err.WriteXMLResponse(w, r, http.StatusOK, response)
	s3err.PostLog(r, http.StatusOK, s3err.ErrNone)
}

func writeSuccessResponseEmpty(w http.ResponseWriter, r *http.Request) {
	s3err.WriteEmptyResponse(w, r, http.StatusOK)
}

func writeFailureResponse(w http.ResponseWriter, r *http.Request, errCode s3err.ErrorCode) {
	s3err.WriteErrorResponse(w, r, errCode)
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
