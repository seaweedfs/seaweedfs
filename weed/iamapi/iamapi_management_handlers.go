package iamapi

import (
	"encoding/xml"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	"net/http"
	"net/url"

	//	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/iam"
)

const (
	version = "2010-05-08"
)

type ListUsersResponse struct {
	XMLName         xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListUsersResponse"`
	ListUsersResult struct {
		Users       []*iam.User `xml:"Users>member"`
		IsTruncated bool        `xml:"IsTruncated"`
	} `xml:"ListUsersResult"`
	ResponseMetadata struct {
		RequestId string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

//  {'Action': 'CreateUser', 'Version': '2010-05-08', 'UserName': 'Bob'}
// {'Action': 'ListUsers', 'Version': '2010-05-08'}
func (iama *IamApiServer) ListUsers(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) ListUsersResponse {
	glog.Info("Do ListUsers")
	resp := ListUsersResponse{}
	for _, ident := range s3cfg.Identities {
		resp.ListUsersResult.Users = append(resp.ListUsersResult.Users, &iam.User{UserName: &ident.Name})
	}
	return resp
}

func (iama *IamApiServer) ListAccessKeys(values url.Values) ListUsersResponse {
	return ListUsersResponse{}
}

func (iama *IamApiServer) DoActions(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeErrorResponse(w, s3err.ErrInvalidRequest, r.URL)
		return
	}
	values := r.PostForm
	s3cfg := &iam_pb.S3ApiConfiguration{}
	if err := iama.ifs.LoadIAMConfig(s3cfg); err != nil {
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}
	glog.Info("values ", values)
	var response interface{}
	switch r.Form.Get("Action") {
	case "ListUsers":
		response = iama.ListUsers(s3cfg, values)
	case "ListAccessKeys":
		response = iama.ListAccessKeys(values)
	default:
		writeErrorResponse(w, s3err.ErrNotImplemented, r.URL)
		return
	}
	writeSuccessResponseXML(w, encodeResponse(response))
}
