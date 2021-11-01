package iamapi

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	"net/http"
)

func writeIamErrorResponse(w http.ResponseWriter, r *http.Request, err error, object string, value string, msg error) {
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
		s3err.WriteXMLResponse(w, r, http.StatusNotFound, errorResp)
	case iam.ErrCodeServiceFailureException:
		s3err.WriteXMLResponse(w, r, http.StatusInternalServerError, errorResp)
	default:
		s3err.WriteXMLResponse(w, r, http.StatusInternalServerError, errorResp)
	}
}
