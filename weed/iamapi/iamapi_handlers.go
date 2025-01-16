package iamapi

import (
	"net/http"

	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func newErrorResponse(errCode string, errMsg string) ErrorResponse {
	errorResp := ErrorResponse{}
	errorResp.Error.Type = "Sender"
	errorResp.Error.Code = &errCode
	errorResp.Error.Message = &errMsg
	return errorResp
}

func writeIamErrorResponse(w http.ResponseWriter, r *http.Request, iamError *IamError) {

	if iamError == nil {
		// Do nothing if there is no error
		glog.Errorf("No error found")
		return
	}

	errCode := iamError.Code
	errMsg := iamError.Error.Error()
	glog.Errorf("Response %+v", errMsg)

	errorResp := newErrorResponse(errCode, errMsg)
	internalErrorResponse := newErrorResponse(iam.ErrCodeServiceFailureException, "Internal server error")

	switch errCode {
	case iam.ErrCodeNoSuchEntityException:
		s3err.WriteXMLResponse(w, r, http.StatusNotFound, errorResp)
	case iam.ErrCodeMalformedPolicyDocumentException:
		s3err.WriteXMLResponse(w, r, http.StatusBadRequest, errorResp)
	case iam.ErrCodeServiceFailureException:
		// We do not want to expose internal server error to the client
		s3err.WriteXMLResponse(w, r, http.StatusInternalServerError, internalErrorResponse)
	default:
		s3err.WriteXMLResponse(w, r, http.StatusInternalServerError, internalErrorResponse)
	}
}
