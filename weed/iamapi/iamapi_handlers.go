package iamapi

import (
	"net/http"

	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func newErrorResponse(errCode string, errMsg string, requestID string) ErrorResponse {
	errorResp := ErrorResponse{}
	errorResp.Error.Type = "Sender"
	errorResp.Error.Code = &errCode
	errorResp.Error.Message = &errMsg
	errorResp.SetRequestId(requestID)
	return errorResp
}

func writeIamErrorResponse(w http.ResponseWriter, r *http.Request, reqID string, iamError *IamError) {
	if iamError == nil {
		glog.Errorf("writeIamErrorResponse called with nil error")
		internalResp := newErrorResponse(iam.ErrCodeServiceFailureException, "Internal server error", reqID)
		s3err.WriteXMLResponse(w, r, http.StatusInternalServerError, internalResp)
		return
	}

	errCode := iamError.Code
	errMsg := iamError.Error.Error()
	glog.Errorf("Response %+v", errMsg)

	errorResp := newErrorResponse(errCode, errMsg, reqID)
	internalErrorResponse := newErrorResponse(iam.ErrCodeServiceFailureException, "Internal server error", reqID)

	switch errCode {
	case iam.ErrCodeNoSuchEntityException:
		s3err.WriteXMLResponse(w, r, http.StatusNotFound, errorResp)
	case iam.ErrCodeMalformedPolicyDocumentException, iam.ErrCodeInvalidInputException:
		s3err.WriteXMLResponse(w, r, http.StatusBadRequest, errorResp)
	case iam.ErrCodeDeleteConflictException:
		s3err.WriteXMLResponse(w, r, http.StatusConflict, errorResp)
	case iam.ErrCodeServiceFailureException:
		// We do not want to expose internal server error to the client
		s3err.WriteXMLResponse(w, r, http.StatusInternalServerError, internalErrorResponse)
	default:
		s3err.WriteXMLResponse(w, r, http.StatusInternalServerError, internalErrorResponse)
	}
}
