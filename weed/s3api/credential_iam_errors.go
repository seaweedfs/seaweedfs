package s3api

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/seaweedfs/seaweedfs/weed/credential"
)

// CredentialErrToIamErrCode maps a credential store error to the appropriate
// IAM error code. Permanent client errors (e.g. duplicate entity, missing
// entity) are mapped to their specific IAM codes; all other errors fall back
// to ServiceFailure so the caller receives an HTTP 500 rather than a 200.
//
// This is shared between the standalone IAM server (weed/iamapi) and the
// embedded IAM handler (weed/s3api) so error granularity stays in sync.
func CredentialErrToIamErrCode(err error) string {
	switch {
	case errors.Is(err, credential.ErrUserAlreadyExists):
		return iam.ErrCodeEntityAlreadyExistsException
	case errors.Is(err, credential.ErrUserNotFound),
		errors.Is(err, credential.ErrAccessKeyNotFound):
		return iam.ErrCodeNoSuchEntityException
	default:
		return iam.ErrCodeServiceFailureException
	}
}
