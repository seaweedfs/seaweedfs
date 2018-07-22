package s3api

import (
	"encoding/xml"
	"net/http"
)

// APIError structure
type APIError struct {
	Code           string
	Description    string
	HTTPStatusCode int
}

// RESTErrorResponse - error response format
type RESTErrorResponse struct {
	XMLName   xml.Name `xml:"Error" json:"-"`
	Code      string   `xml:"Code" json:"Code"`
	Message   string   `xml:"Message" json:"Message"`
	Resource  string   `xml:"Resource" json:"Resource"`
	RequestID string   `xml:"RequestId" json:"RequestId"`
}

// ErrorCode type of error status.
type ErrorCode int

// Error codes, see full list at http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	ErrNone ErrorCode = iota
	ErrMethodNotAllowed
	ErrBucketNotEmpty
	ErrBucketAlreadyExists
	ErrBucketAlreadyOwnedByYou
	ErrNoSuchBucket
	ErrInvalidBucketName
	ErrInvalidDigest
	ErrInternalError
)

// error code to APIError structure, these fields carry respective
// descriptions for all the error responses.
var errorCodeResponse = map[ErrorCode]APIError{
	ErrMethodNotAllowed: {
		Code:           "MethodNotAllowed",
		Description:    "The specified method is not allowed against this resource.",
		HTTPStatusCode: http.StatusMethodNotAllowed,
	},
	ErrBucketNotEmpty: {
		Code:           "BucketNotEmpty",
		Description:    "The bucket you tried to delete is not empty",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrBucketAlreadyExists: {
		Code:           "BucketAlreadyExists",
		Description:    "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrBucketAlreadyOwnedByYou: {
		Code:           "BucketAlreadyOwnedByYou",
		Description:    "Your previous request to create the named bucket succeeded and you already own it.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrInvalidBucketName: {
		Code:           "InvalidBucketName",
		Description:    "The specified bucket is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidDigest: {
		Code:           "InvalidDigest",
		Description:    "The Content-Md5 you specified is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNoSuchBucket: {
		Code:           "NoSuchBucket",
		Description:    "The specified bucket does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrInternalError: {
		Code:           "InternalError",
		Description:    "We encountered an internal error, please try again.",
		HTTPStatusCode: http.StatusInternalServerError,
	},
}

// getAPIError provides API Error for input API error code.
func getAPIError(code ErrorCode) APIError {
	return errorCodeResponse[code]
}
