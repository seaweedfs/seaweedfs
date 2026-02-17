package s3err

import (
	"encoding/xml"
	"fmt"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/util/constants"
)

// APIError structure
type APIError struct {
	Code           string
	Description    string
	HTTPStatusCode int
}

// RESTErrorResponse - error response format
type RESTErrorResponse struct {
	XMLName    xml.Name `xml:"Error" json:"-"`
	Code       string   `xml:"Code" json:"Code"`
	Message    string   `xml:"Message" json:"Message"`
	Resource   string   `xml:"Resource" json:"Resource"`
	RequestID  string   `xml:"RequestId" json:"RequestId"`
	Key        string   `xml:"Key,omitempty" json:"Key,omitempty"`
	BucketName string   `xml:"BucketName,omitempty" json:"BucketName,omitempty"`

	// Underlying HTTP status code for the returned error
	StatusCode int `xml:"-" json:"-"`
}

// Error - Returns S3 error string.
func (e RESTErrorResponse) Error() string {
	if e.Message == "" {
		msg, ok := s3ErrorResponseMap[e.Code]
		if !ok {
			msg = fmt.Sprintf("Error response code %s.", e.Code)
		}
		return msg
	}
	return e.Message
}

// ErrorCode type of error status.
type ErrorCode int

// Error codes, see full list at http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	ErrNone ErrorCode = iota
	ErrAccessDenied
	ErrMethodNotAllowed
	ErrBucketNotEmpty
	ErrBucketAlreadyExists
	ErrBucketAlreadyOwnedByYou
	ErrNoSuchBucket
	ErrNoSuchBucketPolicy
	ErrNoSuchCORSConfiguration
	ErrNoSuchLifecycleConfiguration
	ErrNoSuchKey
	ErrNoSuchUpload
	ErrInvalidBucketName
	ErrInvalidBucketState
	ErrInvalidDigest
	ErrBadDigest
	ErrInvalidMaxKeys
	ErrInvalidMaxUploads
	ErrInvalidMaxParts
	ErrInvalidMaxDeleteObjects
	ErrInvalidPartNumberMarker
	ErrInvalidPart
	ErrInvalidRange
	ErrInternalError
	ErrInvalidCopyDest
	ErrInvalidCopySource
	ErrInvalidTag
	ErrAuthHeaderEmpty
	ErrSignatureVersionNotSupported
	ErrMalformedPOSTRequest
	ErrPOSTFileRequired
	ErrPostPolicyConditionInvalidFormat
	ErrEntityTooSmall
	ErrEntityTooLarge
	ErrMissingFields
	ErrMissingCredTag
	ErrCredMalformed
	ErrMalformedXML
	ErrMalformedDate
	ErrMalformedPresignedDate
	ErrMalformedCredentialDate
	ErrMalformedPolicy
	ErrInvalidPolicyDocument
	ErrMissingSignHeadersTag
	ErrMissingSignTag
	ErrUnsignedHeaders
	ErrInvalidQueryParams
	ErrInvalidQuerySignatureAlgo
	ErrExpiredPresignRequest
	ErrExpiredToken
	ErrMalformedExpires
	ErrNegativeExpires
	ErrMaximumExpires
	ErrSignatureDoesNotMatch
	ErrContentSHA256Mismatch
	ErrInvalidAccessKeyID
	ErrRequestNotReadyYet
	ErrRequestTimeTooSkewed
	ErrMissingDateHeader
	ErrInvalidRequest
	ErrAuthNotSetup
	ErrNotImplemented
	ErrPreconditionFailed
	ErrNotModified

	ErrExistingObjectIsDirectory
	ErrExistingObjectIsFile

	ErrTooManyRequest
	ErrRequestBytesExceed
	ErrServiceUnavailable

	OwnershipControlsNotFoundError
	ErrNoSuchTagSet
	ErrNoSuchObjectLockConfiguration
	ErrNoSuchObjectLegalHold
	ErrInvalidRetentionPeriod
	ErrObjectLockConfigurationNotFoundError
	ErrInvalidUnorderedWithDelimiter

	// SSE-C related errors
	ErrInvalidEncryptionAlgorithm
	ErrInvalidEncryptionKey
	ErrSSECustomerKeyMD5Mismatch
	ErrSSECustomerKeyMissing
	ErrSSECustomerKeyNotNeeded
	ErrSSEEncryptionTypeMismatch

	// SSE-KMS related errors
	ErrKMSKeyNotFound
	ErrKMSAccessDenied
	ErrKMSDisabled
	ErrKMSInvalidCiphertext

	// Bucket encryption errors
	ErrNoSuchBucketEncryptionConfiguration
	ErrInvalidStorageClass
)

// Error message constants for checksum validation
const (
	ErrMsgPayloadChecksumMismatch   = "payload checksum does not match"
	ErrMsgChunkSignatureMismatch    = "chunk signature does not match"
	ErrMsgChecksumAlgorithmMismatch = "checksum algorithm mismatch"
)

// error code to APIError structure, these fields carry respective
// descriptions for all the error responses.
var errorCodeResponse = map[ErrorCode]APIError{
	ErrAccessDenied: {
		Code:           "AccessDenied",
		Description:    "Access Denied.",
		HTTPStatusCode: http.StatusForbidden,
	},
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
		Description:    "The requested bucket name is not available. The bucket name can not be an existing collection, and the bucket namespace is shared by all users of the system. Please select a different name and try again.",
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
	ErrInvalidBucketState: {
		Code:           "InvalidBucketState",
		Description:    "The bucket is not in a valid state for the requested operation",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrInvalidDigest: {
		Code:           "InvalidDigest",
		Description:    "The Content-Md5 you specified is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBadDigest: {
		Code:           "BadDigest",
		Description:    constants.ErrMsgBadDigest,
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxUploads: {
		Code:           "InvalidArgument",
		Description:    "Argument max-uploads must be an integer between 0 and 2147483647",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxKeys: {
		Code:           "InvalidArgument",
		Description:    "Argument maxKeys must be an integer between 0 and 2147483647",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxParts: {
		Code:           "InvalidArgument",
		Description:    "Argument max-parts must be an integer between 0 and 2147483647",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxDeleteObjects: {
		Code:           "InvalidArgument",
		Description:    "Argument objects can contain a list of up to 1000 keys",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPartNumberMarker: {
		Code:           "InvalidArgument",
		Description:    "Argument partNumberMarker must be an integer.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNoSuchBucket: {
		Code:           "NoSuchBucket",
		Description:    "The specified bucket does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchBucketPolicy: {
		Code:           "NoSuchBucketPolicy",
		Description:    "The bucket policy does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchTagSet: {
		Code:           "NoSuchTagSet",
		Description:    "The TagSet does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchObjectLockConfiguration: {
		Code:           "NoSuchObjectLockConfiguration",
		Description:    "The specified object does not have an ObjectLock configuration",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchObjectLegalHold: {
		Code:           "NoSuchObjectLegalHold",
		Description:    "The specified object does not have a legal hold configuration",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrInvalidRetentionPeriod: {
		Code:           "InvalidRetentionPeriod",
		Description:    "The retention period specified is invalid",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNoSuchCORSConfiguration: {
		Code:           "NoSuchCORSConfiguration",
		Description:    "The CORS configuration does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchLifecycleConfiguration: {
		Code:           "NoSuchLifecycleConfiguration",
		Description:    "The lifecycle configuration does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchKey: {
		Code:           "NoSuchKey",
		Description:    "The specified key does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchUpload: {
		Code:           "NoSuchUpload",
		Description:    "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrInternalError: {
		Code:           "InternalError",
		Description:    "We encountered an internal error, please try again.",
		HTTPStatusCode: http.StatusInternalServerError,
	},

	ErrInvalidPart: {
		Code:           "InvalidPart",
		Description:    "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	ErrInvalidCopyDest: {
		Code:           "InvalidRequest",
		Description:    "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidCopySource: {
		Code:           "InvalidArgument",
		Description:    "Copy Source must mention the source bucket and key: sourcebucket/sourcekey.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidTag: {
		Code:           "InvalidTag",
		Description:    "The Tag value you have provided is invalid",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedXML: {
		Code:           "MalformedXML",
		Description:    "The XML you provided was not well-formed or did not validate against our published schema.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedPolicy: {
		Code:           "MalformedPolicy",
		Description:    "Policy has invalid resource.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPolicyDocument: {
		Code:           "InvalidPolicyDocument",
		Description:    "The content of the policy document is invalid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAuthHeaderEmpty: {
		Code:           "InvalidArgument",
		Description:    "Authorization header is invalid -- one and only one ' ' (space) required.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSignatureVersionNotSupported: {
		Code:           "InvalidRequest",
		Description:    "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedPOSTRequest: {
		Code:           "MalformedPOSTRequest",
		Description:    "The body of your POST request is not well-formed multipart/form-data.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrPOSTFileRequired: {
		Code:           "InvalidArgument",
		Description:    "POST requires exactly one file upload per request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrPostPolicyConditionInvalidFormat: {
		Code:           "PostPolicyInvalidKeyName",
		Description:    "Invalid according to Policy: Policy Condition failed",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrEntityTooSmall: {
		Code:           "EntityTooSmall",
		Description:    "Your proposed upload is smaller than the minimum allowed object size.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEntityTooLarge: {
		Code:           "EntityTooLarge",
		Description:    "Your proposed upload exceeds the maximum allowed object size.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingFields: {
		Code:           "MissingFields",
		Description:    "Missing fields in request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingCredTag: {
		Code:           "InvalidRequest",
		Description:    "Missing Credential field for this request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrCredMalformed: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Error parsing the X-Amz-Credential parameter; the Credential is mal-formed; expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedDate: {
		Code:           "MalformedDate",
		Description:    "Invalid date format header, expected to be in ISO8601, RFC1123 or RFC1123Z time format.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedPresignedDate: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Date must be in the ISO8601 Long Format \"yyyyMMdd'T'HHmmss'Z'\"",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingSignHeadersTag: {
		Code:           "InvalidArgument",
		Description:    "Signature header missing SignedHeaders field.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingSignTag: {
		Code:           "AccessDenied",
		Description:    "Signature header missing Signature field.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	ErrUnsignedHeaders: {
		Code:           "AccessDenied",
		Description:    "There were headers present in the request which were not signed",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidQueryParams: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidQuerySignatureAlgo: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Algorithm only supports \"AWS4-HMAC-SHA256\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrExpiredPresignRequest: {
		Code:           "AccessDenied",
		Description:    "Request has expired",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrExpiredToken: {
		Code:           "ExpiredToken",
		Description:    "The provided token has expired.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedExpires: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Expires should be a number",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNegativeExpires: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Expires must be non-negative",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMaximumExpires: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Expires must be less than a week (in seconds); that is, the given X-Amz-Expires must be less than 604800 seconds",
		HTTPStatusCode: http.StatusBadRequest,
	},

	ErrInvalidAccessKeyID: {
		Code:           "InvalidAccessKeyId",
		Description:    "The access key ID you provided does not exist in our records.",
		HTTPStatusCode: http.StatusForbidden,
	},

	ErrRequestNotReadyYet: {
		Code:           "AccessDenied",
		Description:    "Request is not valid yet",
		HTTPStatusCode: http.StatusForbidden,
	},

	ErrRequestTimeTooSkewed: {
		Code:           "RequestTimeTooSkewed",
		Description:    "The difference between the request time and the server's time is too large.",
		HTTPStatusCode: http.StatusForbidden,
	},

	ErrSignatureDoesNotMatch: {
		Code:           "SignatureDoesNotMatch",
		Description:    "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
		HTTPStatusCode: http.StatusForbidden,
	},

	ErrContentSHA256Mismatch: {
		Code:           "XAmzContentSHA256Mismatch",
		Description:    "The provided 'x-amz-content-sha256' header does not match what was computed.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingDateHeader: {
		Code:           "AccessDenied",
		Description:    "AWS authentication requires a valid Date or x-amz-date header",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRequest: {
		Code:           "InvalidRequest",
		Description:    "Invalid Request",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRange: {
		Code:           "InvalidRange",
		Description:    "The requested range is not satisfiable",
		HTTPStatusCode: http.StatusRequestedRangeNotSatisfiable,
	},
	ErrAuthNotSetup: {
		Code:           "InvalidRequest",
		Description:    "Signed request requires setting up SeaweedFS S3 authentication",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNotImplemented: {
		Code:           "NotImplemented",
		Description:    "A header you provided implies functionality that is not implemented",
		HTTPStatusCode: http.StatusNotImplemented,
	},
	ErrPreconditionFailed: {
		Code:           "PreconditionFailed",
		Description:    "At least one of the pre-conditions you specified did not hold",
		HTTPStatusCode: http.StatusPreconditionFailed,
	},
	ErrNotModified: {
		Code:           "NotModified",
		Description:    "The object was not modified since the specified time",
		HTTPStatusCode: http.StatusNotModified,
	},
	ErrExistingObjectIsDirectory: {
		Code:           "ExistingObjectIsDirectory",
		Description:    "Existing Object is a directory.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrExistingObjectIsFile: {
		Code:           "ExistingObjectIsFile",
		Description:    "Existing Object is a file.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrTooManyRequest: {
		Code:           "ErrTooManyRequest",
		Description:    "Too many simultaneous request count",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrRequestBytesExceed: {
		Code:           "ErrRequestBytesExceed",
		Description:    "Simultaneous request bytes exceed limitations",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrServiceUnavailable: {
		Code:           "ServiceUnavailable",
		Description:    "Service Unavailable",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},

	OwnershipControlsNotFoundError: {
		Code:           "OwnershipControlsNotFoundError",
		Description:    "The bucket ownership controls were not found",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrObjectLockConfigurationNotFoundError: {
		Code:           "ObjectLockConfigurationNotFoundError",
		Description:    "Object Lock configuration does not exist for this bucket",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrInvalidUnorderedWithDelimiter: {
		Code:           "InvalidArgument",
		Description:    "Unordered listing cannot be used with delimiter",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// SSE-C related error mappings
	ErrInvalidEncryptionAlgorithm: {
		Code:           "InvalidEncryptionAlgorithmError",
		Description:    "The encryption algorithm specified is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidEncryptionKey: {
		Code:           "InvalidArgument",
		Description:    "Invalid encryption key. Encryption key must be 256-bit AES256.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSSECustomerKeyMD5Mismatch: {
		Code:           "InvalidArgument",
		Description:    "The provided customer encryption key MD5 does not match the key.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSSECustomerKeyMissing: {
		Code:           "InvalidArgument",
		Description:    "Requests specifying Server Side Encryption with Customer provided keys must provide the customer key.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSSECustomerKeyNotNeeded: {
		Code:           "InvalidArgument",
		Description:    "The object was not encrypted with customer provided keys.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSSEEncryptionTypeMismatch: {
		Code:           "InvalidRequest",
		Description:    "The encryption method specified in the request does not match the encryption method used to encrypt the object.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// SSE-KMS error responses
	ErrKMSKeyNotFound: {
		Code:           "KMSKeyNotFoundException",
		Description:    "The specified KMS key does not exist.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrKMSAccessDenied: {
		Code:           "KMSAccessDeniedException",
		Description:    "Access denied to the specified KMS key.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrKMSDisabled: {
		Code:           "KMSKeyDisabledException",
		Description:    "The specified KMS key is disabled.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrKMSInvalidCiphertext: {
		Code:           "InvalidCiphertext",
		Description:    "The provided ciphertext is invalid or corrupted.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// Bucket encryption error responses
	ErrNoSuchBucketEncryptionConfiguration: {
		Code:           "ServerSideEncryptionConfigurationNotFoundError",
		Description:    "The server side encryption configuration was not found.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrInvalidStorageClass: {
		Code:           "InvalidStorageClass",
		Description:    "The storage class you specified is not valid",
		HTTPStatusCode: http.StatusBadRequest,
	},
}

// GetAPIError provides API Error for input API error code.
func GetAPIError(code ErrorCode) APIError {
	return errorCodeResponse[code]
}
