package iam

import (
	"encoding/xml"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/iam"
)

// CommonResponse is embedded in all IAM response types to provide RequestId.
type CommonResponse struct {
	ResponseMetadata struct {
		RequestId string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

// SetRequestId sets a unique request ID based on current timestamp.
func (r *CommonResponse) SetRequestId() {
	r.ResponseMetadata.RequestId = fmt.Sprintf("%d", time.Now().UnixNano())
}

// ListUsersResponse is the response for ListUsers action.
type ListUsersResponse struct {
	CommonResponse
	XMLName         xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListUsersResponse"`
	ListUsersResult struct {
		Users       []*iam.User `xml:"Users>member"`
		IsTruncated bool        `xml:"IsTruncated"`
	} `xml:"ListUsersResult"`
}

// ListAccessKeysResponse is the response for ListAccessKeys action.
type ListAccessKeysResponse struct {
	CommonResponse
	XMLName              xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListAccessKeysResponse"`
	ListAccessKeysResult struct {
		AccessKeyMetadata []*iam.AccessKeyMetadata `xml:"AccessKeyMetadata>member"`
		IsTruncated       bool                     `xml:"IsTruncated"`
	} `xml:"ListAccessKeysResult"`
}

// DeleteAccessKeyResponse is the response for DeleteAccessKey action.
type DeleteAccessKeyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteAccessKeyResponse"`
}

// CreatePolicyResponse is the response for CreatePolicy action.
type CreatePolicyResponse struct {
	CommonResponse
	XMLName            xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreatePolicyResponse"`
	CreatePolicyResult struct {
		Policy iam.Policy `xml:"Policy"`
	} `xml:"CreatePolicyResult"`
}

// CreateUserResponse is the response for CreateUser action.
type CreateUserResponse struct {
	CommonResponse
	XMLName          xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateUserResponse"`
	CreateUserResult struct {
		User iam.User `xml:"User"`
	} `xml:"CreateUserResult"`
}

// DeleteUserResponse is the response for DeleteUser action.
type DeleteUserResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteUserResponse"`
}

// GetUserResponse is the response for GetUser action.
type GetUserResponse struct {
	CommonResponse
	XMLName       xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetUserResponse"`
	GetUserResult struct {
		User iam.User `xml:"User"`
	} `xml:"GetUserResult"`
}

// UpdateUserResponse is the response for UpdateUser action.
type UpdateUserResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateUserResponse"`
}

// CreateAccessKeyResponse is the response for CreateAccessKey action.
type CreateAccessKeyResponse struct {
	CommonResponse
	XMLName               xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateAccessKeyResponse"`
	CreateAccessKeyResult struct {
		AccessKey iam.AccessKey `xml:"AccessKey"`
	} `xml:"CreateAccessKeyResult"`
}

// PutUserPolicyResponse is the response for PutUserPolicy action.
type PutUserPolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ PutUserPolicyResponse"`
}

// DeleteUserPolicyResponse is the response for DeleteUserPolicy action.
type DeleteUserPolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteUserPolicyResponse"`
}

// GetUserPolicyResponse is the response for GetUserPolicy action.
type GetUserPolicyResponse struct {
	CommonResponse
	XMLName             xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetUserPolicyResponse"`
	GetUserPolicyResult struct {
		UserName       string `xml:"UserName"`
		PolicyName     string `xml:"PolicyName"`
		PolicyDocument string `xml:"PolicyDocument"`
	} `xml:"GetUserPolicyResult"`
}

// ErrorResponse is the IAM error response format.
type ErrorResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ErrorResponse"`
	Error   struct {
		iam.ErrorDetails
		Type string `xml:"Type"`
	} `xml:"Error"`
}

// Error represents an IAM API error with code and underlying error.
type Error struct {
	Code  string
	Error error
}

// Policies stores IAM policies (used for managed policy storage).
type Policies struct {
	Policies map[string]interface{} `json:"policies"`
}

// SetUserStatusResponse is the response for SetUserStatus action.
// This is a SeaweedFS extension to enable/disable users without deleting them.
type SetUserStatusResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ SetUserStatusResponse"`
}

// UpdateAccessKeyResponse is the response for UpdateAccessKey action.
type UpdateAccessKeyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateAccessKeyResponse"`
}

// ServiceAccountInfo contains service account details for API responses.
type ServiceAccountInfo struct {
	ServiceAccountId string  `xml:"ServiceAccountId"`
	ParentUser       string  `xml:"ParentUser"`
	Description      string  `xml:"Description,omitempty"`
	AccessKeyId      string  `xml:"AccessKeyId"`
	SecretAccessKey  *string `xml:"SecretAccessKey,omitempty"` // Only returned in Create response
	Status           string  `xml:"Status"`
	Expiration       *string `xml:"Expiration,omitempty"` // ISO 8601 format, nil = no expiration
	CreateDate       string  `xml:"CreateDate"`
}

// CreateServiceAccountResponse is the response for CreateServiceAccount action.
type CreateServiceAccountResponse struct {
	CommonResponse
	XMLName                    xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateServiceAccountResponse"`
	CreateServiceAccountResult struct {
		ServiceAccount ServiceAccountInfo `xml:"ServiceAccount"`
	} `xml:"CreateServiceAccountResult"`
}

// DeleteServiceAccountResponse is the response for DeleteServiceAccount action.
type DeleteServiceAccountResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteServiceAccountResponse"`
}

// ListServiceAccountsResponse is the response for ListServiceAccounts action.
type ListServiceAccountsResponse struct {
	CommonResponse
	XMLName                   xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListServiceAccountsResponse"`
	ListServiceAccountsResult struct {
		ServiceAccounts []*ServiceAccountInfo `xml:"ServiceAccounts>member"`
		IsTruncated     bool                  `xml:"IsTruncated"`
	} `xml:"ListServiceAccountsResult"`
}

// GetServiceAccountResponse is the response for GetServiceAccount action.
type GetServiceAccountResponse struct {
	CommonResponse
	XMLName                 xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetServiceAccountResponse"`
	GetServiceAccountResult struct {
		ServiceAccount ServiceAccountInfo `xml:"ServiceAccount"`
	} `xml:"GetServiceAccountResult"`
}

// UpdateServiceAccountResponse is the response for UpdateServiceAccount action.
type UpdateServiceAccountResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateServiceAccountResponse"`
}

// AttachUserPolicyResponse is the response for AttachUserPolicy action.
type AttachUserPolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ AttachUserPolicyResponse"`
}

// DetachUserPolicyResponse is the response for DetachUserPolicy action.
type DetachUserPolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DetachUserPolicyResponse"`
}

// AttachedPolicy represents a managed policy attached to a user.
type AttachedPolicy struct {
	PolicyName string `xml:"PolicyName"`
	PolicyArn  string `xml:"PolicyArn"`
}

// ListAttachedUserPoliciesResponse is the response for ListAttachedUserPolicies action.
type ListAttachedUserPoliciesResponse struct {
	CommonResponse
	XMLName                        xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListAttachedUserPoliciesResponse"`
	ListAttachedUserPoliciesResult struct {
		AttachedPolicies []*AttachedPolicy `xml:"AttachedPolicies>member"`
		IsTruncated      bool              `xml:"IsTruncated"`
	} `xml:"ListAttachedUserPoliciesResult"`
}
