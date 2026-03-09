package iam

import (
	"encoding/xml"

	"github.com/aws/aws-sdk-go/service/iam"
)

// CommonResponse is embedded in IAM success response types to provide RequestId.
type CommonResponse struct {
	ResponseMetadata struct {
		RequestId string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

// SetRequestId stores the request ID generated for the current HTTP request.
func (r *CommonResponse) SetRequestId(requestID string) {
	r.ResponseMetadata.RequestId = requestID
}

// RequestIDSetter is implemented by IAM responses that can carry a RequestId.
type RequestIDSetter interface {
	SetRequestId(string)
}

// ListUsersResponse is the response for ListUsers action.
type ListUsersResponse struct {
	XMLName         xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListUsersResponse"`
	ListUsersResult struct {
		Users       []*iam.User `xml:"Users>member"`
		IsTruncated bool        `xml:"IsTruncated"`
	} `xml:"ListUsersResult"`
	CommonResponse
}

// ListAccessKeysResponse is the response for ListAccessKeys action.
type ListAccessKeysResponse struct {
	XMLName              xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListAccessKeysResponse"`
	ListAccessKeysResult struct {
		AccessKeyMetadata []*iam.AccessKeyMetadata `xml:"AccessKeyMetadata>member"`
		IsTruncated       bool                     `xml:"IsTruncated"`
	} `xml:"ListAccessKeysResult"`
	CommonResponse
}

// DeleteAccessKeyResponse is the response for DeleteAccessKey action.
type DeleteAccessKeyResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteAccessKeyResponse"`
	CommonResponse
}

// CreatePolicyResponse is the response for CreatePolicy action.
type CreatePolicyResponse struct {
	XMLName            xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreatePolicyResponse"`
	CreatePolicyResult struct {
		Policy iam.Policy `xml:"Policy"`
	} `xml:"CreatePolicyResult"`
	CommonResponse
}

// DeletePolicyResponse is the response for DeletePolicy action.
type DeletePolicyResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeletePolicyResponse"`
	CommonResponse
}

// ListPoliciesResponse is the response for ListPolicies action.
type ListPoliciesResponse struct {
	XMLName            xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListPoliciesResponse"`
	ListPoliciesResult struct {
		Policies    []*iam.Policy `xml:"Policies>member"`
		IsTruncated bool          `xml:"IsTruncated"`
		Marker      string        `xml:"Marker,omitempty"`
	} `xml:"ListPoliciesResult"`
	CommonResponse
}

// GetPolicyResponse is the response for GetPolicy action.
type GetPolicyResponse struct {
	XMLName         xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetPolicyResponse"`
	GetPolicyResult struct {
		Policy iam.Policy `xml:"Policy"`
	} `xml:"GetPolicyResult"`
	CommonResponse
}

// ListPolicyVersionsResponse is the response for ListPolicyVersions action.
type ListPolicyVersionsResponse struct {
	XMLName                  xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListPolicyVersionsResponse"`
	ListPolicyVersionsResult struct {
		Versions    []*iam.PolicyVersion `xml:"Versions>member"`
		IsTruncated bool                 `xml:"IsTruncated"`
		Marker      string               `xml:"Marker,omitempty"`
	} `xml:"ListPolicyVersionsResult"`
	CommonResponse
}

// GetPolicyVersionResponse is the response for GetPolicyVersion action.
type GetPolicyVersionResponse struct {
	XMLName                xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetPolicyVersionResponse"`
	GetPolicyVersionResult struct {
		PolicyVersion iam.PolicyVersion `xml:"PolicyVersion"`
	} `xml:"GetPolicyVersionResult"`
	CommonResponse
}

// CreateUserResponse is the response for CreateUser action.
type CreateUserResponse struct {
	XMLName          xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateUserResponse"`
	CreateUserResult struct {
		User iam.User `xml:"User"`
	} `xml:"CreateUserResult"`
	CommonResponse
}

// DeleteUserResponse is the response for DeleteUser action.
type DeleteUserResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteUserResponse"`
	CommonResponse
}

// GetUserResponse is the response for GetUser action.
type GetUserResponse struct {
	XMLName       xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetUserResponse"`
	GetUserResult struct {
		User iam.User `xml:"User"`
	} `xml:"GetUserResult"`
	CommonResponse
}

// UpdateUserResponse is the response for UpdateUser action.
type UpdateUserResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateUserResponse"`
	CommonResponse
}

// CreateAccessKeyResponse is the response for CreateAccessKey action.
type CreateAccessKeyResponse struct {
	XMLName               xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateAccessKeyResponse"`
	CreateAccessKeyResult struct {
		AccessKey iam.AccessKey `xml:"AccessKey"`
	} `xml:"CreateAccessKeyResult"`
	CommonResponse
}

// PutUserPolicyResponse is the response for PutUserPolicy action.
type PutUserPolicyResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ PutUserPolicyResponse"`
	CommonResponse
}

// DeleteUserPolicyResponse is the response for DeleteUserPolicy action.
type DeleteUserPolicyResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteUserPolicyResponse"`
	CommonResponse
}

// AttachUserPolicyResponse is the response for AttachUserPolicy action.
type AttachUserPolicyResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ AttachUserPolicyResponse"`
	CommonResponse
}

// DetachUserPolicyResponse is the response for DetachUserPolicy action.
type DetachUserPolicyResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DetachUserPolicyResponse"`
	CommonResponse
}

// ListAttachedUserPoliciesResponse is the response for ListAttachedUserPolicies action.
type ListAttachedUserPoliciesResponse struct {
	XMLName                        xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListAttachedUserPoliciesResponse"`
	ListAttachedUserPoliciesResult struct {
		AttachedPolicies []*iam.AttachedPolicy `xml:"AttachedPolicies>member"`
		IsTruncated      bool                  `xml:"IsTruncated"`
		Marker           string                `xml:"Marker,omitempty"`
	} `xml:"ListAttachedUserPoliciesResult"`
	CommonResponse
}

// GetUserPolicyResponse is the response for GetUserPolicy action.
type GetUserPolicyResponse struct {
	XMLName             xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetUserPolicyResponse"`
	GetUserPolicyResult struct {
		UserName       string `xml:"UserName"`
		PolicyName     string `xml:"PolicyName"`
		PolicyDocument string `xml:"PolicyDocument"`
	} `xml:"GetUserPolicyResult"`
	CommonResponse
}

// ErrorResponse is the IAM error response format.
// AWS IAM uses a bare <RequestId> at root level for errors, not <ResponseMetadata>.
type ErrorResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ErrorResponse"`
	Error   struct {
		iam.ErrorDetails
		Type string `xml:"Type"`
	} `xml:"Error"`
	RequestId string `xml:"RequestId"`
}

// SetRequestId stores the request ID generated for the current HTTP request.
func (r *ErrorResponse) SetRequestId(requestID string) {
	r.RequestId = requestID
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
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ SetUserStatusResponse"`
	CommonResponse
}

// UpdateAccessKeyResponse is the response for UpdateAccessKey action.
type UpdateAccessKeyResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateAccessKeyResponse"`
	CommonResponse
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
	XMLName                    xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateServiceAccountResponse"`
	CreateServiceAccountResult struct {
		ServiceAccount ServiceAccountInfo `xml:"ServiceAccount"`
	} `xml:"CreateServiceAccountResult"`
	CommonResponse
}

// DeleteServiceAccountResponse is the response for DeleteServiceAccount action.
type DeleteServiceAccountResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteServiceAccountResponse"`
	CommonResponse
}

// ListServiceAccountsResponse is the response for ListServiceAccounts action.
type ListServiceAccountsResponse struct {
	XMLName                   xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListServiceAccountsResponse"`
	ListServiceAccountsResult struct {
		ServiceAccounts []*ServiceAccountInfo `xml:"ServiceAccounts>member"`
		IsTruncated     bool                  `xml:"IsTruncated"`
	} `xml:"ListServiceAccountsResult"`
	CommonResponse
}

// GetServiceAccountResponse is the response for GetServiceAccount action.
type GetServiceAccountResponse struct {
	XMLName                 xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetServiceAccountResponse"`
	GetServiceAccountResult struct {
		ServiceAccount ServiceAccountInfo `xml:"ServiceAccount"`
	} `xml:"GetServiceAccountResult"`
	CommonResponse
}

// UpdateServiceAccountResponse is the response for UpdateServiceAccount action.
type UpdateServiceAccountResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateServiceAccountResponse"`
	CommonResponse
}

// CreateGroupResponse is the response for CreateGroup action.
type CreateGroupResponse struct {
	XMLName           xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateGroupResponse"`
	CreateGroupResult struct {
		Group iam.Group `xml:"Group"`
	} `xml:"CreateGroupResult"`
	CommonResponse
}

// DeleteGroupResponse is the response for DeleteGroup action.
type DeleteGroupResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteGroupResponse"`
	CommonResponse
}

// UpdateGroupResponse is the response for UpdateGroup action.
type UpdateGroupResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateGroupResponse"`
	CommonResponse
}

// GetGroupResponse is the response for GetGroup action.
type GetGroupResponse struct {
	XMLName        xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetGroupResponse"`
	GetGroupResult struct {
		Group       iam.Group   `xml:"Group"`
		Users       []*iam.User `xml:"Users>member"`
		IsTruncated bool        `xml:"IsTruncated"`
		Marker      string      `xml:"Marker"`
	} `xml:"GetGroupResult"`
	CommonResponse
}

// ListGroupsResponse is the response for ListGroups action.
type ListGroupsResponse struct {
	XMLName          xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListGroupsResponse"`
	ListGroupsResult struct {
		Groups      []*iam.Group `xml:"Groups>member"`
		IsTruncated bool         `xml:"IsTruncated"`
		Marker      string       `xml:"Marker"`
	} `xml:"ListGroupsResult"`
	CommonResponse
}

// AddUserToGroupResponse is the response for AddUserToGroup action.
type AddUserToGroupResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ AddUserToGroupResponse"`
	CommonResponse
}

// RemoveUserFromGroupResponse is the response for RemoveUserFromGroup action.
type RemoveUserFromGroupResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ RemoveUserFromGroupResponse"`
	CommonResponse
}

// AttachGroupPolicyResponse is the response for AttachGroupPolicy action.
type AttachGroupPolicyResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ AttachGroupPolicyResponse"`
	CommonResponse
}

// DetachGroupPolicyResponse is the response for DetachGroupPolicy action.
type DetachGroupPolicyResponse struct {
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DetachGroupPolicyResponse"`
	CommonResponse
}

// ListAttachedGroupPoliciesResponse is the response for ListAttachedGroupPolicies action.
type ListAttachedGroupPoliciesResponse struct {
	XMLName                         xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListAttachedGroupPoliciesResponse"`
	ListAttachedGroupPoliciesResult struct {
		AttachedPolicies []*iam.AttachedPolicy `xml:"AttachedPolicies>member"`
		IsTruncated      bool                  `xml:"IsTruncated"`
		Marker           string                `xml:"Marker"`
	} `xml:"ListAttachedGroupPoliciesResult"`
	CommonResponse
}

// ListGroupsForUserResponse is the response for ListGroupsForUser action.
type ListGroupsForUserResponse struct {
	XMLName                  xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListGroupsForUserResponse"`
	ListGroupsForUserResult struct {
		Groups      []*iam.Group `xml:"Groups>member"`
		IsTruncated bool         `xml:"IsTruncated"`
		Marker      string       `xml:"Marker"`
	} `xml:"ListGroupsForUserResult"`
	CommonResponse
}
