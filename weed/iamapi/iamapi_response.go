package iamapi

import (
	"encoding/xml"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/iam"
)

type CommonResponse struct {
	ResponseMetadata struct {
		RequestId string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

type ListUsersResponse struct {
	CommonResponse
	XMLName         xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListUsersResponse"`
	ListUsersResult struct {
		Users       []*iam.User `xml:"Users>member"`
		IsTruncated bool        `xml:"IsTruncated"`
	} `xml:"ListUsersResult"`
}

type ListAccessKeysResponse struct {
	CommonResponse
	XMLName              xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListAccessKeysResponse"`
	ListAccessKeysResult struct {
		AccessKeyMetadata []*iam.AccessKeyMetadata `xml:"AccessKeyMetadata>member"`
		IsTruncated       bool                     `xml:"IsTruncated"`
	} `xml:"ListAccessKeysResult"`
}

type DeleteAccessKeyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteAccessKeyResponse"`
}

type UpdateAccessKeyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateAccessKeyResponse"`
}

type GetAccessKeyLastUsedResponse struct {
	CommonResponse
	XMLName                xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetAccessKeyLastUsedResponse"`
	GetAccessKeyLastUsedResult struct {
		UserName         *string              `xml:"UserName"`
		AccessKeyLastUsed *iam.AccessKeyLastUsed `xml:"AccessKeyLastUsed"`
	} `xml:"GetAccessKeyLastUsedResult"`
}

type CreatePolicyResponse struct {
	CommonResponse
	XMLName            xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreatePolicyResponse"`
	CreatePolicyResult struct {
		Policy iam.Policy `xml:"Policy"`
	} `xml:"CreatePolicyResult"`
}

type CreateUserResponse struct {
	CommonResponse
	XMLName          xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateUserResponse"`
	CreateUserResult struct {
		User iam.User `xml:"User"`
	} `xml:"CreateUserResult"`
}

type DeleteUserResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteUserResponse"`
}

type GetUserResponse struct {
	CommonResponse
	XMLName       xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetUserResponse"`
	GetUserResult struct {
		User iam.User `xml:"User"`
	} `xml:"GetUserResult"`
}

type UpdateUserResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateUserResponse"`
}

type CreateAccessKeyResponse struct {
	CommonResponse
	XMLName               xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateAccessKeyResponse"`
	CreateAccessKeyResult struct {
		AccessKey iam.AccessKey `xml:"AccessKey"`
	} `xml:"CreateAccessKeyResult"`
}

type PutUserPolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ PutUserPolicyResponse"`
}

type GetUserPolicyResponse struct {
	CommonResponse
	XMLName             xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetUserPolicyResponse"`
	GetUserPolicyResult struct {
		UserName       string `xml:"UserName"`
		PolicyName     string `xml:"PolicyName"`
		PolicyDocument string `xml:"PolicyDocument"`
	} `xml:"GetUserPolicyResult"`
}

type ErrorResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ErrorResponse"`
	Error   struct {
		iam.ErrorDetails
		Type string `xml:"Type"`
	} `xml:"Error"`
}

func (r *CommonResponse) SetRequestId() {
	r.ResponseMetadata.RequestId = fmt.Sprintf("%d", time.Now().UnixNano())
}

type CreateRoleResponse struct {
	CommonResponse
	XMLName          xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateRoleResponse"`
	CreateRoleResult struct {
		Role iam.Role `xml:"Role"`
	} `xml:"CreateRoleResult"`
}

type GetRoleResponse struct {
	CommonResponse
	XMLName       xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetRoleResponse"`
	GetRoleResult struct {
		Role iam.Role `xml:"Role"`
	} `xml:"GetRoleResult"`
}

type ListRolesResponse struct {
	CommonResponse
	XMLName         xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListRolesResponse"`
	ListRolesResult struct {
		Roles       []*iam.Role `xml:"Roles>member"`
		IsTruncated bool        `xml:"IsTruncated"`
		Marker      string      `xml:"Marker,omitempty"`
	} `xml:"ListRolesResult"`
}

type DeleteRoleResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteRoleResponse"`
}

type AttachRolePolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ AttachRolePolicyResponse"`
}

type DetachRolePolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DetachRolePolicyResponse"`
}

type ListAttachedRolePoliciesResponse struct {
	CommonResponse
	XMLName                        xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListAttachedRolePoliciesResponse"`
	ListAttachedRolePoliciesResult struct {
		AttachedPolicies []*iam.AttachedPolicy `xml:"AttachedPolicies>member"`
		IsTruncated      bool                  `xml:"IsTruncated"`
		Marker           string                `xml:"Marker,omitempty"`
	} `xml:"ListAttachedRolePoliciesResult"`
}

// UpdateRoleResponse for UpdateRole API
type UpdateRoleResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateRoleResponse"`
	UpdateRoleResult struct{} `xml:"UpdateRoleResult"`
}

// UpdateAssumeRolePolicyResponse for UpdateAssumeRolePolicy API
type UpdateAssumeRolePolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateAssumeRolePolicyResponse"`
}

// PutRolePolicyResponse for PutRolePolicy API
type PutRolePolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ PutRolePolicyResponse"`
}

// GetRolePolicyResponse for GetRolePolicy API
type GetRolePolicyResponse struct {
	CommonResponse
	XMLName             xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetRolePolicyResponse"`
	GetRolePolicyResult struct {
		RoleName       string `xml:"RoleName"`
		PolicyName     string `xml:"PolicyName"`
		PolicyDocument string `xml:"PolicyDocument"`
	} `xml:"GetRolePolicyResult"`
}

// DeleteRolePolicyResponse for DeleteRolePolicy API
type DeleteRolePolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteRolePolicyResponse"`
}

// ListRolePoliciesResponse for ListRolePolicies API
type ListRolePoliciesResponse struct {
	CommonResponse
	XMLName                xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListRolePoliciesResponse"`
	ListRolePoliciesResult struct {
		PolicyNames []*string `xml:"PolicyNames>member"`
		IsTruncated bool      `xml:"IsTruncated"`
		Marker      string    `xml:"Marker,omitempty"`
	} `xml:"ListRolePoliciesResult"`
}

// GetPolicyResponse for GetPolicy API
type GetPolicyResponse struct {
	CommonResponse
	XMLName        xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetPolicyResponse"`
	GetPolicyResult struct {
		Policy iam.Policy `xml:"Policy"`
	} `xml:"GetPolicyResult"`
}

// GetPolicyVersionResponse for GetPolicyVersion API
type GetPolicyVersionResponse struct {
	CommonResponse
	XMLName                xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetPolicyVersionResponse"`
	GetPolicyVersionResult struct {
		PolicyVersion iam.PolicyVersion `xml:"PolicyVersion"`
	} `xml:"GetPolicyVersionResult"`
}

type CreatePolicyVersionResult struct {
	PolicyVersion iam.PolicyVersion `xml:"PolicyVersion"`
}

type CreatePolicyVersionResponse struct {
	CommonResponse
	XMLName                   xml.Name                  `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreatePolicyVersionResponse"`
	CreatePolicyVersionResult CreatePolicyVersionResult `xml:"CreatePolicyVersionResult"`
}

// DeletePolicyResponse for DeletePolicy API
type DeletePolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeletePolicyResponse"`
}

// ListPoliciesResponse for ListPolicies API
type ListPoliciesResponse struct {
	CommonResponse
	XMLName           xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListPoliciesResponse"`
	ListPoliciesResult struct {
		Policies    []*iam.Policy `xml:"Policies>member"`
		IsTruncated bool          `xml:"IsTruncated"`
		Marker      string        `xml:"Marker,omitempty"`
	} `xml:"ListPoliciesResult"`
}

// CreateGroupResponse for CreateGroup API
type CreateGroupResponse struct {
	CommonResponse
	XMLName          xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateGroupResponse"`
	CreateGroupResult struct {
		Group iam.Group `xml:"Group"`
	} `xml:"CreateGroupResult"`
}

// GetGroupResponse for GetGroup API
type GetGroupResponse struct {
	CommonResponse
	XMLName       xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetGroupResponse"`
	GetGroupResult struct {
		Group       iam.Group   `xml:"Group"`
		Users       []*iam.User `xml:"Users>member"`
		IsTruncated bool        `xml:"IsTruncated"`
		Marker      string      `xml:"Marker,omitempty"`
	} `xml:"GetGroupResult"`
}

// UpdateGroupResponse for UpdateGroup API
type UpdateGroupResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateGroupResponse"`
}

// DeleteGroupResponse for DeleteGroup API
type DeleteGroupResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteGroupResponse"`
}

// ListGroupsResponse for ListGroups API
type ListGroupsResponse struct {
	CommonResponse
	XMLName         xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListGroupsResponse"`
	ListGroupsResult struct {
		Groups      []*iam.Group `xml:"Groups>member"`
		IsTruncated bool         `xml:"IsTruncated"`
		Marker      string       `xml:"Marker,omitempty"`
	} `xml:"ListGroupsResult"`
}

// AddUserToGroupResponse for AddUserToGroup API
type AddUserToGroupResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ AddUserToGroupResponse"`
}

// RemoveUserFromGroupResponse for RemoveUserFromGroup API
type RemoveUserFromGroupResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ RemoveUserFromGroupResponse"`
}

// ListGroupsForUserResponse for ListGroupsForUser API
type ListGroupsForUserResponse struct {
	CommonResponse
	XMLName               xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListGroupsForUserResponse"`
	ListGroupsForUserResult struct {
		Groups      []*iam.Group `xml:"Groups>member"`
		IsTruncated bool         `xml:"IsTruncated"`
		Marker      string       `xml:"Marker,omitempty"`
	} `xml:"ListGroupsForUserResult"`
}

// AttachGroupPolicyResponse for AttachGroupPolicy API
type AttachGroupPolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ AttachGroupPolicyResponse"`
}

// DetachGroupPolicyResponse for DetachGroupPolicy API
type DetachGroupPolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DetachGroupPolicyResponse"`
}

// ListAttachedGroupPoliciesResponse for ListAttachedGroupPolicies API
type ListAttachedGroupPoliciesResponse struct {
	CommonResponse
	XMLName                         xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListAttachedGroupPoliciesResponse"`
	ListAttachedGroupPoliciesResult struct {
		AttachedPolicies []*iam.AttachedPolicy `xml:"AttachedPolicies>member"`
		IsTruncated      bool                  `xml:"IsTruncated"`
		Marker           string                `xml:"Marker,omitempty"`
	} `xml:"ListAttachedGroupPoliciesResult"`
}

