package iamapi

// This file re-exports IAM response types from the shared weed/iam package
// for backwards compatibility with existing code.

import (
	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
)

// Type aliases for IAM response types from shared package
type (
	CommonResponse               = iamlib.CommonResponse
	ListUsersResponse            = iamlib.ListUsersResponse
	ListAccessKeysResponse       = iamlib.ListAccessKeysResponse
	DeleteAccessKeyResponse      = iamlib.DeleteAccessKeyResponse
	CreatePolicyResponse         = iamlib.CreatePolicyResponse
	CreateUserResponse           = iamlib.CreateUserResponse
	DeleteUserResponse           = iamlib.DeleteUserResponse
	GetUserResponse              = iamlib.GetUserResponse
	UpdateUserResponse           = iamlib.UpdateUserResponse
	CreateAccessKeyResponse      = iamlib.CreateAccessKeyResponse
	UpdateAccessKeyResponse      = iamlib.UpdateAccessKeyResponse
	PutUserPolicyResponse        = iamlib.PutUserPolicyResponse
	DeleteUserPolicyResponse     = iamlib.DeleteUserPolicyResponse
	GetUserPolicyResponse            = iamlib.GetUserPolicyResponse
	GetPolicyResponse                = iamlib.GetPolicyResponse
	DeletePolicyResponse             = iamlib.DeletePolicyResponse
	ListPoliciesResponse             = iamlib.ListPoliciesResponse
	AttachUserPolicyResponse         = iamlib.AttachUserPolicyResponse
	DetachUserPolicyResponse         = iamlib.DetachUserPolicyResponse
	ListAttachedUserPoliciesResponse = iamlib.ListAttachedUserPoliciesResponse
	ErrorResponse                    = iamlib.ErrorResponse
	ServiceAccountInfo           = iamlib.ServiceAccountInfo
	CreateServiceAccountResponse = iamlib.CreateServiceAccountResponse
	DeleteServiceAccountResponse = iamlib.DeleteServiceAccountResponse
	ListServiceAccountsResponse  = iamlib.ListServiceAccountsResponse
	GetServiceAccountResponse    = iamlib.GetServiceAccountResponse
	UpdateServiceAccountResponse = iamlib.UpdateServiceAccountResponse
	// Group response types
	CreateGroupResponse               = iamlib.CreateGroupResponse
	DeleteGroupResponse               = iamlib.DeleteGroupResponse
	UpdateGroupResponse               = iamlib.UpdateGroupResponse
	GetGroupResponse                  = iamlib.GetGroupResponse
	ListGroupsResponse                = iamlib.ListGroupsResponse
	AddUserToGroupResponse            = iamlib.AddUserToGroupResponse
	RemoveUserFromGroupResponse       = iamlib.RemoveUserFromGroupResponse
	AttachGroupPolicyResponse         = iamlib.AttachGroupPolicyResponse
	DetachGroupPolicyResponse         = iamlib.DetachGroupPolicyResponse
	ListAttachedGroupPoliciesResponse = iamlib.ListAttachedGroupPoliciesResponse
	ListGroupsForUserResponse         = iamlib.ListGroupsForUserResponse
)
