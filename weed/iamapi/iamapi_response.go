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
	PutUserPolicyResponse        = iamlib.PutUserPolicyResponse
	DeleteUserPolicyResponse     = iamlib.DeleteUserPolicyResponse
	GetUserPolicyResponse        = iamlib.GetUserPolicyResponse
	ErrorResponse                = iamlib.ErrorResponse
	ServiceAccountInfo           = iamlib.ServiceAccountInfo
	CreateServiceAccountResponse = iamlib.CreateServiceAccountResponse
	DeleteServiceAccountResponse = iamlib.DeleteServiceAccountResponse
	ListServiceAccountsResponse  = iamlib.ListServiceAccountsResponse
	GetServiceAccountResponse    = iamlib.GetServiceAccountResponse
	UpdateServiceAccountResponse = iamlib.UpdateServiceAccountResponse
)
