package s3api

import (
	"context"

	"encoding/json"
	"fmt"
	"net/url"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func (s3a *S3ApiServer) executeAction(values url.Values) (interface{}, error) {
	if s3a.embeddedIam == nil {
		return nil, fmt.Errorf("embedded iam is disabled")
	}
	response, iamErr := s3a.embeddedIam.ExecuteAction(values)
	if iamErr != nil {
		return nil, fmt.Errorf("IAM error: %s - %v", iamErr.Code, iamErr.Error)
	}
	return response, nil
}

func (s3a *S3ApiServer) ListUsers(ctx context.Context, req *iam_pb.ListUsersRequest) (*iam_pb.ListUsersResponse, error) {
	values := url.Values{}
	values.Set("Action", "ListUsers")
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	// TODO: map response
	return &iam_pb.ListUsersResponse{}, nil
}

func (s3a *S3ApiServer) CreateUser(ctx context.Context, req *iam_pb.CreateUserRequest) (*iam_pb.CreateUserResponse, error) {
	values := url.Values{}
	values.Set("Action", "CreateUser")
	if req.Identity != nil {
		values.Set("UserName", req.Identity.Name)
	}
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.CreateUserResponse{}, nil
}

func (s3a *S3ApiServer) GetUser(ctx context.Context, req *iam_pb.GetUserRequest) (*iam_pb.GetUserResponse, error) {
	values := url.Values{}
	values.Set("Action", "GetUser")
	values.Set("UserName", req.Username)
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	// TODO: map response
	return &iam_pb.GetUserResponse{}, nil
}

func (s3a *S3ApiServer) UpdateUser(ctx context.Context, req *iam_pb.UpdateUserRequest) (*iam_pb.UpdateUserResponse, error) {
	values := url.Values{}
	values.Set("Action", "UpdateUser")
	values.Set("UserName", req.Username)
	// UpdateUser in DoActions expects "NewUserName" if renaming, but CreateUser just takes UserName.
	// Looking at s3api_embedded_iam.go, UpdateUser uses "NewUserName" to change name.
	if req.Identity != nil && req.Identity.Name != "" {
		values.Set("NewUserName", req.Identity.Name)
	}
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.UpdateUserResponse{}, nil
}

func (s3a *S3ApiServer) DeleteUser(ctx context.Context, req *iam_pb.DeleteUserRequest) (*iam_pb.DeleteUserResponse, error) {
	values := url.Values{}
	values.Set("Action", "DeleteUser")
	values.Set("UserName", req.Username)
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.DeleteUserResponse{}, nil
}

func (s3a *S3ApiServer) ListAccessKeys(ctx context.Context, req *iam_pb.ListAccessKeysRequest) (*iam_pb.ListAccessKeysResponse, error) {
	values := url.Values{}
	values.Set("Action", "ListAccessKeys")
	values.Set("UserName", req.Username)
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	// TODO: map response
	return &iam_pb.ListAccessKeysResponse{}, nil
}

func (s3a *S3ApiServer) CreateAccessKey(ctx context.Context, req *iam_pb.CreateAccessKeyRequest) (*iam_pb.CreateAccessKeyResponse, error) {
	values := url.Values{}
	values.Set("Action", "CreateAccessKey")
	values.Set("UserName", req.Username)
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.CreateAccessKeyResponse{}, nil
}

func (s3a *S3ApiServer) DeleteAccessKey(ctx context.Context, req *iam_pb.DeleteAccessKeyRequest) (*iam_pb.DeleteAccessKeyResponse, error) {
	values := url.Values{}
	values.Set("Action", "DeleteAccessKey")
	values.Set("UserName", req.Username)
	values.Set("AccessKeyId", req.AccessKey)
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.DeleteAccessKeyResponse{}, nil
}

func (s3a *S3ApiServer) PutUserPolicy(ctx context.Context, req *iam_pb.PutUserPolicyRequest) (*iam_pb.PutUserPolicyResponse, error) {
	values := url.Values{}
	values.Set("Action", "PutUserPolicy")
	values.Set("UserName", req.Username)
	values.Set("PolicyName", req.PolicyName)
	values.Set("PolicyDocument", req.PolicyDocument)
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.PutUserPolicyResponse{}, nil
}

func (s3a *S3ApiServer) GetUserPolicy(ctx context.Context, req *iam_pb.GetUserPolicyRequest) (*iam_pb.GetUserPolicyResponse, error) {
	values := url.Values{}
	values.Set("Action", "GetUserPolicy")
	values.Set("UserName", req.Username)
	values.Set("PolicyName", req.PolicyName)
	resp, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	// TODO: map response
	jsonBytes, _ := json.Marshal(resp)
	return &iam_pb.GetUserPolicyResponse{PolicyDocument: string(jsonBytes)}, nil
}

func (s3a *S3ApiServer) DeleteUserPolicy(ctx context.Context, req *iam_pb.DeleteUserPolicyRequest) (*iam_pb.DeleteUserPolicyResponse, error) {
	values := url.Values{}
	values.Set("Action", "DeleteUserPolicy")
	values.Set("UserName", req.Username)
	values.Set("PolicyName", req.PolicyName)
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.DeleteUserPolicyResponse{}, nil
}

func (s3a *S3ApiServer) ListServiceAccounts(ctx context.Context, req *iam_pb.ListServiceAccountsRequest) (*iam_pb.ListServiceAccountsResponse, error) {
	values := url.Values{}
	values.Set("Action", "ListServiceAccounts")
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	// TODO: map response
	return &iam_pb.ListServiceAccountsResponse{}, nil
}

func (s3a *S3ApiServer) CreateServiceAccount(ctx context.Context, req *iam_pb.CreateServiceAccountRequest) (*iam_pb.CreateServiceAccountResponse, error) {
	values := url.Values{}
	values.Set("Action", "CreateServiceAccount")
	if req.ServiceAccount != nil {
		values.Set("CreatedBy", req.ServiceAccount.CreatedBy)
	}
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.CreateServiceAccountResponse{}, nil
}

func (s3a *S3ApiServer) UpdateServiceAccount(ctx context.Context, req *iam_pb.UpdateServiceAccountRequest) (*iam_pb.UpdateServiceAccountResponse, error) {
	values := url.Values{}
	values.Set("Action", "UpdateServiceAccount")
	values.Set("ServiceAccountId", req.Id)
	if req.ServiceAccount != nil {
		values.Set("Status", "Active")
		if req.ServiceAccount.Disabled {
			values.Set("Status", "Inactive")
		}
	}
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.UpdateServiceAccountResponse{}, nil
}

func (s3a *S3ApiServer) DeleteServiceAccount(ctx context.Context, req *iam_pb.DeleteServiceAccountRequest) (*iam_pb.DeleteServiceAccountResponse, error) {
	values := url.Values{}
	values.Set("Action", "DeleteServiceAccount")
	values.Set("ServiceAccountId", req.Id)
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.DeleteServiceAccountResponse{}, nil
}

func (s3a *S3ApiServer) GetServiceAccount(ctx context.Context, req *iam_pb.GetServiceAccountRequest) (*iam_pb.GetServiceAccountResponse, error) {
	values := url.Values{}
	values.Set("Action", "GetServiceAccount")
	values.Set("ServiceAccountId", req.Id)
	_, err := s3a.executeAction(values)
	if err != nil {
		return nil, err
	}
	// TODO: map response
	return &iam_pb.GetServiceAccountResponse{}, nil
}
