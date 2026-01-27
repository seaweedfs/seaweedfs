package s3api

import (
	"context"
	"encoding/json"

	"fmt"
	"net/url"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"google.golang.org/grpc/metadata"
)

func (s3a *S3ApiServer) executeAction(ctx context.Context, values url.Values) (interface{}, error) {
	if s3a.embeddedIam == nil {
		return nil, fmt.Errorf("embedded iam is disabled")
	}

	skipPersist := false
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("is-propagation"); len(vals) > 0 && vals[0] == "true" {
			skipPersist = true
		}
	}

	response, iamErr := s3a.embeddedIam.ExecuteAction(values, skipPersist)
	if iamErr != nil {
		return nil, fmt.Errorf("IAM error: %s - %v", iamErr.Code, iamErr.Error)
	}
	return response, nil
}

func (s3a *S3ApiServer) ListUsers(ctx context.Context, req *iam_pb.ListUsersRequest) (*iam_pb.ListUsersResponse, error) {
	values := url.Values{}
	values.Set("Action", "ListUsers")
	resp, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	iamResp, ok := resp.(iamListUsersResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected IAM ListUsers response type %T", resp)
	}
	var usernames []string
	for _, user := range iamResp.ListUsersResult.Users {
		if user != nil && user.UserName != nil {
			usernames = append(usernames, *user.UserName)
		}
	}
	return &iam_pb.ListUsersResponse{Usernames: usernames}, nil
}

func (s3a *S3ApiServer) CreateUser(ctx context.Context, req *iam_pb.CreateUserRequest) (*iam_pb.CreateUserResponse, error) {
	if req.Identity == nil || req.Identity.Name == "" {
		return nil, fmt.Errorf("username name is required")
	}
	values := url.Values{}
	values.Set("Action", "CreateUser")
	values.Set("UserName", req.Identity.Name)
	_, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.CreateUserResponse{}, nil
}

func (s3a *S3ApiServer) GetUser(ctx context.Context, req *iam_pb.GetUserRequest) (*iam_pb.GetUserResponse, error) {
	if req.Username == "" {
		return nil, fmt.Errorf("username is required")
	}
	values := url.Values{}
	values.Set("Action", "GetUser")
	values.Set("UserName", req.Username)
	resp, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	iamResp, ok := resp.(iamGetUserResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected IAM GetUser response type %T", resp)
	}

	var username string
	if iamResp.GetUserResult.User.UserName != nil {
		username = *iamResp.GetUserResult.User.UserName
	}

	return &iam_pb.GetUserResponse{
		Identity: &iam_pb.Identity{
			Name: username,
		},
	}, nil
}

func (s3a *S3ApiServer) UpdateUser(ctx context.Context, req *iam_pb.UpdateUserRequest) (*iam_pb.UpdateUserResponse, error) {
	if req.Username == "" {
		return nil, fmt.Errorf("username is required")
	}
	values := url.Values{}
	values.Set("Action", "UpdateUser")
	values.Set("UserName", req.Username)
	// UpdateUser in DoActions expects "NewUserName" if renaming, but CreateUser just takes UserName.
	// Looking at s3api_embedded_iam.go, UpdateUser uses "NewUserName" to change name.
	if req.Identity != nil && req.Identity.Name != "" {
		values.Set("NewUserName", req.Identity.Name)
	}
	_, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.UpdateUserResponse{}, nil
}

func (s3a *S3ApiServer) DeleteUser(ctx context.Context, req *iam_pb.DeleteUserRequest) (*iam_pb.DeleteUserResponse, error) {
	if req.Username == "" {
		return nil, fmt.Errorf("username is required")
	}
	values := url.Values{}
	values.Set("Action", "DeleteUser")
	values.Set("UserName", req.Username)
	_, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.DeleteUserResponse{}, nil
}

func (s3a *S3ApiServer) ListAccessKeys(ctx context.Context, req *iam_pb.ListAccessKeysRequest) (*iam_pb.ListAccessKeysResponse, error) {
	if req.Username == "" {
		return nil, fmt.Errorf("username is required")
	}
	values := url.Values{}
	values.Set("Action", "ListAccessKeys")
	values.Set("UserName", req.Username)
	resp, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	iamResp, ok := resp.(iamListAccessKeysResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected IAM ListAccessKeys response type %T", resp)
	}
	var accessKeys []*iam_pb.Credential
	for _, meta := range iamResp.ListAccessKeysResult.AccessKeyMetadata {
		if meta != nil && meta.AccessKeyId != nil && meta.Status != nil {
			accessKeys = append(accessKeys, &iam_pb.Credential{
				AccessKey: *meta.AccessKeyId,
				Status:    *meta.Status,
			})
		}
	}
	return &iam_pb.ListAccessKeysResponse{AccessKeys: accessKeys}, nil
}

func (s3a *S3ApiServer) CreateAccessKey(ctx context.Context, req *iam_pb.CreateAccessKeyRequest) (*iam_pb.CreateAccessKeyResponse, error) {
	if req.Username == "" {
		return nil, fmt.Errorf("username is required")
	}
	values := url.Values{}
	values.Set("Action", "CreateAccessKey")
	values.Set("UserName", req.Username)
	_, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.CreateAccessKeyResponse{}, nil
}

func (s3a *S3ApiServer) DeleteAccessKey(ctx context.Context, req *iam_pb.DeleteAccessKeyRequest) (*iam_pb.DeleteAccessKeyResponse, error) {
	if req.Username == "" {
		return nil, fmt.Errorf("username is required")
	}
	if req.AccessKey == "" {
		return nil, fmt.Errorf("access key is required")
	}
	values := url.Values{}
	values.Set("Action", "DeleteAccessKey")
	values.Set("UserName", req.Username)
	values.Set("AccessKeyId", req.AccessKey)
	_, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.DeleteAccessKeyResponse{}, nil
}

func (s3a *S3ApiServer) PutUserPolicy(ctx context.Context, req *iam_pb.PutUserPolicyRequest) (*iam_pb.PutUserPolicyResponse, error) {
	if req.Username == "" {
		return nil, fmt.Errorf("username is required")
	}
	if req.PolicyName == "" {
		return nil, fmt.Errorf("policy name is required")
	}
	values := url.Values{}
	values.Set("Action", "PutUserPolicy")
	values.Set("UserName", req.Username)
	values.Set("PolicyName", req.PolicyName)
	values.Set("PolicyDocument", req.PolicyDocument)
	_, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.PutUserPolicyResponse{}, nil
}

func (s3a *S3ApiServer) GetUserPolicy(ctx context.Context, req *iam_pb.GetUserPolicyRequest) (*iam_pb.GetUserPolicyResponse, error) {
	if req.Username == "" {
		return nil, fmt.Errorf("username is required")
	}
	if req.PolicyName == "" {
		return nil, fmt.Errorf("policy name is required")
	}
	values := url.Values{}
	values.Set("Action", "GetUserPolicy")
	values.Set("UserName", req.Username)
	values.Set("PolicyName", req.PolicyName)
	resp, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	iamResp, ok := resp.(iamGetUserPolicyResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected IAM GetUserPolicy response type %T", resp)
	}
	return &iam_pb.GetUserPolicyResponse{
		Username:       iamResp.GetUserPolicyResult.UserName,
		PolicyName:     iamResp.GetUserPolicyResult.PolicyName,
		PolicyDocument: iamResp.GetUserPolicyResult.PolicyDocument,
	}, nil
}

func (s3a *S3ApiServer) DeleteUserPolicy(ctx context.Context, req *iam_pb.DeleteUserPolicyRequest) (*iam_pb.DeleteUserPolicyResponse, error) {
	if req.Username == "" {
		return nil, fmt.Errorf("username is required")
	}
	if req.PolicyName == "" {
		return nil, fmt.Errorf("policy name is required")
	}
	values := url.Values{}
	values.Set("Action", "DeleteUserPolicy")
	values.Set("UserName", req.Username)
	values.Set("PolicyName", req.PolicyName)
	_, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.DeleteUserPolicyResponse{}, nil
}

func (s3a *S3ApiServer) ListServiceAccounts(ctx context.Context, req *iam_pb.ListServiceAccountsRequest) (*iam_pb.ListServiceAccountsResponse, error) {
	values := url.Values{}
	values.Set("Action", "ListServiceAccounts")
	resp, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	iamResp, ok := resp.(iamListServiceAccountsResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected IAM ListServiceAccounts response type %T", resp)
	}
	var serviceAccounts []*iam_pb.ServiceAccount
	for _, sa := range iamResp.ListServiceAccountsResult.ServiceAccounts {
		if sa != nil {
			serviceAccounts = append(serviceAccounts, &iam_pb.ServiceAccount{
				Id:          sa.ServiceAccountId,
				ParentUser:  sa.ParentUser,
				Description: sa.Description,
				Credential: &iam_pb.Credential{
					AccessKey: sa.AccessKeyId,
					Status:    sa.Status,
				},
			})
		}
	}
	return &iam_pb.ListServiceAccountsResponse{ServiceAccounts: serviceAccounts}, nil
}

func (s3a *S3ApiServer) CreateServiceAccount(ctx context.Context, req *iam_pb.CreateServiceAccountRequest) (*iam_pb.CreateServiceAccountResponse, error) {
	if req.ServiceAccount == nil || req.ServiceAccount.CreatedBy == "" {
		return nil, fmt.Errorf("service account owner is required")
	}
	values := url.Values{}
	values.Set("Action", "CreateServiceAccount")
	values.Set("CreatedBy", req.ServiceAccount.CreatedBy)
	_, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.CreateServiceAccountResponse{}, nil
}

func (s3a *S3ApiServer) UpdateServiceAccount(ctx context.Context, req *iam_pb.UpdateServiceAccountRequest) (*iam_pb.UpdateServiceAccountResponse, error) {
	if req.Id == "" {
		return nil, fmt.Errorf("service account id is required")
	}
	values := url.Values{}
	values.Set("Action", "UpdateServiceAccount")
	values.Set("ServiceAccountId", req.Id)
	if req.ServiceAccount != nil && req.ServiceAccount.Disabled {
		values.Set("Status", "Inactive")
	}
	_, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.UpdateServiceAccountResponse{}, nil
}

func (s3a *S3ApiServer) DeleteServiceAccount(ctx context.Context, req *iam_pb.DeleteServiceAccountRequest) (*iam_pb.DeleteServiceAccountResponse, error) {
	if req.Id == "" {
		return nil, fmt.Errorf("service account id is required")
	}
	values := url.Values{}
	values.Set("Action", "DeleteServiceAccount")
	values.Set("ServiceAccountId", req.Id)
	_, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	return &iam_pb.DeleteServiceAccountResponse{}, nil
}

func (s3a *S3ApiServer) GetServiceAccount(ctx context.Context, req *iam_pb.GetServiceAccountRequest) (*iam_pb.GetServiceAccountResponse, error) {
	if req.Id == "" {
		return nil, fmt.Errorf("service account id is required")
	}
	values := url.Values{}
	values.Set("Action", "GetServiceAccount")
	values.Set("ServiceAccountId", req.Id)
	resp, err := s3a.executeAction(ctx, values)
	if err != nil {
		return nil, err
	}
	iamResp, ok := resp.(iamGetServiceAccountResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected IAM GetServiceAccount response type %T", resp)
	}

	var serviceAccount *iam_pb.ServiceAccount
	sa := iamResp.GetServiceAccountResult.ServiceAccount
	serviceAccount = &iam_pb.ServiceAccount{
		Id:          sa.ServiceAccountId,
		ParentUser:  sa.ParentUser,
		Description: sa.Description,
		Credential: &iam_pb.Credential{
			AccessKey: sa.AccessKeyId,
			Status:    sa.Status,
		},
	}

	return &iam_pb.GetServiceAccountResponse{
		ServiceAccount: serviceAccount,
	}, nil
}

func (s3a *S3ApiServer) PutPolicy(ctx context.Context, req *iam_pb.PutPolicyRequest) (*iam_pb.PutPolicyResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("policy name is required")
	}
	var doc policy_engine.PolicyDocument
	if err := json.Unmarshal([]byte(req.Content), &doc); err != nil {
		return nil, fmt.Errorf("invalid policy content: %v", err)
	}
	// Optimization: Skip persistent write to Filer if this is a propagation event
	skipPersist := false
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("is-propagation"); len(vals) > 0 && vals[0] == "true" {
			skipPersist = true
		}
	}

	if skipPersist {
		// Update policy engine directly
		if s3a.policyEngine != nil {
			if err := s3a.policyEngine.LoadBucketPolicyFromCache(req.Name, &doc); err != nil {
				glog.Errorf("failed to reload configuration after PutPolicy %s: %v", req.Name, err)
			}
		}
	} else {
		// Normal path: write to store and reload
		if err := s3a.credentialManager.PutPolicy(ctx, req.Name, doc); err != nil {
			return nil, err
		}
		if s3a.embeddedIam != nil {
			if err := s3a.embeddedIam.ReloadConfiguration(); err != nil {
				glog.Errorf("failed to reload configuration after PutPolicy %s: %v", req.Name, err)
			}
		}
	}
	return &iam_pb.PutPolicyResponse{}, nil
}

func (s3a *S3ApiServer) GetPolicy(ctx context.Context, req *iam_pb.GetPolicyRequest) (*iam_pb.GetPolicyResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("policy name is required")
	}
	doc, err := s3a.credentialManager.GetPolicy(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	content, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return &iam_pb.GetPolicyResponse{
		Name:    req.Name,
		Content: string(content),
	}, nil
}

func (s3a *S3ApiServer) DeletePolicy(ctx context.Context, req *iam_pb.DeletePolicyRequest) (*iam_pb.DeletePolicyResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("policy name is required")
	}
	// Optimization: Skip persistent write to Filer if this is a propagation event
	skipPersist := false
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("is-propagation"); len(vals) > 0 && vals[0] == "true" {
			skipPersist = true
		}
	}

	if skipPersist {
		// Delete from policy engine directly
		if s3a.policyEngine != nil {
			if err := s3a.policyEngine.DeleteBucketPolicy(req.Name); err != nil {
				glog.Errorf("failed to reload configuration after DeletePolicy %s: %v", req.Name, err)
			}
		}
	} else {
		// Normal path: delete from store and reload
		if err := s3a.credentialManager.DeletePolicy(ctx, req.Name); err != nil {
			return nil, err
		}
		if s3a.embeddedIam != nil {
			if err := s3a.embeddedIam.ReloadConfiguration(); err != nil {
				glog.Errorf("failed to reload configuration after DeletePolicy %s: %v", req.Name, err)
			}
		}
	}
	return &iam_pb.DeletePolicyResponse{}, nil
}

func (s3a *S3ApiServer) ListPolicies(ctx context.Context, req *iam_pb.ListPoliciesRequest) (*iam_pb.ListPoliciesResponse, error) {
	policies, err := s3a.credentialManager.GetPolicies(ctx)
	if err != nil {
		return nil, err
	}
	var respPolicies []*iam_pb.Policy
	for name, doc := range policies {
		content, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		respPolicies = append(respPolicies, &iam_pb.Policy{
			Name:    name,
			Content: string(content),
		})
	}
	return &iam_pb.ListPoliciesResponse{
		Policies: respPolicies,
	}, nil
}
