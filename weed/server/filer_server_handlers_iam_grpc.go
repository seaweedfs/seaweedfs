package weed_server

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// IamGrpcServer implements the IAM gRPC service on the filer
type IamGrpcServer struct {
	iam_pb.UnimplementedSeaweedIdentityAccessManagementServer
	credentialManager *credential.CredentialManager
}

// NewIamGrpcServer creates a new IAM gRPC server
func NewIamGrpcServer(credentialManager *credential.CredentialManager) *IamGrpcServer {
	return &IamGrpcServer{
		credentialManager: credentialManager,
	}
}

//////////////////////////////////////////////////
// Configuration Management

func (s *IamGrpcServer) GetConfiguration(ctx context.Context, req *iam_pb.GetConfigurationRequest) (*iam_pb.GetConfigurationResponse, error) {
	glog.V(4).Infof("GetConfiguration")
	
	if s.credentialManager == nil {
		return nil, credential.ErrUserNotFound
	}
	
	config, err := s.credentialManager.LoadConfiguration(ctx)
	if err != nil {
		glog.Errorf("Failed to load configuration: %v", err)
		return nil, err
	}
	
	return &iam_pb.GetConfigurationResponse{
		Configuration: config,
	}, nil
}

func (s *IamGrpcServer) PutConfiguration(ctx context.Context, req *iam_pb.PutConfigurationRequest) (*iam_pb.PutConfigurationResponse, error) {
	glog.V(4).Infof("PutConfiguration")
	
	if s.credentialManager == nil {
		return nil, credential.ErrUserNotFound
	}
	
	if req.Configuration == nil {
		return nil, credential.ErrUserNotFound
	}
	
	err := s.credentialManager.SaveConfiguration(ctx, req.Configuration)
	if err != nil {
		glog.Errorf("Failed to save configuration: %v", err)
		return nil, err
	}
	
	return &iam_pb.PutConfigurationResponse{}, nil
}

//////////////////////////////////////////////////
// User Management

func (s *IamGrpcServer) CreateUser(ctx context.Context, req *iam_pb.CreateUserRequest) (*iam_pb.CreateUserResponse, error) {
	glog.V(4).Infof("CreateUser: %s", req.Identity.Name)
	
	if s.credentialManager == nil {
		return nil, credential.ErrUserNotFound
	}
	
	err := s.credentialManager.CreateUser(ctx, req.Identity)
	if err != nil {
		glog.Errorf("Failed to create user %s: %v", req.Identity.Name, err)
		return nil, err
	}
	
	return &iam_pb.CreateUserResponse{}, nil
}

func (s *IamGrpcServer) GetUser(ctx context.Context, req *iam_pb.GetUserRequest) (*iam_pb.GetUserResponse, error) {
	glog.V(4).Infof("GetUser: %s", req.Username)
	
	if s.credentialManager == nil {
		return nil, credential.ErrUserNotFound
	}
	
	identity, err := s.credentialManager.GetUser(ctx, req.Username)
	if err != nil {
		glog.Errorf("Failed to get user %s: %v", req.Username, err)
		return nil, err
	}
	
	return &iam_pb.GetUserResponse{
		Identity: identity,
	}, nil
}

func (s *IamGrpcServer) UpdateUser(ctx context.Context, req *iam_pb.UpdateUserRequest) (*iam_pb.UpdateUserResponse, error) {
	glog.V(4).Infof("UpdateUser: %s", req.Username)
	
	if s.credentialManager == nil {
		return nil, credential.ErrUserNotFound
	}
	
	err := s.credentialManager.UpdateUser(ctx, req.Username, req.Identity)
	if err != nil {
		glog.Errorf("Failed to update user %s: %v", req.Username, err)
		return nil, err
	}
	
	return &iam_pb.UpdateUserResponse{}, nil
}

func (s *IamGrpcServer) DeleteUser(ctx context.Context, req *iam_pb.DeleteUserRequest) (*iam_pb.DeleteUserResponse, error) {
	glog.V(4).Infof("DeleteUser: %s", req.Username)
	
	if s.credentialManager == nil {
		return nil, credential.ErrUserNotFound
	}
	
	err := s.credentialManager.DeleteUser(ctx, req.Username)
	if err != nil {
		glog.Errorf("Failed to delete user %s: %v", req.Username, err)
		return nil, err
	}
	
	return &iam_pb.DeleteUserResponse{}, nil
}

func (s *IamGrpcServer) ListUsers(ctx context.Context, req *iam_pb.ListUsersRequest) (*iam_pb.ListUsersResponse, error) {
	glog.V(4).Infof("ListUsers")
	
	if s.credentialManager == nil {
		return nil, credential.ErrUserNotFound
	}
	
	usernames, err := s.credentialManager.ListUsers(ctx)
	if err != nil {
		glog.Errorf("Failed to list users: %v", err)
		return nil, err
	}
	
	return &iam_pb.ListUsersResponse{
		Usernames: usernames,
	}, nil
}

//////////////////////////////////////////////////
// Access Key Management

func (s *IamGrpcServer) CreateAccessKey(ctx context.Context, req *iam_pb.CreateAccessKeyRequest) (*iam_pb.CreateAccessKeyResponse, error) {
	glog.V(4).Infof("CreateAccessKey for user: %s", req.Username)
	
	if s.credentialManager == nil {
		return nil, credential.ErrUserNotFound
	}
	
	err := s.credentialManager.CreateAccessKey(ctx, req.Username, req.Credential)
	if err != nil {
		glog.Errorf("Failed to create access key for user %s: %v", req.Username, err)
		return nil, err
	}
	
	return &iam_pb.CreateAccessKeyResponse{}, nil
}

func (s *IamGrpcServer) DeleteAccessKey(ctx context.Context, req *iam_pb.DeleteAccessKeyRequest) (*iam_pb.DeleteAccessKeyResponse, error) {
	glog.V(4).Infof("DeleteAccessKey: %s for user: %s", req.AccessKey, req.Username)
	
	if s.credentialManager == nil {
		return nil, credential.ErrUserNotFound
	}
	
	err := s.credentialManager.DeleteAccessKey(ctx, req.Username, req.AccessKey)
	if err != nil {
		glog.Errorf("Failed to delete access key %s for user %s: %v", req.AccessKey, req.Username, err)
		return nil, err
	}
	
	return &iam_pb.DeleteAccessKeyResponse{}, nil
}

func (s *IamGrpcServer) GetUserByAccessKey(ctx context.Context, req *iam_pb.GetUserByAccessKeyRequest) (*iam_pb.GetUserByAccessKeyResponse, error) {
	glog.V(4).Infof("GetUserByAccessKey: %s", req.AccessKey)
	
	if s.credentialManager == nil {
		return nil, credential.ErrAccessKeyNotFound
	}
	
	identity, err := s.credentialManager.GetUserByAccessKey(ctx, req.AccessKey)
	if err != nil {
		glog.Errorf("Failed to get user by access key %s: %v", req.AccessKey, err)
		return nil, err
	}
	
	return &iam_pb.GetUserByAccessKeyResponse{
		Identity: identity,
	}, nil
}
