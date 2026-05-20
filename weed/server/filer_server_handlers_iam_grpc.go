package weed_server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// IamGrpcServer implements the IAM gRPC service on the filer.
// Auth is opt-in: when jwt.filer_signing.key is set in security.toml the
// service requires a Bearer token in the "authorization" metadata signed with
// that key; when it is empty every RPC is accepted unauthenticated, matching
// the rest of SeaweedFS's gRPC surface. Operators who expose the filer gRPC
// port beyond a trusted network should configure the key.
type IamGrpcServer struct {
	iam_pb.UnimplementedSeaweedIdentityAccessManagementServer
	credentialManager *credential.CredentialManager
	adminSigningKey   security.SigningKey
}

// NewIamGrpcServer creates a new IAM gRPC server. If adminSigningKey is empty
// the service runs unauthenticated; otherwise every RPC requires a Bearer
// token signed with the key.
func NewIamGrpcServer(credentialManager *credential.CredentialManager, adminSigningKey security.SigningKey) *IamGrpcServer {
	return &IamGrpcServer{
		credentialManager: credentialManager,
		adminSigningKey:   adminSigningKey,
	}
}

// checkAdminAuth verifies the caller presented a Bearer token signed by the
// filer's write-signing key. It is invoked at the top of every IAM RPC.
// When no signing key is configured the service runs unauthenticated and this
// check is a no-op.
func (s *IamGrpcServer) checkAdminAuth(ctx context.Context) error {
	if len(s.adminSigningKey) == 0 {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	// RFC 6750 §2.1: the "Bearer" auth-scheme name is case-insensitive.
	// Tolerate surrounding whitespace and any spacing between scheme and token.
	raw := strings.TrimSpace(authHeaders[0])
	parts := strings.Fields(raw)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return status.Error(codes.Unauthenticated, "authorization header must use Bearer scheme")
	}
	token := parts[1]
	parsed, err := security.DecodeJwt(s.adminSigningKey, security.EncodedJwt(token), &security.SeaweedFilerAdminClaims{})
	if err != nil || parsed == nil || !parsed.Valid {
		return status.Error(codes.Unauthenticated, "invalid admin token")
	}
	return nil
}

//////////////////////////////////////////////////
// Configuration Management

func (s *IamGrpcServer) GetConfiguration(ctx context.Context, req *iam_pb.GetConfigurationRequest) (*iam_pb.GetConfigurationResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("GetConfiguration")

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
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
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("PutConfiguration")

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	if req.Configuration == nil {
		return nil, status.Errorf(codes.InvalidArgument, "configuration is nil")
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
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil || req.Identity == nil {
		return nil, status.Errorf(codes.InvalidArgument, "identity is required")
	}
	glog.V(4).Infof("IAM: Filer.CreateUser %s", req.Identity.Name)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.CreateUser(ctx, req.Identity)
	if err != nil {
		if err == credential.ErrUserAlreadyExists {
			return nil, status.Errorf(codes.AlreadyExists, "user %s already exists", req.Identity.Name)
		}
		glog.Errorf("Failed to create user %s: %v", req.Identity.Name, err)
		return nil, status.Errorf(codes.Internal, "failed to create user: %v", err)
	}

	return &iam_pb.CreateUserResponse{}, nil
}

func (s *IamGrpcServer) GetUser(ctx context.Context, req *iam_pb.GetUserRequest) (*iam_pb.GetUserResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("GetUser: %s", req.Username)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	identity, err := s.credentialManager.GetUser(ctx, req.Username)
	if err != nil {
		if err == credential.ErrUserNotFound {
			// Fall back to static identities (loaded from -s3.config file)
			if si := s.credentialManager.GetStaticIdentity(req.Username); si != nil {
				return &iam_pb.GetUserResponse{Identity: si}, nil
			}
			return nil, status.Errorf(codes.NotFound, "user %s not found", req.Username)
		}
		glog.Errorf("Failed to get user %s: %v", req.Username, err)
		return nil, status.Errorf(codes.Internal, "failed to get user: %v", err)
	}

	return &iam_pb.GetUserResponse{
		Identity: identity,
	}, nil
}

func (s *IamGrpcServer) UpdateUser(ctx context.Context, req *iam_pb.UpdateUserRequest) (*iam_pb.UpdateUserResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil || req.Identity == nil {
		return nil, status.Errorf(codes.InvalidArgument, "identity is required")
	}
	glog.V(4).Infof("IAM: Filer.UpdateUser %s", req.Username)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.UpdateUser(ctx, req.Username, req.Identity)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return nil, status.Errorf(codes.NotFound, "user %s not found", req.Username)
		}
		glog.Errorf("Failed to update user %s: %v", req.Username, err)
		return nil, status.Errorf(codes.Internal, "failed to update user: %v", err)
	}

	return &iam_pb.UpdateUserResponse{}, nil
}

func (s *IamGrpcServer) DeleteUser(ctx context.Context, req *iam_pb.DeleteUserRequest) (*iam_pb.DeleteUserResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("IAM: Filer.DeleteUser %s", req.Username)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.DeleteUser(ctx, req.Username)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return nil, status.Errorf(codes.NotFound, "user %s not found", req.Username)
		}
		glog.Errorf("Failed to delete user %s: %v", req.Username, err)
		return nil, status.Errorf(codes.Internal, "failed to delete user: %v", err)
	}

	return &iam_pb.DeleteUserResponse{}, nil
}

func (s *IamGrpcServer) ListUsers(ctx context.Context, req *iam_pb.ListUsersRequest) (*iam_pb.ListUsersResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("ListUsers")

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	usernames, err := s.credentialManager.ListUsers(ctx)
	if err != nil {
		glog.Errorf("Failed to list users: %v", err)
		return nil, err
	}

	// Merge static identities (from -s3.config file) into the result
	staticNames := s.credentialManager.GetStaticUsernames()
	if len(staticNames) > 0 {
		dynamicSet := make(map[string]bool, len(usernames))
		for _, name := range usernames {
			dynamicSet[name] = true
		}
		for _, name := range staticNames {
			if !dynamicSet[name] {
				usernames = append(usernames, name)
			}
		}
	}

	return &iam_pb.ListUsersResponse{
		Usernames: usernames,
	}, nil
}

//////////////////////////////////////////////////
// Access Key Management

func (s *IamGrpcServer) CreateAccessKey(ctx context.Context, req *iam_pb.CreateAccessKeyRequest) (*iam_pb.CreateAccessKeyResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil || req.Credential == nil {
		return nil, status.Errorf(codes.InvalidArgument, "credential is required")
	}
	glog.V(4).Infof("CreateAccessKey for user: %s", req.Username)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.CreateAccessKey(ctx, req.Username, req.Credential)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return nil, status.Errorf(codes.NotFound, "user %s not found", req.Username)
		}
		glog.Errorf("Failed to create access key for user %s: %v", req.Username, err)
		return nil, status.Errorf(codes.Internal, "failed to create access key: %v", err)
	}

	return &iam_pb.CreateAccessKeyResponse{}, nil
}

func (s *IamGrpcServer) DeleteAccessKey(ctx context.Context, req *iam_pb.DeleteAccessKeyRequest) (*iam_pb.DeleteAccessKeyResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("DeleteAccessKey: %s for user: %s", req.AccessKey, req.Username)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.DeleteAccessKey(ctx, req.Username, req.AccessKey)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return nil, status.Errorf(codes.NotFound, "user %s not found", req.Username)
		}
		if err == credential.ErrAccessKeyNotFound {
			return nil, status.Errorf(codes.NotFound, "access key %s not found", req.AccessKey)
		}
		glog.Errorf("Failed to delete access key %s for user %s: %v", req.AccessKey, req.Username, err)
		return nil, status.Errorf(codes.Internal, "failed to delete access key: %v", err)
	}

	return &iam_pb.DeleteAccessKeyResponse{}, nil
}

func (s *IamGrpcServer) GetUserByAccessKey(ctx context.Context, req *iam_pb.GetUserByAccessKeyRequest) (*iam_pb.GetUserByAccessKeyResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("GetUserByAccessKey: %s", req.AccessKey)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	identity, err := s.credentialManager.GetUserByAccessKey(ctx, req.AccessKey)
	if err != nil {
		if err == credential.ErrAccessKeyNotFound {
			return nil, status.Errorf(codes.NotFound, "access key %s not found", req.AccessKey)
		}
		glog.Errorf("Failed to get user by access key %s: %v", req.AccessKey, err)
		return nil, status.Errorf(codes.Internal, "failed to get user: %v", err)
	}

	return &iam_pb.GetUserByAccessKeyResponse{
		Identity: identity,
	}, nil
}

//////////////////////////////////////////////////
// Policy Management

func (s *IamGrpcServer) PutPolicy(ctx context.Context, req *iam_pb.PutPolicyRequest) (*iam_pb.PutPolicyResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("IAM: Filer.PutPolicy %s", req.Name)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy name is required")
	}
	if err := credential.ValidatePolicyName(req.Name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.Content == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy content is required")
	}

	var policy policy_engine.PolicyDocument
	if err := json.Unmarshal([]byte(req.Content), &policy); err != nil {
		glog.Errorf("Failed to unmarshal policy %s: %v", req.Name, err)
		return nil, err
	}

	err := s.credentialManager.PutPolicy(ctx, req.Name, policy)
	if err != nil {
		glog.Errorf("Failed to put policy %s: %v", req.Name, err)
		return nil, err
	}

	return &iam_pb.PutPolicyResponse{}, nil
}

func (s *IamGrpcServer) GetPolicy(ctx context.Context, req *iam_pb.GetPolicyRequest) (*iam_pb.GetPolicyResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("GetPolicy: %s", req.Name)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	policy, err := s.credentialManager.GetPolicy(ctx, req.Name)
	if err != nil {
		glog.Errorf("Failed to get policy %s: %v", req.Name, err)
		return nil, err
	}

	if policy == nil {
		return nil, status.Errorf(codes.NotFound, "policy %s not found", req.Name)
	}

	jsonBytes, err := json.Marshal(policy)
	if err != nil {
		glog.Errorf("Failed to marshal policy %s: %v", req.Name, err)
		return nil, err
	}

	return &iam_pb.GetPolicyResponse{
		Name:    req.Name,
		Content: string(jsonBytes),
	}, nil
}

func (s *IamGrpcServer) ListPolicies(ctx context.Context, req *iam_pb.ListPoliciesRequest) (*iam_pb.ListPoliciesResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("ListPolicies")

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	policiesData, err := s.credentialManager.GetPolicies(ctx)
	if err != nil {
		glog.Errorf("Failed to list policies: %v", err)
		return nil, err
	}

	var policies []*iam_pb.Policy
	for name, policy := range policiesData {
		jsonBytes, err := json.Marshal(policy)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to marshal policy %s: %v", name, err)
		}
		policies = append(policies, &iam_pb.Policy{
			Name:    name,
			Content: string(jsonBytes),
		})
	}

	return &iam_pb.ListPoliciesResponse{
		Policies: policies,
	}, nil
}

func (s *IamGrpcServer) DeletePolicy(ctx context.Context, req *iam_pb.DeletePolicyRequest) (*iam_pb.DeletePolicyResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("DeletePolicy: %s", req.Name)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.DeletePolicy(ctx, req.Name)
	if err != nil {
		glog.Errorf("Failed to delete policy %s: %v", req.Name, err)
		return nil, err
	}

	return &iam_pb.DeletePolicyResponse{}, nil
}

//////////////////////////////////////////////////
// Service Account Management

func (s *IamGrpcServer) CreateServiceAccount(ctx context.Context, req *iam_pb.CreateServiceAccountRequest) (*iam_pb.CreateServiceAccountResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil || req.ServiceAccount == nil {
		return nil, status.Errorf(codes.InvalidArgument, "service account is required")
	}
	if err := credential.ValidateServiceAccountId(req.ServiceAccount.Id); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	glog.V(4).Infof("CreateServiceAccount: %s", req.ServiceAccount.Id)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.CreateServiceAccount(ctx, req.ServiceAccount)
	if err != nil {
		glog.Errorf("Failed to create service account %s: %v", req.ServiceAccount.Id, err)
		return nil, status.Errorf(codes.Internal, "failed to create service account: %v", err)
	}

	return &iam_pb.CreateServiceAccountResponse{}, nil
}

func (s *IamGrpcServer) UpdateServiceAccount(ctx context.Context, req *iam_pb.UpdateServiceAccountRequest) (*iam_pb.UpdateServiceAccountResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil || req.ServiceAccount == nil {
		return nil, status.Errorf(codes.InvalidArgument, "service account is required")
	}
	glog.V(4).Infof("UpdateServiceAccount: %s", req.Id)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.UpdateServiceAccount(ctx, req.Id, req.ServiceAccount)
	if err != nil {
		glog.Errorf("Failed to update service account %s: %v", req.Id, err)
		return nil, status.Errorf(codes.Internal, "failed to update service account: %v", err)
	}

	return &iam_pb.UpdateServiceAccountResponse{}, nil
}

func (s *IamGrpcServer) DeleteServiceAccount(ctx context.Context, req *iam_pb.DeleteServiceAccountRequest) (*iam_pb.DeleteServiceAccountResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("DeleteServiceAccount: %s", req.Id)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	err := s.credentialManager.DeleteServiceAccount(ctx, req.Id)
	if err != nil {
		if err == credential.ErrServiceAccountNotFound {
			return nil, status.Errorf(codes.NotFound, "service account %s not found", req.Id)
		}
		glog.Errorf("Failed to delete service account %s: %v", req.Id, err)
		return nil, status.Errorf(codes.Internal, "failed to delete service account: %v", err)
	}

	return &iam_pb.DeleteServiceAccountResponse{}, nil
}

func (s *IamGrpcServer) GetServiceAccount(ctx context.Context, req *iam_pb.GetServiceAccountRequest) (*iam_pb.GetServiceAccountResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("GetServiceAccount: %s", req.Id)

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	sa, err := s.credentialManager.GetServiceAccount(ctx, req.Id)
	if err != nil {
		glog.Errorf("Failed to get service account %s: %v", req.Id, err)
		return nil, status.Errorf(codes.Internal, "failed to get service account: %v", err)
	}

	if sa == nil {
		return nil, status.Errorf(codes.NotFound, "service account %s not found", req.Id)
	}

	return &iam_pb.GetServiceAccountResponse{
		ServiceAccount: sa,
	}, nil
}

func (s *IamGrpcServer) ListServiceAccounts(ctx context.Context, req *iam_pb.ListServiceAccountsRequest) (*iam_pb.ListServiceAccountsResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("ListServiceAccounts")

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	accounts, err := s.credentialManager.ListServiceAccounts(ctx)
	if err != nil {
		glog.Errorf("Failed to list service accounts: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to list service accounts: %v", err)
	}

	return &iam_pb.ListServiceAccountsResponse{
		ServiceAccounts: accounts,
	}, nil
}

func (s *IamGrpcServer) GetServiceAccountByAccessKey(ctx context.Context, req *iam_pb.GetServiceAccountByAccessKeyRequest) (*iam_pb.GetServiceAccountByAccessKeyResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	glog.V(4).Infof("GetServiceAccountByAccessKey: %s", req.AccessKey)
	if req.AccessKey == "" {
		return nil, status.Errorf(codes.InvalidArgument, "access key is required")
	}

	if s.credentialManager == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "credential manager is not configured")
	}

	sa, err := s.credentialManager.GetStore().GetServiceAccountByAccessKey(ctx, req.AccessKey)
	if err != nil {
		if err == credential.ErrAccessKeyNotFound {
			return nil, status.Errorf(codes.NotFound, "access key %s not found", req.AccessKey)
		}
		glog.Errorf("Failed to get service account by access key %s: %v", req.AccessKey, err)
		return nil, status.Errorf(codes.Internal, "failed to get service account: %v", err)
	}

	return &iam_pb.GetServiceAccountByAccessKeyResponse{
		ServiceAccount: sa,
	}, nil
}
