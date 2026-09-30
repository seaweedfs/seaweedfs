package weed_server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/utils"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SetSTSStores gives the IAM service the stores that S3 servers read when they
// run with a filer-typed "oidcProviderStore" and "roleStore". Records written
// here are the ones their STS trusts; the S3 servers pick up changes through
// their /etc/iam metadata subscription. Without the stores, the OIDC provider
// and role RPCs return FailedPrecondition.
func (s *IamGrpcServer) SetSTSStores(oidcProviders integration.OIDCProviderStore, roles integration.RoleStore) {
	s.oidcProviderStore = oidcProviders
	s.roleStore = roles
}

// errSTSRequiresAuth refuses the OIDC provider and role RPCs on a filer whose
// IAM service runs unauthenticated. Unlike users and policies, which keep the
// service's opt-in auth, these grant STS access outright: with them, anyone
// who can reach the port could register an issuer they control, create a role
// trusting it, and exchange a token for S3 credentials.
var errSTSRequiresAuth = status.Error(codes.FailedPrecondition,
	"OIDC provider and role management requires admin authentication: set jwt.filer_signing.key in security.toml")

func (s *IamGrpcServer) requireOIDCProviderStore() (integration.OIDCProviderStore, error) {
	if len(s.adminSigningKey) == 0 {
		return nil, errSTSRequiresAuth
	}
	if s.oidcProviderStore == nil {
		return nil, status.Error(codes.FailedPrecondition, "OIDC provider store not configured on this filer")
	}
	return s.oidcProviderStore, nil
}

func (s *IamGrpcServer) requireRoleStore() (integration.RoleStore, error) {
	if len(s.adminSigningKey) == 0 {
		return nil, errSTSRequiresAuth
	}
	if s.roleStore == nil {
		return nil, status.Error(codes.FailedPrecondition, "role store not configured on this filer")
	}
	return s.roleStore, nil
}

func toPbOIDCProvider(rec *integration.OIDCProviderRecord) *iam_pb.OIDCProvider {
	return &iam_pb.OIDCProvider{
		IssuerUrl:   rec.URL,
		ClientIds:   rec.ClientIDs,
		Thumbprints: rec.Thumbprints,
		AccountId:   rec.AccountID,
		Arn:         rec.ARN,
	}
}

// lookupOIDCProvider returns the stored record, nil when there is none, or an
// error when the store could not be read.
func lookupOIDCProvider(ctx context.Context, store integration.OIDCProviderStore, arn string) (*integration.OIDCProviderRecord, error) {
	rec, err := store.GetProviderByARN(ctx, "", arn)
	if errors.Is(err, integration.ErrOIDCProviderNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read OIDC provider %s: %v", arn, err)
	}
	return rec, nil
}

func (s *IamGrpcServer) PutOIDCProvider(ctx context.Context, req *iam_pb.PutOIDCProviderRequest) (*iam_pb.PutOIDCProviderResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	store, err := s.requireOIDCProviderStore()
	if err != nil {
		return nil, err
	}
	if err := requireSecureIssuer(req.IssuerUrl); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	rec, err := integration.PrepareOIDCProviderRecord(req.AccountId, req.IssuerUrl, req.ClientIds, req.Thumbprints)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	existing, err := lookupOIDCProvider(ctx, store, rec.ARN)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	rec.CreatedAt, rec.UpdatedAt = now, now
	if existing != nil {
		// Put replaces what the request carries and keeps what it cannot.
		rec.CreatedAt = existing.CreatedAt
		rec.Tags = existing.Tags
		rec.AllowedPrincipalTagKeys = existing.AllowedPrincipalTagKeys
		rec.PolicyClaim = existing.PolicyClaim
	}
	if err := store.StoreProvider(ctx, "", rec); err != nil {
		return nil, status.Errorf(codes.Internal, "store OIDC provider: %v", err)
	}
	return &iam_pb.PutOIDCProviderResponse{Arn: rec.ARN}, nil
}

// requireSecureIssuer refuses an issuer served over plain HTTP, as AWS does:
// STS fetches the issuer's signing keys from it, so over HTTP anyone on the
// network path could substitute their own and mint tokens STS accepts. A
// loopback issuer is allowed for local testing.
func requireSecureIssuer(issuerURL string) error {
	u, err := url.Parse(issuerURL)
	if err != nil {
		return fmt.Errorf("invalid issuer URL: %w", err)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		host := u.Hostname()
		if host == "localhost" {
			return nil
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return nil
		}
		return fmt.Errorf("issuer URL must use https (http is allowed only for a loopback host): %s", issuerURL)
	default:
		return fmt.Errorf("issuer URL must use https: %s", issuerURL)
	}
}

func (s *IamGrpcServer) GetOIDCProvider(ctx context.Context, req *iam_pb.GetOIDCProviderRequest) (*iam_pb.GetOIDCProviderResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	store, err := s.requireOIDCProviderStore()
	if err != nil {
		return nil, err
	}
	arn, err := integration.DeriveOIDCProviderARN(req.AccountId, req.IssuerUrl)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	rec, err := lookupOIDCProvider(ctx, store, arn)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, status.Errorf(codes.NotFound, "OIDC provider %s not found", arn)
	}
	return &iam_pb.GetOIDCProviderResponse{Provider: toPbOIDCProvider(rec)}, nil
}

// DeleteOIDCProvider returns NotFound for a provider that does not exist, as
// DeleteUser does for a user; callers treat that as already deleted.
func (s *IamGrpcServer) DeleteOIDCProvider(ctx context.Context, req *iam_pb.DeleteOIDCProviderRequest) (*iam_pb.DeleteOIDCProviderResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	store, err := s.requireOIDCProviderStore()
	if err != nil {
		return nil, err
	}
	arn, err := integration.DeriveOIDCProviderARN(req.AccountId, req.IssuerUrl)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	existing, err := lookupOIDCProvider(ctx, store, arn)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, status.Errorf(codes.NotFound, "OIDC provider %s not found", arn)
	}
	if err := store.DeleteProvider(ctx, "", arn); err != nil {
		return nil, status.Errorf(codes.Internal, "delete OIDC provider: %v", err)
	}
	return &iam_pb.DeleteOIDCProviderResponse{}, nil
}

func (s *IamGrpcServer) ListOIDCProviders(ctx context.Context, req *iam_pb.ListOIDCProvidersRequest) (*iam_pb.ListOIDCProvidersResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	store, err := s.requireOIDCProviderStore()
	if err != nil {
		return nil, err
	}
	records, err := store.ListProviders(ctx, "")
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "list OIDC providers: %v", err)
	}
	resp := &iam_pb.ListOIDCProvidersResponse{}
	for _, rec := range records {
		resp.Providers = append(resp.Providers, toPbOIDCProvider(rec))
	}
	return resp, nil
}

func toPbRole(role *integration.RoleDefinition) (*iam_pb.Role, error) {
	out := &iam_pb.Role{
		RoleName:           role.RoleName,
		RoleArn:            role.RoleArn,
		AttachedPolicies:   role.AttachedPolicies,
		Description:        role.Description,
		MaxSessionDuration: role.MaxSessionDuration,
	}
	if role.TrustPolicy != nil {
		doc, err := json.Marshal(role.TrustPolicy)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode trust policy of role %s: %v", role.RoleName, err)
		}
		out.TrustPolicy = string(doc)
	}
	return out, nil
}

// lookupRole returns the stored role, nil when there is none, or an error when
// the store could not be read.
func lookupRole(ctx context.Context, store integration.RoleStore, name string) (*integration.RoleDefinition, error) {
	role, err := store.GetRole(ctx, "", name)
	if errors.Is(err, integration.ErrRoleNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read role %s: %v", name, err)
	}
	return role, nil
}

// PutRole creates or replaces a role in the filer's role store. A role an S3
// server's IAM config file defines is served by that server ahead of the
// store, so a stored role of the same name has no effect there.
func (s *IamGrpcServer) PutRole(ctx context.Context, req *iam_pb.PutRoleRequest) (*iam_pb.PutRoleResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	store, err := s.requireRoleStore()
	if err != nil {
		return nil, err
	}
	in := req.GetRole()
	if in == nil || in.RoleName == "" {
		return nil, status.Error(codes.InvalidArgument, "role.role_name is required")
	}
	if err := integration.ValidateRoleName(in.RoleName); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if len(in.AttachedPolicies) > integration.MaxManagedPoliciesPerRole {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d managed policies may be attached to a role", integration.MaxManagedPoliciesPerRole)
	}
	if in.TrustPolicy == "" {
		return nil, status.Error(codes.InvalidArgument, "role.trust_policy is required")
	}
	var trust policy.PolicyDocument
	if err := json.Unmarshal([]byte(in.TrustPolicy), &trust); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse trust policy: %v", err)
	}
	// STS finds a role by the name in the ARN a caller presents, so a stored
	// ARN naming another role would be honoured for neither name correctly.
	if in.RoleArn != "" && utils.ExtractRoleNameFromArn(in.RoleArn) != in.RoleName {
		return nil, status.Errorf(codes.InvalidArgument, "role.role_arn %s does not name role %s", in.RoleArn, in.RoleName)
	}
	role := &integration.RoleDefinition{
		RoleName:           in.RoleName,
		RoleArn:            in.RoleArn,
		TrustPolicy:        &trust,
		AttachedPolicies:   in.AttachedPolicies,
		Description:        in.Description,
		MaxSessionDuration: in.MaxSessionDuration,
	}
	if err := integration.PrepareRoleDefinition(in.RoleName, role); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	for _, name := range role.AttachedPolicies {
		existing, err := s.credentialManager.GetPolicy(ctx, name)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read policy %s: %v", name, err)
		}
		if existing == nil {
			return nil, status.Errorf(codes.NotFound, "attached policy %s not found", name)
		}
	}
	// A replaced role keeps its ID, so its sessions stay valid; a role created
	// anew — including after a delete — gets a new one, so sessions of an
	// earlier role of the same name do not carry over. The store's atomic
	// update decides which, against the role as it is when written: a Put
	// racing a DeleteRole cannot write the deleted role back with its old ID.
	err = store.UpdateRole(ctx, "", in.RoleName, func(existing *integration.RoleDefinition) (*integration.RoleDefinition, error) {
		next := *role
		next.CreatedAt = time.Now().UTC()
		next.RoleId = integration.NewRoleID()
		if existing != nil {
			next.CreatedAt = existing.CreatedAt
			if existing.RoleId != "" {
				next.RoleId = existing.RoleId
			}
		}
		return &next, nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "store role: %v", err)
	}
	return &iam_pb.PutRoleResponse{RoleArn: role.RoleArn}, nil
}

func (s *IamGrpcServer) GetRole(ctx context.Context, req *iam_pb.GetRoleRequest) (*iam_pb.GetRoleResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	store, err := s.requireRoleStore()
	if err != nil {
		return nil, err
	}
	if req.RoleName == "" {
		return nil, status.Error(codes.InvalidArgument, "role_name is required")
	}
	role, err := lookupRole(ctx, store, req.RoleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return nil, status.Errorf(codes.NotFound, "role %s not found", req.RoleName)
	}
	out, err := toPbRole(role)
	if err != nil {
		return nil, err
	}
	return &iam_pb.GetRoleResponse{Role: out}, nil
}

// DeleteRole returns NotFound for a role that does not exist, like
// DeleteOIDCProvider. Unlike the IAM API's DeleteRole it does not require the
// policies to be detached first: this API is declarative, and the role and its
// attachments are one object here.
func (s *IamGrpcServer) DeleteRole(ctx context.Context, req *iam_pb.DeleteRoleRequest) (*iam_pb.DeleteRoleResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	store, err := s.requireRoleStore()
	if err != nil {
		return nil, err
	}
	if req.RoleName == "" {
		return nil, status.Error(codes.InvalidArgument, "role_name is required")
	}
	existing, err := lookupRole(ctx, store, req.RoleName)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, status.Errorf(codes.NotFound, "role %s not found", req.RoleName)
	}
	if err := store.DeleteRole(ctx, "", req.RoleName); err != nil {
		return nil, status.Errorf(codes.Internal, "delete role: %v", err)
	}
	return &iam_pb.DeleteRoleResponse{}, nil
}

func (s *IamGrpcServer) ListRoles(ctx context.Context, req *iam_pb.ListRolesRequest) (*iam_pb.ListRolesResponse, error) {
	if err := s.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	store, err := s.requireRoleStore()
	if err != nil {
		return nil, err
	}
	names, err := store.ListRoles(ctx, "")
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "list roles: %v", err)
	}
	resp := &iam_pb.ListRolesResponse{}
	for _, name := range names {
		role, err := lookupRole(ctx, store, name)
		if err != nil {
			return nil, err
		}
		if role == nil {
			continue // deleted between list and read
		}
		out, err := toPbRole(role)
		if err != nil {
			return nil, err
		}
		resp.Roles = append(resp.Roles, out)
	}
	return resp, nil
}
