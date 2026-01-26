package dash

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

var (
	// ErrServiceAccountNotFound is returned when a service account is not found
	ErrServiceAccountNotFound = errors.New("service account not found")
)

const (
	serviceAccountPrefix = "sa:"
	accessKeyPrefix      = "ABIA" // Service account access keys use ABIA prefix

	// Status constants
	StatusActive   = "Active"
	StatusInactive = "Inactive"
)

// GetServiceAccounts returns all service accounts, optionally filtered by parent user
func (s *AdminServer) GetServiceAccounts(ctx context.Context, parentUser string) ([]ServiceAccount, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	pbAccounts, err := s.credentialManager.ListServiceAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list service accounts: %w", err)
	}

	var accounts []ServiceAccount
	for _, sa := range pbAccounts {
		if sa == nil {
			continue
		}
		// Filter by parent user if specified
		if parentUser != "" && sa.ParentUser != parentUser {
			continue
		}

		status := StatusActive
		if sa.Disabled {
			status = StatusInactive
		}

		account := ServiceAccount{
			ID:          sa.Id,
			ParentUser:  sa.ParentUser,
			Description: sa.Description,
			Status:      status,
			CreateDate:  time.Unix(sa.CreatedAt, 0),
		}

		if sa.Expiration > 0 {
			account.Expiration = time.Unix(sa.Expiration, 0)
		}

		if sa.Credential != nil {
			account.AccessKeyId = sa.Credential.AccessKey
		}

		accounts = append(accounts, account)
	}

	// For backward compatibility: also list legacy service accounts stored as identities?
	// The user explicitly wanted to fix the storage location, implies migration or switch.
	// Mixing both might be confusing but safer.
	// However, user feedback implies strict "service accounts should be separate". I will rely only on new store.

	return accounts, nil
}

// GetServiceAccountDetails returns detailed information about a specific service account
func (s *AdminServer) GetServiceAccountDetails(ctx context.Context, id string) (*ServiceAccount, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	sa, err := s.credentialManager.GetServiceAccount(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get service account: %w", err)
	}
	if sa == nil {
		return nil, ErrServiceAccountNotFound
	}

	status := StatusActive
	if sa.Disabled {
		status = StatusInactive
	}

	account := &ServiceAccount{
		ID:          sa.Id,
		ParentUser:  sa.ParentUser,
		Description: sa.Description,
		Status:      status,
		CreateDate:  time.Unix(sa.CreatedAt, 0),
	}

	if sa.Expiration > 0 {
		account.Expiration = time.Unix(sa.Expiration, 0)
	}

	if sa.Credential != nil {
		account.AccessKeyId = sa.Credential.AccessKey
	}

	return account, nil
}

// CreateServiceAccount creates a new service account for a parent user
func (s *AdminServer) CreateServiceAccount(ctx context.Context, req CreateServiceAccountRequest) (*ServiceAccount, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	// Validate parent user exists
	if _, err := s.credentialManager.GetUser(ctx, req.ParentUser); err != nil {
		return nil, fmt.Errorf("parent user not found: %s", req.ParentUser)
	}

	// Generate unique ID and credentials
	uuid := generateAccountId()
	// Maintain consistent ID format: sa:<parent>:<uuid>
	saId := fmt.Sprintf("sa:%s:%s", req.ParentUser, uuid)

	accessKey := accessKeyPrefix + generateAccessKey()[len(accessKeyPrefix):]
	secretKey := generateSecretKey()

	now := time.Now()

	sa := &iam_pb.ServiceAccount{
		Id:          saId,
		ParentUser:  req.ParentUser,
		Description: req.Description,
		Credential: &iam_pb.Credential{
			AccessKey: accessKey,
			SecretKey: secretKey,
			Status:    StatusActive,
		},
		CreatedAt: now.Unix(),
		Disabled:  false,
	}

	if req.Expiration != "" {
		exp, err := time.Parse(time.RFC3339, req.Expiration)
		if err != nil {
			return nil, fmt.Errorf("invalid expiration format: %w", err)
		}
		sa.Expiration = exp.Unix()
	}

	if err := s.credentialManager.CreateServiceAccount(ctx, sa); err != nil {
		return nil, fmt.Errorf("failed to create service account: %w", err)
	}

	glog.V(1).Infof("Created service account %s for user %s", saId, req.ParentUser)

	resp := &ServiceAccount{
		ID:              saId,
		ParentUser:      req.ParentUser,
		Description:     req.Description,
		AccessKeyId:     accessKey,
		SecretAccessKey: secretKey, // Only returned on creation
		Status:          StatusActive,
		CreateDate:      now,
	}

	if sa.Expiration > 0 {
		resp.Expiration = time.Unix(sa.Expiration, 0)
	}

	return resp, nil
}

// UpdateServiceAccount updates an existing service account
func (s *AdminServer) UpdateServiceAccount(ctx context.Context, id string, req UpdateServiceAccountRequest) (*ServiceAccount, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	sa, err := s.credentialManager.GetServiceAccount(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get service account: %w", err)
	}
	if sa == nil {
		return nil, ErrServiceAccountNotFound
	}

	if req.Description != "" {
		sa.Description = req.Description
	}

	if req.Status != "" {
		if req.Status == StatusInactive {
			sa.Disabled = true
		} else {
			sa.Disabled = false
		}
	}

	if req.Expiration != "" {
		exp, err := time.Parse(time.RFC3339, req.Expiration)
		if err != nil {
			return nil, fmt.Errorf("invalid expiration format: %w", err)
		}
		sa.Expiration = exp.Unix()
	}

	if err := s.credentialManager.UpdateServiceAccount(ctx, id, sa); err != nil {
		return nil, fmt.Errorf("failed to update service account: %w", err)
	}

	glog.V(1).Infof("Updated service account %s", id)

	status := StatusActive
	if sa.Disabled {
		status = StatusInactive
	}

	accessKeyId := ""
	if sa.Credential != nil {
		accessKeyId = sa.Credential.AccessKey
	}

	resp := &ServiceAccount{
		ID:          sa.Id,
		ParentUser:  sa.ParentUser,
		Description: sa.Description,
		Status:      status,
		CreateDate:  time.Unix(sa.CreatedAt, 0),
		AccessKeyId: accessKeyId,
	}

	if sa.Expiration > 0 {
		resp.Expiration = time.Unix(sa.Expiration, 0)
	}

	return resp, nil
}

// DeleteServiceAccount deletes a service account
func (s *AdminServer) DeleteServiceAccount(ctx context.Context, id string) error {
	if s.credentialManager == nil {
		return fmt.Errorf("credential manager not available")
	}

	// Verify existence
	sa, err := s.credentialManager.GetServiceAccount(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to check service account: %w", err)
	}
	if sa == nil {
		return ErrServiceAccountNotFound
	}

	if err := s.credentialManager.DeleteServiceAccount(ctx, id); err != nil {
		return fmt.Errorf("failed to delete service account: %w", err)
	}

	glog.V(1).Infof("Deleted service account %s", id)
	return nil
}

// GetServiceAccountByAccessKey finds a service account by its access key
func (s *AdminServer) GetServiceAccountByAccessKey(ctx context.Context, accessKey string) (*ServiceAccount, error) {
	if !strings.HasPrefix(accessKey, accessKeyPrefix) {
		return nil, fmt.Errorf("not a service account access key")
	}

	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	// Efficient lookup is not supported by interface yet, so list and find
	pbAccounts, err := s.credentialManager.ListServiceAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list service accounts: %w", err)
	}

	for _, sa := range pbAccounts {
		if sa.Credential != nil && sa.Credential.AccessKey == accessKey {
			status := StatusActive
			if sa.Disabled {
				status = StatusInactive
			}
			accessKeyId := ""
			if sa.Credential != nil {
				accessKeyId = sa.Credential.AccessKey
			}
			resp := &ServiceAccount{
				ID:          sa.Id,
				ParentUser:  sa.ParentUser,
				Description: sa.Description,
				AccessKeyId: accessKeyId,
				Status:      status,
				CreateDate:  time.Unix(sa.CreatedAt, 0),
			}
			if sa.Expiration > 0 {
				resp.Expiration = time.Unix(sa.Expiration, 0)
			}
			return resp, nil
		}
	}

	return nil, fmt.Errorf("service account not found for access key: %s", accessKey)
}
