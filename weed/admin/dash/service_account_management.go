package dash

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

const (
	createdAtActionPrefix = "createdAt:"
	accessKeyPrefix       = "ABIA" // Service account access keys use ABIA prefix
)

// Helper functions for managing creation timestamps in actions
func getCreationDate(actions []string) time.Time {
	for _, action := range actions {
		if strings.HasPrefix(action, createdAtActionPrefix) {
			timestampStr := strings.TrimPrefix(action, createdAtActionPrefix)
			if timestamp, err := strconv.ParseInt(timestampStr, 10, 64); err == nil {
				return time.Unix(timestamp, 0)
			}
		}
	}
	return time.Now() // Fallback for legacy service accounts without stored creation date
}

func setCreationDate(actions []string, createDate time.Time) []string {
	// Remove any existing createdAt action
	filtered := make([]string, 0, len(actions)+1)
	for _, action := range actions {
		if !strings.HasPrefix(action, createdAtActionPrefix) {
			filtered = append(filtered, action)
		}
	}
	// Add new createdAt action
	filtered = append(filtered, fmt.Sprintf("%s%d", createdAtActionPrefix, createDate.Unix()))
	return filtered
}

// GetServiceAccounts returns all service accounts, optionally filtered by parent user
// NOTE: Service accounts are stored as special identities with "sa:" prefix
func (s *AdminServer) GetServiceAccounts(ctx context.Context, parentUser string) ([]ServiceAccount, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	// Load the current configuration to find service account identities
	config, err := s.credentialManager.LoadConfiguration(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	var accounts []ServiceAccount

	// Service accounts are stored as identities with "sa:" prefix in their name
	// Format: "sa:<parent_user>:<uuid>"
	for _, identity := range config.GetIdentities() {
		if !strings.HasPrefix(identity.GetName(), "sa:") {
			continue
		}

		parts := strings.SplitN(identity.GetName(), ":", 3)
		if len(parts) < 3 {
			continue
		}

		parent := parts[1]
		saId := identity.GetName()

		// Filter by parent user if specified
		if parentUser != "" && parent != parentUser {
			continue
		}

		// Extract description from account display name if available
		description := ""
		status := "Active"
		if identity.Account != nil {
			description = identity.Account.GetDisplayName()
		}

		// Get access key from credentials
		accessKey := ""
		if len(identity.Credentials) > 0 {
			accessKey = identity.Credentials[0].GetAccessKey()
			// Service accounts use ABIA prefix
			if !strings.HasPrefix(accessKey, "ABIA") {
				continue // Not a service account
			}
		}

		// Check if disabled (stored in actions)
		for _, action := range identity.GetActions() {
			if action == "__disabled__" {
				status = "Inactive"
				break
			}
		}

		accounts = append(accounts, ServiceAccount{
			ID:          saId,
			ParentUser:  parent,
			Description: description,
			AccessKeyId: accessKey,
			Status:      status,
			CreateDate:  getCreationDate(identity.GetActions()),
		})
	}

	return accounts, nil
}

// GetServiceAccountDetails returns detailed information about a specific service account
func (s *AdminServer) GetServiceAccountDetails(ctx context.Context, id string) (*ServiceAccount, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	// Get the identity
	identity, err := s.credentialManager.GetUser(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("service account not found: %s", id)
	}

	if !strings.HasPrefix(identity.GetName(), "sa:") {
		return nil, fmt.Errorf("not a service account: %s", id)
	}

	parts := strings.SplitN(identity.GetName(), ":", 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid service account ID format")
	}

	account := &ServiceAccount{
		ID:         id,
		ParentUser: parts[1],
		Status:     "Active",
		CreateDate: getCreationDate(identity.GetActions()),
	}

	if identity.Account != nil {
		account.Description = identity.Account.GetDisplayName()
	}

	if len(identity.Credentials) > 0 {
		account.AccessKeyId = identity.Credentials[0].GetAccessKey()
	}

	// Check if disabled
	for _, action := range identity.GetActions() {
		if action == "__disabled__" {
			account.Status = "Inactive"
			break
		}
	}

	return account, nil
}

// CreateServiceAccount creates a new service account for a parent user
func (s *AdminServer) CreateServiceAccount(ctx context.Context, req CreateServiceAccountRequest) (*ServiceAccount, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	// Validate parent user exists
	_, err := s.credentialManager.GetUser(ctx, req.ParentUser)
	if err != nil {
		return nil, fmt.Errorf("parent user not found: %s", req.ParentUser)
	}

	// Generate unique ID and credentials
	uuid := generateAccountId()
	saId := fmt.Sprintf("sa:%s:%s", req.ParentUser, uuid)
	accessKey := accessKeyPrefix + generateAccessKey()[len(accessKeyPrefix):] // Use ABIA prefix for service accounts
	secretKey := generateSecretKey()

	// Create the service account as a special identity
	now := time.Now()
	identity := &iam_pb.Identity{
		Name: saId,
		Account: &iam_pb.Account{
			Id:          uuid,
			DisplayName: req.Description,
		},
		Credentials: []*iam_pb.Credential{
			{
				AccessKey: accessKey,
				SecretKey: secretKey,
			},
		},
		// Store creation date in actions
		Actions: setCreationDate([]string{}, now),
	}

	// Create the service account
	err = s.credentialManager.CreateUser(ctx, identity)
	if err != nil {
		return nil, fmt.Errorf("failed to create service account: %w", err)
	}

	glog.V(1).Infof("Created service account %s for user %s", saId, req.ParentUser)

	return &ServiceAccount{
		ID:              saId,
		ParentUser:      req.ParentUser,
		Description:     req.Description,
		AccessKeyId:     accessKey,
		SecretAccessKey: secretKey, // Only returned on creation
		Status:          "Active",
		CreateDate:      now,
	}, nil
}

// UpdateServiceAccount updates an existing service account
func (s *AdminServer) UpdateServiceAccount(ctx context.Context, id string, req UpdateServiceAccountRequest) (*ServiceAccount, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	// Get existing identity
	identity, err := s.credentialManager.GetUser(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("service account not found: %s", id)
	}

	if !strings.HasPrefix(identity.GetName(), "sa:") {
		return nil, fmt.Errorf("not a service account: %s", id)
	}

	// Update description if provided
	if req.Description != "" {
		if identity.Account == nil {
			identity.Account = &iam_pb.Account{}
		}
		identity.Account.DisplayName = req.Description
	}

	// Update status by adding/removing __disabled__ action
	if req.Status != "" {
		// Remove existing __disabled__ marker
		newActions := make([]string, 0, len(identity.Actions))
		for _, action := range identity.Actions {
			if action != "__disabled__" {
				newActions = append(newActions, action)
			}
		}
		// Add __disabled__ if setting to Inactive
		if req.Status == "Inactive" {
			newActions = append(newActions, "__disabled__")
		}
		identity.Actions = newActions
	}

	// Update the identity
	err = s.credentialManager.UpdateUser(ctx, id, identity)
	if err != nil {
		return nil, fmt.Errorf("failed to update service account: %w", err)
	}

	glog.V(1).Infof("Updated service account %s", id)

	// Build response
	parts := strings.SplitN(id, ":", 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid service account ID format")
	}

	result := &ServiceAccount{
		ID:          id,
		ParentUser:  parts[1],
		Description: identity.Account.GetDisplayName(),
		Status:      "Active",
		CreateDate:  getCreationDate(identity.Actions),
	}

	if len(identity.Credentials) > 0 {
		result.AccessKeyId = identity.Credentials[0].GetAccessKey()
	}

	for _, action := range identity.Actions {
		if action == "__disabled__" {
			result.Status = "Inactive"
			break
		}
	}

	return result, nil
}

// DeleteServiceAccount deletes a service account
func (s *AdminServer) DeleteServiceAccount(ctx context.Context, id string) error {
	if s.credentialManager == nil {
		return fmt.Errorf("credential manager not available")
	}

	// Verify it's a service account
	identity, err := s.credentialManager.GetUser(ctx, id)
	if err != nil {
		return fmt.Errorf("service account not found: %s", id)
	}

	if !strings.HasPrefix(identity.GetName(), "sa:") {
		return fmt.Errorf("not a service account: %s", id)
	}

	// Delete the identity
	err = s.credentialManager.DeleteUser(ctx, id)
	if err != nil {
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

	// Find identity by access key
	identity, err := s.credentialManager.GetUserByAccessKey(ctx, accessKey)
	if err != nil {
		return nil, fmt.Errorf("service account not found for access key: %s", accessKey)
	}

	if !strings.HasPrefix(identity.GetName(), "sa:") {
		return nil, fmt.Errorf("not a service account")
	}

	parts := strings.SplitN(identity.GetName(), ":", 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid service account ID format")
	}

	account := &ServiceAccount{
		ID:          identity.GetName(),
		ParentUser:  parts[1],
		AccessKeyId: accessKey,
		Status:      "Active",
		CreateDate:  getCreationDate(identity.GetActions()),
	}

	if identity.Account != nil {
		account.Description = identity.Account.GetDisplayName()
	}

	for _, action := range identity.GetActions() {
		if action == "__disabled__" {
			account.Status = "Inactive"
			break
		}
	}

	return account, nil
}
