package dash

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
	createdAtActionPrefix  = "createdAt:"
	expirationActionPrefix = "expiresAt:"
	disabledAction         = "__disabled__"
	serviceAccountPrefix   = "sa:"
	accessKeyPrefix        = "ABIA" // Service account access keys use ABIA prefix

	// Status constants
	StatusActive   = "Active"
	StatusInactive = "Inactive"
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
	return time.Time{} // Return zero time for legacy service accounts without stored creation date
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

// Helper functions for managing expiration timestamps in actions
func getExpiration(actions []string) time.Time {
	for _, action := range actions {
		if strings.HasPrefix(action, expirationActionPrefix) {
			timestampStr := strings.TrimPrefix(action, expirationActionPrefix)
			if timestamp, err := strconv.ParseInt(timestampStr, 10, 64); err == nil {
				return time.Unix(timestamp, 0)
			}
		}
	}
	return time.Time{} // No expiration set
}

func setExpiration(actions []string, expiration time.Time) []string {
	// Remove any existing expiration action
	filtered := make([]string, 0, len(actions)+1)
	for _, action := range actions {
		if !strings.HasPrefix(action, expirationActionPrefix) {
			filtered = append(filtered, action)
		}
	}
	// Add new expiration action if not zero
	if !expiration.IsZero() {
		filtered = append(filtered, fmt.Sprintf("%s%d", expirationActionPrefix, expiration.Unix()))
	}
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
		if !strings.HasPrefix(identity.GetName(), serviceAccountPrefix) {
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
		status := StatusActive
		if identity.Account != nil {
			description = identity.Account.GetDisplayName()
		}

		// Get access key from credentials
		accessKey := ""
		if len(identity.Credentials) > 0 {
			accessKey = identity.Credentials[0].GetAccessKey()
			// Service accounts use ABIA prefix
			if !strings.HasPrefix(accessKey, accessKeyPrefix) {
				continue // Not a service account
			}
		}

		// Check if disabled (stored in actions)
		for _, action := range identity.GetActions() {
			if action == disabledAction {
				status = StatusInactive
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
			Expiration:  getExpiration(identity.GetActions()),
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
		return nil, fmt.Errorf("%w: %s", ErrServiceAccountNotFound, id)
	}

	if !strings.HasPrefix(identity.GetName(), serviceAccountPrefix) {
		return nil, fmt.Errorf("%w: not a service account: %s", ErrServiceAccountNotFound, id)
	}

	parts := strings.SplitN(identity.GetName(), ":", 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid service account ID format")
	}

	account := &ServiceAccount{
		ID:         id,
		ParentUser: parts[1],
		Status:     StatusActive,
		CreateDate: getCreationDate(identity.GetActions()),
		Expiration: getExpiration(identity.GetActions()),
	}

	if identity.Account != nil {
		account.Description = identity.Account.GetDisplayName()
	}

	if len(identity.Credentials) > 0 {
		account.AccessKeyId = identity.Credentials[0].GetAccessKey()
	}

	// Check if disabled
	for _, action := range identity.GetActions() {
		if action == disabledAction {
			account.Status = StatusInactive
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

	// Parse expiration if provided
	var expiration time.Time
	if req.Expiration != "" {
		var err error
		expiration, err = time.Parse(time.RFC3339, req.Expiration)
		if err != nil {
			return nil, fmt.Errorf("invalid expiration format: %w", err)
		}
	}

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
		// Store creation date and expiration in actions
		Actions: setExpiration(setCreationDate([]string{}, now), expiration),
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
		Status:          StatusActive,
		CreateDate:      now,
		Expiration:      expiration,
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
		return nil, fmt.Errorf("%w: %s", ErrServiceAccountNotFound, id)
	}

	if !strings.HasPrefix(identity.GetName(), serviceAccountPrefix) {
		return nil, fmt.Errorf("%w: not a service account: %s", ErrServiceAccountNotFound, id)
	}

	// Update description if provided
	if req.Description != "" {
		if identity.Account == nil {
			identity.Account = &iam_pb.Account{}
		}
		identity.Account.DisplayName = req.Description
	}

	// Update status by adding/removing disabled action
	if req.Status != "" {
		// Remove existing disabled marker
		newActions := make([]string, 0, len(identity.Actions))
		for _, action := range identity.Actions {
			if action != disabledAction {
				newActions = append(newActions, action)
			}
		}
		// Add disabled action if setting to Inactive
		if req.Status == StatusInactive {
			newActions = append(newActions, disabledAction)
		}
		identity.Actions = newActions
	}

	// Update expiration if provided
	if req.Expiration != "" {
		var expiration time.Time
		var err error
		expiration, err = time.Parse(time.RFC3339, req.Expiration)
		if err != nil {
			return nil, fmt.Errorf("invalid expiration format: %w", err)
		}
		identity.Actions = setExpiration(identity.Actions, expiration)
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
		Status:      StatusActive,
		CreateDate:  getCreationDate(identity.Actions),
	}

	if len(identity.Credentials) > 0 {
		result.AccessKeyId = identity.Credentials[0].GetAccessKey()
	}

	for _, action := range identity.Actions {
		if action == disabledAction {
			result.Status = StatusInactive
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
		return fmt.Errorf("%w: %s", ErrServiceAccountNotFound, id)
	}

	if !strings.HasPrefix(identity.GetName(), serviceAccountPrefix) {
		return fmt.Errorf("%w: not a service account: %s", ErrServiceAccountNotFound, id)
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

	if !strings.HasPrefix(identity.GetName(), serviceAccountPrefix) {
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
		Status:      StatusActive,
		CreateDate:  getCreationDate(identity.GetActions()),
		Expiration:  getExpiration(identity.GetActions()),
	}

	if identity.Account != nil {
		account.Description = identity.Account.GetDisplayName()
	}

	for _, action := range identity.GetActions() {
		if action == disabledAction {
			account.Status = StatusInactive
			break
		}
	}

	return account, nil
}

// ServiceAccount represents a service account for the Admin UI
type ServiceAccount struct {
	ID              string    `json:"id"`
	ParentUser      string    `json:"parent_user"`
	Description     string    `json:"description"`
	AccessKeyId     string    `json:"access_key_id"`
	SecretAccessKey string    `json:"secret_access_key,omitempty"` // Only on creation
	Status          string    `json:"status"`
	CreateDate      time.Time `json:"create_date"`
	Expiration      time.Time `json:"expiration,omitempty"` // Changed to time.Time
}

type ServiceAccountsData struct {
	Username        string           `json:"username"`
	ServiceAccounts []ServiceAccount `json:"service_accounts"`
	TotalAccounts   int              `json:"total_accounts"`
	ActiveAccounts  int              `json:"active_accounts"`
	LastUpdated     time.Time        `json:"last_updated"`
	AvailableUsers  []string         `json:"available_users"`
}

// CreateServiceAccountRequest is the request structure for creating a service account
type CreateServiceAccountRequest struct {
	ParentUser  string `json:"parent_user" binding:"required"`
	Description string `json:"description"`
	Expiration  string `json:"expiration"` // RFC3339 string
}

// UpdateServiceAccountRequest is the request structure for updating a service account
type UpdateServiceAccountRequest struct {
	Description string `json:"description"`
	Status      string `json:"status"`     // "Active" or "Inactive"
	Expiration  string `json:"expiration"` // RFC3339 string
}
