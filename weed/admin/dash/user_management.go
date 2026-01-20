package dash

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// CreateObjectStoreUser creates a new user using the credential manager
func (s *AdminServer) CreateObjectStoreUser(req CreateUserRequest) (*ObjectStoreUser, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	ctx := context.Background()

	// Create new identity
	newIdentity := &iam_pb.Identity{
		Name:        req.Username,
		Actions:     req.Actions,
		PolicyNames: req.PolicyNames,
	}

	// Add account if email is provided
	if req.Email != "" {
		newIdentity.Account = &iam_pb.Account{
			Id:           generateAccountId(),
			DisplayName:  req.Username,
			EmailAddress: req.Email,
		}
	}

	// Generate access key if requested
	var accessKey, secretKey string
	if req.GenerateKey {
		accessKey = generateAccessKey()
		secretKey = generateSecretKey()
		newIdentity.Credentials = []*iam_pb.Credential{
			{
				AccessKey: accessKey,
				SecretKey: secretKey,
			},
		}
	}

	// Create user using credential manager
	err := s.credentialManager.CreateUser(ctx, newIdentity)
	if err != nil {
		if err == credential.ErrUserAlreadyExists {
			return nil, fmt.Errorf("user %s already exists", req.Username)
		}
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	// Return created user
	user := &ObjectStoreUser{
		Username:    req.Username,
		Email:       req.Email,
		AccessKey:   accessKey,
		SecretKey:   secretKey,
		Permissions: req.Actions,
		PolicyNames: req.PolicyNames,
	}

	return user, nil
}

// UpdateObjectStoreUser updates an existing user
func (s *AdminServer) UpdateObjectStoreUser(username string, req UpdateUserRequest) (*ObjectStoreUser, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	ctx := context.Background()

	// Get existing user
	identity, err := s.credentialManager.GetUser(ctx, username)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return nil, fmt.Errorf("user %s not found", username)
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	// Create updated identity
	updatedIdentity := &iam_pb.Identity{
		Name:        identity.Name,
		Account:     identity.Account,
		Credentials: identity.Credentials,
		Actions:     identity.Actions,
		PolicyNames: identity.PolicyNames,
	}

	// Update actions if provided
	if req.Actions != nil {
		updatedIdentity.Actions = req.Actions
	}
	// Always update policy names when present in request (even if empty to allow clearing)
	if req.PolicyNames != nil {
		updatedIdentity.PolicyNames = req.PolicyNames
	}

	// Update email if provided
	if req.Email != "" {
		if updatedIdentity.Account == nil {
			updatedIdentity.Account = &iam_pb.Account{
				Id:          generateAccountId(),
				DisplayName: username,
			}
		}
		updatedIdentity.Account.EmailAddress = req.Email
	}

	// Update user using credential manager
	err = s.credentialManager.UpdateUser(ctx, username, updatedIdentity)
	if err != nil {
		return nil, fmt.Errorf("failed to update user: %w", err)
	}

	// Return updated user
	user := &ObjectStoreUser{
		Username:    username,
		Email:       req.Email,
		Permissions: updatedIdentity.Actions,
		PolicyNames: updatedIdentity.PolicyNames,
	}

	// Get first access key for display
	if len(updatedIdentity.Credentials) > 0 {
		user.AccessKey = updatedIdentity.Credentials[0].AccessKey
		user.SecretKey = updatedIdentity.Credentials[0].SecretKey
	}

	return user, nil
}

// DeleteObjectStoreUser deletes a user using the credential manager
func (s *AdminServer) DeleteObjectStoreUser(username string) error {
	if s.credentialManager == nil {
		return fmt.Errorf("credential manager not available")
	}

	ctx := context.Background()

	// Delete user using credential manager
	err := s.credentialManager.DeleteUser(ctx, username)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return fmt.Errorf("user %s not found", username)
		}
		return fmt.Errorf("failed to delete user: %w", err)
	}

	return nil
}

// GetObjectStoreUserDetails returns detailed information about a user
func (s *AdminServer) GetObjectStoreUserDetails(username string) (*UserDetails, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	ctx := context.Background()

	// Get user using credential manager
	identity, err := s.credentialManager.GetUser(ctx, username)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return nil, fmt.Errorf("user %s not found", username)
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	details := &UserDetails{
		Username:    username,
		Actions:     identity.Actions,
		PolicyNames: identity.PolicyNames,
	}

	// Set email from account if available
	if identity.Account != nil {
		details.Email = identity.Account.EmailAddress
	}

	// Convert credentials to access key info
	for _, cred := range identity.Credentials {
		details.AccessKeys = append(details.AccessKeys, AccessKeyInfo{
			AccessKey: cred.AccessKey,
			SecretKey: cred.SecretKey,
			Status:    cred.Status,
			CreatedAt: time.Now().AddDate(0, -1, 0), // Mock creation date
		})
	}

	return details, nil
}

// CreateAccessKey creates a new access key for a user
func (s *AdminServer) CreateAccessKey(username string) (*AccessKeyInfo, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	ctx := context.Background()

	// Check if user exists
	_, err := s.credentialManager.GetUser(ctx, username)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return nil, fmt.Errorf("user %s not found", username)
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	// Generate new access key
	accessKey := generateAccessKey()
	secretKey := generateSecretKey()

	credential := &iam_pb.Credential{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Status:    AccessKeyStatusActive,
	}

	// Create access key using credential manager
	err = s.credentialManager.CreateAccessKey(ctx, username, credential)
	if err != nil {
		return nil, fmt.Errorf("failed to create access key: %w", err)
	}

	return &AccessKeyInfo{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Status:    AccessKeyStatusActive,
		CreatedAt: time.Now(),
	}, nil
}

// DeleteAccessKey deletes an access key for a user
func (s *AdminServer) DeleteAccessKey(username, accessKeyId string) error {
	if s.credentialManager == nil {
		return fmt.Errorf("credential manager not available")
	}

	ctx := context.Background()

	// Delete access key using credential manager
	err := s.credentialManager.DeleteAccessKey(ctx, username, accessKeyId)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return fmt.Errorf("user %s not found", username)
		}
		if err == credential.ErrAccessKeyNotFound {
			return fmt.Errorf("access key %s not found for user %s", accessKeyId, username)
		}
		return fmt.Errorf("failed to delete access key: %w", err)
	}

	return nil
}

// UpdateAccessKeyStatus updates the status of an access key for a user
func (s *AdminServer) UpdateAccessKeyStatus(username, accessKeyId, status string) error {
	if s.credentialManager == nil {
		return fmt.Errorf("credential manager not available")
	}

	// Validate status against allowed values
	if status != AccessKeyStatusActive && status != AccessKeyStatusInactive {
		return fmt.Errorf("invalid status '%s': must be '%s' or '%s'", status, AccessKeyStatusActive, AccessKeyStatusInactive)
	}

	ctx := context.Background()

	// Get user using credential manager
	identity, err := s.credentialManager.GetUser(ctx, username)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return fmt.Errorf("user %s not found", username)
		}
		return fmt.Errorf("failed to get user: %w", err)
	}

	// Find and update the access key status
	found := false
	for _, cred := range identity.Credentials {
		if cred.AccessKey == accessKeyId {
			cred.Status = status
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("access key %s not found for user %s", accessKeyId, username)
	}

	// Update user using credential manager
	err = s.credentialManager.UpdateUser(ctx, username, identity)
	if err != nil {
		return fmt.Errorf("failed to update user access key status: %w", err)
	}

	return nil
}

// GetUserPolicies returns the policies for a user (actions)
func (s *AdminServer) GetUserPolicies(username string) ([]string, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	ctx := context.Background()

	// Get user using credential manager
	identity, err := s.credentialManager.GetUser(ctx, username)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return nil, fmt.Errorf("user %s not found", username)
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	return identity.Actions, nil
}

// UpdateUserPolicies updates the policies (actions) for a user
func (s *AdminServer) UpdateUserPolicies(username string, actions []string) error {
	if s.credentialManager == nil {
		return fmt.Errorf("credential manager not available")
	}

	ctx := context.Background()

	// Get existing user
	identity, err := s.credentialManager.GetUser(ctx, username)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return fmt.Errorf("user %s not found", username)
		}
		return fmt.Errorf("failed to get user: %w", err)
	}

	// Create updated identity with new actions
	updatedIdentity := &iam_pb.Identity{
		Name:        identity.Name,
		Account:     identity.Account,
		Credentials: identity.Credentials,
		Actions:     actions,
		PolicyNames: identity.PolicyNames,
	}

	// Update user using credential manager
	err = s.credentialManager.UpdateUser(ctx, username, updatedIdentity)
	if err != nil {
		return fmt.Errorf("failed to update user policies: %w", err)
	}

	return nil
}

// Helper functions for generating keys and IDs
func generateAccessKey() string {
	// Generate 20-character access key (AWS standard)
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 20)
	for i := range b {
		b[i] = charset[randomInt(len(charset))]
	}
	return string(b)
}

func generateSecretKey() string {
	// Generate 40-character secret key (AWS standard)
	b := make([]byte, 30) // 30 bytes = 40 characters in base64
	rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

func generateAccountId() string {
	// Generate 12-digit account ID
	b := make([]byte, 4)
	rand.Read(b)
	val := (uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]))
	return fmt.Sprintf("%012d", val)
}

func randomInt(max int) int {
	b := make([]byte, 1)
	rand.Read(b)
	return int(b[0]) % max
}

// Struct Definitions

// CreateUserRequest represents the request to create a new user
type CreateUserRequest struct {
	Username    string   `json:"username" binding:"required"`
	Email       string   `json:"email"`
	Actions     []string `json:"actions"`
	PolicyNames []string `json:"policyNames"`
	GenerateKey bool     `json:"generateKey"`
}

// UpdateUserRequest represents the request to update a user
type UpdateUserRequest struct {
	Email       string   `json:"email"`
	Actions     []string `json:"actions"`
	PolicyNames []string `json:"policyNames"`
}

// ObjectStoreUser represents a user in the object store
type ObjectStoreUser struct {
	Username    string   `json:"username"`
	Email       string   `json:"email"`
	AccessKey   string   `json:"accessKey,omitempty"`
	SecretKey   string   `json:"secretKey,omitempty"`
	Permissions []string `json:"permissions"`
	PolicyNames []string `json:"policyNames"`
}

// UserDetails represents detailed information about a user
type UserDetails struct {
	Username    string          `json:"username"`
	Email       string          `json:"email"`
	Actions     []string        `json:"actions"`
	PolicyNames []string        `json:"policyNames"`
	AccessKeys  []AccessKeyInfo `json:"accessKeys"`
}

// AccessKeyInfo represents information about an access key
type AccessKeyInfo struct {
	AccessKey string    `json:"accessKey"`
	SecretKey string    `json:"secretKey,omitempty"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}

const (
	AccessKeyStatusActive   = "Active"
	AccessKeyStatusInactive = "Inactive"
)

// GetObjectStoreUsers retrieves all object store users
func (s *AdminServer) GetObjectStoreUsers(ctx context.Context) ([]ObjectStoreUser, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	usernames, err := s.credentialManager.ListUsers(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}

	var users []ObjectStoreUser
	for _, username := range usernames {
		identity, err := s.credentialManager.GetUser(ctx, username)
		if err != nil {
			// Skip if user not found or error
			continue
		}

		// Skip service accounts from this list
		if isServiceAccount(username) {
			continue
		}
		
		var accessKey string
		if len(identity.Credentials) > 0 {
			accessKey = identity.Credentials[0].AccessKey
		}
        
        email := ""
        if identity.Account != nil {
            email = identity.Account.EmailAddress
        }

		user := ObjectStoreUser{
			Username:    username,
			Email:       email,
			AccessKey:   accessKey,
			Permissions: identity.Actions,
			PolicyNames: identity.PolicyNames,
		}
		users = append(users, user)
	}

	return users, nil
}

func isServiceAccount(username string) bool {
    // Check prefix "system:serviceaccount:"
    // But duplicate logic?
    // Let's assume for now checks "system:serviceaccount:" prefix
    return len(username) > 22 && username[:22] == "system:serviceaccount:"
}

type ObjectStoreUsersData struct {
	TotalUsers  int               `json:"total_users"`
	Users       []ObjectStoreUser `json:"users"`
	LastUpdated time.Time         `json:"last_updated"`
}
