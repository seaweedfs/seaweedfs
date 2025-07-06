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
		Name:    req.Username,
		Actions: req.Actions,
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
		return nil, fmt.Errorf("failed to create user: %v", err)
	}

	// Return created user
	user := &ObjectStoreUser{
		Username:    req.Username,
		Email:       req.Email,
		AccessKey:   accessKey,
		SecretKey:   secretKey,
		Permissions: req.Actions,
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
		return nil, fmt.Errorf("failed to get user: %v", err)
	}

	// Create updated identity
	updatedIdentity := &iam_pb.Identity{
		Name:        identity.Name,
		Account:     identity.Account,
		Credentials: identity.Credentials,
		Actions:     identity.Actions,
	}

	// Update actions if provided
	if len(req.Actions) > 0 {
		updatedIdentity.Actions = req.Actions
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
		return nil, fmt.Errorf("failed to update user: %v", err)
	}

	// Return updated user
	user := &ObjectStoreUser{
		Username:    username,
		Email:       req.Email,
		Permissions: updatedIdentity.Actions,
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
		return fmt.Errorf("failed to delete user: %v", err)
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
		return nil, fmt.Errorf("failed to get user: %v", err)
	}

	details := &UserDetails{
		Username: username,
		Actions:  identity.Actions,
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
		return nil, fmt.Errorf("failed to get user: %v", err)
	}

	// Generate new access key
	accessKey := generateAccessKey()
	secretKey := generateSecretKey()

	credential := &iam_pb.Credential{
		AccessKey: accessKey,
		SecretKey: secretKey,
	}

	// Create access key using credential manager
	err = s.credentialManager.CreateAccessKey(ctx, username, credential)
	if err != nil {
		return nil, fmt.Errorf("failed to create access key: %v", err)
	}

	return &AccessKeyInfo{
		AccessKey: accessKey,
		SecretKey: secretKey,
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
		return fmt.Errorf("failed to delete access key: %v", err)
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
		return nil, fmt.Errorf("failed to get user: %v", err)
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
		return fmt.Errorf("failed to get user: %v", err)
	}

	// Create updated identity with new actions
	updatedIdentity := &iam_pb.Identity{
		Name:        identity.Name,
		Account:     identity.Account,
		Credentials: identity.Credentials,
		Actions:     actions,
	}

	// Update user using credential manager
	err = s.credentialManager.UpdateUser(ctx, username, updatedIdentity)
	if err != nil {
		return fmt.Errorf("failed to update user policies: %v", err)
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
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%012d", b[0]<<24|b[1]<<16|b[2]<<8|b[3])
}

func randomInt(max int) int {
	b := make([]byte, 1)
	rand.Read(b)
	return int(b[0]) % max
}
