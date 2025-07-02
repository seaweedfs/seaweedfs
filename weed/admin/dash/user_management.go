package dash

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// CreateObjectStoreUser creates a new user in identity.json
func (s *AdminServer) CreateObjectStoreUser(req CreateUserRequest) (*ObjectStoreUser, error) {
	// Use credential manager if available
	if s.credentialManager != nil {
		return s.createObjectStoreUserWithManager(req)
	}

	// Fall back to original file-based approach
	return s.createObjectStoreUserLegacy(req)
}

// createObjectStoreUserWithManager creates a user using the credential manager
func (s *AdminServer) createObjectStoreUserWithManager(req CreateUserRequest) (*ObjectStoreUser, error) {
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

// createObjectStoreUserLegacy creates a user using the legacy file-based approach
func (s *AdminServer) createObjectStoreUserLegacy(req CreateUserRequest) (*ObjectStoreUser, error) {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// Load existing configuration
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf); err != nil {
			if err != filer_pb.ErrNotFound {
				return err
			}
		}
		if buf.Len() > 0 {
			return filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to load IAM configuration: %v", err)
	}

	// Check if user already exists
	for _, identity := range s3cfg.Identities {
		if identity.Name == req.Username {
			return nil, fmt.Errorf("user %s already exists", req.Username)
		}
	}

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

	// Add to configuration
	s3cfg.Identities = append(s3cfg.Identities, newIdentity)

	// Save configuration
	err = s.saveS3Configuration(s3cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to save IAM configuration: %v", err)
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
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// Load existing configuration
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf); err != nil {
			return err
		}
		if buf.Len() > 0 {
			return filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to load IAM configuration: %v", err)
	}

	// Find and update user
	var updatedIdentity *iam_pb.Identity
	for _, identity := range s3cfg.Identities {
		if identity.Name == username {
			updatedIdentity = identity
			break
		}
	}

	if updatedIdentity == nil {
		return nil, fmt.Errorf("user %s not found", username)
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

	// Save configuration
	err = s.saveS3Configuration(s3cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to save IAM configuration: %v", err)
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

// DeleteObjectStoreUser deletes a user from identity.json
func (s *AdminServer) DeleteObjectStoreUser(username string) error {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// Load existing configuration
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf); err != nil {
			return err
		}
		if buf.Len() > 0 {
			return filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to load IAM configuration: %v", err)
	}

	// Find and remove user
	found := false
	for i, identity := range s3cfg.Identities {
		if identity.Name == username {
			s3cfg.Identities = append(s3cfg.Identities[:i], s3cfg.Identities[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("user %s not found", username)
	}

	// Save configuration
	return s.saveS3Configuration(s3cfg)
}

// GetObjectStoreUserDetails returns detailed information about a user
func (s *AdminServer) GetObjectStoreUserDetails(username string) (*UserDetails, error) {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// Load existing configuration
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf); err != nil {
			return err
		}
		if buf.Len() > 0 {
			return filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to load IAM configuration: %v", err)
	}

	// Find user
	for _, identity := range s3cfg.Identities {
		if identity.Name == username {
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
	}

	return nil, fmt.Errorf("user %s not found", username)
}

// CreateAccessKey creates a new access key for a user
func (s *AdminServer) CreateAccessKey(username string) (*AccessKeyInfo, error) {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// Load existing configuration
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf); err != nil {
			return err
		}
		if buf.Len() > 0 {
			return filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to load IAM configuration: %v", err)
	}

	// Find user
	var targetIdentity *iam_pb.Identity
	for _, identity := range s3cfg.Identities {
		if identity.Name == username {
			targetIdentity = identity
			break
		}
	}

	if targetIdentity == nil {
		return nil, fmt.Errorf("user %s not found", username)
	}

	// Generate new access key
	accessKey := generateAccessKey()
	secretKey := generateSecretKey()

	newCredential := &iam_pb.Credential{
		AccessKey: accessKey,
		SecretKey: secretKey,
	}

	// Add to user's credentials
	targetIdentity.Credentials = append(targetIdentity.Credentials, newCredential)

	// Save configuration
	err = s.saveS3Configuration(s3cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to save IAM configuration: %v", err)
	}

	return &AccessKeyInfo{
		AccessKey: accessKey,
		SecretKey: secretKey,
		CreatedAt: time.Now(),
	}, nil
}

// DeleteAccessKey deletes an access key for a user
func (s *AdminServer) DeleteAccessKey(username, accessKeyId string) error {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// Load existing configuration
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf); err != nil {
			return err
		}
		if buf.Len() > 0 {
			return filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to load IAM configuration: %v", err)
	}

	// Find user and remove access key
	for _, identity := range s3cfg.Identities {
		if identity.Name == username {
			for i, cred := range identity.Credentials {
				if cred.AccessKey == accessKeyId {
					identity.Credentials = append(identity.Credentials[:i], identity.Credentials[i+1:]...)
					return s.saveS3Configuration(s3cfg)
				}
			}
			return fmt.Errorf("access key %s not found for user %s", accessKeyId, username)
		}
	}

	return fmt.Errorf("user %s not found", username)
}

// GetUserPolicies returns the policies for a user (actions)
func (s *AdminServer) GetUserPolicies(username string) ([]string, error) {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// Load existing configuration
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf); err != nil {
			return err
		}
		if buf.Len() > 0 {
			return filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to load IAM configuration: %v", err)
	}

	// Find user and return policies
	for _, identity := range s3cfg.Identities {
		if identity.Name == username {
			return identity.Actions, nil
		}
	}

	return nil, fmt.Errorf("user %s not found", username)
}

// UpdateUserPolicies updates the policies (actions) for a user
func (s *AdminServer) UpdateUserPolicies(username string, actions []string) error {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// Load existing configuration
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf); err != nil {
			return err
		}
		if buf.Len() > 0 {
			return filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to load IAM configuration: %v", err)
	}

	// Find user and update policies
	for _, identity := range s3cfg.Identities {
		if identity.Name == username {
			identity.Actions = actions
			return s.saveS3Configuration(s3cfg)
		}
	}

	return fmt.Errorf("user %s not found", username)
}

// saveS3Configuration saves the S3 configuration to identity.json
func (s *AdminServer) saveS3Configuration(s3cfg *iam_pb.S3ApiConfiguration) error {
	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ProtoToText(&buf, s3cfg); err != nil {
			return fmt.Errorf("failed to marshal configuration: %v", err)
		}

		return filer.SaveInsideFiler(client, filer.IamConfigDirectory, filer.IamIdentityFile, buf.Bytes())
	})
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
