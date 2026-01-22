package filer_etc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/protobuf/encoding/protojson"
)

func (store *FilerEtcStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	glog.V(1).Infof("Loading IAM configuration from %s/%s (using current active filer)",
		filer.IamConfigDirectory, filer.IamIdentityFile)

	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Use ReadInsideFiler instead of ReadEntry since identity.json is small
		// and stored inline. ReadEntry requires a master client for chunked files,
		// but ReadInsideFiler only reads inline content.
		content, err := filer.ReadInsideFiler(client, filer.IamConfigDirectory, filer.IamIdentityFile)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				glog.V(1).Infof("IAM identity file not found at %s/%s, no credentials loaded",
					filer.IamConfigDirectory, filer.IamIdentityFile)
				return nil
			}
			glog.Errorf("Failed to read IAM identity file from %s/%s: %v",
				filer.IamConfigDirectory, filer.IamIdentityFile, err)
			return err
		}

		if len(content) == 0 {
			glog.V(1).Infof("IAM identity file at %s/%s is empty",
				filer.IamConfigDirectory, filer.IamIdentityFile)
			return nil
		}

		glog.V(2).Infof("Read %d bytes from %s/%s",
			len(content), filer.IamConfigDirectory, filer.IamIdentityFile)

		if err := filer.ParseS3ConfigurationFromBytes(content, s3cfg); err != nil {
			glog.Errorf("Failed to parse IAM configuration from %s/%s: %v",
				filer.IamConfigDirectory, filer.IamIdentityFile, err)
			return err
		}

		glog.V(1).Infof("Successfully parsed IAM configuration with %d identities and %d accounts",
			len(s3cfg.Identities), len(s3cfg.Accounts))
		return nil
	})

	if err != nil {
		return s3cfg, err
	}

	// Log loaded identities for debugging
	if glog.V(2) {
		for _, identity := range s3cfg.Identities {
			credCount := len(identity.Credentials)
			actionCount := len(identity.Actions)
			glog.V(2).Infof("  Identity: %s (credentials: %d, actions: %d)",
				identity.Name, credCount, actionCount)
			for _, cred := range identity.Credentials {
				glog.V(3).Infof("    Access Key: %s", cred.AccessKey)
			}
		}
	}

	return s3cfg, nil
}

func (store *FilerEtcStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ProtoToText(&buf, config); err != nil {
			return fmt.Errorf("failed to marshal configuration: %w", err)
		}
		return filer.SaveInsideFiler(client, filer.IamConfigDirectory, filer.IamIdentityFile, buf.Bytes())
	})
}

// validateIdentity validates identity input to prevent security vulnerabilities and data corruption
func validateIdentity(identity *iam_pb.Identity) error {
	if identity == nil {
		return fmt.Errorf("identity cannot be nil")
	}
	
	// Validate username
	if identity.Name == "" {
		return fmt.Errorf("username cannot be empty")
	}
	
	// Prevent path traversal attacks
	if strings.Contains(identity.Name, "/") || strings.Contains(identity.Name, "\\") {
		return fmt.Errorf("username cannot contain path separators: %s", identity.Name)
	}
	if strings.Contains(identity.Name, "..") {
		return fmt.Errorf("username cannot contain path traversal patterns: %s", identity.Name)
	}
	
	// Prevent control characters and other problematic characters
	if strings.ContainsAny(identity.Name, "\x00\n\r\t") {
		return fmt.Errorf("username contains invalid control characters: %s", identity.Name)
	}
	
	// Validate credentials exist and are unique
	// if len(identity.Credentials) == 0 {
	// 	return fmt.Errorf("identity must have at least one credential")
	// }
	
	accessKeys := make(map[string]bool)
	for i, cred := range identity.Credentials {
		if cred == nil {
			return fmt.Errorf("credential at index %d is nil", i)
		}
		if cred.AccessKey == "" {
			return fmt.Errorf("credential at index %d has empty access key", i)
		}
		if cred.SecretKey == "" {
			return fmt.Errorf("credential at index %d has empty secret key", i)
		}
		
		// Check for duplicate access keys within same user
		if accessKeys[cred.AccessKey] {
			return fmt.Errorf("duplicate access key within same user: %s", cred.AccessKey)
		}
		accessKeys[cred.AccessKey] = true
	}
	
	return nil
}


func (store *FilerEtcStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	// Validate input to prevent security vulnerabilities
	if err := validateIdentity(identity); err != nil {
		return fmt.Errorf("invalid identity: %w", err)
	}
	
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Check if user exists (check both individual file and legacy config is ideal, but checking file is minimal requirement)
		// For thoroughness we could list, but just trying to read the file is faster check for existence
		if _, err := filer.ReadInsideFiler(client, filer.IamUsersDirectory, identity.Name+".json"); err == nil {
			return credential.ErrUserAlreadyExists
		}

		// Save to individual file
		glog.V(0).Infof("DEBUG: Creating user %s at %s/%s.json", identity.Name, filer.IamUsersDirectory, identity.Name)
		return store.saveIdentityToFiler(client, identity)
	})
}

// Helper to save identity to individual file
func (store *FilerEtcStore) saveIdentityToFiler(client filer_pb.SeaweedFilerClient, identity *iam_pb.Identity) error {
	data, err := protojson.Marshal(identity)
	if err != nil {
		return fmt.Errorf("failed to marshal identity: %w", err)
	}
	err = filer.SaveInsideFiler(client, filer.IamUsersDirectory, identity.Name+".json", data)
	if err != nil {
		glog.Errorf("DEBUG: Failed to save user %s: %v", identity.Name, err)
	} else {
		glog.V(0).Infof("DEBUG: Successfully saved user %s (%d bytes)", identity.Name, len(data))
	}
	return err
}

func (store *FilerEtcStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	glog.V(0).Infof("DEBUG: GetUser called for %s", username)
	var identity *iam_pb.Identity
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		path := username + ".json"
		glog.V(0).Infof("DEBUG: Reading user file %s/%s", filer.IamUsersDirectory, path)
		data, err := filer.ReadInsideFiler(client, filer.IamUsersDirectory, path)
		if err != nil {
			glog.V(0).Infof("DEBUG: ReadInsideFiler failed for %s: %v", path, err)
			if err == filer_pb.ErrNotFound {
				// Fallback to legacy config check (optional, but good for transition)
				// For now fast fail or we can reuse LoadConfiguration logic if strictly needed.
				// Given migration runs on startup, we assume individual files exist.
				return credential.ErrUserNotFound
			}
			return err
		}
		glog.V(0).Infof("DEBUG: ReadInsideFiler succeeded for %s, data len: %d", path, len(data))

		identity = &iam_pb.Identity{}
		if err := protojson.Unmarshal(data, identity); err != nil {
			glog.Errorf("DEBUG: Failed to unmarshal user %s: %v", username, err)
			return fmt.Errorf("failed to parse user file: %w", err)
		}
		return nil
	})

	return identity, err
}

func (store *FilerEtcStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	glog.V(0).Infof("DEBUG: UpdateUser called for %s (new name: %s)", username, identity.Name)
	// Validate input to prevent security vulnerabilities
	if err := validateIdentity(identity); err != nil {
		return fmt.Errorf("invalid identity: %w", err)
	}
	
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Ensure user exists
		path := username + ".json"
		if _, err := filer.ReadInsideFiler(client, filer.IamUsersDirectory, path); err != nil {
			glog.V(0).Infof("DEBUG: UpdateUser: User %s not found (err: %v)", username, err)
			if err == filer_pb.ErrNotFound {
				return credential.ErrUserNotFound
			}
			return err
		}
		
		// If renaming (identity.Name != username), use create-then-delete pattern
		// to prevent data loss if the create operation fails
		if username != identity.Name {
			// Step 1: Create new file first (safe - if this fails, old file remains intact)
			if err := store.saveIdentityToFiler(client, identity); err != nil {
				return fmt.Errorf("failed to create renamed user file: %w", err)
			}
			
			// Step 2: Delete old file (if this fails, we have duplicate but no data loss)
			if err := filer.DeleteInsideFiler(client, filer.IamUsersDirectory, username+".json"); err != nil {
				// Rollback: Delete the newly created file to maintain consistency
				glog.Warningf("Failed to delete old user file %s after rename, attempting rollback", username)
				if rollbackErr := filer.DeleteInsideFiler(client, filer.IamUsersDirectory, identity.Name+".json"); rollbackErr != nil {
					glog.Errorf("Rollback failed: could not delete newly created file %s: %v", identity.Name, rollbackErr)
					return fmt.Errorf("rename failed and rollback failed: original error: %w, rollback error: %v", err, rollbackErr)
				}
				return fmt.Errorf("failed to delete old user file during rename: %w", err)
			}
			
			return nil
		}

		// Not a rename, just update the file in place
		return store.saveIdentityToFiler(client, identity)
	})
}

func (store *FilerEtcStore) DeleteUser(ctx context.Context, username string) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		err := filer.DeleteInsideFiler(client, filer.IamUsersDirectory, username+".json")
		if err == filer_pb.ErrNotFound {
			return credential.ErrUserNotFound
		}
		return err
	})
}

func (store *FilerEtcStore) ListUsers(ctx context.Context) ([]string, error) {
	var usernames []string
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		req := &filer_pb.ListEntriesRequest{
			Directory: filer.IamUsersDirectory,
			Limit:     100000, // TODO: Implement pagination when user count grows
		}
		stream, err := client.ListEntries(ctx, req)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}
		
		for {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			
			resp, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			if resp.Entry.IsDirectory {
				continue
			}
			if len(resp.Entry.Name) > 5 && resp.Entry.Name[len(resp.Entry.Name)-5:] == ".json" {
				usernames = append(usernames, resp.Entry.Name[:len(resp.Entry.Name)-5])
			}
		}
		
		// Warn if approaching limit
		if len(usernames) > 50000 {
			glog.Warningf("IAM: User count (%d) is high. Consider implementing pagination or access key indexing for better performance", len(usernames))
		}
		
		return nil
	})
	return usernames, err
}

func (store *FilerEtcStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	// 1. Try legacy monolithic config first
	config, err := store.LoadConfiguration(ctx)
	if err == nil {
		for _, identity := range config.Identities {
			for _, credential := range identity.Credentials {
				if credential.AccessKey == accessKey {
					return identity, nil
				}
			}
		}
	}

	// 2. Try individual user files
	// Note: This is O(n) scan - consider implementing access key index for production
	// List all users from /iam/users directory
	var foundIdentity *iam_pb.Identity
	scannedCount := 0

	err = store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// List entries in /iam/users
		// Use direct gRPC call to avoid needing MasterClient
		req := &filer_pb.ListEntriesRequest{
			Directory: filer.IamUsersDirectory,
			Limit:     1000, // Limit scan to prevent excessive load
		}
		
		stream, err := client.ListEntries(ctx, req)
		if err != nil {
			return err
		}

		for {
			// Check context cancellation to allow early termination
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			
			resp, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			
			if resp.Entry.IsDirectory {
				continue
			}
			
			scannedCount++
			
			// Read user file
			data, err := filer.ReadInsideFiler(client, filer.IamUsersDirectory, resp.Entry.Name)
			if err != nil {
				glog.Warningf("Failed to read user file %s: %v", resp.Entry.Name, err)
				continue
			}

			user := &iam_pb.Identity{}
			if err := protojson.Unmarshal(data, user); err != nil {
				glog.Warningf("Failed to parse user file %s: %v", resp.Entry.Name, err)
				continue
			}

			// Check credentials
			for _, cred := range user.Credentials {
				if cred.AccessKey == accessKey {
					foundIdentity = user
					return nil // Found!
				}
			}
		}
		return nil
	})

	if foundIdentity != nil {
		// Warn about performance if scanning many users
		if scannedCount > 100 {
			glog.V(1).Infof("GetUserByAccessKey scanned %d users - consider implementing access key index", scannedCount)
		}
		return foundIdentity, nil
	}

	return nil, credential.ErrAccessKeyNotFound
}

func (store *FilerEtcStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	user, err := store.GetUser(ctx, username)
	if err != nil {
		return err
	}
	
	for _, existingCred := range user.Credentials {
		if existingCred.AccessKey == cred.AccessKey {
			return fmt.Errorf("access key %s already exists", cred.AccessKey)
		}
	}
	
	user.Credentials = append(user.Credentials, cred)
	return store.UpdateUser(ctx, username, user)
}

func (store *FilerEtcStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	user, err := store.GetUser(ctx, username)
	if err != nil {
		return err
	}
	
	found := false
	for i, cred := range user.Credentials {
		if cred.AccessKey == accessKey {
			user.Credentials = append(user.Credentials[:i], user.Credentials[i+1:]...)
			found = true
			break
		}
	}
	
	if !found {
		return credential.ErrAccessKeyNotFound
	}
	
	return store.UpdateUser(ctx, username, user)
}
