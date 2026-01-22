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
	if len(identity.Credentials) == 0 {
		return fmt.Errorf("identity must have at least one credential")
	}
	
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
	glog.V(1).Infof("[Scalable IAM] CreateUser: Starting creation for user '%s'", identity.Name)
	
	// Validate input to prevent security vulnerabilities
	if err := validateIdentity(identity); err != nil {
		glog.Errorf("[Scalable IAM] CreateUser: Validation failed for user '%s': %v", identity.Name, err)
		return fmt.Errorf("invalid identity: %w", err)
	}
	
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		glog.V(2).Infof("[Scalable IAM] CreateUser: Checking if user '%s' already exists at %s/%s.json", identity.Name, filer.IamUsersDirectory, identity.Name)
		
		// Check if user exists
		if _, err := filer.ReadInsideFiler(client, filer.IamUsersDirectory, identity.Name+".json"); err == nil {
			glog.Warningf("[Scalable IAM] CreateUser: User '%s' already exists", identity.Name)
			return credential.ErrUserAlreadyExists
		}

		// Save to individual file
		glog.V(1).Infof("[Scalable IAM] CreateUser: Saving user '%s' with %d credentials to individual file", identity.Name, len(identity.Credentials))
		if err := store.saveIdentityToFiler(client, identity); err != nil {
			glog.Errorf("[Scalable IAM] CreateUser: Failed to save user '%s': %v", identity.Name, err)
			return err
		}
		
		glog.V(0).Infof("[Scalable IAM] ✓ CreateUser: Successfully created user '%s'", identity.Name)
		return nil
	})
}

// Helper to save identity to individual file
func (store *FilerEtcStore) saveIdentityToFiler(client filer_pb.SeaweedFilerClient, identity *iam_pb.Identity) error {
	data, err := protojson.Marshal(identity)
	if err != nil {
		return fmt.Errorf("failed to marshal identity: %w", err)
	}
	err = filer.SaveInsideFiler(client, filer.IamUsersDirectory, identity.Name+".json", data)
	return err
}

func (store *FilerEtcStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	glog.V(2).Infof("[Scalable IAM] GetUser: Looking up user '%s'", username)
	
	var identity *iam_pb.Identity
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		path := username + ".json"
		glog.V(3).Infof("[Scalable IAM] GetUser: Reading file %s/%s", filer.IamUsersDirectory, path)
		
		data, err := filer.ReadInsideFiler(client, filer.IamUsersDirectory, path)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				glog.V(2).Infof("[Scalable IAM] GetUser: User '%s' not found", username)
				return credential.ErrUserNotFound
			}
			glog.Errorf("[Scalable IAM] GetUser: Error reading user '%s': %v", username, err)
			return err
		}

		identity = &iam_pb.Identity{}
		if err := protojson.Unmarshal(data, identity); err != nil {
			glog.Errorf("[Scalable IAM] GetUser: Failed to parse user file '%s': %v", username, err)
			return fmt.Errorf("failed to parse user file: %w", err)
		}
		
		glog.V(2).Infof("[Scalable IAM] GetUser: Successfully retrieved user '%s' with %d credentials", username, len(identity.Credentials))
		return nil
	})

	return identity, err
}

func (store *FilerEtcStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	glog.V(1).Infof("[Scalable IAM] UpdateUser: Updating user '%s' (new name: '%s')", username, identity.Name)
	
	// Validate input
	if err := validateIdentity(identity); err != nil {
		glog.Errorf("[Scalable IAM] UpdateUser: Validation failed for identity '%s': %v", identity.Name, err)
		return fmt.Errorf("invalid identity: %w", err)
	}
	
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Ensure user exists
		path := username + ".json"
		if _, err := filer.ReadInsideFiler(client, filer.IamUsersDirectory, path); err != nil {
			glog.V(0).Infof("DEBUG: UpdateUser: User %s not found (err: %v)", username, err)
			if err == filer_pb.ErrNotFound {
				glog.Warningf("[Scalable IAM] UpdateUser: User '%s' not found", username)
				return credential.ErrUserNotFound
			}
			return err
		}
		
		// If renaming, use create-then-delete pattern
		if username != identity.Name {
			glog.V(1).Infof("[Scalable IAM] UpdateUser: Renaming user '%s' → '%s'", username, identity.Name)
			
			// Step 1: Create new file
			if err := store.saveIdentityToFiler(client, identity); err != nil {
				glog.Errorf("[Scalable IAM] UpdateUser: Failed to create renamed user file '%s': %v", identity.Name, err)
				return fmt.Errorf("failed to create renamed user file: %w", err)
			}
			glog.V(2).Infof("[Scalable IAM] UpdateUser: Created new file for '%s'", identity.Name)
			
			// Step 2: Delete old file
			if err := filer.DeleteInsideFiler(client, filer.IamUsersDirectory, username+".json"); err != nil {
				glog.Errorf("[Scalable IAM] UpdateUser: Failed to delete old file '%s', attempting rollback", username)
				// Rollback
				if rollbackErr := filer.DeleteInsideFiler(client, filer.IamUsersDirectory, identity.Name+".json"); rollbackErr != nil {
					glog.Errorf("[Scalable IAM] UpdateUser: ROLLBACK FAILED for '%s': %v", identity.Name, rollbackErr)
					return fmt.Errorf("rename failed and rollback failed: original error: %w, rollback error: %v", err, rollbackErr)
				}
				glog.V(1).Infof("[Scalable IAM] UpdateUser: Rollback successful, reverted '%s'", identity.Name)
				return fmt.Errorf("failed to delete old user file during rename: %w", err)
			}
			
			glog.V(0).Infof("[Scalable IAM] ✓ UpdateUser: Successfully renamed '%s' → '%s'", username, identity.Name)
			return nil
		}

		// Not a rename, just update in place
		glog.V(2).Infof("[Scalable IAM] UpdateUser: Updating user '%s' in place", username)
		if err := store.saveIdentityToFiler(client, identity); err != nil {
			glog.Errorf("[Scalable IAM] UpdateUser: Failed to save updates for '%s': %v", username, err)
			return err
		}
		glog.V(0).Infof("[Scalable IAM] ✓ UpdateUser: Successfully updated user '%s'", username)
		return nil
	})
}

func (store *FilerEtcStore) DeleteUser(ctx context.Context, username string) error {
	glog.V(1).Infof("[Scalable IAM] DeleteUser: Deleting user '%s'", username)
	
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		err := filer.DeleteInsideFiler(client, filer.IamUsersDirectory, username+".json")
		if err == filer_pb.ErrNotFound {
			glog.Warningf("[Scalable IAM] DeleteUser: User '%s' not found", username)
			return credential.ErrUserNotFound
		}
		if err != nil {
			glog.Errorf("[Scalable IAM] DeleteUser: Failed to delete '%s': %v", username, err)
			return err
		}
		glog.V(0).Infof("[Scalable IAM] ✓ DeleteUser: Successfully deleted user '%s'", username)
		return nil
	})
}

func (store *FilerEtcStore) ListUsers(ctx context.Context) ([]string, error) {
	glog.V(1).Infof("[Scalable IAM] ListUsers: Starting user listing from %s", filer.IamUsersDirectory)
	
	var usernames []string
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		req := &filer_pb.ListEntriesRequest{
			Directory: filer.IamUsersDirectory,
			Limit:     100000,
		}
		
		glog.V(2).Infof("[Scalable IAM] ListUsers: Listing entries with limit=%d", req.Limit)
		
		stream, err := client.ListEntries(ctx, req)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				glog.V(1).Infof("[Scalable IAM] ListUsers: Directory %s not found, returning empty list", filer.IamUsersDirectory)
				return nil
			}
			glog.Errorf("[Scalable IAM] ListUsers: Failed to list entries: %v", err)
			return err
		}
		
		for {
			// Check context cancellation
			select {
			case <-ctx.Done():
				glog.Warningf("[Scalable IAM] ListUsers: Context cancelled after %d users", len(usernames))
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
		
		glog.V(1).Infof("[Scalable IAM] ListUsers: Found %d users", len(usernames))
		
		// Warn if approaching limit
		if len(usernames) > 50000 {
			glog.Warningf("[Scalable IAM] ListUsers: High user count (%d). Consider implementing pagination or access key indexing!", len(usernames))
		}
		
		return nil
	})
	
	if err != nil {
		glog.Errorf("[Scalable IAM] ListUsers: Failed with error: %v", err)
	}
	
	return usernames, err
}

func (store *FilerEtcStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	glog.V(1).Infof("[Scalable IAM] GetUserByAccessKey: Searching for access key '%s'", accessKey)
	
	// 1. Try legacy monolithic config first
	glog.V(2).Infof("[Scalable IAM] GetUserByAccessKey: Checking legacy config first")
	config, err := store.LoadConfiguration(ctx)
	if err == nil {
		for _, identity := range config.Identities {
			for _, credential := range identity.Credentials {
				if credential.AccessKey == accessKey {
					glog.V(1).Infof("[Scalable IAM] GetUserByAccessKey: Found user '%s' in legacy config", identity.Name)
					return identity, nil
				}
			}
		}
		glog.V(2).Infof("[Scalable IAM] GetUserByAccessKey: Not found in legacy config (%d identities checked)", len(config.Identities))
	}

	// 2. Try individual user files with pagination
	glog.V(1).Infof("[Scalable IAM] GetUserByAccessKey: Starting paginated scan of %s", filer.IamUsersDirectory)
	var foundIdentity *iam_pb.Identity
	scannedCount := 0
	batchSize := uint32(1000)
	lastFileName := ""
	batchNum := 0

	err = store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		for {
			batchNum++
			glog.V(2).Infof("[Scalable IAM] GetUserByAccessKey: Fetching batch #%d (limit=%d, startFrom='%s')", batchNum, batchSize, lastFileName)
			
			req := &filer_pb.ListEntriesRequest{
				Directory:          filer.IamUsersDirectory,
				Limit:              batchSize,
				StartFromFileName:  lastFileName,
				InclusiveStartFrom: false,
			}
			
			stream, err := client.ListEntries(ctx, req)
			if err != nil {
				glog.Errorf("[Scalable IAM] GetUserByAccessKey: Failed to list entries in batch #%d: %v", batchNum, err)
				return err
			}

			entryCount := 0
			for {
				// Check context cancellation
				select {
				case <-ctx.Done():
					glog.Warningf("[Scalable IAM] GetUserByAccessKey: Context cancelled after scanning %d users", scannedCount)
					return ctx.Err()
				default:
				}
				
				resp, err := stream.Recv()
				if err != nil {
					if err == io.EOF {
						glog.V(3).Infof("[Scalable IAM] GetUserByAccessKey: Batch #%d complete (%d entries)", batchNum, entryCount)
						break
					}
					glog.Errorf("[Scalable IAM] GetUserByAccessKey: Stream error in batch #%d: %v", batchNum, err)
					return err
				}
				
				entryCount++
				lastFileName = resp.Entry.Name

				if resp.Entry.IsDirectory {
					continue
				}
				
				scannedCount++
				if scannedCount%100 == 0 {
					glog.V(2).Infof("[Scalable IAM] GetUserByAccessKey: Scanned %d users so far...", scannedCount)
				}
				
				// Read user file
				data, err := filer.ReadInsideFiler(client, filer.IamUsersDirectory, resp.Entry.Name)
				if err != nil {
					glog.Warningf("[Scalable IAM] GetUserByAccessKey: Failed to read user file %s: %v", resp.Entry.Name, err)
					continue
				}

				user := &iam_pb.Identity{}
				if err := protojson.Unmarshal(data, user); err != nil {
					glog.Warningf("[Scalable IAM] GetUserByAccessKey: Failed to parse user file %s: %v", resp.Entry.Name, err)
					continue
				}

				// Check credentials
				for _, cred := range user.Credentials {
					if cred.AccessKey == accessKey {
						glog.V(0).Infof("[Scalable IAM] ✓ GetUserByAccessKey: Found user '%s' after scanning %d users in %d batches", user.Name, scannedCount, batchNum)
						foundIdentity = user
						return nil
					}
				}
			}

			if foundIdentity != nil {
				return nil
			}

			// If we received fewer entries than the limit, we've reached the end
			if uint32(entryCount) < batchSize {
				glog.V(2).Infof("[Scalable IAM] GetUserByAccessKey: Reached end of directory (batch #%d had %d < %d entries)", batchNum, entryCount, batchSize)
				break
			}
		}
		return nil
	})

	if foundIdentity != nil {
		if scannedCount > 100 {
			glog.Warningf("[Scalable IAM] GetUserByAccessKey: Performance warning - scanned %d users. Consider implementing access key index!", scannedCount)
		}
		return foundIdentity, nil
	}

	if err != nil {
		glog.Errorf("[Scalable IAM] GetUserByAccessKey: Search failed after scanning %d users: %v", scannedCount, err)
		return nil, err
	}

	glog.V(1).Infof("[Scalable IAM] GetUserByAccessKey: Access key '%s' not found after scanning %d users in %d batches", accessKey, scannedCount, batchNum)
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
