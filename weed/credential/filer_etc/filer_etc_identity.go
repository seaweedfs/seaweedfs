package filer_etc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

const (
	IamIdentitiesDirectory      = "identities"
	IamServiceAccountsDirectory = "service_accounts"
	IamLegacyIdentityFile       = "identity.json"
	IamLegacyIdentityOldFile    = "identity.json.old"
)

func (store *FilerEtcStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// 1. Load from legacy single file (low priority)
	content, foundLegacy, err := store.readInsideFiler(filer.IamConfigDirectory, IamLegacyIdentityFile)
	if err != nil {
		return s3cfg, err
	}
	if foundLegacy && len(content) > 0 {
		if err := filer.ParseS3ConfigurationFromBytes(content, s3cfg); err != nil {
			glog.Errorf("Failed to parse legacy IAM configuration: %v", err)
			return s3cfg, err
		}
	}

	// 2. Load from multi-file structure (high priority, overrides legacy details)
	if _, err := store.loadFromMultiFile(ctx, s3cfg); err != nil {
		return s3cfg, err
	}

	// 3. Load service accounts
	if err := store.loadServiceAccountsFromMultiFile(ctx, s3cfg); err != nil {
		return s3cfg, fmt.Errorf("failed to load service accounts: %w", err)
	}

	// 4. Perform migration if we loaded legacy config
	// This ensures that all identities (including legacy ones) are written to individual files
	// and the legacy file is renamed.
	if foundLegacy {
		if err := store.migrateToMultiFile(ctx, s3cfg); err != nil {
			glog.Errorf("Failed to migrate IAM configuration to multi-file layout: %v", err)
			return s3cfg, nil
		}
	}

	return s3cfg, nil
}

func (store *FilerEtcStore) loadFromMultiFile(ctx context.Context, s3cfg *iam_pb.S3ApiConfiguration) (bool, error) {
	var hasIdentities bool

	// Helper to find existing identity index
	findIdentity := func(name string) int {
		for i, identity := range s3cfg.Identities {
			if identity.Name == name {
				return i
			}
		}
		return -1
	}

	// 1. List identities
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamIdentitiesDirectory
		entries, err := listEntries(ctx, client, dir)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				// If directory doesn't exist, it's not multi-file yet
				return nil
			}
			return err
		}

		for _, entry := range entries {
			if entry.IsDirectory {
				continue
			}
			hasIdentities = true

			var content []byte
			if len(entry.Content) > 0 {
				content = entry.Content
			} else {
				c, err := filer.ReadInsideFiler(client, dir, entry.Name)
				if err != nil {
					glog.Warningf("Failed to read identity file %s: %v", entry.Name, err)
					continue
				}
				content = c
			}

			if len(content) > 0 {
				identity := &iam_pb.Identity{}
				if err := json.Unmarshal(content, identity); err != nil {
					glog.Warningf("Failed to unmarshal identity %s: %v", entry.Name, err)
					continue
				}

				// Merge logic: Overwrite existing or Append
				idx := findIdentity(identity.Name)
				if idx != -1 {
					s3cfg.Identities[idx] = identity
				} else {
					s3cfg.Identities = append(s3cfg.Identities, identity)
				}
			}
		}
		return nil
	})

	if err != nil {
		return false, err
	}

	return hasIdentities, nil
}

func (store *FilerEtcStore) migrateToMultiFile(ctx context.Context, s3cfg *iam_pb.S3ApiConfiguration) error {
	glog.Infof("Migrating IAM configuration to multi-file layout...")

	// 1. Save all identities
	for _, identity := range s3cfg.Identities {
		if err := store.saveIdentity(ctx, identity); err != nil {
			return err
		}
	}

	// 2. Save all service accounts
	for _, sa := range s3cfg.ServiceAccounts {
		if err := store.saveServiceAccount(ctx, sa); err != nil {
			return err
		}
	}

	// 3. Rename legacy file
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.AtomicRenameEntry(ctx, &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: filer.IamConfigDirectory,
			OldName:      IamLegacyIdentityFile,
			NewDirectory: filer.IamConfigDirectory,
			NewName:      IamLegacyIdentityOldFile,
		})
		return err
	})
}

func (store *FilerEtcStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	// 1. Save all identities
	for _, identity := range config.Identities {
		if err := store.saveIdentity(ctx, identity); err != nil {
			return err
		}
	}

	// 2. Save all service accounts
	for _, sa := range config.ServiceAccounts {
		if err := store.saveServiceAccount(ctx, sa); err != nil {
			return err
		}
	}

	// 3. Cleanup removed identities (Full Sync)
	if err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamIdentitiesDirectory
		entries, err := listEntries(ctx, client, dir)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}

		validNames := make(map[string]bool)
		for _, id := range config.Identities {
			validNames[id.Name+".json"] = true
		}

		for _, entry := range entries {
			if !entry.IsDirectory && !validNames[entry.Name] {
				// Delete obsolete identity file
				if _, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
					Directory: dir,
					Name:      entry.Name,
				}); err != nil {
					glog.Warningf("Failed to delete obsolete identity file %s: %v", entry.Name, err)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// 4. Cleanup removed service accounts (Full Sync)
	if err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamServiceAccountsDirectory
		entries, err := listEntries(ctx, client, dir)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}

		validNames := make(map[string]bool)
		for _, sa := range config.ServiceAccounts {
			validNames[sa.Id+".json"] = true
		}

		for _, entry := range entries {
			if !entry.IsDirectory && !validNames[entry.Name] {
				if _, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
					Directory: dir,
					Name:      entry.Name,
				}); err != nil {
					glog.Warningf("Failed to delete obsolete service account file %s: %v", entry.Name, err)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (store *FilerEtcStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	// Check if user exists (read specific file)
	existing, err := store.GetUser(ctx, identity.Name)
	if err == nil && existing != nil {
		return credential.ErrUserAlreadyExists
	}
	return store.saveIdentity(ctx, identity)
}

func (store *FilerEtcStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	var identity *iam_pb.Identity
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, filer.IamConfigDirectory+"/"+IamIdentitiesDirectory, username+".json")
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return credential.ErrUserNotFound
			}
			return err
		}
		if len(data) == 0 {
			return credential.ErrUserNotFound
		}
		identity = &iam_pb.Identity{}
		return json.Unmarshal(data, identity)
	})
	return identity, err
}

func (store *FilerEtcStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	// Ensure user exists
	if _, err := store.GetUser(ctx, username); err != nil {
		return err
	}
	// If username changes, we need to delete old and create new. But usually UpdateUser keeps username unless renames are allowed.
	// identity.Name vs username.
	if username != identity.Name {
		// Rename case
		if err := store.DeleteUser(ctx, username); err != nil {
			return err
		}
	}
	return store.saveIdentity(ctx, identity)
}

func (store *FilerEtcStore) DeleteUser(ctx context.Context, username string) error {
	// Verify existence first to return ErrUserNotFound if applicable
	if _, err := store.GetUser(ctx, username); err != nil {
		return err
	}

	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
			Directory: filer.IamConfigDirectory + "/" + IamIdentitiesDirectory,
			Name:      username + ".json",
		})
		if err != nil {
			if strings.Contains(err.Error(), filer_pb.ErrNotFound.Error()) {
				return credential.ErrUserNotFound
			}
			return err
		}
		return nil
	})
}

func (store *FilerEtcStore) ListUsers(ctx context.Context) ([]string, error) {
	var usernames []string
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		entries, err := listEntries(ctx, client, filer.IamConfigDirectory+"/"+IamIdentitiesDirectory)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}
		for _, entry := range entries {
			if !entry.IsDirectory && len(entry.Name) > 5 && entry.Name[len(entry.Name)-5:] == ".json" {
				usernames = append(usernames, entry.Name[:len(entry.Name)-5])
			}
		}
		return nil
	})
	return usernames, err
}

func (store *FilerEtcStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	// Optimized: Iterate over identity files directly instead of loading full config.
	// This avoids triggering migration side effects.

	var foundIdentity *iam_pb.Identity

	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamIdentitiesDirectory
		entries, err := listEntries(ctx, client, dir)
		if err != nil {
			// If not found, check legacy file? No, optimization requested to avoid side effects.
			// If migration hasn't run, this will return empty/not found.
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}

		for _, entry := range entries {
			if entry.IsDirectory || !strings.HasSuffix(entry.Name, ".json") {
				continue
			}

			// Read file content
			var content []byte
			if len(entry.Content) > 0 {
				content = entry.Content
			} else {
				c, err := filer.ReadInsideFiler(client, dir, entry.Name)
				if err != nil {
					continue
				}
				content = c
			}

			if len(content) > 0 {
				identity := &iam_pb.Identity{}
				if err := json.Unmarshal(content, identity); err != nil {
					continue
				}

				for _, cred := range identity.Credentials {
					if cred.AccessKey == accessKey {
						foundIdentity = identity
						return nil // Found match, stop iteration
					}
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	if foundIdentity != nil {
		return foundIdentity, nil
	}

	return nil, credential.ErrAccessKeyNotFound
}

func (store *FilerEtcStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	identity, err := store.GetUser(ctx, username)
	if err != nil {
		return err
	}

	for _, existing := range identity.Credentials {
		if existing.AccessKey == cred.AccessKey {
			return fmt.Errorf("access key %s already exists", cred.AccessKey)
		}
	}

	identity.Credentials = append(identity.Credentials, cred)
	return store.saveIdentity(ctx, identity)
}

func (store *FilerEtcStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	identity, err := store.GetUser(ctx, username)
	if err != nil {
		return err
	}

	found := false
	for i, cred := range identity.Credentials {
		if cred.AccessKey == accessKey {
			identity.Credentials = append(identity.Credentials[:i], identity.Credentials[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return credential.ErrAccessKeyNotFound
	}

	return store.saveIdentity(ctx, identity)
}

// Helpers

func (store *FilerEtcStore) saveIdentity(ctx context.Context, identity *iam_pb.Identity) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		data, err := json.MarshalIndent(identity, "", "  ")
		if err != nil {
			return err
		}
		return filer.SaveInsideFiler(client, filer.IamConfigDirectory+"/"+IamIdentitiesDirectory, identity.Name+".json", data)
	})
}

func (store *FilerEtcStore) readInsideFiler(dir string, name string) ([]byte, bool, error) {
	var content []byte
	found := false
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		c, err := filer.ReadInsideFiler(client, dir, name)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}
		content = c
		found = true
		return nil
	})
	return content, found, err
}

func listEntries(ctx context.Context, client filer_pb.SeaweedFilerClient, dir string) ([]*filer_pb.Entry, error) {
	var entries []*filer_pb.Entry
	err := filer_pb.SeaweedList(ctx, client, dir, "", func(entry *filer_pb.Entry, isLast bool) error {
		entries = append(entries, entry)
		return nil
	}, "", false, 100000)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// AttachUserPolicy attaches a managed policy to a user by policy name
func (store *FilerEtcStore) AttachUserPolicy(ctx context.Context, username string, policyName string) error {
	// Get user
	identity, err := store.GetUser(ctx, username)
	if err != nil {
		return err
	}

	// Verify policy exists
	policy, err := store.GetPolicy(ctx, policyName)
	if err != nil {
		return err
	}
	if policy == nil {
		return credential.ErrPolicyNotFound
	}

	// Check if already attached
	for _, p := range identity.PolicyNames {
		if p == policyName {
			return credential.ErrPolicyAlreadyAttached
		}
	}

	identity.PolicyNames = append(identity.PolicyNames, policyName)
	return store.saveIdentity(ctx, identity)
}

// DetachUserPolicy detaches a managed policy from a user
func (store *FilerEtcStore) DetachUserPolicy(ctx context.Context, username string, policyName string) error {
	identity, err := store.GetUser(ctx, username)
	if err != nil {
		return err
	}

	found := false
	var newPolicies []string
	for _, p := range identity.PolicyNames {
		if p == policyName {
			found = true
		} else {
			newPolicies = append(newPolicies, p)
		}
	}

	if !found {
		return credential.ErrPolicyNotAttached
	}

	identity.PolicyNames = newPolicies
	return store.saveIdentity(ctx, identity)
}

// ListAttachedUserPolicies returns the list of policy names attached to a user
func (store *FilerEtcStore) ListAttachedUserPolicies(ctx context.Context, username string) ([]string, error) {
	identity, err := store.GetUser(ctx, username)
	if err != nil {
		return nil, err
	}
	return identity.PolicyNames, nil
}
