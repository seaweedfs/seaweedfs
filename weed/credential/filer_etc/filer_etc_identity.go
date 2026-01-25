package filer_etc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

const (
	IamIdentitiesDirectory   = "identities"
	IamConfigurationFile     = "configuration.json"
	IamLegacyIdentityFile    = "identity.json"
	IamLegacyIdentityOldFile = "identity.json.old"
)

func (store *FilerEtcStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// Try to load from multi-file structure first (identities directory)
	hasMultiFile, err := store.loadFromMultiFile(ctx, s3cfg)
	if err != nil {
		return s3cfg, err
	}

	if hasMultiFile {
		return s3cfg, nil
	}

	// Fallback to legacy single file
	content, found, err := store.readInsideFiler(filer.IamConfigDirectory, IamLegacyIdentityFile)
	if err != nil {
		return s3cfg, err
	}
	if !found {
		// No config found at all
		return s3cfg, nil
	}

	if len(content) > 0 {
		if err := filer.ParseS3ConfigurationFromBytes(content, s3cfg); err != nil {
			glog.Errorf("Failed to parse legacy IAM configuration: %v", err)
			return s3cfg, err
		}
	}

	// Perform migration if we loaded legacy config
	if err := store.migrateToMultiFile(ctx, s3cfg); err != nil {
		glog.Errorf("Failed to migrate IAM configuration to multi-file layout: %v", err)
		// Return the loaded config anyway, migration failed but we have data
		return s3cfg, nil
	}

	return s3cfg, nil
}

func (store *FilerEtcStore) loadFromMultiFile(ctx context.Context, s3cfg *iam_pb.S3ApiConfiguration) (bool, error) {
	var hasIdentities bool

	// 1. List identities
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamIdentitiesDirectory
		entries, err := listEntries(ctx, client, dir)
		if err != nil {
			// If directory doesn't exist, it's not multi-file yet
			return nil
		}

		for _, entry := range entries {
			if entry.IsDirectory {
				continue
			}
			hasIdentities = true

			// Read each identity file
			// We can use the content from the entry if it's small (filer usually returns content for small files in ListEntries if configured,
			// but ListEntries might not return content depending on implementation.
			// filer.ListEntries calls ListEntriesRequest.
			// Safest to read individually or check entry.Content

			var content []byte
			if len(entry.Content) > 0 {
				content = entry.Content
			} else {
				// Read file content
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
				s3cfg.Identities = append(s3cfg.Identities, identity)
			}
		}
		return nil
	})

	if err != nil {
		return false, err
	}

	// 2. Load configuration.json (Accounts, etc.)
	content, found, err := store.readInsideFiler(filer.IamConfigDirectory, IamConfigurationFile)
	if err != nil {
		return false, err
	}
	if found && len(content) > 0 {
		// We use a temporary struct to load just the non-identity parts,
		// but ParseS3ConfigurationFromBytes expects full config.
		// It's robust to missing fields.
		tempCfg := &iam_pb.S3ApiConfiguration{}
		if err := filer.ParseS3ConfigurationFromBytes(content, tempCfg); err == nil {
			s3cfg.Accounts = tempCfg.Accounts
			// Copy other fields if any
		}
		return true, nil // Found configuration file, so it is multi-file
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

	// 2. Save rest of configuration
	if err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Create config with only accounts
		cleanCfg := &iam_pb.S3ApiConfiguration{
			Accounts: s3cfg.Accounts,
		}
		var buf bytes.Buffer
		if err := filer.ProtoToText(&buf, cleanCfg); err != nil {
			return err
		}
		return filer.SaveInsideFiler(client, filer.IamConfigDirectory, IamConfigurationFile, buf.Bytes())
	}); err != nil {
		return err
	}

	// 3. Rename legacy file
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// We use atomic rename if possible, but Filer 'AtomicRenameEntry' exists in filer_pb
		// util.JoinPath(filer.IamConfigDirectory, IamLegacyIdentityFile)

		_, err := client.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
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

	// 2. Save configuration file (accounts)
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		cleanCfg := &iam_pb.S3ApiConfiguration{
			Accounts: config.Accounts,
		}
		var buf bytes.Buffer
		if err := filer.ProtoToText(&buf, cleanCfg); err != nil {
			return err
		}
		return filer.SaveInsideFiler(client, filer.IamConfigDirectory, IamConfigurationFile, buf.Bytes())
	})
	if err != nil {
		return err
	}

	// 3. Cleanup removed identities (Full Sync)
	// Get list of existing identity files
	// Compare with config.Identities
	// Delete unknown ones

	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamIdentitiesDirectory
		entries, err := listEntries(ctx, client, dir)
		if err != nil {
			return nil // Should exist by now
		}

		validNames := make(map[string]bool)
		for _, id := range config.Identities {
			validNames[id.Name+".json"] = true
		}

		for _, entry := range entries {
			if !entry.IsDirectory && !validNames[entry.Name] {
				// Delete obsolete identity file
				client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
					Directory: dir,
					Name:      entry.Name,
				})
			}
		}
		return nil
	})
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
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		err := client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
			Directory: filer.IamConfigDirectory + "/" + IamIdentitiesDirectory,
			Name:      username + ".json",
		})
		if err != nil {
			// Map specific gRPC error to ErrUserNotFound if possible, but DeleteEntry usually returns success even if not found
			// unless strict. 'credential.ErrUserNotFound' is expected by caller?
			// The caller `DeleteUser` in handlers usually explicitly checks `ErrUserNotFound`.
			// Ideally we verify existence first?
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

// Access Key methods still need to operate on the identity object
// We can reuse GetUser / UpdateUser logic to avoid duplicating file IO code here,
// or implement optimized read-modify-write.
// Reusing GetUser/saveIdentity is cleanest.

func (store *FilerEtcStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	// This is inefficient in multi-file: requires scanning all files.
	// Assuming number of users is not huge.
	// For huge number of users, we'd need an index.

	s3cfg, err := store.LoadConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	for _, identity := range s3cfg.Identities {
		for _, credential := range identity.Credentials {
			if credential.AccessKey == accessKey {
				// Return the specific identity
				return identity, nil
			}
		}
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
		data, err := json.Marshal(identity)
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
