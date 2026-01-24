package filer_multiple

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

const (
	IdentitiesDirectory = "/etc/seaweedfs/identities"
)

func init() {
	credential.Stores = append(credential.Stores, &FilerMultipleStore{})
}

// FilerMultipleStore implements CredentialStore using SeaweedFS filer for storage
// storing each identity in a separate file
type FilerMultipleStore struct {
	filerAddressFunc func() pb.ServerAddress // Function to get current active filer
	grpcDialOption   grpc.DialOption
	mu               sync.RWMutex // Protects filerAddressFunc and grpcDialOption
}

func (store *FilerMultipleStore) GetName() credential.CredentialStoreTypeName {
	return credential.StoreTypeFilerMultiple
}

func (store *FilerMultipleStore) Initialize(configuration util.Configuration, prefix string) error {
	// Handle nil configuration gracefully
	if configuration != nil {
		filerAddr := configuration.GetString(prefix + "filer")
		if filerAddr != "" {
			// Static configuration - use fixed address
			store.mu.Lock()
			store.filerAddressFunc = func() pb.ServerAddress {
				return pb.ServerAddress(filerAddr)
			}
			store.mu.Unlock()
		}
	}
	// Note: filerAddressFunc can be set later via SetFilerAddressFunc method
	return nil
}

// SetFilerAddressFunc sets a function that returns the current active filer address
// This enables high availability by using the currently active filer
func (store *FilerMultipleStore) SetFilerAddressFunc(getFiler func() pb.ServerAddress, grpcDialOption grpc.DialOption) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.filerAddressFunc = getFiler
	store.grpcDialOption = grpcDialOption
}

// withFilerClient executes a function with a filer client
func (store *FilerMultipleStore) withFilerClient(fn func(client filer_pb.SeaweedFilerClient) error) error {
	store.mu.RLock()
	if store.filerAddressFunc == nil {
		store.mu.RUnlock()
		return fmt.Errorf("filer_multiple: filer not yet available - please wait for filer discovery to complete and try again")
	}

	filerAddress := store.filerAddressFunc()
	dialOption := store.grpcDialOption
	store.mu.RUnlock()

	if filerAddress == "" {
		return fmt.Errorf("filer_multiple: no filer discovered yet - please ensure a filer is running and accessible")
	}

	// Use the pb.WithGrpcFilerClient helper similar to existing code
	return pb.WithGrpcFilerClient(false, 0, filerAddress, dialOption, fn)
}

func (store *FilerMultipleStore) Shutdown() {
	// No cleanup needed for file store
}

func (store *FilerMultipleStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// List and process all identity files in the directory using streaming callback
		return filer_pb.SeaweedList(ctx, client, IdentitiesDirectory, "", func(entry *filer_pb.Entry, isLast bool) error {
			if entry.IsDirectory || !strings.HasSuffix(entry.Name, ".json") {
				return nil
			}

			content, err := filer.ReadInsideFiler(client, IdentitiesDirectory, entry.Name)
			if err != nil {
				glog.Warningf("Failed to read identity file %s: %v", entry.Name, err)
				return nil // Continue with next file
			}

			identity := &iam_pb.Identity{}
			if err := json.Unmarshal(content, identity); err != nil {
				glog.Warningf("Failed to parse identity file %s: %v", entry.Name, err)
				return nil // Continue with next file
			}

			s3cfg.Identities = append(s3cfg.Identities, identity)
			return nil
		}, "", false, 10000)
	})

	if err != nil {
		// If listing failed because directory doesn't exist, treat as empty config
		if err == filer_pb.ErrNotFound {
			return s3cfg, nil
		}
		return s3cfg, err
	}

	return s3cfg, nil
}

func (store *FilerMultipleStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	// This operation is expensive for multiple files mode as it would overwrite everything
	// But we implement it for interface compliance.
	// We will write each identity to a separate file and remove stale files.

	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// 1. List existing identity files
		existingFileNames := make(map[string]bool)
		err := filer_pb.SeaweedList(ctx, client, IdentitiesDirectory, "", func(entry *filer_pb.Entry, isLast bool) error {
			if !entry.IsDirectory && strings.HasSuffix(entry.Name, ".json") {
				existingFileNames[entry.Name] = true
			}
			return nil
		}, "", false, 10000)

		if err != nil && err != filer_pb.ErrNotFound {
			return fmt.Errorf("failed to list existing identities: %w", err)
		}

		// 2. Build a set of identity keys present in the provided config
		newKeys := make(map[string]bool)
		for _, identity := range config.Identities {
			newKeys[identity.Name+".json"] = true
		}

		// 3. Write/overwrite each identity using saveIdentity
		for _, identity := range config.Identities {
			if err := store.saveIdentity(ctx, client, identity); err != nil {
				return err
			}
		}

		// 4. Delete any existing files whose identity key is not in the new set
		for filename := range existingFileNames {
			if !newKeys[filename] {
				err := filer_pb.DoRemove(ctx, client, IdentitiesDirectory, filename, false, false, false, false, nil)
				if err != nil && err != filer_pb.ErrNotFound {
					glog.Warningf("failed to remove stale identity file %s: %v", filename, err)
				}
			}
		}

		return nil
	})
}

func (store *FilerMultipleStore) saveIdentity(ctx context.Context, client filer_pb.SeaweedFilerClient, identity *iam_pb.Identity) error {
	data, err := json.Marshal(identity)
	if err != nil {
		return fmt.Errorf("failed to marshal identity %s: %w", identity.Name, err)
	}

	filename := identity.Name + ".json"
	return filer.SaveInsideFiler(client, IdentitiesDirectory, filename, data)
}

func (store *FilerMultipleStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		filename := identity.Name + ".json"
		// Check if exists
		exists, err := store.exists(ctx, client, IdentitiesDirectory, filename)
		if err != nil {
			return err
		}
		if exists {
			return credential.ErrUserAlreadyExists
		}

		return store.saveIdentity(ctx, client, identity)
	})
}

func (store *FilerMultipleStore) exists(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, name string) (bool, error) {
	request := &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	}
	resp, err := filer_pb.LookupEntry(ctx, client, request)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return resp.Entry != nil, nil
}

func (store *FilerMultipleStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	var identity *iam_pb.Identity
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		filename := username + ".json"
		content, err := filer.ReadInsideFiler(client, IdentitiesDirectory, filename)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return credential.ErrUserNotFound
			}
			return err
		}

		identity = &iam_pb.Identity{}
		if err := json.Unmarshal(content, identity); err != nil {
			return fmt.Errorf("failed to parse identity: %w", err)
		}
		return nil
	})
	return identity, err
}

func (store *FilerMultipleStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		filename := username + ".json"
		// Check if exists
		exists, err := store.exists(ctx, client, IdentitiesDirectory, filename)
		if err != nil {
			return err
		}
		if !exists {
			return credential.ErrUserNotFound
		}

		// If username changed (renamed), we need to create new file and then delete old one
		if identity.Name != username {
			// Check if the new username already exists to prevent overwrites
			newFilename := identity.Name + ".json"
			exists, err := store.exists(ctx, client, IdentitiesDirectory, newFilename)
			if err != nil {
				return err
			}
			if exists {
				return fmt.Errorf("user %s already exists", identity.Name)
			}

			// Create new identity file FIRST
			if err := store.saveIdentity(ctx, client, identity); err != nil {
				return err
			}

			// Delete old user file SECOND
			err = filer_pb.DoRemove(ctx, client, IdentitiesDirectory, filename, false, false, false, false, nil)
			if err != nil && err != filer_pb.ErrNotFound {
				// Rollback: try to remove the newly created file if deleting the old one failed
				_ = filer_pb.DoRemove(ctx, client, IdentitiesDirectory, newFilename, false, false, false, false, nil)
				return fmt.Errorf("failed to remove old identity file %s: %w", filename, err)
			}
			return nil
		}

		return store.saveIdentity(ctx, client, identity)
	})
}

func (store *FilerMultipleStore) DeleteUser(ctx context.Context, username string) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		filename := username + ".json"
		err := filer_pb.DoRemove(ctx, client, IdentitiesDirectory, filename, false, false, false, false, nil)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}
		return nil
	})
}

func (store *FilerMultipleStore) ListUsers(ctx context.Context) ([]string, error) {
	var usernames []string
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		err := filer_pb.SeaweedList(ctx, client, IdentitiesDirectory, "", func(entry *filer_pb.Entry, isLast bool) error {
			if !entry.IsDirectory && strings.HasSuffix(entry.Name, ".json") {
				name := strings.TrimSuffix(entry.Name, ".json")
				usernames = append(usernames, name)
			}
			return nil
		}, "", false, 10000)

		if err != nil {
			if err == filer_pb.ErrNotFound {
				// Treat as empty if directory not found
				return nil
			}
			return err
		}
		return nil
	})
	return usernames, err
}

func (store *FilerMultipleStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	// This is inefficient in file store without index.
	// We must iterate all users.
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	for _, identity := range config.Identities {
		for _, credential := range identity.Credentials {
			if credential.AccessKey == accessKey {
				return identity, nil
			}
		}
	}

	return nil, credential.ErrAccessKeyNotFound
}

func (store *FilerMultipleStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	identity, err := store.GetUser(ctx, username)
	if err != nil {
		return err
	}

	// Check duplicates
	for _, existing := range identity.Credentials {
		if existing.AccessKey == cred.AccessKey {
			return fmt.Errorf("access key already exists")
		}
	}

	identity.Credentials = append(identity.Credentials, cred)
	return store.UpdateUser(ctx, username, identity)
}

func (store *FilerMultipleStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
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

	return store.UpdateUser(ctx, username, identity)
}
