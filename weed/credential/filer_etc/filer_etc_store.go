package filer_etc

import (
	"bytes"
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

func init() {
	credential.Stores = append(credential.Stores, &FilerEtcStore{})
}

// FilerEtcStore implements CredentialStore using SeaweedFS filer for storage
type FilerEtcStore struct {
	filerGrpcAddress string
	grpcDialOption   grpc.DialOption
}

func (store *FilerEtcStore) GetName() credential.CredentialStoreTypeName {
	return credential.StoreTypeFilerEtc
}

func (store *FilerEtcStore) Initialize(configuration util.Configuration, prefix string) error {
	// Handle nil configuration gracefully
	if configuration != nil {
		store.filerGrpcAddress = configuration.GetString(prefix + "filer")
		// TODO: Initialize grpcDialOption based on configuration
	}
	// Note: filerGrpcAddress can be set later via SetFilerClient method
	return nil
}

// SetFilerClient sets the filer client details for the file store
func (store *FilerEtcStore) SetFilerClient(filerAddress string, grpcDialOption grpc.DialOption) {
	store.filerGrpcAddress = filerAddress
	store.grpcDialOption = grpcDialOption
}

// withFilerClient executes a function with a filer client
func (store *FilerEtcStore) withFilerClient(fn func(client filer_pb.SeaweedFilerClient) error) error {
	if store.filerGrpcAddress == "" {
		return fmt.Errorf("filer address not configured")
	}

	// Use the pb.WithGrpcFilerClient helper similar to existing code
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(store.filerGrpcAddress), store.grpcDialOption, fn)
}

func (store *FilerEtcStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
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

	return s3cfg, err
}

func (store *FilerEtcStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ProtoToText(&buf, config); err != nil {
			return fmt.Errorf("failed to marshal configuration: %v", err)
		}
		return filer.SaveInsideFiler(client, filer.IamConfigDirectory, filer.IamIdentityFile, buf.Bytes())
	})
}

func (store *FilerEtcStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	// Load existing configuration
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	// Check if user already exists
	for _, existingIdentity := range config.Identities {
		if existingIdentity.Name == identity.Name {
			return credential.ErrUserAlreadyExists
		}
	}

	// Add new identity
	config.Identities = append(config.Identities, identity)

	// Save configuration
	return store.SaveConfiguration(ctx, config)
}

func (store *FilerEtcStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %v", err)
	}

	for _, identity := range config.Identities {
		if identity.Name == username {
			return identity, nil
		}
	}

	return nil, credential.ErrUserNotFound
}

func (store *FilerEtcStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	// Find and update the user
	for i, existingIdentity := range config.Identities {
		if existingIdentity.Name == username {
			config.Identities[i] = identity
			return store.SaveConfiguration(ctx, config)
		}
	}

	return credential.ErrUserNotFound
}

func (store *FilerEtcStore) DeleteUser(ctx context.Context, username string) error {
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	// Find and remove the user
	for i, identity := range config.Identities {
		if identity.Name == username {
			config.Identities = append(config.Identities[:i], config.Identities[i+1:]...)
			return store.SaveConfiguration(ctx, config)
		}
	}

	return credential.ErrUserNotFound
}

func (store *FilerEtcStore) ListUsers(ctx context.Context) ([]string, error) {
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %v", err)
	}

	var usernames []string
	for _, identity := range config.Identities {
		usernames = append(usernames, identity.Name)
	}

	return usernames, nil
}

func (store *FilerEtcStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %v", err)
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

func (store *FilerEtcStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	// Find the user and add the credential
	for _, identity := range config.Identities {
		if identity.Name == username {
			// Check if access key already exists
			for _, existingCred := range identity.Credentials {
				if existingCred.AccessKey == cred.AccessKey {
					return fmt.Errorf("access key %s already exists", cred.AccessKey)
				}
			}

			identity.Credentials = append(identity.Credentials, cred)
			return store.SaveConfiguration(ctx, config)
		}
	}

	return credential.ErrUserNotFound
}

func (store *FilerEtcStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	// Find the user and remove the credential
	for _, identity := range config.Identities {
		if identity.Name == username {
			for i, cred := range identity.Credentials {
				if cred.AccessKey == accessKey {
					identity.Credentials = append(identity.Credentials[:i], identity.Credentials[i+1:]...)
					return store.SaveConfiguration(ctx, config)
				}
			}
			return credential.ErrAccessKeyNotFound
		}
	}

	return credential.ErrUserNotFound
}

func (store *FilerEtcStore) Shutdown() {
	// No cleanup needed for file store
}
