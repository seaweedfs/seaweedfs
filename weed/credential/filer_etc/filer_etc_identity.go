package filer_etc

import (
	"bytes"
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func (store *FilerEtcStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Use ReadInsideFiler instead of ReadEntry since identity.json is small
		// and stored inline. ReadEntry requires a master client for chunked files,
		// but ReadInsideFiler only reads inline content.
		content, err := filer.ReadInsideFiler(client, filer.IamConfigDirectory, filer.IamIdentityFile)
		if err != nil {
			if err != filer_pb.ErrNotFound {
				return err
			}
			return nil
		}
		if len(content) > 0 {
			return filer.ParseS3ConfigurationFromBytes(content, s3cfg)
		}
		return nil
	})

	return s3cfg, err
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

func (store *FilerEtcStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	// Load existing configuration
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
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
		return nil, fmt.Errorf("failed to load configuration: %w", err)
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
		return fmt.Errorf("failed to load configuration: %w", err)
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
		return fmt.Errorf("failed to load configuration: %w", err)
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
		return nil, fmt.Errorf("failed to load configuration: %w", err)
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
		return nil, fmt.Errorf("failed to load configuration: %w", err)
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
		return fmt.Errorf("failed to load configuration: %w", err)
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
		return fmt.Errorf("failed to load configuration: %w", err)
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
