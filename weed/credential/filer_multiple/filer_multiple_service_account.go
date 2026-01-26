package filer_multiple

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

const (
	ServiceAccountsDirectory = "/etc/seaweedfs/service_accounts"
)

func (store *FilerMultipleStore) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	// For filer_multiple, we write to the defined directory
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		filename := sa.Id + ".json"
		exists, err := store.exists(ctx, client, ServiceAccountsDirectory, filename)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("service account %s already exists", sa.Id)
		}
		return store.saveServiceAccount(ctx, client, sa)
	})
}

func (store *FilerMultipleStore) UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		if sa.Id != id {
			return fmt.Errorf("service account ID mismatch")
		}
		filename := sa.Id + ".json"
		exists, err := store.exists(ctx, client, ServiceAccountsDirectory, filename)
		if err != nil {
			return err
		}
		if !exists {
			return credential.ErrUserNotFound // Reuse or add ErrServiceAccountNotFound
		}
		return store.saveServiceAccount(ctx, client, sa)
	})
}

func (store *FilerMultipleStore) DeleteServiceAccount(ctx context.Context, id string) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		filename := id + ".json"
		err := filer_pb.DoRemove(ctx, client, ServiceAccountsDirectory, filename, false, false, false, false, nil)
		if err != nil && err != filer_pb.ErrNotFound {
			return err
		}
		return nil
	})
}

func (store *FilerMultipleStore) GetServiceAccount(ctx context.Context, id string) (*iam_pb.ServiceAccount, error) {
	var sa *iam_pb.ServiceAccount
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		filename := id + ".json"
		content, err := filer.ReadInsideFiler(client, ServiceAccountsDirectory, filename)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}
		if len(content) == 0 {
			return nil
		}
		sa = &iam_pb.ServiceAccount{}
		if err := json.Unmarshal(content, sa); err != nil {
			return fmt.Errorf("failed to unmarshal service account: %w", err)
		}
		return nil
	})
	return sa, err
}

func (store *FilerMultipleStore) ListServiceAccounts(ctx context.Context) ([]*iam_pb.ServiceAccount, error) {
	// Not implemented fully yet for filer_multiple in this pass
	return nil, nil
}

func (store *FilerMultipleStore) saveServiceAccount(ctx context.Context, client filer_pb.SeaweedFilerClient, sa *iam_pb.ServiceAccount) error {
	// ... marshal and save
	return nil
}
