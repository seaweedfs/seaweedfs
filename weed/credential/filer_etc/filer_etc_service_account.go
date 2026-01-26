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

func (store *FilerEtcStore) loadServiceAccountsFromMultiFile(ctx context.Context, s3cfg *iam_pb.S3ApiConfiguration) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamServiceAccountsDirectory
		entries, err := listEntries(ctx, client, dir)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}

		for _, entry := range entries {
			if entry.IsDirectory {
				continue
			}

			var content []byte
			if len(entry.Content) > 0 {
				content = entry.Content
			} else {
				c, err := filer.ReadInsideFiler(client, dir, entry.Name)
				if err != nil {
					glog.Warningf("Failed to read service account file %s: %v", entry.Name, err)
					continue
				}
				content = c
			}

			if len(content) > 0 {
				sa := &iam_pb.ServiceAccount{}
				if err := json.Unmarshal(content, sa); err != nil {
					glog.Warningf("Failed to unmarshal service account %s: %v", entry.Name, err)
					continue
				}
				s3cfg.ServiceAccounts = append(s3cfg.ServiceAccounts, sa)
			}
		}
		return nil
	})
}

func (store *FilerEtcStore) saveServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		data, err := json.Marshal(sa)
		if err != nil {
			return err
		}
		return filer.SaveInsideFiler(client, filer.IamConfigDirectory+"/"+IamServiceAccountsDirectory, sa.Id+".json", data)
	})
}

func (store *FilerEtcStore) deleteServiceAccount(ctx context.Context, saId string) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
			Directory: filer.IamConfigDirectory + "/" + IamServiceAccountsDirectory,
			Name:      saId + ".json",
		})
		if err != nil && !strings.Contains(err.Error(), filer_pb.ErrNotFound.Error()) {
			return err
		}
		return nil
	})
}

func (store *FilerEtcStore) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	existing, err := store.GetServiceAccount(ctx, sa.Id)
	if err == nil && existing != nil {
		return fmt.Errorf("service account %s already exists", sa.Id)
	}
	return store.saveServiceAccount(ctx, sa)
}

func (store *FilerEtcStore) UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error {
	if sa.Id != id {
		return fmt.Errorf("service account ID mismatch")
	}
	return store.saveServiceAccount(ctx, sa)
}

func (store *FilerEtcStore) DeleteServiceAccount(ctx context.Context, id string) error {
	return store.deleteServiceAccount(ctx, id)
}

func (store *FilerEtcStore) GetServiceAccount(ctx context.Context, id string) (*iam_pb.ServiceAccount, error) {
	var sa *iam_pb.ServiceAccount
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, filer.IamConfigDirectory+"/"+IamServiceAccountsDirectory, id+".json")
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}
		if len(data) == 0 {
			return nil
		}
		sa = &iam_pb.ServiceAccount{}
		return json.Unmarshal(data, sa)
	})
	return sa, err
}

func (store *FilerEtcStore) ListServiceAccounts(ctx context.Context) ([]*iam_pb.ServiceAccount, error) {
	var accounts []*iam_pb.ServiceAccount
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamServiceAccountsDirectory
		entries, err := listEntries(ctx, client, dir)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}

		for _, entry := range entries {
			if entry.IsDirectory {
				continue
			}

			var content []byte
			if len(entry.Content) > 0 {
				content = entry.Content
			} else {
				c, err := filer.ReadInsideFiler(client, dir, entry.Name)
				if err != nil {
					glog.Warningf("Failed to read service account file %s: %v", entry.Name, err)
					continue
				}
				content = c
			}

			if len(content) > 0 {
				sa := &iam_pb.ServiceAccount{}
				if err := json.Unmarshal(content, sa); err != nil {
					glog.Warningf("Failed to unmarshal service account %s: %v", entry.Name, err)
					continue
				}
				accounts = append(accounts, sa)
			}
		}
		return nil
	})
	return accounts, err
}

func (store *FilerEtcStore) GetServiceAccountByAccessKey(ctx context.Context, accessKey string) (*iam_pb.ServiceAccount, error) {
	accounts, err := store.ListServiceAccounts(ctx)
	if err != nil {
		return nil, err
	}
	for _, sa := range accounts {
		if sa.Credential != nil && sa.Credential.AccessKey == accessKey {
			return sa, nil
		}
	}
	return nil, credential.ErrAccessKeyNotFound
}
