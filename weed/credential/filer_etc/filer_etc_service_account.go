package filer_etc

import (
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

// validateServiceAccount performs basic validation of the service account
func validateServiceAccount(sa *iam_pb.ServiceAccount) error {
	if sa == nil {
		return fmt.Errorf("service account cannot be nil")
	}
	if sa.Id == "" {
		return fmt.Errorf("service account ID is required")
	}
	// Prevent path traversal
	if strings.Contains(sa.Id, "/") || strings.Contains(sa.Id, "\\") || strings.Contains(sa.Id, "..") {
		return fmt.Errorf("service account ID contains invalid characters")
	}
	if sa.ParentUser == "" {
		return fmt.Errorf("service account parent user is required")
	}
	return nil
}

func (store *FilerEtcStore) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	glog.V(1).Infof("[Scalable IAM] CreateServiceAccount: Creating service account %s", sa.Id)
	
	if err := validateServiceAccount(sa); err != nil {
		return err
	}

	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Check if exists
		path := sa.Id + ".json"
		if _, err := filer.ReadInsideFiler(client, filer.IamServiceAccountsDirectory, path); err == nil {
			return fmt.Errorf("service account %s already exists", sa.Id)
		}

		return store.saveServiceAccountToFiler(client, sa)
	})
}

func (store *FilerEtcStore) GetServiceAccount(ctx context.Context, serviceAccountId string) (*iam_pb.ServiceAccount, error) {
	glog.V(2).Infof("[Scalable IAM] GetServiceAccount: Looking up %s", serviceAccountId)
	
	var sa *iam_pb.ServiceAccount
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		path := serviceAccountId + ".json"
		data, err := filer.ReadInsideFiler(client, filer.IamServiceAccountsDirectory, path)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return credential.ErrServiceAccountNotFound
			}
			return err
		}
		
		sa = &iam_pb.ServiceAccount{}
		if err := protojson.Unmarshal(data, sa); err != nil {
			return fmt.Errorf("failed to parse service account file: %w", err)
		}
		return nil
	})
	return sa, err
}

func (store *FilerEtcStore) UpdateServiceAccount(ctx context.Context, serviceAccountId string, sa *iam_pb.ServiceAccount) error {
	glog.V(1).Infof("[Scalable IAM] UpdateServiceAccount: Updating %s", serviceAccountId)
	
	if err := validateServiceAccount(sa); err != nil {
		return err
	}
	if sa.Id != serviceAccountId {
		return fmt.Errorf("service account ID mismatch: expected %s, got %s", serviceAccountId, sa.Id)
	}

	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Check if exists
		path := serviceAccountId + ".json"
		if _, err := filer.ReadInsideFiler(client, filer.IamServiceAccountsDirectory, path); err != nil {
			if err == filer_pb.ErrNotFound {
				return fmt.Errorf("service account not found")
			}
			return err
		}
		
		return store.saveServiceAccountToFiler(client, sa)
	})
}

func (store *FilerEtcStore) DeleteServiceAccount(ctx context.Context, serviceAccountId string) error {
	glog.V(1).Infof("[Scalable IAM] DeleteServiceAccount: Deleting %s", serviceAccountId)
	
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		err := filer.DeleteInsideFiler(client, filer.IamServiceAccountsDirectory, serviceAccountId+".json")
		if err == filer_pb.ErrNotFound {
			return credential.ErrServiceAccountNotFound
		}
		return err
	})
}

func (store *FilerEtcStore) ListServiceAccounts(ctx context.Context, parentUser string) ([]*iam_pb.ServiceAccount, error) {
	glog.V(1).Infof("[Scalable IAM] ListServiceAccounts: Listing service accounts (parentUser='%s')", parentUser)
	
	var accounts []*iam_pb.ServiceAccount
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		limit := uint32(defaultPaginationLimit)
		var startFileName string
		
		for {
			req := &filer_pb.ListEntriesRequest{
				Directory:          filer.IamServiceAccountsDirectory,
				Limit:              limit,
				StartFromFileName:  startFileName,
				InclusiveStartFrom: false,
			}
			
			stream, err := client.ListEntries(ctx, req)
			if err != nil {
				if err == filer_pb.ErrNotFound {
					return nil
				}
				return err
			}
			
			entryCount := 0
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
				entryCount++
				startFileName = resp.Entry.Name
				
				if resp.Entry.IsDirectory || !strings.HasSuffix(resp.Entry.Name, ".json") {
					continue
				}
				
				// Read file
				data, err := filer.ReadInsideFiler(client, filer.IamServiceAccountsDirectory, resp.Entry.Name)
				if err != nil {
					glog.Warningf("Failed to read service account %s: %v", resp.Entry.Name, err)
					continue
				}
				
				sa := &iam_pb.ServiceAccount{}
				if err := protojson.Unmarshal(data, sa); err != nil {
					glog.Warningf("Failed to parse service account %s: %v", resp.Entry.Name, err)
					continue
				}
				
				if parentUser == "" || sa.ParentUser == parentUser {
					accounts = append(accounts, sa)
				}
			}
			
			if entryCount < int(limit) {
				break
			}
		}
		return nil
	})
	
	return accounts, err
}

func (store *FilerEtcStore) saveServiceAccountToFiler(client filer_pb.SeaweedFilerClient, sa *iam_pb.ServiceAccount) error {
	data, err := protojson.Marshal(sa)
	if err != nil {
		return fmt.Errorf("failed to marshal service account: %w", err)
	}
	return filer.SaveInsideFiler(client, filer.IamServiceAccountsDirectory, sa.Id+".json", data)
}
