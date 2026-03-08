package filer_etc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

const IamGroupsDirectory = "groups"

func (store *FilerEtcStore) loadGroupsFromMultiFile(ctx context.Context, s3cfg *iam_pb.S3ApiConfiguration) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamGroupsDirectory
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
				c, err := filer.ReadInsideFiler(ctx, client, dir, entry.Name)
				if err != nil {
					glog.Warningf("Failed to read group file %s: %v", entry.Name, err)
					continue
				}
				content = c
			}

			if len(content) > 0 {
				g := &iam_pb.Group{}
				if err := json.Unmarshal(content, g); err != nil {
					glog.Warningf("Failed to unmarshal group %s: %v", entry.Name, err)
					continue
				}
				s3cfg.Groups = append(s3cfg.Groups, g)
			}
		}
		return nil
	})
}

func (store *FilerEtcStore) saveGroup(ctx context.Context, group *iam_pb.Group) error {
	if group == nil {
		return fmt.Errorf("group is nil")
	}
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		data, err := json.MarshalIndent(group, "", "  ")
		if err != nil {
			return err
		}
		return filer.SaveInsideFiler(client, filer.IamConfigDirectory+"/"+IamGroupsDirectory, group.Name+".json", data)
	})
}

func (store *FilerEtcStore) deleteGroupFile(ctx context.Context, groupName string) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
			Directory: filer.IamConfigDirectory + "/" + IamGroupsDirectory,
			Name:      groupName + ".json",
		})
		if err != nil {
			if strings.Contains(err.Error(), filer_pb.ErrNotFound.Error()) {
				return credential.ErrGroupNotFound
			}
			return err
		}
		if resp != nil && resp.Error != "" {
			if strings.Contains(resp.Error, filer_pb.ErrNotFound.Error()) {
				return credential.ErrGroupNotFound
			}
			return fmt.Errorf("delete group %s: %s", groupName, resp.Error)
		}
		return nil
	})
}

func (store *FilerEtcStore) CreateGroup(ctx context.Context, group *iam_pb.Group) error {
	if group == nil || group.Name == "" {
		return fmt.Errorf("group name is required")
	}
	existing, err := store.GetGroup(ctx, group.Name)
	if err != nil {
		if !errors.Is(err, credential.ErrGroupNotFound) {
			return err
		}
	} else if existing != nil {
		return credential.ErrGroupAlreadyExists
	}
	return store.saveGroup(ctx, group)
}

func (store *FilerEtcStore) GetGroup(ctx context.Context, groupName string) (*iam_pb.Group, error) {
	var group *iam_pb.Group
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(ctx, client, filer.IamConfigDirectory+"/"+IamGroupsDirectory, groupName+".json")
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return credential.ErrGroupNotFound
			}
			return err
		}
		if len(data) == 0 {
			return credential.ErrGroupNotFound
		}
		group = &iam_pb.Group{}
		return json.Unmarshal(data, group)
	})
	return group, err
}

func (store *FilerEtcStore) DeleteGroup(ctx context.Context, groupName string) error {
	if _, err := store.GetGroup(ctx, groupName); err != nil {
		return err
	}
	return store.deleteGroupFile(ctx, groupName)
}

func (store *FilerEtcStore) ListGroups(ctx context.Context) ([]string, error) {
	var names []string
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		entries, err := listEntries(ctx, client, filer.IamConfigDirectory+"/"+IamGroupsDirectory)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}
		for _, entry := range entries {
			if !entry.IsDirectory && strings.HasSuffix(entry.Name, ".json") {
				names = append(names, strings.TrimSuffix(entry.Name, ".json"))
			}
		}
		return nil
	})
	return names, err
}

func (store *FilerEtcStore) UpdateGroup(ctx context.Context, group *iam_pb.Group) error {
	if group == nil || group.Name == "" {
		return fmt.Errorf("group name is required")
	}
	if _, err := store.GetGroup(ctx, group.Name); err != nil {
		return err
	}
	return store.saveGroup(ctx, group)
}
