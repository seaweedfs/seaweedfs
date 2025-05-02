package user

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// HomeManager handles user home directory operations
type HomeManager struct {
	filerClient FilerClient
}

// FilerClient defines the interface for interacting with the filer
type FilerClient interface {
	WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error
	GetDataCenter() string
	AdjustedUrl(location *filer_pb.Location) string
}

// NewHomeManager creates a new home directory manager
func NewHomeManager(filerClient FilerClient) *HomeManager {
	return &HomeManager{
		filerClient: filerClient,
	}
}

// EnsureHomeDirectory creates the user's home directory if it doesn't exist
func (hm *HomeManager) EnsureHomeDirectory(user *User) error {
	if user.HomeDir == "" {
		return fmt.Errorf("user has no home directory configured")
	}

	glog.V(0).Infof("Ensuring home directory exists for user %s: %s", user.Username, user.HomeDir)

	// Check if home directory exists and create it if needed
	err := hm.createDirectoryIfNotExists(user.HomeDir, user)
	if err != nil {
		return fmt.Errorf("failed to ensure home directory: %v", err)
	}

	// Update user permissions map to include the home directory with full access if not already present
	if user.Permissions == nil {
		user.Permissions = make(map[string][]string)
	}

	// Only add permissions if not already present
	if _, exists := user.Permissions[user.HomeDir]; !exists {
		user.Permissions[user.HomeDir] = []string{"all"}
		glog.V(0).Infof("Added full permissions for user %s to home directory %s",
			user.Username, user.HomeDir)
	}

	return nil
}

// createDirectoryIfNotExists creates a directory path if it doesn't exist
func (hm *HomeManager) createDirectoryIfNotExists(dirPath string, user *User) error {
	// Split the path into components
	components := strings.Split(strings.Trim(dirPath, "/"), "/")
	currentPath := "/"

	for _, component := range components {
		if component == "" {
			continue
		}

		nextPath := filepath.Join(currentPath, component)
		err := hm.createSingleDirectory(nextPath, user)
		if err != nil {
			return err
		}

		currentPath = nextPath
	}

	return nil
}

// createSingleDirectory creates a single directory if it doesn't exist
func (hm *HomeManager) createSingleDirectory(dirPath string, user *User) error {
	var dirExists bool

	err := hm.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		dir, name := util.FullPath(dirPath).DirAndName()

		// Check if directory exists
		resp, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})

		if err != nil || resp.Entry == nil {
			// Directory doesn't exist, create it
			glog.V(0).Infof("Creating directory %s for user %s", dirPath, user.Username)

			err = filer_pb.Mkdir(hm, string(dir), name, func(entry *filer_pb.Entry) {
				// Set appropriate permissions
				entry.Attributes.FileMode = uint32(0700 | os.ModeDir) // rwx------ for user
				entry.Attributes.Uid = user.Uid
				entry.Attributes.Gid = user.Gid

				// Set creation and modification times
				now := time.Now().Unix()
				entry.Attributes.Crtime = now
				entry.Attributes.Mtime = now

				// Add extended attributes
				if entry.Extended == nil {
					entry.Extended = make(map[string][]byte)
				}
				entry.Extended["creator"] = []byte(user.Username)
				entry.Extended["auto_created"] = []byte("true")
			})

			if err != nil {
				return fmt.Errorf("failed to create directory %s: %v", dirPath, err)
			}
		} else if !resp.Entry.IsDirectory {
			return fmt.Errorf("path %s exists but is not a directory", dirPath)
		} else {
			dirExists = true

			// Update ownership if needed
			if resp.Entry.Attributes.Uid != user.Uid || resp.Entry.Attributes.Gid != user.Gid {
				glog.V(0).Infof("Updating ownership of directory %s for user %s", dirPath, user.Username)

				entry := resp.Entry
				entry.Attributes.Uid = user.Uid
				entry.Attributes.Gid = user.Gid

				_, updateErr := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
					Directory: dir,
					Entry:     entry,
				})

				if updateErr != nil {
					glog.Warningf("Failed to update directory ownership: %v", updateErr)
				}
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	if !dirExists {
		// Verify the directory was created
		verifyErr := hm.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			dir, name := util.FullPath(dirPath).DirAndName()
			resp, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
				Directory: dir,
				Name:      name,
			})

			if err != nil || resp.Entry == nil {
				return fmt.Errorf("directory not found after creation")
			}

			if !resp.Entry.IsDirectory {
				return fmt.Errorf("path exists but is not a directory")
			}

			dirExists = true
			return nil
		})

		if verifyErr != nil {
			return fmt.Errorf("failed to verify directory creation: %v", verifyErr)
		}
	}

	return nil
}

// Implement necessary methods to satisfy the filer_pb.FilerClient interface
func (hm *HomeManager) AdjustedUrl(location *filer_pb.Location) string {
	return hm.filerClient.AdjustedUrl(location)
}

func (hm *HomeManager) GetDataCenter() string {
	return hm.filerClient.GetDataCenter()
}

// WithFilerClient delegates to the underlying filer client
func (hm *HomeManager) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return hm.filerClient.WithFilerClient(streamingMode, fn)
}
