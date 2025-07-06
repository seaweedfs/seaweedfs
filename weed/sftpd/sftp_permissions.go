package sftpd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
)

// Permission constants for clarity and consistency
const (
	PermRead      = "read"
	PermWrite     = "write"
	PermExecute   = "execute"
	PermList      = "list"
	PermDelete    = "delete"
	PermMkdir     = "mkdir"
	PermTraverse  = "traverse"
	PermAll       = "*"
	PermAdmin     = "admin"
	PermReadWrite = "readwrite"
)

// Entry represents a filesystem entry with attributes
type Entry struct {
	IsDirectory bool
	Attributes  *EntryAttributes
	IsSymlink   bool   // Added to track symlinks
	Target      string // For symlinks, stores the target path
}

// EntryAttributes contains file attributes
type EntryAttributes struct {
	Uid           uint32
	Gid           uint32
	FileMode      uint32
	SymlinkTarget string
}

// PermissionError represents a permission-related erro

// CheckFilePermission verifies if a user has the required permission on a path
// It first checks if the path is in the user's home directory with explicit permissions.
// If not, it falls back to Unix permission checking followed by explicit permission checking.
// Parameters:
//   - user: The user requesting access
//   - path: The filesystem path to check
//   - perm: The permission being requested (read, write, execute, etc.)
//
// Returns:
//   - nil if permission is granted, error otherwise
func (fs *SftpServer) CheckFilePermission(path string, perm string) error {

	if fs.user == nil {
		glog.V(0).Infof("permission denied. No user associated with the SftpServer.")
		return os.ErrPermission
	}

	// Special case for "create" or "write" permissions on non-existent paths
	// Check parent directory permissions instead
	entry, err := fs.getEntry(path)
	if err != nil {
		// If the path doesn't exist and we're checking for create/write/mkdir permission,
		// check permissions on the parent directory instead
		if err == os.ErrNotExist {
			parentPath := filepath.Dir(path)
			// Check if user can write to the parent directory
			return fs.CheckFilePermission(parentPath, perm)
		}
		return fmt.Errorf("failed to get entry for path %s: %w", path, err)
	}

	// Rest of the function remains the same...
	// Handle symlinks by resolving them
	if entry.Attributes.GetSymlinkTarget() != "" {
		// Get the actual entry for the resolved path
		entry, err = fs.getEntry(entry.Attributes.GetSymlinkTarget())
		if err != nil {
			return fmt.Errorf("failed to get entry for resolved path %s: %w", entry.Attributes.SymlinkTarget, err)
		}
	}

	// Special case: root user always has permission
	if fs.user.Username == "root" || fs.user.Uid == 0 {
		return nil
	}

	// Check if path is within user's home directory and has explicit permissions
	if isPathInHomeDirectory(fs.user, path) {
		// Check if user has explicit permissions for this path
		if HasExplicitPermission(fs.user, path, perm, entry.IsDirectory) {
			return nil
		}
	} else {
		// For paths outside home directory or without explicit home permissions,
		// check UNIX-style perms first
		isOwner := fs.user.Uid == entry.Attributes.Uid
		isGroup := fs.user.Gid == entry.Attributes.Gid
		mode := os.FileMode(entry.Attributes.FileMode)

		if HasUnixPermission(isOwner, isGroup, mode, entry.IsDirectory, perm) {
			return nil
		}

		// Then check explicit ACLs
		if HasExplicitPermission(fs.user, path, perm, entry.IsDirectory) {
			return nil
		}
	}
	glog.V(0).Infof("permission denied for user %s on path %s for permission %s", fs.user.Username, path, perm)
	return os.ErrPermission
}

// isPathInHomeDirectory checks if a path is in the user's home directory
func isPathInHomeDirectory(user *user.User, path string) bool {
	return strings.HasPrefix(path, user.HomeDir)
}

// HasUnixPermission checks if the user has the required Unix permission
// Uses bit masks for clarity and maintainability
func HasUnixPermission(isOwner, isGroup bool, fileMode os.FileMode, isDirectory bool, requiredPerm string) bool {
	const (
		ownerRead  = 0400
		ownerWrite = 0200
		ownerExec  = 0100
		groupRead  = 0040
		groupWrite = 0020
		groupExec  = 0010
		otherRead  = 0004
		otherWrite = 0002
		otherExec  = 0001
	)

	// Check read permission
	hasRead := (isOwner && (fileMode&ownerRead != 0)) ||
		(isGroup && (fileMode&groupRead != 0)) ||
		(fileMode&otherRead != 0)

	// Check write permission
	hasWrite := (isOwner && (fileMode&ownerWrite != 0)) ||
		(isGroup && (fileMode&groupWrite != 0)) ||
		(fileMode&otherWrite != 0)

	// Check execute permission
	hasExec := (isOwner && (fileMode&ownerExec != 0)) ||
		(isGroup && (fileMode&groupExec != 0)) ||
		(fileMode&otherExec != 0)

	switch requiredPerm {
	case PermRead:
		return hasRead
	case PermWrite:
		return hasWrite
	case PermExecute:
		return hasExec
	case PermList:
		if isDirectory {
			return hasRead && hasExec
		}
		return hasRead
	case PermDelete:
		return hasWrite
	case PermMkdir:
		return isDirectory && hasWrite
	case PermTraverse:
		return isDirectory && hasExec
	case PermReadWrite:
		return hasRead && hasWrite
	case PermAll, PermAdmin:
		return hasRead && hasWrite && hasExec
	}
	return false
}

// HasExplicitPermission checks if the user has explicit permission from user config
func HasExplicitPermission(user *user.User, filepath, requiredPerm string, isDirectory bool) bool {
	// Find the most specific permission that applies to this path
	var bestMatch string
	var perms []string

	for p, userPerms := range user.Permissions {
		// Check if the path is either the permission path exactly or is under that path
		if strings.HasPrefix(filepath, p) && len(p) > len(bestMatch) {
			bestMatch = p
			perms = userPerms
		}
	}

	// No matching permissions found
	if bestMatch == "" {
		return false
	}

	// Check if user has admin role
	if containsString(perms, PermAdmin) {
		return true
	}

	// If user has list permission and is requesting traverse/execute permission, grant it
	if isDirectory && requiredPerm == PermExecute && containsString(perms, PermList) {
		return true
	}

	// Check if the required permission is in the list
	for _, perm := range perms {
		if perm == requiredPerm || perm == PermAll {
			return true
		}

		// Handle combined permissions
		if perm == PermReadWrite && (requiredPerm == PermRead || requiredPerm == PermWrite) {
			return true
		}

		// Directory-specific permissions
		if isDirectory && perm == PermList && requiredPerm == PermRead {
			return true
		}
		if isDirectory && perm == PermTraverse && requiredPerm == PermExecute {
			return true
		}
	}

	return false
}

// Helper function to check if a string is in a slice
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}
