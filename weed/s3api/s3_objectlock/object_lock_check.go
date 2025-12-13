package s3_objectlock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// ====================================================================
// SHARED OBJECT LOCK CHECKING FUNCTIONS
// ====================================================================
// These functions are used by S3 API, Admin UI, and shell commands for
// checking Object Lock status before bucket deletion.

// EntryHasActiveLock checks if an entry has an active retention or legal hold
// This is a standalone function that can be used by any component
func EntryHasActiveLock(entry *filer_pb.Entry, currentTime time.Time) bool {
	if entry == nil || entry.Extended == nil {
		return false
	}

	// Check for active legal hold
	if legalHoldBytes, exists := entry.Extended[s3_constants.ExtLegalHoldKey]; exists {
		if string(legalHoldBytes) == s3_constants.LegalHoldOn {
			return true
		}
	}

	// Check for active retention
	if modeBytes, exists := entry.Extended[s3_constants.ExtObjectLockModeKey]; exists {
		mode := string(modeBytes)
		if mode == s3_constants.RetentionModeCompliance || mode == s3_constants.RetentionModeGovernance {
			// Check if retention is still active
			if dateBytes, dateExists := entry.Extended[s3_constants.ExtRetentionUntilDateKey]; dateExists {
				timestamp, err := strconv.ParseInt(string(dateBytes), 10, 64)
				if err != nil {
					// Fail-safe: if we can't parse the retention date, assume the object is locked
					// to prevent accidental data loss
					glog.Warningf("Failed to parse retention date '%s' for entry, assuming locked: %v", string(dateBytes), err)
					return true
				}
				retainUntil := time.Unix(timestamp, 0)
				if retainUntil.After(currentTime) {
					return true
				}
			}
		}
	}

	return false
}

// HasObjectsWithActiveLocks checks if any objects in the bucket have active retention or legal hold
// This function uses the filer gRPC client to scan the bucket directory
func HasObjectsWithActiveLocks(client filer_pb.SeaweedFilerClient, bucketPath string) (bool, error) {
	hasLocks := false
	currentTime := time.Now()

	err := recursivelyCheckLocksWithClient(client, bucketPath, &hasLocks, currentTime)
	if err != nil {
		return false, fmt.Errorf("error checking for locked objects: %w", err)
	}

	return hasLocks, nil
}

// recursivelyCheckLocksWithClient recursively checks all objects and versions for active locks
func recursivelyCheckLocksWithClient(client filer_pb.SeaweedFilerClient, dir string, hasLocks *bool, currentTime time.Time) error {
	if *hasLocks {
		return nil // Early exit if already found a locked object
	}

	// List entries in the directory with pagination
	lastFileName := ""
	for {
		resp, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory:          dir,
			StartFromFileName:  lastFileName,
			InclusiveStartFrom: false,
			Limit:              10000,
		})
		if err != nil {
			return fmt.Errorf("failed to list directory %s: %w", dir, err)
		}

		entriesReceived := false
		for {
			entryResp, recvErr := resp.Recv()
			if recvErr != nil {
				if errors.Is(recvErr, io.EOF) {
					break // Normal end of stream
				}
				return fmt.Errorf("failed to receive entry from %s: %w", dir, recvErr)
			}
			entriesReceived = true
			entry := entryResp.Entry
			lastFileName = entry.Name

			if *hasLocks {
				return nil // Early exit
			}

			// Skip special directories
			if entry.Name == s3_constants.MultipartUploadsFolder {
				continue
			}

			if entry.IsDirectory {
				subDir := path.Join(dir, entry.Name)
				if strings.HasSuffix(entry.Name, s3_constants.VersionsFolder) {
					// Check all version files
					if err := checkVersionsForLocksWithClient(client, subDir, hasLocks, currentTime); err != nil {
						return err
					}
				} else {
					// Recursively check subdirectories
					if err := recursivelyCheckLocksWithClient(client, subDir, hasLocks, currentTime); err != nil {
						return err
					}
				}
			} else {
				// Check if this object has an active lock
				if EntryHasActiveLock(entry, currentTime) {
					*hasLocks = true
					glog.V(2).Infof("Found object with active lock: %s/%s", dir, entry.Name)
					return nil
				}
			}
		}

		if !entriesReceived {
			break
		}
	}
	return nil
}

// checkVersionsForLocksWithClient checks all versions in a .versions directory for active locks
func checkVersionsForLocksWithClient(client filer_pb.SeaweedFilerClient, versionsDir string, hasLocks *bool, currentTime time.Time) error {
	lastFileName := ""
	for {
		resp, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory:          versionsDir,
			StartFromFileName:  lastFileName,
			InclusiveStartFrom: false,
			Limit:              10000,
		})
		if err != nil {
			return fmt.Errorf("failed to list versions directory %s: %w", versionsDir, err)
		}

		entriesReceived := false
		for {
			entryResp, recvErr := resp.Recv()
			if recvErr != nil {
				if errors.Is(recvErr, io.EOF) {
					break // Normal end of stream
				}
				return fmt.Errorf("failed to receive entry from %s: %w", versionsDir, recvErr)
			}
			entriesReceived = true
			entry := entryResp.Entry
			lastFileName = entry.Name

			if *hasLocks {
				return nil
			}

			if EntryHasActiveLock(entry, currentTime) {
				*hasLocks = true
				glog.V(2).Infof("Found version with active lock: %s/%s", versionsDir, entry.Name)
				return nil
			}
		}

		if !entriesReceived {
			break
		}
	}
	return nil
}

// IsObjectLockEnabled checks if Object Lock is enabled on a bucket entry
func IsObjectLockEnabled(entry *filer_pb.Entry) bool {
	if entry == nil || entry.Extended == nil {
		return false
	}

	enabledBytes, exists := entry.Extended[s3_constants.ExtObjectLockEnabledKey]
	if !exists {
		return false
	}

	enabled := string(enabledBytes)
	return enabled == s3_constants.ObjectLockEnabled || enabled == "true"
}

// CheckBucketForLockedObjects is a unified function that checks if a bucket has Object Lock enabled
// and if so, scans for objects with active locks. This combines the bucket lookup and lock check
// into a single operation used by S3 API, Admin UI, and shell commands.
// Returns an error if the bucket has locked objects or if the check fails.
func CheckBucketForLockedObjects(client filer_pb.SeaweedFilerClient, bucketsPath, bucketName string) error {
	// Look up the bucket entry
	lookupResp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
		Directory: bucketsPath,
		Name:      bucketName,
	})
	if err != nil {
		return fmt.Errorf("bucket not found: %w", err)
	}

	// Check if Object Lock is enabled
	if !IsObjectLockEnabled(lookupResp.Entry) {
		return nil // No Object Lock, nothing to check
	}

	// Check for objects with active locks
	bucketPath := bucketsPath + "/" + bucketName
	hasLockedObjects, checkErr := HasObjectsWithActiveLocks(client, bucketPath)
	if checkErr != nil {
		return fmt.Errorf("failed to check for locked objects: %w", checkErr)
	}
	if hasLockedObjects {
		return fmt.Errorf("bucket has objects with active Object Lock retention or legal hold")
	}

	return nil
}

