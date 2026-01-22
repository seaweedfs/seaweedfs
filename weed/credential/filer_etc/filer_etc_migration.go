package filer_etc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// MigrateUsersToIndividualFiles migrates users from iam_config.json to individual files
// This should be called once during service initialization
func (store *FilerEtcStore) MigrateUsersToIndividualFiles(ctx context.Context) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Step 1: Check if individual user files already exist
		entries, err := filer.ListEntry(nil, client, filer.IamUsersDirectory, "", 1000, "")
		hasIndividualUsers := err == nil && len(entries) > 0
		
		if hasIndividualUsers {
			glog.V(0).Infof("IAM Migration: Individual user files already exist in %s, skipping migration", filer.IamUsersDirectory)
			return nil
		}
		
		// Step 2: Read iam_config.json via HTTP (ReadInsideFiler only works for inline content)
		// Get the filer address
		store.mu.RLock()
		if store.filerAddressFunc == nil {
			store.mu.RUnlock()
			glog.V(0).Infof("IAM Migration: Filer not yet available, skipping migration")
			return nil
		}
		filerAddress := store.filerAddressFunc()
		store.mu.RUnlock()
		
		if filerAddress == "" {
			glog.V(0).Infof("IAM Migration: No filer discovered, skipping migration")
			return nil
		}
		
		// Read file via HTTP
		fileUrl := fmt.Sprintf("http://%s%s/%s", filerAddress, filer.IamConfigDirectory, filer.IamIdentityFile)
		resp, err := http.Get(fileUrl)
		if err != nil {
			glog.V(0).Infof("IAM Migration: Failed to read %s: %v, skipping migration", fileUrl, err)
			return nil
		}
		defer resp.Body.Close()
		
		if resp.StatusCode == 404 {
			glog.V(0).Infof("IAM Migration: %s/%s not found, skipping migration", filer.IamConfigDirectory, filer.IamIdentityFile)
			return nil
		}
		
		if resp.StatusCode != 200 {
			glog.V(0).Infof("IAM Migration: Failed to read %s (status %d), skipping migration", fileUrl, resp.StatusCode)
			return nil
		}
		
		content, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body from %s: %w", fileUrl, err)
		}
		
		if len(content) == 0 {
			glog.V(0).Infof("IAM Migration: %s/%s is empty, skipping migration", filer.IamConfigDirectory, filer.IamIdentityFile)
			return nil
		}
		
		// Step 3: Parse configuration
		tempCfg := &iam_pb.S3ApiConfiguration{}
		if err := filer.ParseS3ConfigurationFromBytes(content, tempCfg); err != nil {
			return fmt.Errorf("failed to parse %s/%s: %w", filer.IamConfigDirectory, filer.IamIdentityFile, err)
		}
		
		if len(tempCfg.Identities) == 0 {
			glog.V(0).Infof("IAM Migration: No users found in %s/%s, skipping migration", filer.IamConfigDirectory, filer.IamIdentityFile)
			return nil
		}
		
		// Step 4: Migrate each user to individual file
		glog.V(0).Infof("IAM Migration: Starting migration of %d users from %s/%s to %s",
			len(tempCfg.Identities), filer.IamConfigDirectory, filer.IamIdentityFile, filer.IamUsersDirectory)
		
		successCount := 0
		for _, identity := range tempCfg.Identities {
			userFile := fmt.Sprintf("%s.json", identity.Name)
			var userBuf bytes.Buffer
			if err := filer.ProtoToText(&userBuf, identity); err != nil {
				glog.Warningf("IAM Migration: Failed to serialize user %s: %v", identity.Name, err)
				continue
			}
			
			if err := filer.SaveInsideFiler(client, filer.IamUsersDirectory, userFile, userBuf.Bytes()); err != nil {
				glog.Warningf("IAM Migration: Failed to save user %s to %s/%s: %v", 
					identity.Name, filer.IamUsersDirectory, userFile, err)
				continue
			}
			
			glog.V(0).Infof("IAM Migration: ✓ Migrated user '%s' to %s/%s", identity.Name, filer.IamUsersDirectory, userFile)
			successCount++
		}
		
		glog.V(0).Infof("IAM Migration: Successfully migrated %d/%d users to individual files", successCount, len(tempCfg.Identities))
		
		return nil
	})
}
