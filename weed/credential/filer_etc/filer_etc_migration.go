package filer_etc

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/protobuf/encoding/protojson"
)

const migrationLogLevel = glog.Level(0)

// MigrateUsersToIndividualFiles migrates users from iam_config.json to individual files
// MigrateToIndividualFiles migrates users, accounts and service accounts from identity.json to individual files
// This should be called once during service initialization
func (store *FilerEtcStore) MigrateToIndividualFiles(ctx context.Context) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Step 1: Check if individual user/account/service_account files already exist
		userEntries, err := filer.ListEntry(nil, client, filer.IamUsersDirectory, "", 1, "")
		hasIndividualUsers := err == nil && len(userEntries) > 0

		accountEntries, err := filer.ListEntry(nil, client, filer.IamAccountsDirectory, "", 1, "")
		hasIndividualAccounts := err == nil && len(accountEntries) > 0

		saEntries, err := filer.ListEntry(nil, client, filer.IamServiceAccountsDirectory, "", 1, "")
		hasIndividualSA := err == nil && len(saEntries) > 0

		if hasIndividualUsers && hasIndividualAccounts && hasIndividualSA {
			glog.V(migrationLogLevel).Infof("IAM Migration: Individual user/account/service_account files already exist, skipping migration")
			return nil
		}
		
		var successCount, accSuccessCount, saSuccessCount int

		// Step 2: Read iam_config.json via HTTP (ReadInsideFiler only works for inline content)
		// Get the filer address
		store.mu.RLock()
		if store.filerAddressFunc == nil {
			store.mu.RUnlock()
			glog.V(migrationLogLevel).Infof("IAM Migration: Filer not yet available, skipping migration")
			return nil
		}
		filerAddress := store.filerAddressFunc()
		store.mu.RUnlock()

		if filerAddress == "" {
			glog.V(migrationLogLevel).Infof("IAM Migration: No filer discovered, skipping migration")
			return nil
		}

		// Read file via HTTP
		fileUrl := fmt.Sprintf("http://%s%s/%s", filerAddress, filer.IamConfigDirectory, filer.IamIdentityFile)
		resp, err := http.Get(fileUrl)
		if err != nil {
			glog.V(migrationLogLevel).Infof("IAM Migration: Failed to read %s: %v, skipping migration", fileUrl, err)
			return nil
		}
		defer resp.Body.Close()

		if resp.StatusCode == 404 {
			glog.V(migrationLogLevel).Infof("IAM Migration: %s/%s not found, skipping migration", filer.IamConfigDirectory, filer.IamIdentityFile)
			return nil
		}

		if resp.StatusCode != 200 {
			glog.V(migrationLogLevel).Infof("IAM Migration: Failed to read %s (status %d), skipping migration", fileUrl, resp.StatusCode)
			return nil
		}

		content, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body from %s: %w", fileUrl, err)
		}

		if len(content) == 0 {
			glog.V(migrationLogLevel).Infof("IAM Migration: %s/%s is empty, skipping migration", filer.IamConfigDirectory, filer.IamIdentityFile)
			return nil
		}

		// Step 3: Parse configuration
		tempCfg := &iam_pb.S3ApiConfiguration{}
		if err := filer.ParseS3ConfigurationFromBytes(content, tempCfg); err != nil {
			return fmt.Errorf("failed to parse %s/%s: %w", filer.IamConfigDirectory, filer.IamIdentityFile, err)
		}

		// Step 4: Migrate each user to individual file if users dir is empty
		if !hasIndividualUsers && len(tempCfg.Identities) > 0 {
			glog.V(migrationLogLevel).Infof("IAM Migration: Starting migration of %d users from %s/%s to %s",
				len(tempCfg.Identities), filer.IamConfigDirectory, filer.IamIdentityFile, filer.IamUsersDirectory)
			for _, identity := range tempCfg.Identities {
				userFile := fmt.Sprintf("%s.json", identity.Name)

				// Use protojson.Marshal to match runtime format (saveIdentityToFiler)
				// This ensures migrated files can be read by GetUser, UpdateUser, etc.
				data, err := protojson.Marshal(identity)
				if err != nil {
					glog.Warningf("IAM Migration: Failed to serialize user %s: %v", identity.Name, err)
					continue
				}

				if err := filer.SaveInsideFiler(client, filer.IamUsersDirectory, userFile, data); err != nil {
					glog.Warningf("IAM Migration: Failed to save user %s to %s/%s: %v",
						identity.Name, filer.IamUsersDirectory, userFile, err)
					continue
				}

				glog.V(migrationLogLevel).Infof("IAM Migration: ✓ Migrated user '%s' to %s/%s", identity.Name, filer.IamUsersDirectory, userFile)
				successCount++
			}
			glog.V(migrationLogLevel).Infof("IAM Migration: Successfully migrated %d/%d users to individual files", successCount, len(tempCfg.Identities))
		}

		// Step 5: Migrate each account to individual file if accounts dir is empty
		if !hasIndividualAccounts && len(tempCfg.Accounts) > 0 {
			glog.V(migrationLogLevel).Infof("IAM Migration: Starting migration of %d accounts from %s/%s to %s",
				len(tempCfg.Accounts), filer.IamConfigDirectory, filer.IamIdentityFile, filer.IamAccountsDirectory)
			for _, account := range tempCfg.Accounts {
				accFile := fmt.Sprintf("%s.json", account.Id)

				data, err := protojson.Marshal(account)
				if err != nil {
					glog.Warningf("IAM Migration: Failed to serialize account %s: %v", account.Id, err)
					continue
				}

				if err := filer.SaveInsideFiler(client, filer.IamAccountsDirectory, accFile, data); err != nil {
					glog.Warningf("IAM Migration: Failed to save account %s to %s/%s: %v",
						account.Id, filer.IamAccountsDirectory, accFile, err)
					continue
				}

				glog.V(migrationLogLevel).Infof("IAM Migration: ✓ Migrated account '%s' to %s/%s", account.Id, filer.IamAccountsDirectory, accFile)
				accSuccessCount++
			}
			glog.V(migrationLogLevel).Infof("IAM Migration: Successfully migrated %d/%d accounts to individual files", accSuccessCount, len(tempCfg.Accounts))
		}

		// Step 6: Migrate each service account to individual file if SA dir is empty
		if !hasIndividualSA && len(tempCfg.ServiceAccounts) > 0 {
			glog.V(migrationLogLevel).Infof("IAM Migration: Starting migration of %d service accounts from %s/%s to %s",
				len(tempCfg.ServiceAccounts), filer.IamConfigDirectory, filer.IamIdentityFile, filer.IamServiceAccountsDirectory)
			for _, sa := range tempCfg.ServiceAccounts {
				saFile := fmt.Sprintf("%s.json", sa.Id)

				data, err := protojson.Marshal(sa)
				if err != nil {
					glog.Warningf("IAM Migration: Failed to serialize service account %s: %v", sa.Id, err)
					continue
				}

				if err := filer.SaveInsideFiler(client, filer.IamServiceAccountsDirectory, saFile, data); err != nil {
					glog.Warningf("IAM Migration: Failed to save service account %s to %s/%s: %v",
						sa.Id, filer.IamServiceAccountsDirectory, saFile, err)
					continue
				}

				glog.V(migrationLogLevel).Infof("IAM Migration: ✓ Migrated service account '%s' to %s/%s", sa.Id, filer.IamServiceAccountsDirectory, saFile)
				saSuccessCount++
			}
			glog.V(migrationLogLevel).Infof("IAM Migration: Successfully migrated %d/%d service accounts to individual files", saSuccessCount, len(tempCfg.ServiceAccounts))
		}

		// Step 7: Archive legacy identity.json if migration occurred
		totalMigrated := successCount + accSuccessCount + saSuccessCount
		if totalMigrated > 0 {
			glog.V(migrationLogLevel).Infof("IAM Migration: Migration completed (Total items: %d). Archiving legacy %s", totalMigrated, filer.IamIdentityFile)

			// 1. Save backup to identity.json.migrated
			migratedFile := filer.IamIdentityFile + ".migrated"
			if err := filer.SaveInsideFiler(client, filer.IamConfigDirectory, migratedFile, content); err != nil {
				glog.Errorf("IAM Migration: Failed to create archive %s: %v", migratedFile, err)
				// Don't delete if backup failed
			} else {
				glog.V(migrationLogLevel).Infof("IAM Migration: Archive created at %s/%s", filer.IamConfigDirectory, migratedFile)

				// 2. Delete original identity.json
				if err := filer.DeleteInsideFiler(client, filer.IamConfigDirectory, filer.IamIdentityFile); err != nil {
					glog.Errorf("IAM Migration: Failed to delete legacy %s: %v", filer.IamIdentityFile, err)
				} else {
					glog.V(migrationLogLevel).Infof("IAM Migration: Successfully archived and removed legacy %s", filer.IamIdentityFile)
				}
			}
		} else if !hasIndividualUsers && !hasIndividualAccounts && !hasIndividualSA {
		    // Case: Empty config file or nothing to migrate, but also no individual files existed.
            // Maybe we should verify if we read any content. We did check len(content) == 0 earlier.
            // If we are here, it means we parsed it but found nothing to migrate (e.g. empty lists).
            // In that case, should we archive? Probably not necessary if it's empty, but if it had content that was just empty lists...
            // The logic `totalMigrated > 0` safer.
		}

		return nil
	})
}
