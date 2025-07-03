package credential

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// MigrateCredentials migrates credentials from one store to another
func MigrateCredentials(fromStoreName, toStoreName CredentialStoreTypeName, configuration util.Configuration, fromPrefix, toPrefix string) error {
	ctx := context.Background()

	// Create source credential manager
	fromCM, err := NewCredentialManager(fromStoreName, configuration, fromPrefix)
	if err != nil {
		return fmt.Errorf("failed to create source credential manager (%s): %v", fromStoreName, err)
	}
	defer fromCM.Shutdown()

	// Create destination credential manager
	toCM, err := NewCredentialManager(toStoreName, configuration, toPrefix)
	if err != nil {
		return fmt.Errorf("failed to create destination credential manager (%s): %v", toStoreName, err)
	}
	defer toCM.Shutdown()

	// Load configuration from source
	glog.Infof("Loading configuration from %s store...", fromStoreName)
	config, err := fromCM.LoadConfiguration(ctx)
	if err != nil {
		return fmt.Errorf("failed to load configuration from source store: %v", err)
	}

	if config == nil || len(config.Identities) == 0 {
		glog.Info("No identities found in source store")
		return nil
	}

	glog.Infof("Found %d identities in source store", len(config.Identities))

	// Migrate each identity
	var migrated, failed int
	for _, identity := range config.Identities {
		glog.V(1).Infof("Migrating user: %s", identity.Name)

		// Check if user already exists in destination
		existingUser, err := toCM.GetUser(ctx, identity.Name)
		if err != nil && err != ErrUserNotFound {
			glog.Errorf("Failed to check if user %s exists in destination: %v", identity.Name, err)
			failed++
			continue
		}

		if existingUser != nil {
			glog.Warningf("User %s already exists in destination store, skipping", identity.Name)
			continue
		}

		// Create user in destination
		err = toCM.CreateUser(ctx, identity)
		if err != nil {
			glog.Errorf("Failed to create user %s in destination store: %v", identity.Name, err)
			failed++
			continue
		}

		migrated++
		glog.V(1).Infof("Successfully migrated user: %s", identity.Name)
	}

	glog.Infof("Migration completed: %d migrated, %d failed", migrated, failed)

	if failed > 0 {
		return fmt.Errorf("migration completed with %d failures", failed)
	}

	return nil
}

// ExportCredentials exports credentials from a store to a configuration
func ExportCredentials(storeName CredentialStoreTypeName, configuration util.Configuration, prefix string) (*iam_pb.S3ApiConfiguration, error) {
	ctx := context.Background()

	// Create credential manager
	cm, err := NewCredentialManager(storeName, configuration, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to create credential manager (%s): %v", storeName, err)
	}
	defer cm.Shutdown()

	// Load configuration
	config, err := cm.LoadConfiguration(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %v", err)
	}

	return config, nil
}

// ImportCredentials imports credentials from a configuration to a store
func ImportCredentials(storeName CredentialStoreTypeName, configuration util.Configuration, prefix string, config *iam_pb.S3ApiConfiguration) error {
	ctx := context.Background()

	// Create credential manager
	cm, err := NewCredentialManager(storeName, configuration, prefix)
	if err != nil {
		return fmt.Errorf("failed to create credential manager (%s): %v", storeName, err)
	}
	defer cm.Shutdown()

	// Import each identity
	var imported, failed int
	for _, identity := range config.Identities {
		glog.V(1).Infof("Importing user: %s", identity.Name)

		// Check if user already exists
		existingUser, err := cm.GetUser(ctx, identity.Name)
		if err != nil && err != ErrUserNotFound {
			glog.Errorf("Failed to check if user %s exists: %v", identity.Name, err)
			failed++
			continue
		}

		if existingUser != nil {
			glog.Warningf("User %s already exists, skipping", identity.Name)
			continue
		}

		// Create user
		err = cm.CreateUser(ctx, identity)
		if err != nil {
			glog.Errorf("Failed to create user %s: %v", identity.Name, err)
			failed++
			continue
		}

		imported++
		glog.V(1).Infof("Successfully imported user: %s", identity.Name)
	}

	glog.Infof("Import completed: %d imported, %d failed", imported, failed)

	if failed > 0 {
		return fmt.Errorf("import completed with %d failures", failed)
	}

	return nil
}

// ValidateCredentials validates that all credentials in a store are accessible
func ValidateCredentials(storeName CredentialStoreTypeName, configuration util.Configuration, prefix string) error {
	ctx := context.Background()

	// Create credential manager
	cm, err := NewCredentialManager(storeName, configuration, prefix)
	if err != nil {
		return fmt.Errorf("failed to create credential manager (%s): %v", storeName, err)
	}
	defer cm.Shutdown()

	// Load configuration
	config, err := cm.LoadConfiguration(ctx)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	if config == nil || len(config.Identities) == 0 {
		glog.Info("No identities found in store")
		return nil
	}

	glog.Infof("Validating %d identities...", len(config.Identities))

	// Validate each identity
	var validated, failed int
	for _, identity := range config.Identities {
		// Check if user can be retrieved
		user, err := cm.GetUser(ctx, identity.Name)
		if err != nil {
			glog.Errorf("Failed to retrieve user %s: %v", identity.Name, err)
			failed++
			continue
		}

		if user == nil {
			glog.Errorf("User %s not found", identity.Name)
			failed++
			continue
		}

		// Validate access keys
		for _, credential := range identity.Credentials {
			accessKeyUser, err := cm.GetUserByAccessKey(ctx, credential.AccessKey)
			if err != nil {
				glog.Errorf("Failed to retrieve user by access key %s: %v", credential.AccessKey, err)
				failed++
				continue
			}

			if accessKeyUser == nil || accessKeyUser.Name != identity.Name {
				glog.Errorf("Access key %s does not map to correct user %s", credential.AccessKey, identity.Name)
				failed++
				continue
			}
		}

		validated++
		glog.V(1).Infof("Successfully validated user: %s", identity.Name)
	}

	glog.Infof("Validation completed: %d validated, %d failed", validated, failed)

	if failed > 0 {
		return fmt.Errorf("validation completed with %d failures", failed)
	}

	return nil
}
