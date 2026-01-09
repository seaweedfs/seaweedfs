package s3api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"

	// Import KMS providers to register them
	_ "github.com/seaweedfs/seaweedfs/weed/kms/aws"
	// _ "github.com/seaweedfs/seaweedfs/weed/kms/azure"  // TODO: Fix Azure SDK compatibility issues
	_ "github.com/seaweedfs/seaweedfs/weed/kms/gcp"
	_ "github.com/seaweedfs/seaweedfs/weed/kms/local"
	_ "github.com/seaweedfs/seaweedfs/weed/kms/openbao"
	"google.golang.org/grpc"
)

type Action string

type Iam interface {
	Check(f http.HandlerFunc, actions ...Action) http.HandlerFunc
}

type IdentityAccessManagement struct {
	m sync.RWMutex

	identities        []*Identity
	accessKeyIdent    map[string]*Identity
	nameToIdentity    map[string]*Identity // O(1) lookup by identity name
	accounts          map[string]*Account
	emailAccount      map[string]*Account
	hashes            map[string]*sync.Pool
	hashCounters      map[string]*int32
	identityAnonymous *Identity
	hashMu            sync.RWMutex
	domain            string
	isAuthEnabled     bool
	credentialManager *credential.CredentialManager
	filerClient       filer_pb.SeaweedFilerClient
	grpcDialOption    grpc.DialOption

	// IAM Integration for advanced features
	iamIntegration IAMIntegration

	// Bucket policy engine for evaluating bucket policies
	policyEngine *BucketPolicyEngine

	// useStaticConfig indicates if the configuration was loaded from a static file
	useStaticConfig bool

	// staticIdentityNames tracks identity names loaded from the static config file
	// These identities are immutable and cannot be updated by dynamic configuration
	staticIdentityNames map[string]bool
}

type Identity struct {
	Name         string
	Account      *Account
	Credentials  []*Credential
	Actions      []Action
	PolicyNames  []string // Attached IAM policy names
	PrincipalArn string   // ARN for IAM authorization (e.g., "arn:aws:iam::account-id:user/username")
	Disabled     bool     // User status: false = enabled (default), true = disabled
}

// Account represents a system user, a system user can
// configure multiple IAM-Users, IAM-Users can configure
// permissions respectively, and each IAM-User can
// configure multiple security credentials
type Account struct {
	//Name is also used to display the "DisplayName" as the owner of the bucket or object
	DisplayName  string
	EmailAddress string

	//Id is used to identify an Account when granting cross-account access(ACLs) to buckets and objects
	Id string
}

// Predefined Accounts
var (
	// AccountAdmin is used as the default account for IAM-Credentials access without Account configured
	AccountAdmin = Account{
		DisplayName:  "admin",
		EmailAddress: "admin@example.com",
		Id:           s3_constants.AccountAdminId,
	}

	// AccountAnonymous is used to represent the account for anonymous access
	AccountAnonymous = Account{
		DisplayName:  "anonymous",
		EmailAddress: "anonymous@example.com",
		Id:           s3_constants.AccountAnonymousId,
	}
)

type Credential struct {
	AccessKey  string
	SecretKey  string
	Status     string // Access key status: "Active" or "Inactive" (empty treated as "Active")
	Expiration int64  // Unix timestamp when credential expires (0 = no expiration)
}

// isCredentialExpired checks if a credential has expired
func (c *Credential) isCredentialExpired() bool {
	return c.Expiration > 0 && c.Expiration < time.Now().Unix()
}

func NewIdentityAccessManagement(option *S3ApiServerOption) *IdentityAccessManagement {
	return NewIdentityAccessManagementWithStore(option, "")
}

func NewIdentityAccessManagementWithStore(option *S3ApiServerOption, explicitStore string) *IdentityAccessManagement {
	iam := &IdentityAccessManagement{
		domain:       option.DomainName,
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}

	// Always initialize credential manager with fallback to defaults
	credentialManager, err := credential.NewCredentialManagerWithDefaults(credential.CredentialStoreTypeName(explicitStore))
	if err != nil {
		glog.Fatalf("failed to initialize credential manager: %v", err)
	}

	// For stores that need filer client details, set them temporarily
	// This will be updated to use FilerClient's GetCurrentFiler after FilerClient is created
	if store := credentialManager.GetStore(); store != nil {
		if filerFuncSetter, ok := store.(interface {
			SetFilerAddressFunc(func() pb.ServerAddress, grpc.DialOption)
		}); ok {
			// Temporary setup: use first filer until FilerClient is available
			// See s3api_server.go where this is updated to FilerClient.GetCurrentFiler
			if len(option.Filers) > 0 {
				getFiler := func() pb.ServerAddress {
					if len(option.Filers) > 0 {
						return option.Filers[0]
					}
					return ""
				}
				filerFuncSetter.SetFilerAddressFunc(getFiler, option.GrpcDialOption)
				glog.V(1).Infof("Credential store configured with temporary filer function (will be updated after FilerClient creation)")
			}
		}
	}

	iam.credentialManager = credentialManager

	// Track whether any configuration was successfully loaded
	configLoaded := false

	// First, try to load configurations from file or filer
	// First, try to load configurations from file or filer
	startConfigFile := option.Config
	if startConfigFile == "" {
		startConfigFile = option.IamConfig
	}

	if startConfigFile != "" {
		glog.V(3).Infof("loading static config file %s", startConfigFile)
		if err := iam.loadS3ApiConfigurationFromFile(startConfigFile); err != nil {
			glog.Fatalf("fail to load config file %s: %v", startConfigFile, err)
		}

		// Track identity names from static config to protect them from dynamic updates
		// Must be done under lock to avoid race conditions
		iam.m.Lock()
		iam.useStaticConfig = true
		iam.staticIdentityNames = make(map[string]bool)
		for _, identity := range iam.identities {
			iam.staticIdentityNames[identity.Name] = true
		}
		configLoaded = len(iam.identities) > 0
		iam.m.Unlock()
	}

	// Always try to load/merge config from credential manager (filer)
	// This ensures we get both static users (from file) and dynamic users (from filer)
	glog.V(3).Infof("loading dynamic config from credential manager")
	if err := iam.loadS3ApiConfigurationFromFiler(option); err != nil {
		glog.Warningf("fail to load config: %v", err)
	}

	// Only consider config loaded if we actually have identities
	// Don't block environment variable fallback just because filer call succeeded
	iam.m.RLock()
	configLoaded = len(iam.identities) > 0
	iam.m.RUnlock()

	// Only use environment variables as fallback if no configuration was loaded
	if !configLoaded {
		accessKeyId := os.Getenv("AWS_ACCESS_KEY_ID")
		secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

		if accessKeyId != "" && secretAccessKey != "" {
			glog.V(1).Infof("No S3 configuration found, using AWS environment variables as fallback")

			// Create environment variable identity name
			identityNameSuffix := accessKeyId
			if len(accessKeyId) > 8 {
				identityNameSuffix = accessKeyId[:8]
			}

			// Create admin identity with environment variable credentials
			envIdentity := &Identity{
				Name:    "admin-" + identityNameSuffix,
				Account: &AccountAdmin,
				Credentials: []*Credential{
					{
						AccessKey: accessKeyId,
						SecretKey: secretAccessKey,
					},
				},
				Actions: []Action{
					s3_constants.ACTION_ADMIN,
				},
			}

			// Set as the only configuration
			iam.m.Lock()
			if len(iam.identities) == 0 {
				iam.identities = []*Identity{envIdentity}
				iam.accessKeyIdent = map[string]*Identity{accessKeyId: envIdentity}
				iam.nameToIdentity = map[string]*Identity{envIdentity.Name: envIdentity}
				iam.isAuthEnabled = true
			}
			iam.m.Unlock()

			glog.V(1).Infof("Added admin identity from AWS environment variables: %s", envIdentity.Name)
		}
	}

	return iam
}

func (iam *IdentityAccessManagement) loadS3ApiConfigurationFromFiler(option *S3ApiServerOption) error {
	return iam.LoadS3ApiConfigurationFromCredentialManager()
}

func (iam *IdentityAccessManagement) loadS3ApiConfigurationFromFile(fileName string) error {
	content, readErr := os.ReadFile(fileName)
	if readErr != nil {
		glog.Warningf("fail to read %s : %v", fileName, readErr)
		return fmt.Errorf("fail to read %s : %v", fileName, readErr)
	}

	// Initialize KMS if configuration contains KMS settings
	if err := iam.initializeKMSFromConfig(content); err != nil {
		glog.Warningf("KMS initialization failed: %v", err)
	}

	return iam.LoadS3ApiConfigurationFromBytes(content)
}

func (iam *IdentityAccessManagement) LoadS3ApiConfigurationFromBytes(content []byte) error {
	s3ApiConfiguration := &iam_pb.S3ApiConfiguration{}
	if err := filer.ParseS3ConfigurationFromBytes(content, s3ApiConfiguration); err != nil {
		glog.Warningf("unmarshal error: %v", err)
		return fmt.Errorf("unmarshal error: %w", err)
	}

	if err := filer.CheckDuplicateAccessKey(s3ApiConfiguration); err != nil {
		return err
	}

	if err := iam.loadS3ApiConfiguration(s3ApiConfiguration); err != nil {
		return err
	}
	return nil
}

func (iam *IdentityAccessManagement) loadS3ApiConfiguration(config *iam_pb.S3ApiConfiguration) error {
	// Check if we need to merge with existing static configuration
	iam.m.RLock()
	hasStaticConfig := iam.useStaticConfig && len(iam.staticIdentityNames) > 0
	iam.m.RUnlock()

	if hasStaticConfig {
		// Merge mode: preserve static identities, add/update dynamic ones
		return iam.mergeS3ApiConfiguration(config)
	}

	// Normal mode: completely replace configuration
	return iam.replaceS3ApiConfiguration(config)
}

// replaceS3ApiConfiguration completely replaces the current configuration (used when no static config)
func (iam *IdentityAccessManagement) replaceS3ApiConfiguration(config *iam_pb.S3ApiConfiguration) error {
	var identities []*Identity
	var identityAnonymous *Identity
	accessKeyIdent := make(map[string]*Identity)
	nameToIdentity := make(map[string]*Identity)
	accounts := make(map[string]*Account)
	emailAccount := make(map[string]*Account)
	foundAccountAdmin := false
	foundAccountAnonymous := false

	for _, account := range config.Accounts {
		glog.V(3).Infof("loading account  name=%s, id=%s", account.DisplayName, account.Id)
		accounts[account.Id] = &Account{
			Id:           account.Id,
			DisplayName:  account.DisplayName,
			EmailAddress: account.EmailAddress,
		}
		switch account.Id {
		case AccountAdmin.Id:
			foundAccountAdmin = true
		case AccountAnonymous.Id:
			foundAccountAnonymous = true
		}
		if account.EmailAddress != "" {
			emailAccount[account.EmailAddress] = accounts[account.Id]
		}
	}
	if !foundAccountAdmin {
		accounts[AccountAdmin.Id] = &Account{
			DisplayName:  AccountAdmin.DisplayName,
			EmailAddress: AccountAdmin.EmailAddress,
			Id:           AccountAdmin.Id,
		}
		emailAccount[AccountAdmin.EmailAddress] = accounts[AccountAdmin.Id]
	}
	if !foundAccountAnonymous {
		accounts[AccountAnonymous.Id] = &Account{
			DisplayName:  AccountAnonymous.DisplayName,
			EmailAddress: AccountAnonymous.EmailAddress,
			Id:           AccountAnonymous.Id,
		}
		emailAccount[AccountAnonymous.EmailAddress] = accounts[AccountAnonymous.Id]
	}
	for _, ident := range config.Identities {
		glog.V(3).Infof("loading identity %s (disabled=%v)", ident.Name, ident.Disabled)
		t := &Identity{
			Name:         ident.Name,
			Credentials:  nil,
			Actions:      nil,
			PrincipalArn: generatePrincipalArn(ident.Name),
			Disabled:     ident.Disabled, // false (default) = enabled, true = disabled
			PolicyNames:  ident.PolicyNames,
		}
		switch {
		case ident.Name == AccountAnonymous.Id:
			t.Account = &AccountAnonymous
			identityAnonymous = t
		case ident.Account == nil:
			t.Account = &AccountAdmin
		default:
			if account, ok := accounts[ident.Account.Id]; ok {
				t.Account = account
			} else {
				t.Account = &AccountAdmin
				glog.Warningf("identity %s is associated with a non exist account ID, the association is invalid", ident.Name)
			}
		}

		for _, action := range ident.Actions {
			t.Actions = append(t.Actions, Action(action))
		}
		for _, cred := range ident.Credentials {
			t.Credentials = append(t.Credentials, &Credential{
				AccessKey: cred.AccessKey,
				SecretKey: cred.SecretKey,
				Status:    cred.Status, // Load access key status
			})
			accessKeyIdent[cred.AccessKey] = t
		}
		identities = append(identities, t)
		nameToIdentity[t.Name] = t
	}

	// Load service accounts and add their credentials to the parent identity
	for _, sa := range config.ServiceAccounts {
		if sa.Credential == nil {
			continue
		}

		// Skip disabled service accounts - they should not be able to authenticate
		if sa.Disabled {
			glog.V(3).Infof("Skipping disabled service account %s", sa.Id)
			continue
		}

		// Find the parent identity
		parentIdent, ok := nameToIdentity[sa.ParentUser]
		if !ok {
			glog.Warningf("Service account %s has non-existent parent user %s, skipping", sa.Id, sa.ParentUser)
			continue
		}

		// Add service account credential to parent identity with expiration
		cred := &Credential{
			AccessKey:  sa.Credential.AccessKey,
			SecretKey:  sa.Credential.SecretKey,
			Status:     sa.Credential.Status,
			Expiration: sa.Expiration, // Populate expiration from service account
		}
		parentIdent.Credentials = append(parentIdent.Credentials, cred)
		accessKeyIdent[sa.Credential.AccessKey] = parentIdent
		glog.V(3).Infof("Loaded service account %s for parent %s (expiration: %d)", sa.Id, sa.ParentUser, sa.Expiration)
	}

	iam.m.Lock()
	// atomically switch
	iam.identities = identities
	iam.identityAnonymous = identityAnonymous
	iam.accounts = accounts
	iam.emailAccount = emailAccount
	iam.accessKeyIdent = accessKeyIdent
	iam.nameToIdentity = nameToIdentity
	if !iam.isAuthEnabled { // one-directional, no toggling
		iam.isAuthEnabled = len(identities) > 0
	}
	iam.m.Unlock()

	// Log configuration summary
	glog.V(1).Infof("Loaded %d identities, %d accounts, %d access keys. Auth enabled: %v",
		len(identities), len(accounts), len(accessKeyIdent), iam.isAuthEnabled)

	if glog.V(2) {
		glog.V(2).Infof("Access key to identity mapping:")
		for accessKey, identity := range accessKeyIdent {
			glog.V(2).Infof("  %s -> %s (actions: %d)", accessKey, identity.Name, len(identity.Actions))
		}
	}

	return nil
}

// mergeS3ApiConfiguration merges dynamic configuration with existing static configuration
// Static identities (from file) are preserved and cannot be updated
// Dynamic identities (from filer/admin) can be added or updated
func (iam *IdentityAccessManagement) mergeS3ApiConfiguration(config *iam_pb.S3ApiConfiguration) error {
	// Start with current configuration (which includes static identities)
	iam.m.RLock()
	identities := make([]*Identity, len(iam.identities))
	copy(identities, iam.identities)
	identityAnonymous := iam.identityAnonymous
	accessKeyIdent := make(map[string]*Identity)
	for k, v := range iam.accessKeyIdent {
		accessKeyIdent[k] = v
	}
	nameToIdentity := make(map[string]*Identity)
	for k, v := range iam.nameToIdentity {
		nameToIdentity[k] = v
	}
	accounts := make(map[string]*Account)
	for k, v := range iam.accounts {
		accounts[k] = v
	}
	emailAccount := make(map[string]*Account)
	for k, v := range iam.emailAccount {
		emailAccount[k] = v
	}
	staticNames := make(map[string]bool)
	for k, v := range iam.staticIdentityNames {
		staticNames[k] = v
	}
	iam.m.RUnlock()

	// Process accounts from dynamic config (can add new accounts)
	for _, account := range config.Accounts {
		if _, exists := accounts[account.Id]; !exists {
			glog.V(3).Infof("adding dynamic account: name=%s, id=%s", account.DisplayName, account.Id)
			accounts[account.Id] = &Account{
				Id:           account.Id,
				DisplayName:  account.DisplayName,
				EmailAddress: account.EmailAddress,
			}
			if account.EmailAddress != "" {
				emailAccount[account.EmailAddress] = accounts[account.Id]
			}
		}
	}

	// Ensure default accounts exist
	if _, exists := accounts[AccountAdmin.Id]; !exists {
		accounts[AccountAdmin.Id] = &Account{
			DisplayName:  AccountAdmin.DisplayName,
			EmailAddress: AccountAdmin.EmailAddress,
			Id:           AccountAdmin.Id,
		}
		emailAccount[AccountAdmin.EmailAddress] = accounts[AccountAdmin.Id]
	}
	if _, exists := accounts[AccountAnonymous.Id]; !exists {
		accounts[AccountAnonymous.Id] = &Account{
			DisplayName:  AccountAnonymous.DisplayName,
			EmailAddress: AccountAnonymous.EmailAddress,
			Id:           AccountAnonymous.Id,
		}
		emailAccount[AccountAnonymous.EmailAddress] = accounts[AccountAnonymous.Id]
	}

	// Process identities from dynamic config
	for _, ident := range config.Identities {
		// Skip static identities - they cannot be updated
		if staticNames[ident.Name] {
			glog.V(3).Infof("skipping static identity %s (immutable)", ident.Name)
			continue
		}

		glog.V(3).Infof("loading/updating dynamic identity %s (disabled=%v)", ident.Name, ident.Disabled)
		t := &Identity{
			Name:         ident.Name,
			Credentials:  nil,
			Actions:      nil,
			PrincipalArn: generatePrincipalArn(ident.Name),
			Disabled:     ident.Disabled,
			PolicyNames:  ident.PolicyNames,
		}

		switch {
		case ident.Name == AccountAnonymous.Id:
			t.Account = &AccountAnonymous
			identityAnonymous = t
		case ident.Account == nil:
			t.Account = &AccountAdmin
		default:
			if account, ok := accounts[ident.Account.Id]; ok {
				t.Account = account
			} else {
				t.Account = &AccountAdmin
				glog.Warningf("identity %s is associated with a non exist account ID, the association is invalid", ident.Name)
			}
		}

		for _, action := range ident.Actions {
			t.Actions = append(t.Actions, Action(action))
		}
		for _, cred := range ident.Credentials {
			t.Credentials = append(t.Credentials, &Credential{
				AccessKey: cred.AccessKey,
				SecretKey: cred.SecretKey,
				Status:    cred.Status,
			})
			accessKeyIdent[cred.AccessKey] = t
		}

		// Update or add the identity
		existingIdx := -1
		for i, existing := range identities {
			if existing.Name == ident.Name {
				existingIdx = i
				break
			}
		}

		if existingIdx >= 0 {
			// Before replacing, remove stale accessKeyIdent entries for the old identity
			oldIdentity := identities[existingIdx]
			for _, oldCred := range oldIdentity.Credentials {
				// Only remove if it still points to this identity
				if accessKeyIdent[oldCred.AccessKey] == oldIdentity {
					delete(accessKeyIdent, oldCred.AccessKey)
				}
			}
			// Replace existing dynamic identity
			identities[existingIdx] = t
		} else {
			// Add new dynamic identity
			identities = append(identities, t)
		}
		nameToIdentity[t.Name] = t
	}

	// Process service accounts from dynamic config
	for _, sa := range config.ServiceAccounts {
		if sa.Credential == nil {
			continue
		}

		// Skip disabled service accounts
		if sa.Disabled {
			glog.V(3).Infof("Skipping disabled service account %s", sa.Id)
			continue
		}

		// Find the parent identity
		parentIdent, ok := nameToIdentity[sa.ParentUser]
		if !ok {
			glog.Warningf("Service account %s has non-existent parent user %s, skipping", sa.Id, sa.ParentUser)
			continue
		}

		// Skip if parent is a static identity (we don't modify static identities)
		if staticNames[sa.ParentUser] {
			glog.V(3).Infof("Skipping service account %s for static parent %s", sa.Id, sa.ParentUser)
			continue
		}

		// Check if this access key already exists in parent's credentials to avoid duplicates
		alreadyExists := false
		for _, existingCred := range parentIdent.Credentials {
			if existingCred.AccessKey == sa.Credential.AccessKey {
				alreadyExists = true
				break
			}
		}

		if alreadyExists {
			glog.V(3).Infof("Service account %s credential already exists for parent %s, skipping", sa.Id, sa.ParentUser)
			// Ensure accessKeyIdent mapping is correct
			accessKeyIdent[sa.Credential.AccessKey] = parentIdent
			continue
		}

		// Add service account credential to parent identity
		cred := &Credential{
			AccessKey:  sa.Credential.AccessKey,
			SecretKey:  sa.Credential.SecretKey,
			Status:     sa.Credential.Status,
			Expiration: sa.Expiration,
		}
		parentIdent.Credentials = append(parentIdent.Credentials, cred)
		accessKeyIdent[sa.Credential.AccessKey] = parentIdent
		glog.V(3).Infof("Loaded service account %s for dynamic parent %s (expiration: %d)", sa.Id, sa.ParentUser, sa.Expiration)
	}

	iam.m.Lock()
	// atomically switch
	iam.identities = identities
	iam.identityAnonymous = identityAnonymous
	iam.accounts = accounts
	iam.emailAccount = emailAccount
	iam.accessKeyIdent = accessKeyIdent
	iam.nameToIdentity = nameToIdentity
	if !iam.isAuthEnabled {
		iam.isAuthEnabled = len(identities) > 0
	}
	iam.m.Unlock()

	// Log configuration summary
	staticCount := len(staticNames)
	dynamicCount := len(identities) - staticCount
	glog.V(1).Infof("Merged config: %d static + %d dynamic identities = %d total, %d accounts, %d access keys. Auth enabled: %v",
		staticCount, dynamicCount, len(identities), len(accounts), len(accessKeyIdent), iam.isAuthEnabled)

	if glog.V(2) {
		glog.V(2).Infof("Access key to identity mapping:")
		for accessKey, identity := range accessKeyIdent {
			identityType := "dynamic"
			if staticNames[identity.Name] {
				identityType = "static"
			}
			glog.V(2).Infof("  %s -> %s (%s, actions: %d)", accessKey, identity.Name, identityType, len(identity.Actions))
		}
	}

	return nil
}

func (iam *IdentityAccessManagement) isEnabled() bool {
	return iam.isAuthEnabled
}

func (iam *IdentityAccessManagement) IsStaticConfig() bool {
	iam.m.RLock()
	defer iam.m.RUnlock()
	return iam.useStaticConfig
}

// IsStaticIdentity checks if an identity was loaded from the static config file
func (iam *IdentityAccessManagement) IsStaticIdentity(identityName string) bool {
	iam.m.RLock()
	defer iam.m.RUnlock()
	return iam.staticIdentityNames[identityName]
}

func (iam *IdentityAccessManagement) lookupByAccessKey(accessKey string) (identity *Identity, cred *Credential, found bool) {
	iam.m.RLock()
	defer iam.m.RUnlock()

	// Helper function to truncate access key for logging to avoid credential exposure
	truncate := func(key string) string {
		const mask = "***"
		if len(key) > 4 {
			return key[:4] + mask
		}
		// For very short keys, never log the full key
		return mask
	}

	truncatedKey := truncate(accessKey)

	glog.V(4).Infof("Looking up access key: %s (len=%d, total keys registered: %d)",
		truncatedKey, len(accessKey), len(iam.accessKeyIdent))

	if ident, ok := iam.accessKeyIdent[accessKey]; ok {
		// Check if user is disabled
		if ident.Disabled {
			glog.V(2).Infof("User %s is disabled, rejecting access key %s", ident.Name, truncatedKey)
			return nil, nil, false
		}

		for _, credential := range ident.Credentials {
			if credential.AccessKey == accessKey {
				// Check if access key is inactive (empty Status treated as Active for backward compatibility)
				if credential.Status == iamAccessKeyStatusInactive {
					glog.V(2).Infof("Access key %s for identity %s is inactive", truncatedKey, ident.Name)
					return nil, nil, false
				}
				glog.V(4).Infof("Found access key %s for identity %s", truncatedKey, ident.Name)
				return ident, credential, true
			}
		}
	}

	glog.V(2).Infof("Could not find access key %s (len=%d). Available keys: %d, Auth enabled: %v",
		truncatedKey, len(accessKey), len(iam.accessKeyIdent), iam.isAuthEnabled)

	// Log all registered access keys at higher verbosity for debugging
	if glog.V(3) {
		glog.V(3).Infof("Registered access keys:")
		for key := range iam.accessKeyIdent {
			glog.V(3).Infof("  - %s (len=%d)", truncate(key), len(key))
		}
	}

	return nil, nil, false
}

// LookupByAccessKey is an exported wrapper for lookupByAccessKey.
// It returns the identity and credential associated with the given access key.
//
// WARNING: The returned pointers reference internal data structures.
// Callers MUST NOT modify the returned Identity or Credential objects.
// If mutation is needed, make a copy first.
func (iam *IdentityAccessManagement) LookupByAccessKey(accessKey string) (identity *Identity, cred *Credential, found bool) {
	return iam.lookupByAccessKey(accessKey)
}

func (iam *IdentityAccessManagement) lookupAnonymous() (identity *Identity, found bool) {
	iam.m.RLock()
	defer iam.m.RUnlock()
	if iam.identityAnonymous != nil {
		return iam.identityAnonymous, true
	}
	return nil, false
}

func (iam *IdentityAccessManagement) lookupByIdentityName(name string) *Identity {
	iam.m.RLock()
	defer iam.m.RUnlock()

	return iam.nameToIdentity[name]
}

// generatePrincipalArn generates an ARN for a user identity
func generatePrincipalArn(identityName string) string {
	// Handle special cases
	switch identityName {
	case AccountAnonymous.Id:
		return "arn:aws:iam::user/anonymous"
	case AccountAdmin.Id:
		return "arn:aws:iam::user/admin"
	default:
		return fmt.Sprintf("arn:aws:iam::user/%s", identityName)
	}
}

func (iam *IdentityAccessManagement) GetAccountNameById(canonicalId string) string {
	iam.m.RLock()
	defer iam.m.RUnlock()
	if account, ok := iam.accounts[canonicalId]; ok {
		return account.DisplayName
	}
	return ""
}

func (iam *IdentityAccessManagement) GetAccountIdByEmail(email string) string {
	iam.m.RLock()
	defer iam.m.RUnlock()
	if account, ok := iam.emailAccount[email]; ok {
		return account.Id
	}
	return ""
}

func (iam *IdentityAccessManagement) Auth(f http.HandlerFunc, action Action) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !iam.isEnabled() {
			f(w, r)
			return
		}

		identity, errCode := iam.authRequest(r, action)
		if errCode != s3err.ErrNone {
			glog.V(3).Infof("auth error: %v", errCode)
		}

		iam.handleAuthResult(w, r, identity, errCode, f)
	}
}

// AuthPostPolicy is a specialized authentication wrapper for PostPolicy requests.
// It allows requests with multipart/form-data to proceed even if classified as Anonymous,
// because the actual authentication (signature verification) for ALL PostPolicy requests is
// performed unconditionally in PostPolicyBucketHandler.doesPolicySignatureMatch().
// This delegation only defers the initial authentication classification; it does NOT bypass
// signature verification, which is mandatory for all PostPolicy uploads.
func (iam *IdentityAccessManagement) AuthPostPolicy(f http.HandlerFunc, action Action) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !iam.isEnabled() {
			f(w, r)
			return
		}

		// Optimization: Use authRequestWithAuthType to avoid re-parsing headers for classification
		identity, errCode, authType := iam.authRequestWithAuthType(r, action)

		// Special handling for PostPolicy: if AccessDenied (likely because Anonymous to private bucket)
		// AND it looks like a PostPolicy request, allow it to proceed to handler for verification.
		if errCode == s3err.ErrAccessDenied {
			if authType == authTypeAnonymous &&
				r.Method == http.MethodPost &&
				strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") {

				glog.V(3).Infof("Delegating PostPolicy auth to handler")
				r.Header.Set(s3_constants.AmzAuthType, "PostPolicy")
				f(w, r)
				return
			}
		}

		if errCode != s3err.ErrNone {
			glog.V(3).Infof("auth error: %v", errCode)
		}

		iam.handleAuthResult(w, r, identity, errCode, f)
	}
}

func (iam *IdentityAccessManagement) handleAuthResult(w http.ResponseWriter, r *http.Request, identity *Identity, errCode s3err.ErrorCode, f http.HandlerFunc) {
	if errCode == s3err.ErrNone {
		// Store the authenticated identity in request context (secure, cannot be spoofed)
		if identity != nil && identity.Name != "" {
			ctx := s3_constants.SetIdentityNameInContext(r.Context(), identity.Name)
			// Also store the full identity object for handlers that need it (e.g., ListBuckets)
			// This is especially important for JWT users whose identity is not in the identities list
			ctx = s3_constants.SetIdentityInContext(ctx, identity)
			r = r.WithContext(ctx)
		}
		f(w, r)
		return
	}
	s3err.WriteErrorResponse(w, r, errCode)
}

// Wrapper to maintain backward compatibility
func (iam *IdentityAccessManagement) authRequest(r *http.Request, action Action) (*Identity, s3err.ErrorCode) {
	identity, err, _ := iam.authRequestWithAuthType(r, action)
	return identity, err
}

// check whether the request has valid access keys
func (iam *IdentityAccessManagement) authRequestWithAuthType(r *http.Request, action Action) (*Identity, s3err.ErrorCode, authType) {
	var identity *Identity
	var s3Err s3err.ErrorCode
	var found bool
	var amzAuthType string

	reqAuthType := getRequestAuthType(r)

	switch reqAuthType {
	case authTypeUnknown:
		glog.V(4).Infof("unknown auth type")
		r.Header.Set(s3_constants.AmzAuthType, "Unknown")
		return identity, s3err.ErrAccessDenied, reqAuthType
	case authTypePresignedV2, authTypeSignedV2:
		glog.V(4).Infof("v2 auth type")
		identity, s3Err = iam.isReqAuthenticatedV2(r)
		amzAuthType = "SigV2"
	case authTypeStreamingSigned, authTypeSigned, authTypePresigned:
		glog.V(4).Infof("v4 auth type")
		identity, s3Err = iam.reqSignatureV4Verify(r)
		amzAuthType = "SigV4"
	case authTypeStreamingUnsigned:
		glog.V(4).Infof("unsigned streaming upload")
		identity, s3Err = iam.reqSignatureV4Verify(r)
		amzAuthType = "SigV4"
	case authTypeJWT:
		glog.V(4).Infof("jwt auth type detected, iamIntegration != nil? %t", iam.iamIntegration != nil)
		r.Header.Set(s3_constants.AmzAuthType, "Jwt")
		if iam.iamIntegration != nil {
			identity, s3Err = iam.authenticateJWTWithIAM(r)
			amzAuthType = "Jwt"
		} else {
			glog.V(2).Infof("IAM integration is nil, returning ErrNotImplemented")
			return identity, s3err.ErrNotImplemented, reqAuthType
		}
	case authTypeAnonymous:
		amzAuthType = "Anonymous"
		if identity, found = iam.lookupAnonymous(); !found {
			r.Header.Set(s3_constants.AmzAuthType, amzAuthType)
			return identity, s3err.ErrAccessDenied, reqAuthType
		}
	default:
		return identity, s3err.ErrNotImplemented, reqAuthType
	}

	if len(amzAuthType) > 0 {
		r.Header.Set(s3_constants.AmzAuthType, amzAuthType)
	}
	if s3Err != s3err.ErrNone {
		return identity, s3Err, reqAuthType
	}

	glog.V(4).Infof("user name: %v actions: %v, action: %v", identity.Name, identity.Actions, action)
	bucket, object := s3_constants.GetBucketAndObject(r)
	prefix := s3_constants.GetPrefix(r)

	// For List operations, use prefix for permission checking if available
	if action == s3_constants.ACTION_LIST && object == "" && prefix != "" {
		// List operation with prefix - check permission for the prefix path
		object = prefix
	} else if (object == "/" || object == "") && prefix != "" {
		// Using the aws cli with s3, and s3api, and with boto3, the object is often set to "/" or empty
		// but the prefix is set to the actual object key for permission checking
		object = prefix
	}

	// For ListBuckets, authorization is performed in the handler by iterating
	// through buckets and checking permissions for each. Skip the global check here.
	policyAllows := false

	if action == s3_constants.ACTION_LIST && bucket == "" {
		// ListBuckets operation - authorization handled per-bucket in the handler
	} else {
		// First check bucket policy if one exists
		// Bucket policies can grant or deny access to specific users/principals
		// Following AWS semantics:
		// - Explicit DENY in bucket policy → immediate rejection
		// - Explicit ALLOW in bucket policy → grant access (bypass IAM checks)
		// - No policy or indeterminate → fall through to IAM checks
		if iam.policyEngine != nil && bucket != "" {
			principal := buildPrincipalARN(identity)
			// Phase 1: Evaluate bucket policy without object entry.
			// Tag-based conditions (s3:ExistingObjectTag) are re-checked by handlers
			// after fetching the entry, which is the Phase 2 check.
			allowed, evaluated, err := iam.policyEngine.EvaluatePolicy(bucket, object, string(action), principal, r, nil)

			if err != nil {
				// SECURITY: Fail-close on policy evaluation errors
				// If we can't evaluate the policy, deny access rather than falling through to IAM
				glog.Errorf("Error evaluating bucket policy for %s/%s: %v - denying access", bucket, object, err)
				return identity, s3err.ErrAccessDenied, reqAuthType
			} else if evaluated {
				// A bucket policy exists and was evaluated with a matching statement
				if allowed {
					// Policy explicitly allows this action - grant access immediately
					// This bypasses IAM checks to support cross-account access and policy-only principals
					glog.V(3).Infof("Bucket policy allows %s to %s on %s/%s (bypassing IAM)", identity.Name, action, bucket, object)
					policyAllows = true
				} else {
					// Policy explicitly denies this action - deny access immediately
					// Note: Explicit Deny in bucket policy overrides all other permissions
					glog.V(3).Infof("Bucket policy explicitly denies %s to %s on %s/%s", identity.Name, action, bucket, object)
					return identity, s3err.ErrAccessDenied, reqAuthType
				}
			}
			// If not evaluated (no policy or no matching statements), fall through to IAM/identity checks
		}

		// Only check IAM if bucket policy didn't explicitly allow
		if !policyAllows {
			// Use centralized permission check
			if errCode := iam.VerifyActionPermission(r, identity, action, bucket, object); errCode != s3err.ErrNone {
				return identity, errCode, reqAuthType
			}
		}
	}

	r.Header.Set(s3_constants.AmzAccountId, identity.Account.Id)

	return identity, s3err.ErrNone, reqAuthType

}

// AuthSignatureOnly performs only signature verification without any authorization checks.
// This is used for IAM API operations where authorization is handled separately based on
// the specific IAM action (e.g., self-service vs admin operations).
// Returns the authenticated identity and any signature verification error.
func (iam *IdentityAccessManagement) AuthSignatureOnly(r *http.Request) (*Identity, s3err.ErrorCode) {
	var identity *Identity
	var s3Err s3err.ErrorCode
	var authType string
	switch getRequestAuthType(r) {
	case authTypeUnknown:
		glog.V(3).Infof("unknown auth type")
		r.Header.Set(s3_constants.AmzAuthType, "Unknown")
		return identity, s3err.ErrAccessDenied
	case authTypePresignedV2, authTypeSignedV2:
		glog.V(3).Infof("v2 auth type")
		identity, s3Err = iam.isReqAuthenticatedV2(r)
		authType = "SigV2"
	case authTypeStreamingSigned, authTypeSigned, authTypePresigned:
		glog.V(3).Infof("v4 auth type")
		identity, s3Err = iam.reqSignatureV4Verify(r)
		authType = "SigV4"

	case authTypeStreamingUnsigned:
		glog.V(3).Infof("unsigned streaming upload")
		identity, s3Err = iam.reqSignatureV4Verify(r)
		authType = "SigV4"
	case authTypeJWT:
		glog.V(3).Infof("jwt auth type detected, iamIntegration != nil? %t", iam.iamIntegration != nil)
		r.Header.Set(s3_constants.AmzAuthType, "Jwt")
		if iam.iamIntegration != nil {
			identity, s3Err = iam.authenticateJWTWithIAM(r)
			authType = "Jwt"
		} else {
			glog.V(2).Infof("IAM integration is nil, returning ErrNotImplemented")
			return identity, s3err.ErrNotImplemented
		}
	case authTypeAnonymous:
		// Anonymous users cannot use IAM API
		return identity, s3err.ErrAccessDenied
	default:
		return identity, s3err.ErrNotImplemented
	}

	if len(authType) > 0 {
		r.Header.Set(s3_constants.AmzAuthType, authType)
	}
	if s3Err != s3err.ErrNone {
		return identity, s3Err
	}

	// Set account ID header for downstream handlers
	if identity != nil && identity.Account != nil {
		r.Header.Set(s3_constants.AmzAccountId, identity.Account.Id)
	}

	return identity, s3err.ErrNone
}

func (identity *Identity) canDo(action Action, bucket string, objectKey string) bool {
	if identity == nil {
		return false
	}
	if identity.isAdmin() {
		return true
	}
	for _, a := range identity.Actions {
		// Case where the Resource provided is
		// 	"Resource": [
		//		"arn:aws:s3:::*"
		//	]
		if a == action {
			return true
		}
	}
	// Intelligent path concatenation to avoid double slashes
	fullPath := bucket
	if objectKey != "" && !strings.HasPrefix(objectKey, "/") {
		fullPath += "/"
	}
	fullPath += objectKey

	if bucket == "" {
		glog.V(3).Infof("identity %s is not allowed to perform action %s on %s -- bucket is empty", identity.Name, action, "/"+strings.TrimPrefix(objectKey, "/"))
		return false
	}
	glog.V(3).Infof("checking if %s can perform %s on bucket '%s'", identity.Name, action, fullPath)

	target := string(action) + ":" + fullPath
	adminTarget := s3_constants.ACTION_ADMIN + ":" + fullPath
	limitedByBucket := string(action) + ":" + bucket
	adminLimitedByBucket := s3_constants.ACTION_ADMIN + ":" + bucket

	for _, a := range identity.Actions {
		act := string(a)
		if strings.HasSuffix(act, "*") {
			if strings.HasPrefix(target, act[:len(act)-1]) {
				return true
			}
			if strings.HasPrefix(adminTarget, act[:len(act)-1]) {
				return true
			}
		} else {
			if act == limitedByBucket {
				return true
			}
			if act == adminLimitedByBucket {
				return true
			}
		}
	}
	//log error
	glog.V(3).Infof("identity %s is not allowed to perform action %s on %s", identity.Name, action, bucket+"/"+objectKey)
	return false
}

func (identity *Identity) isAdmin() bool {
	if identity == nil {
		return false
	}
	return slices.Contains(identity.Actions, s3_constants.ACTION_ADMIN)
}

// buildPrincipalARN builds an ARN for an identity to use in bucket policy evaluation
func buildPrincipalARN(identity *Identity) string {
	if identity == nil {
		return "*" // Anonymous
	}

	// Check if this is the anonymous user identity (authenticated as anonymous)
	// S3 policies expect Principal: "*" for anonymous access
	if identity.Name == s3_constants.AccountAnonymousId ||
		(identity.Account != nil && identity.Account.Id == s3_constants.AccountAnonymousId) {
		return "*" // Anonymous user
	}

	// Build an AWS-compatible principal ARN
	// Format: arn:aws:iam::account-id:user/user-name
	accountId := identity.Account.Id
	if accountId == "" {
		accountId = "000000000000" // Default account ID
	}

	userName := identity.Name
	if userName == "" {
		userName = "unknown"
	}

	return fmt.Sprintf("arn:aws:iam::%s:user/%s", accountId, userName)
}

// GetCredentialManager returns the credential manager instance
func (iam *IdentityAccessManagement) GetCredentialManager() *credential.CredentialManager {
	return iam.credentialManager
}

// LoadS3ApiConfigurationFromCredentialManager loads configuration using the credential manager
func (iam *IdentityAccessManagement) LoadS3ApiConfigurationFromCredentialManager() error {
	glog.V(1).Infof("Loading S3 API configuration from credential manager")

	s3ApiConfiguration, err := iam.credentialManager.LoadConfiguration(context.Background())
	if err != nil {
		glog.Errorf("Failed to load configuration from credential manager: %v", err)
		return fmt.Errorf("failed to load configuration from credential manager: %w", err)
	}

	glog.V(2).Infof("Credential manager returned %d identities and %d accounts",
		len(s3ApiConfiguration.Identities), len(s3ApiConfiguration.Accounts))

	if err := iam.loadS3ApiConfiguration(s3ApiConfiguration); err != nil {
		glog.Errorf("Failed to load S3 API configuration: %v", err)
		return err
	}

	glog.V(1).Infof("Successfully loaded S3 API configuration from credential manager")
	return nil
}

// initializeKMSFromConfig loads KMS configuration from TOML format
func (iam *IdentityAccessManagement) initializeKMSFromConfig(configContent []byte) error {
	// JSON-only KMS configuration
	if err := iam.initializeKMSFromJSON(configContent); err == nil {
		glog.V(1).Infof("Successfully loaded KMS configuration from JSON format")
		return nil
	}

	glog.V(2).Infof("No KMS configuration found in S3 config - SSE-KMS will not be available")
	return nil
}

// initializeKMSFromJSON loads KMS configuration from JSON format when provided in the same file
func (iam *IdentityAccessManagement) initializeKMSFromJSON(configContent []byte) error {
	// Parse as generic JSON and extract optional "kms" block
	var m map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(configContent))), &m); err != nil {
		return err
	}
	kmsVal, ok := m["kms"]
	if !ok {
		return fmt.Errorf("no KMS section found")
	}

	// Load KMS configuration directly from the parsed JSON data
	return kms.LoadKMSFromConfig(kmsVal)
}

// SetIAMIntegration sets the IAM integration for advanced authentication and authorization
func (iam *IdentityAccessManagement) SetIAMIntegration(integration *S3IAMIntegration) {
	iam.m.Lock()
	defer iam.m.Unlock()
	iam.iamIntegration = integration
	// When IAM integration is configured, authentication must be enabled
	// to ensure requests go through proper auth checks
	if integration != nil {
		iam.isAuthEnabled = true
	}
}

// authenticateJWTWithIAM authenticates JWT tokens using the IAM integration
func (iam *IdentityAccessManagement) authenticateJWTWithIAM(r *http.Request) (*Identity, s3err.ErrorCode) {
	ctx := r.Context()

	// Use IAM integration to authenticate JWT
	iamIdentity, errCode := iam.iamIntegration.AuthenticateJWT(ctx, r)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}

	// Convert IAMIdentity to existing Identity structure
	identity := &Identity{
		Name:        iamIdentity.Name,
		Account:     iamIdentity.Account,
		Actions:     []Action{}, // Empty - authorization handled by policy engine
		PolicyNames: iamIdentity.PolicyNames,
	}

	// Store session info in request headers for later authorization
	r.Header.Set("X-SeaweedFS-Session-Token", iamIdentity.SessionToken)
	r.Header.Set("X-SeaweedFS-Principal", iamIdentity.Principal)

	return identity, s3err.ErrNone
}

// IAM authorization path type constants
// iamAuthPath represents the type of IAM authorization path
type iamAuthPath string

// IAM authorization path constants
const (
	iamAuthPathJWT       iamAuthPath = "jwt"
	iamAuthPathSTS_V4    iamAuthPath = "sts_v4"
	iamAuthPathStatic_V4 iamAuthPath = "static_v4"
	iamAuthPathNone      iamAuthPath = "none"
)

// determineIAMAuthPath determines the IAM authorization path based on available tokens and principals
func determineIAMAuthPath(sessionToken, principal, principalArn string) iamAuthPath {
	if sessionToken != "" && principal != "" {
		return iamAuthPathJWT
	} else if sessionToken != "" && principalArn != "" {
		return iamAuthPathSTS_V4
	} else if principalArn != "" {
		return iamAuthPathStatic_V4
	}
	return iamAuthPathNone
}

// VerifyActionPermission checks if the identity is allowed to perform the action on the resource.
// It handles both traditional identities (via Actions) and IAM/STS identities (via Policy).
func (iam *IdentityAccessManagement) VerifyActionPermission(r *http.Request, identity *Identity, action Action, bucket, object string) s3err.ErrorCode {
	// Fail closed if identity is nil
	if identity == nil {
		glog.V(3).Infof("VerifyActionPermission called with nil identity for action %s on %s/%s", action, bucket, object)
		return s3err.ErrAccessDenied
	}

	// Traditional identities (with Actions from -s3.config) use legacy auth,
	// JWT/STS identities (no Actions) use IAM authorization
	if len(identity.Actions) > 0 {
		if !identity.canDo(action, bucket, object) {
			return s3err.ErrAccessDenied
		}
		return s3err.ErrNone
	} else if iam.iamIntegration != nil {
		return iam.authorizeWithIAM(r, identity, action, bucket, object)
	}

	return s3err.ErrAccessDenied
}

// authorizeWithIAM authorizes requests using the IAM integration policy engine
func (iam *IdentityAccessManagement) authorizeWithIAM(r *http.Request, identity *Identity, action Action, bucket string, object string) s3err.ErrorCode {
	ctx := r.Context()

	// Get session info from request headers
	// First check for JWT-based authentication headers (X-SeaweedFS-Session-Token)
	sessionToken := r.Header.Get("X-SeaweedFS-Session-Token")
	principal := r.Header.Get("X-SeaweedFS-Principal")

	// Fallback to AWS Signature V4 STS token if JWT token not present
	// This handles the case where STS AssumeRoleWithWebIdentity generates temporary credentials
	// that include an X-Amz-Security-Token header (in addition to the access key and secret)
	if sessionToken == "" {
		sessionToken = r.Header.Get("X-Amz-Security-Token")
		if sessionToken == "" {
			// Also check query parameters for presigned URLs with STS tokens
			sessionToken = r.URL.Query().Get("X-Amz-Security-Token")
		}
	}

	// Create IAMIdentity for authorization
	iamIdentity := &IAMIdentity{
		Name:        identity.Name,
		Account:     identity.Account,
		PolicyNames: identity.PolicyNames,
	}

	// Determine authorization path and configure identity
	authPath := determineIAMAuthPath(sessionToken, principal, identity.PrincipalArn)
	switch authPath {
	case iamAuthPathJWT:
		// JWT-based authentication - use session token and principal from headers
		iamIdentity.Principal = principal
		iamIdentity.SessionToken = sessionToken
		glog.V(3).Infof("Using JWT-based IAM authorization for principal: %s", principal)
	case iamAuthPathSTS_V4:
		// STS V4 signature authentication - use session token (from X-Amz-Security-Token) with principal ARN
		iamIdentity.Principal = identity.PrincipalArn
		iamIdentity.SessionToken = sessionToken
		glog.V(3).Infof("Using STS V4 signature IAM authorization for principal: %s with session token", identity.PrincipalArn)
	case iamAuthPathStatic_V4:
		// Static V4 signature authentication - use principal ARN without session token
		iamIdentity.Principal = identity.PrincipalArn
		iamIdentity.SessionToken = ""
		glog.V(3).Infof("Using static V4 signature IAM authorization for principal: %s", identity.PrincipalArn)
	default:
		glog.V(3).Info("No valid principal information for IAM authorization")
		return s3err.ErrAccessDenied
	}

	// Use IAM integration for authorization
	return iam.iamIntegration.AuthorizeAction(ctx, iamIdentity, action, bucket, object, r)
}
