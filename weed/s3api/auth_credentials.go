package s3api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"

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

	// background polling
	stopChan     chan struct{}
	shutdownOnce sync.Once

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
	PolicyNames  []string               // Attached IAM policy names
	PrincipalArn string                 // ARN for IAM authorization (e.g., "arn:aws:iam::account-id:user/username")
	Disabled     bool                   // User status: false = enabled (default), true = disabled
	Claims       map[string]interface{} // JWT claims for policy substitution
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
	iam.stopChan = make(chan struct{})

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
		iam.m.Unlock()
	}

	// Always try to load/merge config from credential manager (filer/db)
	// This ensures we get both static users (from file) and dynamic users (from backend)
	glog.V(3).Infof("loading dynamic config from credential manager")
	if err := iam.loadS3ApiConfigurationFromFiler(option); err != nil {
		glog.Warningf("fail to load config: %v", err)
	}

	// Determine whether to start background polling for updates
	// We poll if using a store that doesn't support real-time events (like Postgres)
	if store := iam.credentialManager.GetStore(); store != nil {
		storeName := store.GetName()
		if storeName == credential.StoreTypePostgres {
			glog.V(1).Infof("Starting background IAM polling for store: %s", storeName)
			go iam.pollIamConfigChanges(1 * time.Minute)
		}
	}

	// Check for AWS environment variables and merge them if present
	// This serves as an in-memory "static" configuration
	iam.loadEnvironmentVariableCredentials()

	// Determine whether to enable S3 authentication based on configuration
	// For "weed mini" without any S3 config, default to allowing all access (isAuthEnabled = false)
	// If any credentials are configured (via file, filer, or env vars), enable authentication
	iam.m.Lock()
	iam.isAuthEnabled = len(iam.identities) > 0
	iam.m.Unlock()

	if iam.isAuthEnabled {
		// Credentials were configured - enable authentication
		glog.V(1).Infof("S3 authentication enabled (%d identities configured)", len(iam.identities))
	} else {
		// No credentials configured
		if startConfigFile != "" {
			// Config file was specified but contained no identities - this is unusual, log a warning
			glog.Warningf("S3 config file %s specified but no identities loaded - authentication disabled", startConfigFile)
		} else {
			// No config file and no identities - this is the normal allow-all case
			glog.V(1).Infof("S3 authentication disabled - no credentials configured (allowing all access)")
		}
	}

	return iam
}

func (iam *IdentityAccessManagement) pollIamConfigChanges(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
				glog.Warningf("failed to reload IAM configuration via polling: %v", err)
			}
		case <-iam.stopChan:
			return
		}
	}
}

func (iam *IdentityAccessManagement) Shutdown() {
	iam.shutdownOnce.Do(func() {
		if iam.stopChan != nil {
			close(iam.stopChan)
		}
		if iam.credentialManager != nil {
			iam.credentialManager.Shutdown()
		}
	})
}

// loadEnvironmentVariableCredentials loads AWS credentials from environment variables
// and adds them as a static admin identity. This function is idempotent and can be
// called multiple times (e.g., after configuration reloads).
func (iam *IdentityAccessManagement) loadEnvironmentVariableCredentials() {
	accessKeyId := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if accessKeyId == "" || secretAccessKey == "" {
		return
	}

	// Create environment variable identity name
	identityNameSuffix := accessKeyId
	if len(accessKeyId) > 8 {
		identityNameSuffix = accessKeyId[:8]
	}
	identityName := "admin-" + identityNameSuffix

	// Create admin identity with environment variable credentials
	envIdentity := &Identity{
		Name:    identityName,
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

	iam.m.Lock()
	defer iam.m.Unlock()

	// Initialize maps if they are nil
	if iam.staticIdentityNames == nil {
		iam.staticIdentityNames = make(map[string]bool)
	}
	if iam.accessKeyIdent == nil {
		iam.accessKeyIdent = make(map[string]*Identity)
	}
	if iam.nameToIdentity == nil {
		iam.nameToIdentity = make(map[string]*Identity)
	}

	// Check if identity already exists (avoid duplicates)
	exists := false
	for _, ident := range iam.identities {
		if ident.Name == identityName {
			exists = true
			break
		}
	}

	if !exists {
		glog.Infof("Added admin identity from AWS environment variables: name=%s, accessKey=%s", envIdentity.Name, accessKeyId)

		// Add to identities list
		iam.identities = append(iam.identities, envIdentity)

		// Update credential mappings
		iam.accessKeyIdent[accessKeyId] = envIdentity
		iam.nameToIdentity[envIdentity.Name] = envIdentity

		// Treat env var identity as static (immutable)
		iam.staticIdentityNames[envIdentity.Name] = true

		// Ensure defaults exist
		if iam.accounts == nil {
			iam.accounts = make(map[string]*Account)
		}
		iam.accounts[AccountAdmin.Id] = &AccountAdmin
		iam.accounts[AccountAnonymous.Id] = &AccountAnonymous

		if iam.emailAccount == nil {
			iam.emailAccount = make(map[string]*Account)
		}
		iam.emailAccount[AccountAdmin.EmailAddress] = &AccountAdmin
		iam.emailAccount[AccountAnonymous.EmailAddress] = &AccountAnonymous
	}
}

func (iam *IdentityAccessManagement) loadS3ApiConfigurationFromFiler(option *S3ApiServerOption) (err error) {
	// Try to load configuration with retries to handle transient connectivity issues during startup
	for i := 0; i < 10; i++ {
		err = iam.doLoadS3ApiConfigurationFromFiler(option)
		if err == nil {
			return nil
		}
		if errors.Is(err, filer_pb.ErrNotFound) {
			return err
		}
		glog.Warningf("fail to load config from filer (attempt %d/10): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	return err
}

func (iam *IdentityAccessManagement) doLoadS3ApiConfigurationFromFiler(option *S3ApiServerOption) error {
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
	// Update authentication state based on whether identities exist
	// Once enabled, keep it enabled (one-way toggle)
	authJustEnabled := iam.updateAuthenticationState(len(identities))
	iam.m.Unlock()

	if authJustEnabled {
		glog.V(1).Infof("S3 authentication enabled - credentials were added dynamically")
	}

	// Re-add environment variable credentials if they exist
	// This ensures env var credentials persist across configuration reloads
	iam.loadEnvironmentVariableCredentials()

	// Log configuration summary - always log to help debugging
	glog.Infof("Loaded %d identities, %d accounts, %d access keys. Auth enabled: %v",
		len(iam.identities), len(iam.accounts), len(iam.accessKeyIdent), iam.isAuthEnabled)

	if glog.V(2) {
		glog.V(2).Infof("Access key to identity mapping:")
		iam.m.RLock()
		for accessKey, identity := range iam.accessKeyIdent {
			glog.V(2).Infof("  %s -> %s (actions: %d)", accessKey, identity.Name, len(identity.Actions))
		}
		iam.m.RUnlock()
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
	// Update authentication state based on whether identities exist
	// Once enabled, keep it enabled (one-way toggle)
	authJustEnabled := iam.updateAuthenticationState(len(identities))
	iam.m.Unlock()

	if authJustEnabled {
		glog.V(1).Infof("S3 authentication enabled because credentials were added dynamically")
	}

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

// isEnabled reports whether S3 auth should be enforced for this server.
//
// Auth is considered enabled if either:
//   - we have any locally managed identities/credentials (iam.isAuthEnabled), or
//   - an external IAM integration has been configured (iam.iamIntegration != nil).
func (iam *IdentityAccessManagement) isEnabled() bool {
	iam.m.RLock()
	defer iam.m.RUnlock()
	return iam.isAuthEnabled || iam.iamIntegration != nil
}

func (iam *IdentityAccessManagement) LookupByAccessKey(accessKey string) (identity *Identity, cred *Credential, found bool) {
	iam.m.RLock()
	defer iam.m.RUnlock()

	// 1. Try local lookup first
	if identity, found = iam.accessKeyIdent[accessKey]; found {
		// Check if identity is disabled
		if identity.Disabled {
			return nil, nil, false
		}
		// Find specific credential to check expiration and status
		for _, c := range identity.Credentials {
			if c.AccessKey == accessKey {

				// Check expiration
				if c.isCredentialExpired() {
					glog.V(1).Infof("Credential for %s has expired", identity.Name)
					return nil, nil, false
				}

				// Check status (defaults to Active if empty)
				if c.Status == "Inactive" {
					glog.V(1).Infof("Credential for %s is inactive", identity.Name)
					return nil, nil, false
				}

				return identity, c, true
			}
		}
		// Should not happen if accessKeyIdent is consistent, but safe fallback
		return identity, nil, true
	}

	// 2. If not found locally and IAM integration is enabled, try external lookup
	if iam.iamIntegration != nil {
		// Note: We release the read lock before calling external integration to avoid blocking
		// However, we're currently holding it. For now, since this interfaces might need locking internally,
		// we should probably let them handle it or assume they are thread-safe.
		// A better approach would be to unlock, call integration, then lock again if needed,
		// but simple lookup should be fine.
		//
		// TODO: Refactor IAM integration to be more decoupled
	}

	return nil, nil, false
}

func (iam *IdentityAccessManagement) LookupAnonymous() (identity *Identity, found bool) {
	iam.m.RLock()
	defer iam.m.RUnlock()
	if iam.identityAnonymous != nil {
		return iam.identityAnonymous, true
	}
	return nil, false
}

// GetUser returns the identity for the given username
func (iam *IdentityAccessManagement) GetUser(username string) (*Identity, error) {
	iam.m.RLock()
	defer iam.m.RUnlock()

	identity, ok := iam.nameToIdentity[username]
	if !ok {
		return nil, fmt.Errorf("user not found: %s", username)
	}
	return identity, nil
}

// updateAuthenticationState updates the isAuthEnabled flag based on configuration
// returns true if authentication was just enabled
func (iam *IdentityAccessManagement) updateAuthenticationState(identityCount int) bool {
	wasEnabled := iam.isAuthEnabled
	// Enable if we have identities OR if we have external integration
	// But here we only check identities count
	isEnabled := identityCount > 0

	iam.isAuthEnabled = isEnabled
	return !wasEnabled && isEnabled
}

// generatePrincipalArn generates a mock ARN for an identity
func generatePrincipalArn(name string) string {
	// Format: arn:partition:service:region:account-id:resource-type/resource-id
	// We use a simplified format for embedded IAM
	return fmt.Sprintf("arn:aws:iam::%s:user/%s", s3_constants.AccountAdminId, name)
}

// initializeKMSFromConfig initializes KMS providers based on configuration
func (iam *IdentityAccessManagement) initializeKMSFromConfig(content []byte) error {
	// Parse a minimal struct to get KMS config
	type KmsConfig struct {
		Kms struct {
			Type       string            `json:"type"`
			Properties map[string]string `json:"properties"`
		} `json:"kms"`
	}

	var config KmsConfig
	// Check if content is JSON or YAML/TOML?
	// The content passed to ParseS3ConfigurationFromBytes supports both via viper,
	// but here we might need to parse it manually if it's JSON.
	// Assuming JSON for s3 config.json
	if err := json.Unmarshal(content, &config); err != nil {
		// Failed to unmarshal as JSON, might be OK if it's not JSON
		return nil
	}

	if config.Kms.Type == "" {
		return nil
	}

	glog.V(1).Infof("Initializing KMS provider of type: %s", config.Kms.Type)

	// In a real implementation, we would pass these properties to the KMS factory
	// For now, we assume the side-effect imports have registered the providers
	// and we might need to configure them.
	// This part depends on how weed/kms is implemented.

	return nil
}

// Accessor methods for policy engine integration

func (iam *IdentityAccessManagement) LoadS3ApiConfigurationFromCredentialManager() error {
	glog.V(1).Infof("IAM: reloading configuration from credential manager")
	glog.V(1).Infof("Loading S3 API configuration from credential manager")
	if iam.credentialManager != nil {
		config, err := iam.credentialManager.LoadConfiguration(context.Background())
		if err != nil {
			return err
		}
		return iam.loadS3ApiConfiguration(config)
	}
	return nil
}
