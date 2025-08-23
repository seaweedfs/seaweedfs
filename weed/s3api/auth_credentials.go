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

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/kms"
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
}

type Identity struct {
	Name        string
	Account     *Account
	Credentials []*Credential
	Actions     []Action
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
	AccessKey string
	SecretKey string
}

// "Permission": "FULL_CONTROL"|"WRITE"|"WRITE_ACP"|"READ"|"READ_ACP"
func (action Action) getPermission() Permission {
	switch act := strings.Split(string(action), ":")[0]; act {
	case s3_constants.ACTION_ADMIN:
		return Permission("FULL_CONTROL")
	case s3_constants.ACTION_WRITE:
		return Permission("WRITE")
	case s3_constants.ACTION_WRITE_ACP:
		return Permission("WRITE_ACP")
	case s3_constants.ACTION_READ:
		return Permission("READ")
	case s3_constants.ACTION_READ_ACP:
		return Permission("READ_ACP")
	default:
		return Permission("")
	}
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

	// For stores that need filer client details, set them
	if store := credentialManager.GetStore(); store != nil {
		if filerClientSetter, ok := store.(interface {
			SetFilerClient(string, grpc.DialOption)
		}); ok {
			filerClientSetter.SetFilerClient(string(option.Filer), option.GrpcDialOption)
		}
	}

	iam.credentialManager = credentialManager

	// Track whether any configuration was successfully loaded
	configLoaded := false

	// First, try to load configurations from file or filer
	if option.Config != "" {
		glog.V(3).Infof("loading static config file %s", option.Config)
		if err := iam.loadS3ApiConfigurationFromFile(option.Config); err != nil {
			glog.Fatalf("fail to load config file %s: %v", option.Config, err)
		}
		// Mark as loaded since an explicit config file was provided
		// This prevents fallback to environment variables even if no identities were loaded
		// (e.g., config file contains only KMS settings)
		configLoaded = true
	} else {
		glog.V(3).Infof("no static config file specified... loading config from credential manager")
		if err := iam.loadS3ApiConfigurationFromFiler(option); err != nil {
			glog.Warningf("fail to load config: %v", err)
		} else {
			// Check if any identities were actually loaded from filer
			iam.m.RLock()
			if len(iam.identities) > 0 {
				configLoaded = true
			}
			iam.m.RUnlock()
		}
	}

	// Only use environment variables as fallback if no configuration was loaded
	if !configLoaded {
		accessKeyId := os.Getenv("AWS_ACCESS_KEY_ID")
		secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

		if accessKeyId != "" && secretAccessKey != "" {
			glog.V(0).Infof("No S3 configuration found, using AWS environment variables as fallback")

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
				iam.isAuthEnabled = true
			}
			iam.m.Unlock()

			glog.V(0).Infof("Added admin identity from AWS environment variables: %s", envIdentity.Name)
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
	var identities []*Identity
	var identityAnonymous *Identity
	accessKeyIdent := make(map[string]*Identity)
	accounts := make(map[string]*Account)
	emailAccount := make(map[string]*Account)
	foundAccountAdmin := false
	foundAccountAnonymous := false

	for _, account := range config.Accounts {
		glog.V(3).Infof("loading account  name=%s, id=%s", account.DisplayName, account.Id)
		switch account.Id {
		case AccountAdmin.Id:
			AccountAdmin = Account{
				Id:           account.Id,
				DisplayName:  account.DisplayName,
				EmailAddress: account.EmailAddress,
			}
			accounts[account.Id] = &AccountAdmin
			foundAccountAdmin = true
		case AccountAnonymous.Id:
			AccountAnonymous = Account{
				Id:           account.Id,
				DisplayName:  account.DisplayName,
				EmailAddress: account.EmailAddress,
			}
			accounts[account.Id] = &AccountAnonymous
			foundAccountAnonymous = true
		default:
			t := Account{
				Id:           account.Id,
				DisplayName:  account.DisplayName,
				EmailAddress: account.EmailAddress,
			}
			accounts[account.Id] = &t
		}
		if account.EmailAddress != "" {
			emailAccount[account.EmailAddress] = accounts[account.Id]
		}
	}
	if !foundAccountAdmin {
		accounts[AccountAdmin.Id] = &AccountAdmin
		emailAccount[AccountAdmin.EmailAddress] = &AccountAdmin
	}
	if !foundAccountAnonymous {
		accounts[AccountAnonymous.Id] = &AccountAnonymous
		emailAccount[AccountAnonymous.EmailAddress] = &AccountAnonymous
	}
	for _, ident := range config.Identities {
		glog.V(3).Infof("loading identity %s", ident.Name)
		t := &Identity{
			Name:        ident.Name,
			Credentials: nil,
			Actions:     nil,
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
			})
			accessKeyIdent[cred.AccessKey] = t
		}
		identities = append(identities, t)
	}

	iam.m.Lock()
	// atomically switch
	iam.identities = identities
	iam.identityAnonymous = identityAnonymous
	iam.accounts = accounts
	iam.emailAccount = emailAccount
	iam.accessKeyIdent = accessKeyIdent
	if !iam.isAuthEnabled { // one-directional, no toggling
		iam.isAuthEnabled = len(identities) > 0
	}
	iam.m.Unlock()

	return nil
}

func (iam *IdentityAccessManagement) isEnabled() bool {
	return iam.isAuthEnabled
}

func (iam *IdentityAccessManagement) lookupByAccessKey(accessKey string) (identity *Identity, cred *Credential, found bool) {
	iam.m.RLock()
	defer iam.m.RUnlock()
	if ident, ok := iam.accessKeyIdent[accessKey]; ok {
		for _, credential := range ident.Credentials {
			if credential.AccessKey == accessKey {
				return ident, credential, true
			}
		}
	}
	glog.V(1).Infof("could not find accessKey %s", accessKey)
	return nil, nil, false
}

func (iam *IdentityAccessManagement) lookupAnonymous() (identity *Identity, found bool) {
	iam.m.RLock()
	defer iam.m.RUnlock()
	if iam.identityAnonymous != nil {
		return iam.identityAnonymous, true
	}
	return nil, false
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
		glog.V(3).Infof("auth error: %v", errCode)

		if errCode == s3err.ErrNone {
			if identity != nil && identity.Name != "" {
				r.Header.Set(s3_constants.AmzIdentityId, identity.Name)
			}
			f(w, r)
			return
		}
		s3err.WriteErrorResponse(w, r, errCode)
	}
}

// check whether the request has valid access keys
func (iam *IdentityAccessManagement) authRequest(r *http.Request, action Action) (*Identity, s3err.ErrorCode) {
	var identity *Identity
	var s3Err s3err.ErrorCode
	var found bool
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
	case authTypePostPolicy:
		glog.V(3).Infof("post policy auth type")
		r.Header.Set(s3_constants.AmzAuthType, "PostPolicy")
		return identity, s3err.ErrNone
	case authTypeStreamingUnsigned:
		glog.V(3).Infof("unsigned streaming upload")
		return identity, s3err.ErrNone
	case authTypeJWT:
		glog.V(3).Infof("jwt auth type")
		r.Header.Set(s3_constants.AmzAuthType, "Jwt")
		return identity, s3err.ErrNotImplemented
	case authTypeAnonymous:
		authType = "Anonymous"
		if identity, found = iam.lookupAnonymous(); !found {
			r.Header.Set(s3_constants.AmzAuthType, authType)
			return identity, s3err.ErrAccessDenied
		}
	default:
		return identity, s3err.ErrNotImplemented
	}

	if len(authType) > 0 {
		r.Header.Set(s3_constants.AmzAuthType, authType)
	}
	if s3Err != s3err.ErrNone {
		return identity, s3Err
	}

	glog.V(3).Infof("user name: %v actions: %v, action: %v", identity.Name, identity.Actions, action)
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
	if action == s3_constants.ACTION_LIST && bucket == "" {
		// ListBuckets operation - authorization handled per-bucket in the handler
	} else {
		if !identity.canDo(action, bucket, object) {
			return identity, s3err.ErrAccessDenied
		}
	}

	r.Header.Set(s3_constants.AmzAccountId, identity.Account.Id)

	return identity, s3err.ErrNone

}

func (identity *Identity) canDo(action Action, bucket string, objectKey string) bool {
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
	if bucket == "" {
		glog.V(3).Infof("identity %s is not allowed to perform action %s on %s -- bucket is empty", identity.Name, action, bucket+objectKey)
		return false
	}
	glog.V(3).Infof("checking if %s can perform %s on bucket '%s'", identity.Name, action, bucket+objectKey)
	target := string(action) + ":" + bucket + objectKey
	adminTarget := s3_constants.ACTION_ADMIN + ":" + bucket + objectKey
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
	glog.V(3).Infof("identity %s is not allowed to perform action %s on %s", identity.Name, action, bucket+objectKey)
	return false
}

func (identity *Identity) isAdmin() bool {
	return slices.Contains(identity.Actions, s3_constants.ACTION_ADMIN)
}

// GetCredentialManager returns the credential manager instance
func (iam *IdentityAccessManagement) GetCredentialManager() *credential.CredentialManager {
	return iam.credentialManager
}

// LoadS3ApiConfigurationFromCredentialManager loads configuration using the credential manager
func (iam *IdentityAccessManagement) LoadS3ApiConfigurationFromCredentialManager() error {
	s3ApiConfiguration, err := iam.credentialManager.LoadConfiguration(context.Background())
	if err != nil {
		return fmt.Errorf("failed to load configuration from credential manager: %w", err)
	}

	return iam.loadS3ApiConfiguration(s3ApiConfiguration)
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
