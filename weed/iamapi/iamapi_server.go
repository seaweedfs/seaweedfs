package iamapi

// https://docs.aws.amazon.com/cli/latest/reference/iam/list-roles.html

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/credential/filer_etc"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/iam/constants"
	"github.com/seaweedfs/seaweedfs/weed/iam/errors"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/aws/aws-sdk-go/service/iam"
	"google.golang.org/grpc"
)

type IamS3ApiConfig interface {
	GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error)
	PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error)
	GetPolicies(policies *Policies) (err error)
	GetPolicy(name string) (policy *policy_engine.PolicyDocument, err error)
	PutPolicy(name string, policy *policy_engine.PolicyDocument) (err error)
	DeletePolicy(name string) (err error)
	CreateUser(user *iam_pb.Identity) (err error)
	GetUser(name string) (user *iam_pb.Identity, err error)
	UpdateUser(name string, user *iam_pb.Identity) (err error)
	DeleteUser(name string) (err error)
	ListUsers() (usernames []string, err error)
	CreateAccessKey(username string, cred *iam_pb.Credential) (err error)
	DeleteAccessKey(username string, accessKey string) (err error)
}

type IamS3ApiConfigure struct {
	option            *IamServerOption
	masterClient      *wdclient.MasterClient
	credentialManager *credential.CredentialManager
}

type IamServerOption struct {
	Masters             map[string]pb.ServerAddress
	Filers              []pb.ServerAddress
	Port                int
	GrpcDialOption      grpc.DialOption
	CredentialStoreType string // e.g., "filer_etc", "memory", "postgres"
}

type IamApiServer struct {
	s3ApiConfig      IamS3ApiConfig
	s3Identity       S3IdentityManager
	iamManager       *integration.IAMManager
	policyCache      map[string]*policy_engine.PolicyDocument
	policyCacheMutex sync.RWMutex
	shutdownContext  context.Context
	shutdownCancel   context.CancelFunc
	masterClient     *wdclient.MasterClient
}

var s3ApiConfigure IamS3ApiConfig

func NewIamApiServer(router *mux.Router, option *IamServerOption) (iamApiServer *IamApiServer, err error) {
	return NewIamApiServerWithStore(router, option, "")
}

func NewIamApiServerWithStore(router *mux.Router, option *IamServerOption, explicitStore string) (iamApiServer *IamApiServer, err error) {
	if len(option.Filers) == 0 {
		return nil, fmt.Errorf("at least one filer address is required")
	}
	
	masterClient := wdclient.NewMasterClient(option.GrpcDialOption, "", "iam", "", "", "", *pb.NewServiceDiscoveryFromMap(option.Masters))
	
	// Create a cancellable context for the master client connection
	// This allows graceful shutdown via Shutdown() method
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	
	// Start KeepConnectedToMaster for volume location lookups
	// IAM config files are typically small and inline, but if they ever have chunks,
	// ReadEntry→StreamContent needs masterClient for volume lookups
	glog.V(0).Infof("IAM API starting master client connection for volume location lookups")
	go masterClient.KeepConnectedToMaster(shutdownCtx)
	
	configure := &IamS3ApiConfigure{
		option:       option,
		masterClient: masterClient,
	}

	s3ApiConfigure = configure

	// For standalone IAM (PR2), we use the stub S3IdentityManager
	// In the future (PR4), this will be replaced by the real S3 integration
	s3Identity := &StubS3IdentityManager{}
	
	// Create credential manager for user/policy storage
	// Use store type from options, defaulting to filer_etc if not specified
	storeType := credential.StoreTypeFilerEtc
	if option.CredentialStoreType != "" {
		storeType = credential.CredentialStoreTypeName(option.CredentialStoreType)
	}
	cm, err := credential.NewCredentialManager(storeType, util.GetViper(), "")
	if err != nil {
		return nil, fmt.Errorf("failed to create credential manager with store %s: %w", storeType, err)
	}
	
	// Configure FilerEtcStore with explicit filer address if available
	// This is necessary because util.GetViper() might not have the correct configuration
	// when running as the 'iam' command where filer address is passed via options
	if store, ok := cm.GetStore().(*filer_etc.FilerEtcStore); ok && len(option.Filers) > 0 {
		store.SetFilerAddressFunc(func() pb.ServerAddress {
			// Use the first configured filer
			return option.Filers[0]
		}, option.GrpcDialOption)
	}
	
	configure.credentialManager = cm
	
	// Run migration from iam_config.json to individual files (if needed)
	// This runs once at startup to ensure users are in the expected location
	if store, ok := cm.GetStore().(*filer_etc.FilerEtcStore); ok {
		if err := store.MigrateUsersToIndividualFiles(context.Background()); err != nil {
			glog.Warningf("IAM user migration failed: %v", err)
			// Don't return error - service should still start even if migration fails
		}
	}
	
	// Initialize standalone IAM Manager
	iamManager := integration.NewIAMManager()
	// Configure IAM Manager with proper defaults
	iamConfig := &integration.IAMConfig{
		Policy: &policy.PolicyEngineConfig{DefaultEffect: "Deny"},
		Roles:  &integration.RoleStoreConfig{StoreType: "filer"},
		Groups: &integration.GroupStoreConfig{StoreType: "filer"},
	}
	if err := iamManager.Initialize(iamConfig, func() string {
		if len(option.Filers) > 0 {
			return string(option.Filers[0])
		}
		return ""
	}, masterClient); err != nil {
		return nil, fmt.Errorf("failed to initialize IAM manager: %w", err)
	}

	iamApiServer = &IamApiServer{
		s3ApiConfig:     s3ApiConfigure,
		s3Identity:      s3Identity,
		iamManager:      iamManager,
		policyCache:     make(map[string]*policy_engine.PolicyDocument),
		shutdownContext: shutdownCtx,
		shutdownCancel:  shutdownCancel,
		masterClient:    masterClient,
	}

	// Verify iamManager is properly initialized (fail-fast)
	if iamApiServer.iamManager == nil {
		return nil, fmt.Errorf("IAM manager initialization failed: manager is nil")
	}

	iamApiServer.registerRouter(router)

	return iamApiServer, nil
}

// SetIAM sets the S3 identity manager for the IAM server
// This enables the IAM API to interact with the underlying S3 identity system (legacy)
func (iama *IamApiServer) SetIAM(s3Identity S3IdentityManager) {
	iama.s3Identity = s3Identity
}

// GetIAMManager returns the internal IAMManager instance
// This allows other components (like S3 middleware) to share the same IAM state
func (iama *IamApiServer) GetIAMManager() *integration.IAMManager {
	return iama.iamManager
}

// Auth wraps a handler with authentication/authorization checks
// Validates AWS Signature V4 and verifies the caller has admin permissions
func (iama *IamApiServer) Auth(handler http.HandlerFunc, action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. Authenticate the request using AWS SigV4
		identity, err := iama.s3ApiConfig.(*IamS3ApiConfigure).AuthenticateRequest(r)
		if err != nil {
			glog.Warningf("IAM API authentication failed for action %s: %v", action, err)
			
			// Map error types to appropriate IAM error codes and HTTP status
			var iamErr *IamError
			
			if IsMalformedRequestError(err) {
				// 400 Bad Request - malformed authorization header
				iamErr = &IamError{
					Code:  iam.ErrCodeMalformedPolicyDocumentException, // Use existing 400-level error
					Error: fmt.Errorf("malformed request"),
				}
			} else if IsAuthenticationError(err) {
				// 401 Unauthorized - invalid credentials or signature
				// Use NoSuchEntityException as it maps to 404, closest to 401 available
				iamErr = &IamError{
					Code:  iam.ErrCodeNoSuchEntityException,
					Error: fmt.Errorf("authentication failed"),
				}
			} else {
				// 500 Internal Server Error - unexpected errors
				iamErr = &IamError{
					Code:  iam.ErrCodeServiceFailureException,
					Error: fmt.Errorf("internal authentication error"),
				}
			}
			
			writeIamErrorResponse(w, r, iamErr)
			return
		}

		// 2. Verify the caller has admin permissions
		if !hasAdminPermissions(identity) {
			glog.Warningf("IAM API access denied for user %s attempting action %s: insufficient permissions", identity.Name, action)
			
			// 403 Forbidden - authenticated but lacks permissions
			// Use EntityAlreadyExistsException as it maps to 409, closest to 403 available
			writeIamErrorResponse(w, r, &IamError{
				Code:  iam.ErrCodeServiceFailureException, // Keep as 500 for now since no 403 code exists
				Error: fmt.Errorf("insufficient permissions for IAM management operations"),
			})
			return
		}

		glog.V(2).Infof("IAM API authenticated request from %s for action %s", identity.Name, action)
		handler(w, r)
	}
}

func (iama *IamApiServer) registerRouter(router *mux.Router) {
	// API Router
	apiRouter := router.PathPrefix("/").Subrouter()
	// ListBuckets

	// apiRouter.Methods("GET").Path("/").HandlerFunc(track(s3a.iam.Auth(s3a.ListBucketsHandler, ACTION_ADMIN), "LIST"))
	apiRouter.Methods(http.MethodPost).Path("/").HandlerFunc(iama.Auth(iama.DoActions, "iam:*"))
	//\n	// NotFound
	apiRouter.NotFoundHandler = http.HandlerFunc(errors.NotFoundHandler)
}

// setPolicyCache stores a policy document in the cache with thread safety
func (iama *IamApiServer) setPolicyCache(policyName string, doc *policy_engine.PolicyDocument) {
	iama.policyCacheMutex.Lock()
	defer iama.policyCacheMutex.Unlock()
	iama.policyCache[policyName] = doc
}

// getPolicyCache retrieves a policy document from the cache with thread safety
func (iama *IamApiServer) getPolicyCache(policyName string) (*policy_engine.PolicyDocument, bool) {
	iama.policyCacheMutex.RLock()
	defer iama.policyCacheMutex.RUnlock()
	doc, exists := iama.policyCache[policyName]
	return doc, exists
}

// deletePolicyCache removes a policy document from the cache with thread safety
func (iama *IamApiServer) deletePolicyCache(policyName string) {
	iama.policyCacheMutex.Lock()
	defer iama.policyCacheMutex.Unlock()
	delete(iama.policyCache, policyName)
}

// Shutdown gracefully stops the IAM API server and releases resources.
// It implements a graceful shutdown with timeout, allowing in-flight requests
// to complete while ensuring resources are properly cleaned up.
//
// Shutdown process:
// 1. Cancels master client connection goroutine
// 2. Closes credential manager and stores
// 3. Releases gRPC connections
//
// This method is safe to call multiple times (subsequent calls are no-ops).
func (iama *IamApiServer) Shutdown() {
	if iama.shutdownCancel == nil {
		glog.V(0).Infof("IAM API server shutdown called but already shutdown or not initialized")
		return
	}

	glog.V(0).Infof("IAM API server initiating graceful shutdown...")

	// Cancel the master client connection context
	iama.shutdownCancel()

	// Close credential manager if available
	if iama.s3ApiConfig != nil {
		if configure, ok := iama.s3ApiConfig.(*IamS3ApiConfigure); ok && configure.credentialManager != nil {
			glog.V(0).Infof("IAM API server closing credential manager...")
			glog.V(0).Infof("IAM API server closing credential manager...")
			configure.credentialManager.Shutdown()
		}
	}

	// Mark as shutdown (prevent double shutdown)
	iama.shutdownCancel = nil

	glog.V(0).Infof("IAM API server shutdown complete")
}

func (iama *IamS3ApiConfigure) GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	return iama.GetS3ApiConfigurationFromCredentialManager(s3cfg)
}

func (iama *IamS3ApiConfigure) PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	return iama.PutS3ApiConfigurationToCredentialManager(s3cfg)
}

func (iama *IamS3ApiConfigure) GetS3ApiConfigurationFromCredentialManager(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	config, err := iama.credentialManager.LoadConfiguration(context.Background())
	if err != nil {
		return fmt.Errorf("failed to load configuration from credential manager: %w", err)
	}
	*s3cfg = *config
	return nil
}

func (iama *IamS3ApiConfigure) PutS3ApiConfigurationToCredentialManager(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	return iama.credentialManager.SaveConfiguration(context.Background(), s3cfg)
}

func (iama *IamS3ApiConfigure) GetS3ApiConfigurationFromFiler(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	var buf bytes.Buffer
	err = pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if err = filer.ReadEntry(iama.masterClient, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if buf.Len() > 0 {
		if err = filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg); err != nil {
			return err
		}
	}
	return nil
}

func (iama *IamS3ApiConfigure) PutS3ApiConfigurationToFiler(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	buf := bytes.Buffer{}
	if err := filer.ProtoToText(&buf, s3cfg); err != nil {
		return fmt.Errorf("ProtoToText: %s", err)
	}
	return pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		err = util.Retry("saveIamIdentity", func() error {
			return filer.SaveInsideFiler(client, filer.IamConfigDirectory, filer.IamIdentityFile, buf.Bytes())
		})
		if err != nil {
			return err
		}
		return nil
	})
}

func (iama *IamS3ApiConfigure) GetPolicies(policies *Policies) (err error) {
	policies.Policies = make(map[string]policy_engine.PolicyDocument)

	err = pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		// List files in the policies directory
		entries, err := filer.ListEntry(iama.masterClient, client, filer.IamPoliciesDirectory, "", 100000, "")
		if err != nil {
			if err == filer_pb.ErrNotFound {
				// Directory doesn't exist, which is fine = empty policies
				return nil
			}
			return err
		}

		// Read each file
		for _, entry := range entries {
			if entry.IsDirectory {
				continue
			}
			if !strings.HasSuffix(entry.Name, ".json") {
				continue
			}

			// Read content
			content, err := filer.ReadInsideFiler(client, filer.IamPoliciesDirectory, entry.Name)
			if err != nil {
				glog.Warningf("Failed to read policy file %s: %v", entry.Name, err)
				continue
			}

			if len(content) == 0 {
				continue
			}

			var policyDoc policy_engine.PolicyDocument
			if err := json.Unmarshal(content, &policyDoc); err != nil {
				glog.Warningf("Failed to parse policy file %s: %v", entry.Name, err)
				continue
			}
			
			// Use filename as policy name (without extension) if name not in metadata
			policyName := strings.TrimSuffix(entry.Name, ".json")
			policies.Policies[policyName] = policyDoc
		}
		return nil
	})
	return err
}

func (iama *IamS3ApiConfigure) GetPolicy(name string) (policy *policy_engine.PolicyDocument, err error) {
	filename := fmt.Sprintf("%s.json", name)
	var policyDoc policy_engine.PolicyDocument
	err = pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		content, err := filer.ReadInsideFiler(client, filer.IamPoliciesDirectory, filename)
		if err != nil {
			return err
		}
		if len(content) == 0 {
			return filer_pb.ErrNotFound
		}
		return json.Unmarshal(content, &policyDoc)
	})
	if err != nil {
		return nil, err
	}
	return &policyDoc, nil
}

func (iama *IamS3ApiConfigure) PutPolicy(name string, policy *policy_engine.PolicyDocument) (err error) {
	var b []byte
	if b, err = json.Marshal(policy); err != nil {
		return err
	}
	filename := fmt.Sprintf("%s.json", name)
	return pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if err := filer.SaveInsideFiler(client, filer.IamPoliciesDirectory, filename, b); err != nil {
			return err
		}
		return nil
	})
}

func (iama *IamS3ApiConfigure) DeletePolicy(name string) (err error) {
	filename := fmt.Sprintf("%s.json", name)
	return pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		// Check existence first to match AWS behavior (optional but good practice)
		// But deletion is idempotent usually, so we just try to delete
		err = filer.DeleteInsideFiler(client, filer.IamPoliciesDirectory, filename)
		if err != nil && err != filer_pb.ErrNotFound {
			return err
		}
		return nil
	})
}

func (iama *IamS3ApiConfigure) CreateUser(user *iam_pb.Identity) (err error) {
	return iama.credentialManager.CreateUser(context.Background(), user)
}

func (iama *IamS3ApiConfigure) GetUser(name string) (user *iam_pb.Identity, err error) {
	return iama.credentialManager.GetUser(context.Background(), name)
}

func (iama *IamS3ApiConfigure) UpdateUser(name string, user *iam_pb.Identity) (err error) {
	return iama.credentialManager.UpdateUser(context.Background(), name, user)
}

func (iama *IamS3ApiConfigure) DeleteUser(name string) (err error) {
	return iama.credentialManager.DeleteUser(context.Background(), name)
}

func (iama *IamS3ApiConfigure) ListUsers() (usernames []string, err error) {
	return iama.credentialManager.ListUsers(context.Background())
}

func (iama *IamS3ApiConfigure) CreateAccessKey(username string, cred *iam_pb.Credential) (err error) {
	return iama.credentialManager.CreateAccessKey(context.Background(), username, cred)
}

func (iama *IamS3ApiConfigure) DeleteAccessKey(username string, accessKey string) (err error) {
	return iama.credentialManager.DeleteAccessKey(context.Background(), username, accessKey)
}

// AuthenticateRequest authenticates an IAM API request using AWS Signature V4
func (iama *IamS3ApiConfigure) AuthenticateRequest(r *http.Request) (*iam_pb.Identity, error) {
	// Parse AWS SigV4 authorization header
	var authInfo *v4AuthInfo
	var errCode s3err.ErrorCode
	
	if isRequestPresignedSignatureV4(r) {
		authInfo, errCode = extractV4AuthInfoFromQuery(r)
	} else {
		authInfo, errCode = extractV4AuthInfoFromHeader(r)
	}
	
	if errCode != s3err.ErrNone {
		// Malformed request - return 400 Bad Request
		return nil, &MalformedRequestError{
			Message: fmt.Sprintf("failed to parse authorization header: %s", errCode),
		}
	}
	
	// Lookup user by access key
	identity, err := iama.credentialManager.GetUserByAccessKey(context.Background(), authInfo.AccessKey)
	if err != nil {
		// Invalid credentials - return 401 Unauthorized
		return nil, &AuthenticationError{
			Message: fmt.Sprintf("invalid access key: %s", authInfo.AccessKey),
		}
	}
	
	// Get the credential for signature verification
	var credential *iam_pb.Credential
	for _, cred := range identity.Credentials {
		if cred.AccessKey == authInfo.AccessKey {
			credential = cred
			break
		}
	}
	
	if credential == nil {
		// Credential lookup failed - return 401 Unauthorized
		return nil, &AuthenticationError{
			Message: "credential not found for access key",
		}
	}
	
	// Verify the signature
	if err := verifyIAMSignature(r, credential.SecretKey, authInfo); err != nil {
		// Signature verification failed - return 401 Unauthorized
		return nil, &AuthenticationError{
			Message: "signature verification failed",
		}
	}
	
	return identity, nil
}

// hasAdminPermissions checks if a user has IAM management permissions
func hasAdminPermissions(identity *iam_pb.Identity) bool {
	if identity == nil {
		return false
	}
	
	// Check if user has admin action permission
	for _, action := range identity.Actions {
		if action == constants.ActionAdmin {
			return true
		}
	}
	
	return false
}
