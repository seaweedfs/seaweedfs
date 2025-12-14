package s3api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	. "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	util_http_client "github.com/seaweedfs/seaweedfs/weed/util/http/client"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

type S3ApiServerOption struct {
	Filers                    []pb.ServerAddress
	Masters                   []pb.ServerAddress // For filer discovery
	Port                      int
	Config                    string
	DomainName                string
	AllowedOrigins            []string
	BucketsPath               string
	GrpcDialOption            grpc.DialOption
	AllowDeleteBucketNotEmpty bool
	LocalFilerSocket          string
	DataCenter                string
	FilerGroup                string
	IamConfig                 string // Advanced IAM configuration file path
	ConcurrentUploadLimit     int64
	ConcurrentFileUploadLimit int64
	EnableIam                 bool // Enable embedded IAM API on the same port
}

type S3ApiServer struct {
	s3_pb.UnimplementedSeaweedS3Server
	option                *S3ApiServerOption
	iam                   *IdentityAccessManagement
	iamIntegration        *S3IAMIntegration // Advanced IAM integration for JWT authentication
	cb                    *CircuitBreaker
	randomClientId        int32
	filerGuard            *security.Guard
	filerClient           *wdclient.FilerClient
	client                util_http_client.HTTPClientInterface
	bucketRegistry        *BucketRegistry
	credentialManager     *credential.CredentialManager
	bucketConfigCache     *BucketConfigCache
	policyEngine          *BucketPolicyEngine // Engine for evaluating bucket policies
	inFlightDataSize      int64
	inFlightUploads       int64
	inFlightDataLimitCond *sync.Cond
	embeddedIam           *EmbeddedIamApi // Embedded IAM API server (when enabled)
}

func NewS3ApiServer(router *mux.Router, option *S3ApiServerOption) (s3ApiServer *S3ApiServer, err error) {
	return NewS3ApiServerWithStore(router, option, "")
}

func NewS3ApiServerWithStore(router *mux.Router, option *S3ApiServerOption, explicitStore string) (s3ApiServer *S3ApiServer, err error) {
	if len(option.Filers) == 0 {
		return nil, fmt.Errorf("at least one filer address is required")
	}

	startTsNs := time.Now().UnixNano()

	v := util.GetViper()
	signingKey := v.GetString("jwt.filer_signing.key")
	v.SetDefault("jwt.filer_signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.filer_signing.expires_after_seconds")

	readSigningKey := v.GetString("jwt.filer_signing.read.key")
	v.SetDefault("jwt.filer_signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.filer_signing.read.expires_after_seconds")

	v.SetDefault("cors.allowed_origins.values", "*")

	if len(option.AllowedOrigins) == 0 {
		allowedOrigins := v.GetString("cors.allowed_origins.values")
		domains := strings.Split(allowedOrigins, ",")
		option.AllowedOrigins = domains
	}

	iam := NewIdentityAccessManagementWithStore(option, explicitStore)

	// Initialize bucket policy engine first
	policyEngine := NewBucketPolicyEngine()

	// Initialize FilerClient for volume location caching
	// Uses the battle-tested vidMap with filer-based lookups
	// Supports multiple filer addresses with automatic failover for high availability
	var filerClient *wdclient.FilerClient
	if len(option.Masters) > 0 && option.FilerGroup != "" {
		// Enable filer discovery via master
		masterMap := make(map[string]pb.ServerAddress)
		for i, addr := range option.Masters {
			masterMap[fmt.Sprintf("master%d", i)] = addr
		}
		masterClient := wdclient.NewMasterClient(option.GrpcDialOption, option.FilerGroup, cluster.S3Type, "", "", "", *pb.NewServiceDiscoveryFromMap(masterMap))
		// Start the master client connection loop - required for GetMaster() to work
		go masterClient.KeepConnectedToMaster(context.Background())

		filerClient = wdclient.NewFilerClient(option.Filers, option.GrpcDialOption, option.DataCenter, &wdclient.FilerClientOption{
			MasterClient:      masterClient,
			FilerGroup:        option.FilerGroup,
			DiscoveryInterval: 5 * time.Minute,
		})
		glog.V(0).Infof("S3 API initialized FilerClient with %d filer(s) and discovery enabled (group: %s, masters: %v)",
			len(option.Filers), option.FilerGroup, option.Masters)
	} else {
		filerClient = wdclient.NewFilerClient(option.Filers, option.GrpcDialOption, option.DataCenter)
		glog.V(0).Infof("S3 API initialized FilerClient with %d filer(s) (no discovery)", len(option.Filers))
	}

	// Update credential store to use FilerClient's current filer for HA
	if store := iam.credentialManager.GetStore(); store != nil {
		if filerFuncSetter, ok := store.(interface {
			SetFilerAddressFunc(func() pb.ServerAddress, grpc.DialOption)
		}); ok {
			// Use FilerClient's GetCurrentFiler for true HA
			filerFuncSetter.SetFilerAddressFunc(filerClient.GetCurrentFiler, option.GrpcDialOption)
			glog.V(1).Infof("Updated credential store to use FilerClient's current active filer (HA-aware)")
		}
	}

	s3ApiServer = &S3ApiServer{
		option:                option,
		iam:                   iam,
		randomClientId:        util.RandomInt32(),
		filerGuard:            security.NewGuard([]string{}, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec),
		filerClient:           filerClient,
		cb:                    NewCircuitBreaker(option),
		credentialManager:     iam.credentialManager,
		bucketConfigCache:     NewBucketConfigCache(60 * time.Minute), // Increased TTL since cache is now event-driven
		policyEngine:          policyEngine,                           // Initialize bucket policy engine
		inFlightDataLimitCond: sync.NewCond(new(sync.Mutex)),
	}

	// Set s3a reference in circuit breaker for upload limiting
	s3ApiServer.cb.s3a = s3ApiServer

	// Pass policy engine to IAM for bucket policy evaluation
	// This avoids circular dependency by not passing the entire S3ApiServer
	iam.policyEngine = policyEngine

	// Initialize advanced IAM system if config is provided
	if option.IamConfig != "" {
		glog.V(1).Infof("Loading advanced IAM configuration from: %s", option.IamConfig)

		// Use FilerClient's GetCurrentFiler for HA-aware filer selection
		iamManager, err := loadIAMManagerFromConfig(option.IamConfig, func() string {
			return string(filerClient.GetCurrentFiler())
		})
		if err != nil {
			glog.Errorf("Failed to load IAM configuration: %v", err)
		} else {
			// Create S3 IAM integration with the loaded IAM manager
			// filerAddress not actually used, just for backward compatibility
			s3iam := NewS3IAMIntegration(iamManager, "")

			// Set IAM integration in server
			s3ApiServer.iamIntegration = s3iam

			// Set the integration in the traditional IAM for compatibility
			iam.SetIAMIntegration(s3iam)

			glog.V(1).Infof("Advanced IAM system initialized successfully with HA filer support")
		}
	}

	// Initialize embedded IAM API if enabled
	if option.EnableIam {
		s3ApiServer.embeddedIam = NewEmbeddedIamApi(s3ApiServer.credentialManager, iam)
		glog.V(0).Infof("Embedded IAM API initialized (use -iam=false to disable)")
	}

	if option.Config != "" {
		grace.OnReload(func() {
			if err := s3ApiServer.iam.loadS3ApiConfigurationFromFile(option.Config); err != nil {
				glog.Errorf("fail to load config file %s: %v", option.Config, err)
			} else {
				glog.V(1).Infof("Loaded %d identities from config file %s", len(s3ApiServer.iam.identities), option.Config)
			}
		})
	}
	s3ApiServer.bucketRegistry = NewBucketRegistry(s3ApiServer)
	if option.LocalFilerSocket == "" {
		if s3ApiServer.client, err = util_http.NewGlobalHttpClient(); err != nil {
			return nil, err
		}
	} else {
		s3ApiServer.client = &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", option.LocalFilerSocket)
				},
			},
		}
	}

	s3ApiServer.registerRouter(router)

	// Initialize the global SSE-S3 key manager with filer access
	if err := InitializeGlobalSSES3KeyManager(s3ApiServer); err != nil {
		return nil, fmt.Errorf("failed to initialize SSE-S3 key manager: %w", err)
	}

	go s3ApiServer.subscribeMetaEvents("s3", startTsNs, filer.DirectoryEtcRoot, []string{option.BucketsPath})
	return s3ApiServer, nil
}

// getFilerAddress returns the current active filer address
// Uses FilerClient's tracked current filer which is updated on successful operations
// This provides better availability than always using the first filer
func (s3a *S3ApiServer) getFilerAddress() pb.ServerAddress {
	if s3a.filerClient != nil {
		return s3a.filerClient.GetCurrentFiler()
	}
	// Fallback to first filer if filerClient not initialized
	if len(s3a.option.Filers) > 0 {
		return s3a.option.Filers[0]
	}
	glog.Warningf("getFilerAddress: no filer addresses available")
	return ""
}

// syncBucketPolicyToEngine syncs a bucket policy to the policy engine
// This helper method centralizes the logic for loading bucket policies into the engine
// to avoid duplication and ensure consistent error handling
func (s3a *S3ApiServer) syncBucketPolicyToEngine(bucket string, policyDoc *policy.PolicyDocument) {
	if s3a.policyEngine == nil {
		return
	}

	if policyDoc != nil {
		if err := s3a.policyEngine.LoadBucketPolicyFromCache(bucket, policyDoc); err != nil {
			glog.Errorf("Failed to sync bucket policy for %s to policy engine: %v", bucket, err)
		}
	} else {
		// No policy - ensure it's removed from engine if it was there
		s3a.policyEngine.DeleteBucketPolicy(bucket)
	}
}

// checkPolicyWithEntry re-evaluates bucket policy with the object entry metadata.
// This is used by handlers after fetching the entry to enforce tag-based conditions
// like s3:ExistingObjectTag/<key>.
//
// Returns:
//   - s3err.ErrCode: ErrNone if allowed, ErrAccessDenied if denied
//   - bool: true if policy was evaluated (has policy for bucket), false if no policy
func (s3a *S3ApiServer) checkPolicyWithEntry(r *http.Request, bucket, object, action, principal string, objectEntry map[string][]byte) (s3err.ErrorCode, bool) {
	if s3a.policyEngine == nil {
		return s3err.ErrNone, false
	}

	// Skip if no policy for this bucket
	if !s3a.policyEngine.HasPolicyForBucket(bucket) {
		return s3err.ErrNone, false
	}

	allowed, evaluated, err := s3a.policyEngine.EvaluatePolicy(bucket, object, action, principal, r, objectEntry)
	if err != nil {
		glog.Errorf("checkPolicyWithEntry: error evaluating policy for %s/%s: %v", bucket, object, err)
		return s3err.ErrInternalError, true
	}

	if !evaluated {
		return s3err.ErrNone, false
	}

	if !allowed {
		glog.V(3).Infof("checkPolicyWithEntry: policy denied access to %s/%s for principal %s", bucket, object, principal)
		return s3err.ErrAccessDenied, true
	}

	return s3err.ErrNone, true
}

// recheckPolicyWithObjectEntry performs the second phase of policy evaluation after
// an object's entry is fetched. It extracts identity from context and checks for
// tag-based conditions like s3:ExistingObjectTag/<key>.
//
// Returns s3err.ErrNone if allowed, or an error code if denied or on error.
func (s3a *S3ApiServer) recheckPolicyWithObjectEntry(r *http.Request, bucket, object, action string, objectEntry map[string][]byte, handlerName string) s3err.ErrorCode {
	identityRaw := GetIdentityFromContext(r)
	var identity *Identity
	if identityRaw != nil {
		var ok bool
		identity, ok = identityRaw.(*Identity)
		if !ok {
			glog.Errorf("%s: unexpected identity type in context for %s/%s", handlerName, bucket, object)
			return s3err.ErrInternalError
		}
	}
	principal := buildPrincipalARN(identity)
	errCode, _ := s3a.checkPolicyWithEntry(r, bucket, object, action, principal, objectEntry)
	return errCode
}

// classifyDomainNames classifies domains into path-style and virtual-host style domains.
// A domain is considered path-style if:
//  1. It contains a dot (has subdomains)
//  2. Its parent domain is also in the list of configured domains
//
// For example, if domains are ["s3.example.com", "develop.s3.example.com"],
// then "develop.s3.example.com" is path-style (parent "s3.example.com" is in the list),
// while "s3.example.com" is virtual-host style.
func classifyDomainNames(domainNames []string) (pathStyleDomains, virtualHostDomains []string) {
	for _, domainName := range domainNames {
		parts := strings.SplitN(domainName, ".", 2)
		if len(parts) == 2 && slices.Contains(domainNames, parts[1]) {
			// This is a subdomain and its parent is also in the list
			// Register as path-style: domain.com/bucket/object
			pathStyleDomains = append(pathStyleDomains, domainName)
		} else {
			// This is a top-level domain or its parent is not in the list
			// Register as virtual-host style: bucket.domain.com/object
			virtualHostDomains = append(virtualHostDomains, domainName)
		}
	}
	return pathStyleDomains, virtualHostDomains
}

// handleCORSOriginValidation handles the common CORS origin validation logic
func (s3a *S3ApiServer) handleCORSOriginValidation(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin != "" {
		if len(s3a.option.AllowedOrigins) == 0 || s3a.option.AllowedOrigins[0] == "*" {
			origin = "*"
		} else {
			originFound := false
			for _, allowedOrigin := range s3a.option.AllowedOrigins {
				if origin == allowedOrigin {
					originFound = true
					break
				}
			}
			if !originFound {
				writeFailureResponse(w, r, http.StatusForbidden)
				return false
			}
		}
	}

	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Expose-Headers", "*")
	w.Header().Set("Access-Control-Allow-Methods", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	return true
}

func (s3a *S3ApiServer) registerRouter(router *mux.Router) {
	// API Router
	apiRouter := router.PathPrefix("/").Subrouter()

	// Readiness Probe
	apiRouter.Methods(http.MethodGet).Path("/status").HandlerFunc(s3a.StatusHandler)
	apiRouter.Methods(http.MethodGet).Path("/healthz").HandlerFunc(s3a.StatusHandler)

	var routers []*mux.Router
	if s3a.option.DomainName != "" {
		domainNames := strings.Split(s3a.option.DomainName, ",")
		pathStyleDomains, virtualHostDomains := classifyDomainNames(domainNames)

		// Register path-style domains
		for _, domain := range pathStyleDomains {
			routers = append(routers, apiRouter.Host(domain).PathPrefix("/{bucket}").Subrouter())
		}

		// Register virtual-host style domains
		for _, virtualHost := range virtualHostDomains {
			routers = append(routers, apiRouter.Host(
				fmt.Sprintf("%s.%s", "{bucket:.+}", virtualHost)).Subrouter())
		}
	}
	routers = append(routers, apiRouter.PathPrefix("/{bucket}").Subrouter())

	// Get CORS middleware instance with caching
	corsMiddleware := s3a.getCORSMiddleware()

	for _, bucket := range routers {
		// Apply CORS middleware to bucket routers for automatic CORS header handling
		bucket.Use(corsMiddleware.Handler)

		// Bucket-specific OPTIONS handler for CORS preflight requests
		// Use PathPrefix to catch all bucket-level preflight routes including /bucket/object
		bucket.PathPrefix("/").Methods(http.MethodOptions).HandlerFunc(corsMiddleware.HandleOptionsRequest)

		// each case should follow the next rule:
		// - requesting object with query must precede any other methods
		// - requesting object must precede any methods with buckets
		// - requesting bucket with query must precede raw methods with buckets
		// - requesting bucket must be processed in the end

		// objects with query

		// CopyObjectPart
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", `.*?(\/|%2F).*?`).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.CopyObjectPartHandler, ACTION_WRITE)), "PUT")).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// PutObjectPart
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutObjectPartHandler, ACTION_WRITE)), "PUT")).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// CompleteMultipartUpload
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.CompleteMultipartUploadHandler, ACTION_WRITE)), "POST")).Queries("uploadId", "{uploadId:.*}")
		// NewMultipartUpload
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.NewMultipartUploadHandler, ACTION_WRITE)), "POST")).Queries("uploads", "")
		// AbortMultipartUpload
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.AbortMultipartUploadHandler, ACTION_WRITE)), "DELETE")).Queries("uploadId", "{uploadId:.*}")
		// ListObjectParts
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.ListObjectPartsHandler, ACTION_READ)), "GET")).Queries("uploadId", "{uploadId:.*}")
		// ListMultipartUploads
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.ListMultipartUploadsHandler, ACTION_READ)), "GET")).Queries("uploads", "")

		// GetObjectTagging
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetObjectTaggingHandler, ACTION_READ)), "GET")).Queries("tagging", "")
		// PutObjectTagging
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutObjectTaggingHandler, ACTION_TAGGING)), "PUT")).Queries("tagging", "")
		// DeleteObjectTagging
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.DeleteObjectTaggingHandler, ACTION_TAGGING)), "DELETE")).Queries("tagging", "")

		// PutObjectACL
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutObjectAclHandler, ACTION_WRITE_ACP)), "PUT")).Queries("acl", "")
		// PutObjectRetention
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutObjectRetentionHandler, ACTION_WRITE)), "PUT")).Queries("retention", "")
		// PutObjectLegalHold
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutObjectLegalHoldHandler, ACTION_WRITE)), "PUT")).Queries("legal-hold", "")

		// GetObjectACL
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetObjectAclHandler, ACTION_READ_ACP)), "GET")).Queries("acl", "")
		// GetObjectRetention
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetObjectRetentionHandler, ACTION_READ)), "GET")).Queries("retention", "")
		// GetObjectLegalHold
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetObjectLegalHoldHandler, ACTION_READ)), "GET")).Queries("legal-hold", "")

		// objects with query

		// raw objects

		// HeadObject
		bucket.Methods(http.MethodHead).Path("/{object:.+}").HandlerFunc(track(s3a.AuthWithPublicRead(func(w http.ResponseWriter, r *http.Request) {
			limitedHandler, _ := s3a.cb.Limit(s3a.HeadObjectHandler, ACTION_READ)
			limitedHandler(w, r)
		}, ACTION_READ), "GET"))

		// GetObject, but directory listing is not supported
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(track(s3a.AuthWithPublicRead(func(w http.ResponseWriter, r *http.Request) {
			limitedHandler, _ := s3a.cb.Limit(s3a.GetObjectHandler, ACTION_READ)
			limitedHandler(w, r)
		}, ACTION_READ), "GET"))

		// CopyObject
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.CopyObjectHandler, ACTION_WRITE)), "COPY"))
		// PutObject
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutObjectHandler, ACTION_WRITE)), "PUT"))
		// DeleteObject
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.DeleteObjectHandler, ACTION_WRITE)), "DELETE"))

		// raw objects

		// buckets with query

		// DeleteMultipleObjects
		bucket.Methods(http.MethodPost).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.DeleteMultipleObjectsHandler, ACTION_WRITE)), "DELETE")).Queries("delete", "")

		// GetBucketACL
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetBucketAclHandler, ACTION_READ_ACP)), "GET")).Queries("acl", "")
		// PutBucketACL
		bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutBucketAclHandler, ACTION_WRITE_ACP)), "PUT")).Queries("acl", "")

		// GetBucketPolicy
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetBucketPolicyHandler, ACTION_READ)), "GET")).Queries("policy", "")
		// PutBucketPolicy
		bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutBucketPolicyHandler, ACTION_WRITE)), "PUT")).Queries("policy", "")
		// DeleteBucketPolicy
		bucket.Methods(http.MethodDelete).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.DeleteBucketPolicyHandler, ACTION_WRITE)), "DELETE")).Queries("policy", "")

		// GetBucketCors
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetBucketCorsHandler, ACTION_READ)), "GET")).Queries("cors", "")
		// PutBucketCors
		bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutBucketCorsHandler, ACTION_WRITE)), "PUT")).Queries("cors", "")
		// DeleteBucketCors
		bucket.Methods(http.MethodDelete).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.DeleteBucketCorsHandler, ACTION_WRITE)), "DELETE")).Queries("cors", "")

		// GetBucketLifecycleConfiguration
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetBucketLifecycleConfigurationHandler, ACTION_READ)), "GET")).Queries("lifecycle", "")
		// PutBucketLifecycleConfiguration
		bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutBucketLifecycleConfigurationHandler, ACTION_WRITE)), "PUT")).Queries("lifecycle", "")
		// DeleteBucketLifecycleConfiguration
		bucket.Methods(http.MethodDelete).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.DeleteBucketLifecycleHandler, ACTION_WRITE)), "DELETE")).Queries("lifecycle", "")

		// GetBucketLocation
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetBucketLocationHandler, ACTION_READ)), "GET")).Queries("location", "")

		// GetBucketRequestPayment
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetBucketRequestPaymentHandler, ACTION_READ)), "GET")).Queries("requestPayment", "")

		// GetBucketVersioning
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetBucketVersioningHandler, ACTION_READ)), "GET")).Queries("versioning", "")
		bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutBucketVersioningHandler, ACTION_WRITE)), "PUT")).Queries("versioning", "")

		// GetObjectLockConfiguration / PutObjectLockConfiguration (bucket-level operations)
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetObjectLockConfigurationHandler, ACTION_READ)), "GET")).Queries("object-lock", "")
		bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutObjectLockConfigurationHandler, ACTION_WRITE)), "PUT")).Queries("object-lock", "")

		// GetBucketTagging
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetBucketTaggingHandler, ACTION_TAGGING)), "GET")).Queries("tagging", "")
		bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutBucketTaggingHandler, ACTION_TAGGING)), "PUT")).Queries("tagging", "")
		bucket.Methods(http.MethodDelete).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.DeleteBucketTaggingHandler, ACTION_TAGGING)), "DELETE")).Queries("tagging", "")

		// GetBucketEncryption
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetBucketEncryptionHandler, ACTION_ADMIN)), "GET")).Queries("encryption", "")
		bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutBucketEncryptionHandler, ACTION_ADMIN)), "PUT")).Queries("encryption", "")
		bucket.Methods(http.MethodDelete).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.DeleteBucketEncryptionHandler, ACTION_ADMIN)), "DELETE")).Queries("encryption", "")

		// GetPublicAccessBlockHandler
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetPublicAccessBlockHandler, ACTION_ADMIN)), "GET")).Queries("publicAccessBlock", "")
		bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutPublicAccessBlockHandler, ACTION_ADMIN)), "PUT")).Queries("publicAccessBlock", "")
		bucket.Methods(http.MethodDelete).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.DeletePublicAccessBlockHandler, ACTION_ADMIN)), "DELETE")).Queries("publicAccessBlock", "")

		// ListObjectsV2
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.AuthWithPublicRead(func(w http.ResponseWriter, r *http.Request) {
			limitedHandler, _ := s3a.cb.Limit(s3a.ListObjectsV2Handler, ACTION_LIST)
			limitedHandler(w, r)
		}, ACTION_LIST), "LIST")).Queries("list-type", "2")

		// ListObjectVersions
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.ListObjectVersionsHandler, ACTION_LIST)), "LIST")).Queries("versions", "")

		// buckets with query
		// PutBucketOwnershipControls
		bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.PutBucketOwnershipControls, ACTION_ADMIN), "PUT")).Queries("ownershipControls", "")

		//GetBucketOwnershipControls
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.GetBucketOwnershipControls, ACTION_READ), "GET")).Queries("ownershipControls", "")

		//DeleteBucketOwnershipControls
		bucket.Methods(http.MethodDelete).HandlerFunc(track(s3a.iam.Auth(s3a.DeleteBucketOwnershipControls, ACTION_ADMIN), "DELETE")).Queries("ownershipControls", "")

		// raw buckets

		// PostPolicy
		bucket.Methods(http.MethodPost).HeadersRegexp("Content-Type", "multipart/form-data*").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PostPolicyBucketHandler, ACTION_WRITE)), "POST"))

		// HeadBucket
		bucket.Methods(http.MethodHead).HandlerFunc(track(s3a.AuthWithPublicRead(func(w http.ResponseWriter, r *http.Request) {
			limitedHandler, _ := s3a.cb.Limit(s3a.HeadBucketHandler, ACTION_READ)
			limitedHandler(w, r)
		}, ACTION_READ), "GET"))

		// PutBucket
		bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutBucketHandler, ACTION_ADMIN)), "PUT"))

		// DeleteBucket
		bucket.Methods(http.MethodDelete).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.DeleteBucketHandler, ACTION_DELETE_BUCKET)), "DELETE"))

		// ListObjectsV1 (Legacy)
		bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.AuthWithPublicRead(func(w http.ResponseWriter, r *http.Request) {
			limitedHandler, _ := s3a.cb.Limit(s3a.ListObjectsV1Handler, ACTION_LIST)
			limitedHandler(w, r)
		}, ACTION_LIST), "LIST"))

		// raw buckets

	}

	// Global OPTIONS handler for service-level requests (non-bucket requests)
	// This handles requests like OPTIONS /, OPTIONS /status, OPTIONS /healthz
	// Place this after bucket handlers to avoid interfering with bucket CORS middleware
	apiRouter.Methods(http.MethodOptions).PathPrefix("/").HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			// Only handle if this is not a bucket-specific request
			vars := mux.Vars(r)
			bucket := vars["bucket"]
			if bucket != "" {
				// This is a bucket-specific request, let bucket CORS middleware handle it
				http.NotFound(w, r)
				return
			}

			if s3a.handleCORSOriginValidation(w, r) {
				writeSuccessResponseEmpty(w, r)
			}
		})

	// Embedded IAM API (POST to "/" with Action parameter)
	// This must be before ListBuckets since IAM uses POST and ListBuckets uses GET
	if s3a.embeddedIam != nil {
		apiRouter.Methods(http.MethodPost).Path("/").HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.embeddedIam.DoActions, ACTION_ADMIN)), "IAM"))
		glog.V(0).Infof("Embedded IAM API enabled on S3 port")
	}

	// ListBuckets
	apiRouter.Methods(http.MethodGet).Path("/").HandlerFunc(track(s3a.iam.Auth(s3a.ListBucketsHandler, ACTION_LIST), "LIST"))

	// NotFound
	apiRouter.NotFoundHandler = http.HandlerFunc(s3err.NotFoundHandler)

}

// loadIAMManagerFromConfig loads the advanced IAM manager from configuration file
func loadIAMManagerFromConfig(configPath string, filerAddressProvider func() string) (*integration.IAMManager, error) {
	// Read configuration file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse configuration structure
	var configRoot struct {
		STS       *sts.STSConfig                `json:"sts"`
		Policy    *policy.PolicyEngineConfig    `json:"policy"`
		Providers []map[string]interface{}      `json:"providers"`
		Roles     []*integration.RoleDefinition `json:"roles"`
		Policies  []struct {
			Name     string                 `json:"name"`
			Document *policy.PolicyDocument `json:"document"`
		} `json:"policies"`
	}

	if err := json.Unmarshal(configData, &configRoot); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Ensure a valid policy engine config exists
	if configRoot.Policy == nil {
		// Provide a secure default if not specified in the config file
		// Default to Deny with in-memory store so that JSON-defined policies work without filer
		glog.V(1).Infof("No policy engine config provided; using defaults (DefaultEffect=%s, StoreType=%s)", sts.EffectDeny, sts.StoreTypeMemory)
		configRoot.Policy = &policy.PolicyEngineConfig{
			DefaultEffect: sts.EffectDeny,
			StoreType:     sts.StoreTypeMemory,
		}
	}

	// Create IAM configuration
	iamConfig := &integration.IAMConfig{
		STS:    configRoot.STS,
		Policy: configRoot.Policy,
		Roles: &integration.RoleStoreConfig{
			StoreType: sts.StoreTypeMemory, // Use memory store for JSON config-based setup
		},
	}

	// Initialize IAM manager
	iamManager := integration.NewIAMManager()
	if err := iamManager.Initialize(iamConfig, filerAddressProvider); err != nil {
		return nil, fmt.Errorf("failed to initialize IAM manager: %w", err)
	}

	// Load identity providers
	providerFactory := sts.NewProviderFactory()
	for _, providerConfig := range configRoot.Providers {
		provider, err := providerFactory.CreateProvider(&sts.ProviderConfig{
			Name:    providerConfig["name"].(string),
			Type:    providerConfig["type"].(string),
			Enabled: true,
			Config:  providerConfig["config"].(map[string]interface{}),
		})
		if err != nil {
			glog.Warningf("Failed to create provider %s: %v", providerConfig["name"], err)
			continue
		}
		if provider != nil {
			if err := iamManager.RegisterIdentityProvider(provider); err != nil {
				glog.Warningf("Failed to register provider %s: %v", providerConfig["name"], err)
			} else {
				glog.V(1).Infof("Registered identity provider: %s", providerConfig["name"])
			}
		}
	}

	// Load policies
	for _, policyDef := range configRoot.Policies {
		if err := iamManager.CreatePolicy(context.Background(), "", policyDef.Name, policyDef.Document); err != nil {
			glog.Warningf("Failed to create policy %s: %v", policyDef.Name, err)
		}
	}

	// Load roles
	for _, roleDef := range configRoot.Roles {
		if err := iamManager.CreateRole(context.Background(), "", roleDef.RoleName, roleDef); err != nil {
			glog.Warningf("Failed to create role %s: %v", roleDef.RoleName, err)
		}
	}

	glog.V(1).Infof("Loaded %d providers, %d policies and %d roles from config", len(configRoot.Providers), len(configRoot.Policies), len(configRoot.Roles))

	return iamManager, nil
}
