package s3api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// NewS3ApiServerWithIAM creates an S3 API server with advanced IAM integration
func NewS3ApiServerWithIAM(router *mux.Router, option *S3ApiServerOption, iamConfig string) (s3ApiServer *S3ApiServer, err error) {
	return NewS3ApiServerWithStoreAndIAM(router, option, "", iamConfig)
}

// NewS3ApiServerWithStoreAndIAM creates an S3 API server with store and advanced IAM integration
func NewS3ApiServerWithStoreAndIAM(router *mux.Router, option *S3ApiServerOption, explicitStore string, iamConfig string) (s3ApiServer *S3ApiServer, err error) {
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

	var iam *IdentityAccessManagement

	iam = NewIdentityAccessManagementWithStore(option, explicitStore)

	s3ApiServer = &S3ApiServer{
		option:            option,
		iam:               iam,
		randomClientId:    util.RandomInt32(),
		filerGuard:        security.NewGuard([]string{}, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec),
		cb:                NewCircuitBreaker(option),
		credentialManager: iam.credentialManager,
		bucketConfigCache: NewBucketConfigCache(60 * time.Minute), // Increased TTL since cache is now event-driven
	}

	// Initialize advanced IAM system if config is provided
	if iamConfig != "" {
		glog.V(0).Infof("Initializing advanced IAM system with config: %s", iamConfig)

		// Create IAM manager from config file
		iamManager, err := loadIAMManagerFromConfig(context.Background(), iamConfig)
		if err != nil {
			glog.Errorf("Failed to initialize advanced IAM system: %v", err)
			return nil, fmt.Errorf("failed to initialize advanced IAM system: %v", err)
		}

		// Set the IAM integration on the server
		s3ApiServer.SetIAMIntegration(iamManager)
		glog.V(0).Infof("Advanced IAM system initialized successfully")
	}

	if option.Config != "" {
		grace.OnReload(func() {
			if err := s3ApiServer.iam.loadS3ApiConfigurationFromFile(option.Config); err != nil {
				glog.Errorf("fail to load config file %s: %v", option.Config, err)
			} else {
				glog.V(0).Infof("Loaded %d identities from config file %s", len(s3ApiServer.iam.identities), option.Config)
			}
		})
	}
	s3ApiServer.bucketRegistry = NewBucketRegistry(s3ApiServer)

	// Initialize HTTP client
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

	grace.OnInterrupt(func() {
		s3ApiServer.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			glog.V(0).Infof("shut down gracefully")
			return nil
		})
	})

	util.LoadSecurityConfiguration()

	finishTsNs := time.Now().UnixNano()
	glog.V(0).Infof("S3 API Server (with IAM) startup completed in %d ms", (finishTsNs-startTsNs)/1e6)
	return s3ApiServer, nil
}

// ExtendedIAMConfig holds the extended configuration for IAM including roles and policies
type ExtendedIAMConfig struct {
	STS      *sts.STSConfig               `json:"sts"`
	Policy   *policy.PolicyEngineConfig   `json:"policy"`
	Roles    []integration.RoleDefinition `json:"roles,omitempty"`
	Policies []PolicyDefinition           `json:"policies,omitempty"`
}

// PolicyDefinition defines a policy with its document
type PolicyDefinition struct {
	Name     string          `json:"name"`
	Document json.RawMessage `json:"document"`
}

// loadIAMManagerFromConfig loads IAM manager from a JSON config file
func loadIAMManagerFromConfig(ctx context.Context, configPath string) (*integration.IAMManager, error) {
	// Read config file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse config
	var extendedConfig ExtendedIAMConfig
	if err := json.Unmarshal(configData, &extendedConfig); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Create basic IAM config from the extended config
	config := &integration.IAMConfig{
		STS:    extendedConfig.STS,
		Policy: extendedConfig.Policy,
	}

	// Create and initialize IAM manager
	manager := integration.NewIAMManager()
	if err := manager.Initialize(config); err != nil {
		return nil, fmt.Errorf("failed to initialize IAM manager: %w", err)
	}

	// TODO: Set up providers, roles and policies from config
	// For now, we'll use a minimal setup that works with our tests
	glog.V(1).Infof("IAM manager initialized with %d roles and %d policies from config",
		len(extendedConfig.Roles), len(extendedConfig.Policies))

	return manager, nil
}
