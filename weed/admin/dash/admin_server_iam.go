package dash

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/ldap"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
)

// RoleMappingRule defines a rule to map external groups to IAM roles
type RoleMappingRule struct {
	Value string `json:"value"` // Group name (e.g. "developers")
	Role  string `json:"role"`  // IAM Role name (e.g. "ReadOnly")
}

// RoleMappingConfig holds role mapping rules
type RoleMappingConfig struct {
	Rules []RoleMappingRule `json:"rules"`
}

type ProvidersConfig struct {
	STS struct {
		Providers []struct {
			Name        string                 `json:"name"`
			Type        string                 `json:"type"`
			Config      map[string]interface{} `json:"config"`
			RoleMapping RoleMappingConfig      `json:"roleMapping"`
		} `json:"providers"`
	} `json:"sts"`
}

// cachedRoleMappings stores loaded mappings
var cachedRoleMappings map[string]string

// initIAMManager initializes the IAM manager
func (s *AdminServer) initIAMManager() {
	s.iamManager = integration.NewIAMManager()
	cachedRoleMappings = make(map[string]string)

	// Initial configuration
	iamConfig := &integration.IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: 24 * time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 24 * time.Hour},
			Issuer:           "seaweedfs-admin",
			SigningKey:       []byte(randomString(32)), // Generate ephemeral key if not persistent
		},
		Policy: &policy.PolicyEngineConfig{
			DefaultEffect: "Deny", // Secure default
		},
		Roles: &integration.RoleStoreConfig{
			StoreType: "cached-filer",
		},
	}

// Try to load custom configuration from Filer
	var configData []byte
	var filerAddress string

	// Wait for Filer discovery (up to 30 seconds)
	for i := 0; i < 30; i++ {
		filerAddress = s.GetFilerAddress()
		if filerAddress != "" {
			break
		}
		if i%5 == 0 {
			glog.V(0).Infof("Waiting for Filer discovery to load IAM config...")
		}
		time.Sleep(1 * time.Second)
	}

	if filerAddress != "" {
		url := fmt.Sprintf("http://%s/etc/iam/iam_config.json", filerAddress)
		glog.V(0).Infof("Loading IAM configuration from %s", url)
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			defer resp.Body.Close()
			if data, err := io.ReadAll(resp.Body); err == nil {
				configData = data
				glog.V(0).Infof("Successfully loaded IAM config of size: %d bytes", len(configData))
				if err := json.Unmarshal(data, iamConfig); err != nil {
					glog.Errorf("Failed to parse IAM config into IAMConfig struct: %v", err)
				}
			}
		} else {
			glog.Errorf("Failed to load IAM config from Filer at %s: err=%v, status=%s", url, err, resp.Status)
			if resp != nil {
				resp.Body.Close()
			}
		}
	} else {
		glog.Errorf("Failed to load IAM config: No Filer discovered after 30 seconds")
	}


	// Clear providers from config to prevent auto-loading failure
	// We will manually load and register them below
	if iamConfig.STS != nil {
		iamConfig.STS.Providers = nil
	}

	// Initialize with filer address provider
	err := s.iamManager.Initialize(iamConfig, func() string {
		return s.GetFilerAddress()
	}, nil)
	if err != nil {
		glog.Errorf("Failed to initialize IAM manager: %v", err)
		return
	}

	// Load providers and role mappings
	if len(configData) > 0 {
		var provConfig ProvidersConfig
		if err := json.Unmarshal(configData, &provConfig); err == nil {
			for _, p := range provConfig.STS.Providers {
				// Cache role mappings
				for _, rule := range p.RoleMapping.Rules {
					cachedRoleMappings[rule.Value] = rule.Role
				}

				if p.Type == "ldap" {
					ldapProvider := ldap.NewLDAPProvider(p.Name)
					if err := ldapProvider.Initialize(p.Config); err != nil {
						glog.Errorf("Failed to initialize LDAP provider %s: %v", p.Name, err)
						continue
					}
					if err := s.iamManager.RegisterIdentityProvider(ldapProvider); err != nil {
						glog.Errorf("Failed to register LDAP provider: %v", err)
					} else {
						glog.V(0).Infof("Registered LDAP provider: %s", p.Name)
					}
				}
			}
		}
	}
}

// ResolveRolesFromGroups maps LDAP groups to SeaweedFS IAM Roles
func (s *AdminServer) ResolveRolesFromGroups(groups []string) []string {
	var roles []string
	
	// Check cached mappings
	for _, group := range groups {
		if role, ok := cachedRoleMappings[group]; ok {
			// Avoid duplicates
			found := false
			for _, r := range roles {
				if r == role {
					found = true
					break
				}
			}
			if !found {
				roles = append(roles, role)
			}
		}
	}
	
	return roles
}

func randomString(n int) string {
    bytes := make([]byte, n)
    if _, err := rand.Read(bytes); err != nil {
        return "fallback_random_string_" + time.Now().String()
    }
    return hex.EncodeToString(bytes)
}
