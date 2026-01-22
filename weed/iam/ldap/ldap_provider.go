package ldap

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

// LDAPConfig holds configuration for LDAP provider
type LDAPConfig struct {
	URL            string `json:"url"`
	BaseDN         string `json:"baseDN"`
	BindDN         string `json:"bindDN"`
	BindPassword   string `json:"bindPassword"`
	UserFilter     string `json:"userFilter"`     // e.g. "(sAMAccountName=%s)" or "(uid=%s)"
	GroupFilter    string `json:"groupFilter"`    // e.g. "(&(objectClass=group)(member=%s))"
	GroupNameAttr  string `json:"groupNameAttr"`  // Attribute to use for group name (e.g. "cn")
	StartTLS       bool   `json:"startTLS"`
	SkipVerifyConn bool   `json:"skipVerifyConn"` // Skip TLS verification
}

// LDAPProvider implements IdentityProvider for LDAP
type LDAPProvider struct {
	name   string
	config LDAPConfig
}

// NewLDAPProvider creates a new LDAP provider
func NewLDAPProvider(name string) *LDAPProvider {
	return &LDAPProvider{
		name: name,
	}
}

// Name returns the provider name
func (p *LDAPProvider) Name() string {
	return p.name
}

// Initialize initializes the provider with configuration
func (p *LDAPProvider) Initialize(config interface{}) error {
	// Simple map to struct conversion (would typically use mapstructure)
	configMap, ok := config.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid config format")
	}

	// Helper to safely get string from map
	getString := func(key string) string {
		if val, ok := configMap[key].(string); ok {
			return val
		}
		return ""
	}

	getBool := func(key string) bool {
		if val, ok := configMap[key].(bool); ok {
			return val
		}
		return false
	}

	p.config = LDAPConfig{
		URL:            getString("url"),
		BaseDN:         getString("baseDN"),
		BindDN:         getString("bindDN"),
		BindPassword:   getString("bindPassword"),
		UserFilter:     getString("userFilter"),
		GroupFilter:    getString("groupFilter"),
		GroupNameAttr:  getString("groupNameAttr"),
		StartTLS:       getBool("startTLS"),
		SkipVerifyConn: getBool("skipVerifyConn"),
	}

	// Set defaults
	if p.config.UserFilter == "" {
		p.config.UserFilter = "(uid=%s)"
	}
	if p.config.GroupFilter == "" {
		p.config.GroupFilter = "(&(objectClass=groupOfNames)(member=%s))"
	}
	if p.config.GroupNameAttr == "" {
		p.config.GroupNameAttr = "cn"
	}

	return nil
}

// Authenticate validates credentials against LDAP
func (p *LDAPProvider) Authenticate(ctx context.Context, credentials string) (*providers.ExternalIdentity, error) {
	// Parse credentials (username:password)
	parts := strings.SplitN(credentials, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid credentials format")
	}
	username, password := parts[0], parts[1]

	if username == "" || password == "" {
		return nil, fmt.Errorf("empty credentials")
	}

	// Connect to LDAP with timeout
	conn, err := p.connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// 1. Bind as Admin/Service User to search for the user DN
	if p.config.BindDN != "" {
		if err := conn.Bind(p.config.BindDN, p.config.BindPassword); err != nil {
			return nil, fmt.Errorf("LDAP bind failed: %w", err)
		}
	}

	// 2. Search for the user to get their DN
	searchRequest := ldap.NewSearchRequest(
		p.config.BaseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf(p.config.UserFilter, ldap.EscapeFilter(username)),
		[]string{"dn", "cn", "mail", "displayName"},
		nil,
	)

	sr, err := conn.Search(searchRequest)
	if err != nil {
		return nil, fmt.Errorf("LDAP user search failed: %w", err)
	}

	if len(sr.Entries) != 1 {
		return nil, fmt.Errorf("ldap search filter=%s baseDN=%s returned %d results", 
            fmt.Sprintf(p.config.UserFilter, ldap.EscapeFilter(username)), 
            p.config.BaseDN, 
            len(sr.Entries))
	}

	userEntry := sr.Entries[0]
	userDN := userEntry.DN

	// 3. Bind as the User to verify password
	if err := conn.Bind(userDN, password); err != nil {
		return nil, fmt.Errorf("authentication failed")
	}

	// 4. Re-bind as Admin to search for groups (if needed, depending on ACLs)
	// Often users clarify can't read their own groups efficiently, or we need to search 'memberOf' which might require specific permissions.
	// For simplicity, let's assume we can search with the user's binding OR re-bind as admin. 
	// Re-binding as admin is safer for compatibility.
	if p.config.BindDN != "" {
		if err := conn.Bind(p.config.BindDN, p.config.BindPassword); err != nil {
			// If we can't re-bind, log warning but continue? No, likely system error.
			return nil, fmt.Errorf("LDAP re-bind failed: %w", err)
		}
	}

	// 5. Search for groups
	// We need to handle substitution carefully. Standard group search: (&(objectClass=group)(member=<UserDN>))
	groupFilter := fmt.Sprintf(p.config.GroupFilter, ldap.EscapeFilter(userDN))
	// Some setups use username instead of DN in group member attribute:
	if strings.Contains(p.config.GroupFilter, "uid=") || strings.Contains(p.config.GroupFilter, "sAMAccountName=") {
		groupFilter = fmt.Sprintf(p.config.GroupFilter, ldap.EscapeFilter(username))
	}

	groupReq := ldap.NewSearchRequest(
		p.config.BaseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		groupFilter,
		[]string{p.config.GroupNameAttr},
		nil,
	)

	gr, err := conn.Search(groupReq)
	if err != nil {
		glog.Warningf("LDAP group search failed for user %s: %v", username, err)
		// Don't fail auth, just no groups
	}

	var groups []string
	if gr != nil {
		for _, entry := range gr.Entries {
			groups = append(groups, entry.GetAttributeValue(p.config.GroupNameAttr))
		}
	}

	// Attributes mapping
	attributes := make(map[string]string)
	attributes["dn"] = userDN

	// Return identity
	glog.V(0).Infof("LDAP authentication successful for user: %s, groups: %v", username, groups)
	return &providers.ExternalIdentity{
		UserID:      username,
		DisplayName: userEntry.GetAttributeValue("displayName"),
		Email:       userEntry.GetAttributeValue("mail"),
		Groups:      groups,
		Provider:    p.name,
		Attributes:  attributes,
	}, nil

}

// connect establishes an LDAP connection
func (p *LDAPProvider) connect() (*ldap.Conn, error) {
	// Set 5s timeout for connection
	ldap.DefaultTimeout = 5 * time.Second

	var conn *ldap.Conn
	var err error

	if p.config.StartTLS {
		conn, err = ldap.DialURL(p.config.URL)
		if err != nil {
			return nil, fmt.Errorf("dial failed: %w", err)
		}
		if err = conn.StartTLS(&tls.Config{InsecureSkipVerify: p.config.SkipVerifyConn}); err != nil {
			conn.Close()
			return nil, fmt.Errorf("startTLS failed: %w", err)
		}
	} else if strings.HasPrefix(p.config.URL, "ldaps://") {
		conn, err = ldap.DialURL(p.config.URL, ldap.DialWithTLSConfig(&tls.Config{InsecureSkipVerify: p.config.SkipVerifyConn}))
	} else {
		conn, err = ldap.DialURL(p.config.URL)
	}

	if err != nil {
		return nil, fmt.Errorf("connection failed: %w", err)
	}

	return conn, nil
}

// stub methods for interface satisfaction
func (p *LDAPProvider) GetUserInfo(ctx context.Context, userID string) (*providers.ExternalIdentity, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *LDAPProvider) ValidateToken(ctx context.Context, token string) (*providers.TokenClaims, error) {
	return nil, fmt.Errorf("not implemented")
}
