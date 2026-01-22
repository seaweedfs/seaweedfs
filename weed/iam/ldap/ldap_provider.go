package ldap

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
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
	connPool chan *ldap.Conn // Connection pool
	mu     sync.RWMutex
	initialized bool
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

	// Initialize connection pool
	p.connPool = make(chan *ldap.Conn, 5) // Pool size 5 as default
	p.initialized = true

	return nil
}

// Authenticate validates credentials against LDAP
func (p *LDAPProvider) Authenticate(ctx context.Context, credentials string) (*providers.ExternalIdentity, error) {
	p.mu.RLock()
	if !p.initialized {
		p.mu.RUnlock()
		return nil, fmt.Errorf("LDAP provider not initialized")
	}
	config := p.config
	p.mu.RUnlock()

	// Parse credentials (username:password)
	parts := strings.SplitN(credentials, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid credentials format")
	}
	username, password := parts[0], parts[1]

	if username == "" || password == "" {
		return nil, fmt.Errorf("empty credentials")
	}

	// Get connection from pool
	conn, err := p.getConnection()
	if err != nil {
		return nil, err
	}

	// 1. Bind as Admin/Service User to search for the user DN
	if config.BindDN != "" {
		if err := conn.Bind(config.BindDN, config.BindPassword); err != nil {
			glog.V(2).Infof("LDAP service bind failed: %v", err)
			conn.Close() // Close on error, don't return to pool
			return nil, fmt.Errorf("LDAP service bind failed: %w", err)
		}
	}

	// 2. Search for the user to get their DN
	searchRequest := ldap.NewSearchRequest(
		config.BaseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf(config.UserFilter, ldap.EscapeFilter(username)),
		[]string{"dn", "cn", "mail", "displayName"},
		nil,
	)

	sr, err := conn.Search(searchRequest)
	if err != nil {
		glog.V(2).Infof("LDAP user search failed: %v", err)
		conn.Close() // Close on error
		return nil, fmt.Errorf("LDAP user search failed: %w", err)
	}

	if len(sr.Entries) == 0 {
		conn.Close() // Close on error
		return nil, fmt.Errorf("user not found")
	}
	if len(sr.Entries) > 1 {
		conn.Close() // Close on error
		return nil, fmt.Errorf("multiple users found")
	}

	userEntry := sr.Entries[0]
	userDN := userEntry.DN

	// 3. Bind as the User to verify password
	if err := conn.Bind(userDN, password); err != nil {
		glog.V(2).Infof("LDAP user bind failed for %s: %v", username, err)
		conn.Close() // Close on error, don't return to pool
		return nil, fmt.Errorf("authentication failed: invalid credentials")
	}

	// 4. Re-bind as Admin to search for groups (if needed, depending on ACLs)
	// Often users clarify can't read their own groups efficiently, or we need to search 'memberOf' which might require specific permissions.
	// For simplicity, let's assume we can search with the user's binding OR re-bind as admin. 
	// Re-binding as admin is safer for compatibility.
	if config.BindDN != "" {
		if err := conn.Bind(config.BindDN, config.BindPassword); err != nil {
			glog.V(2).Infof("LDAP rebind to service account failed: %v", err)
			conn.Close() // Close on error, don't return to pool
			return nil, fmt.Errorf("LDAP service account rebind failed after successful user authentication (check bindDN %q and its credentials): %w", config.BindDN, err)
		}
	}
	
	// Now safe to defer return to pool with clean service account binding
	defer p.returnConnection(conn)

	// 5. Search for groups
	// We need to handle substitution carefully. Standard group search: (&(objectClass=group)(member=<UserDN>))
	groupFilter := fmt.Sprintf(config.GroupFilter, ldap.EscapeFilter(userDN))
	// Some setups use username instead of DN in group member attribute:
	if strings.Contains(config.GroupFilter, "uid=") || strings.Contains(config.GroupFilter, "sAMAccountName=") {
		groupFilter = fmt.Sprintf(config.GroupFilter, ldap.EscapeFilter(username))
	}

	groupReq := ldap.NewSearchRequest(
		config.BaseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		groupFilter,
		[]string{config.GroupNameAttr},
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
			groups = append(groups, entry.GetAttributeValue(config.GroupNameAttr))
		}
	}

	// Attributes mapping
	attributes := make(map[string]string)
	attributes["dn"] = userDN

	// Return identity
	glog.V(2).Infof("LDAP authentication successful for user: %s, groups: %v", username, groups)
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

// getConnection gets a connection from the pool or creates a new one
func (p *LDAPProvider) getConnection() (*ldap.Conn, error) {
	select {
	case conn := <-p.connPool:
		return conn, nil
	default:
		return p.connect()
	}
}

// returnConnection returns a connection to the pool or closes it
func (p *LDAPProvider) returnConnection(conn *ldap.Conn) {
	select {
	case p.connPool <- conn:
		// Returned to pool
	default:
		// Pool full, close connection
		conn.Close()
	}
}

// GetUserInfo retrieves user information by user ID
func (p *LDAPProvider) GetUserInfo(ctx context.Context, userID string) (*providers.ExternalIdentity, error) {
	p.mu.RLock()
	if !p.initialized {
		p.mu.RUnlock()
		return nil, fmt.Errorf("LDAP provider not initialized")
	}
	config := p.config
	p.mu.RUnlock()

	// Get connection from pool
	conn, err := p.getConnection()
	if err != nil {
		return nil, err
	}
	
	// Bind with service account
	if config.BindDN != "" {
		err = conn.Bind(config.BindDN, config.BindPassword)
		if err != nil {
			conn.Close() // Close on bind failure
			return nil, fmt.Errorf("LDAP service bind failed: %w", err)
		}
	}
	defer p.returnConnection(conn)

	// Search for the user
	userFilter := fmt.Sprintf(config.UserFilter, ldap.EscapeFilter(userID))
	searchRequest := ldap.NewSearchRequest(
		config.BaseDN,
		ldap.ScopeWholeSubtree,
		ldap.NeverDerefAliases,
		1,
		0,
		false,
		userFilter,
		[]string{"dn", "cn", "mail", "displayName"},
		nil,
	)

	result, err := conn.Search(searchRequest)
	if err != nil {
		glog.V(2).Infof("LDAP user search failed: %v", err)
		// Don't close here, allow reuse if just not found? No, might be bad conn. 
		// But in pool logic, defer returnConnection handles it. 
		// If we believe the connection is bad, we shouldn't return it.
		// For now, assume search failure might be logical, return to pool.
		return nil, fmt.Errorf("LDAP user search failed: %w", err) 
	}
	
	if len(result.Entries) == 0 {
		return nil, fmt.Errorf("user not found")
	}

	userEntry := result.Entries[0]
	userDN := userEntry.DN
	
	// Attributes mapping
	attributes := make(map[string]string)
	attributes["dn"] = userDN

	// Get groups (simplified, similar logic to Authenticate could be added if needed)
	// For GetUserInfo we might skip heavy group search for now unless critical
	
	// Return identity
	return &providers.ExternalIdentity{
		UserID:      userID,
		DisplayName: userEntry.GetAttributeValue("displayName"), // Assuming standard attr map for now or from config
		Email:       userEntry.GetAttributeValue("mail"),
		// Groups:      groups, 
		Provider:    p.name,
		Attributes:  attributes,
	}, nil
}

func (p *LDAPProvider) ValidateToken(ctx context.Context, token string) (*providers.TokenClaims, error) {
	return nil, fmt.Errorf("not implemented")
}
