package ldap

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-ldap/ldap/v3"
	"github.com/mitchellh/mapstructure"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

// LDAPConfig holds configuration for LDAP provider
type LDAPConfig struct {
	// Server is the LDAP server URL (ldap:// or ldaps://)
	Server string `json:"server"`

	// BindDN is the DN used to bind for searches (optional for anonymous bind)
	BindDN string `json:"bindDN,omitempty"`

	// BindPassword is the password for the bind DN
	BindPassword string `json:"bindPassword,omitempty"`

	// BaseDN is the base DN for user searches
	BaseDN string `json:"baseDN"`

	// UserFilter is the filter to find users (use %s for username placeholder)
	// Example: "(uid=%s)" or "(cn=%s)" or "(&(objectClass=person)(uid=%s))"
	UserFilter string `json:"userFilter"`

	// GroupFilter is the filter to find user groups (use %s for user DN placeholder)
	// Example: "(member=%s)" or "(memberUid=%s)"
	GroupFilter string `json:"groupFilter,omitempty"`

	// GroupBaseDN is the base DN for group searches (defaults to BaseDN)
	GroupBaseDN string `json:"groupBaseDN,omitempty"`

	// Attributes to retrieve from LDAP
	Attributes LDAPAttributes `json:"attributes,omitempty"`

	// UseTLS enables StartTLS
	UseTLS bool `json:"useTLS,omitempty"`

	// InsecureSkipVerify skips TLS certificate verification
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`

	// ConnectionTimeout is the connection timeout
	ConnectionTimeout time.Duration `json:"connectionTimeout,omitempty"`

	// PoolSize is the number of connections in the pool (default: 10)
	PoolSize int `json:"poolSize,omitempty"`

	// Audience is the expected audience for tokens (optional)
	Audience string `json:"audience,omitempty"`
}

// LDAPAttributes maps LDAP attribute names
type LDAPAttributes struct {
	Email       string `json:"email,omitempty"`       // Default: mail
	DisplayName string `json:"displayName,omitempty"` // Default: cn
	Groups      string `json:"groups,omitempty"`      // Default: memberOf
	UID         string `json:"uid,omitempty"`         // Default: uid
}

// connectionPool manages a pool of LDAP connections for reuse
type connectionPool struct {
	conns  chan *ldap.Conn
	mu     sync.Mutex
	size   int
	closed uint32 // atomic flag: 1 if closed, 0 if open
}

// LDAPProvider implements the IdentityProvider interface for LDAP
type LDAPProvider struct {
	name        string
	config      *LDAPConfig
	initialized bool
	mu          sync.RWMutex
	pool        *connectionPool
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
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.initialized {
		return fmt.Errorf("LDAP provider already initialized")
	}

	cfg := &LDAPConfig{}

	// Check if input is already the correct struct type
	if c, ok := config.(*LDAPConfig); ok {
		cfg = c
	} else {
		// Parse from map using mapstructure with weak typing and time duration hook
		decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
			DecodeHook: mapstructure.ComposeDecodeHookFunc(
				mapstructure.StringToTimeDurationHookFunc(),
			),
			Result:           cfg,
			TagName:          "json",
			WeaklyTypedInput: true,
		})
		if err != nil {
			return fmt.Errorf("failed to create config decoder: %w", err)
		}

		if err := decoder.Decode(config); err != nil {
			return fmt.Errorf("failed to decode LDAP configuration: %w", err)
		}
	}

	// Validate required fields
	if cfg.Server == "" {
		return fmt.Errorf("LDAP server URL is required")
	}
	if cfg.BaseDN == "" {
		return fmt.Errorf("LDAP base DN is required")
	}
	if cfg.UserFilter == "" {
		cfg.UserFilter = "(cn=%s)" // Default filter
	}

	// Warn if BindDN is configured but BindPassword is empty
	if cfg.BindDN != "" && cfg.BindPassword == "" {
		glog.Warningf("LDAP provider '%s' configured with BindDN but no BindPassword", p.name)
	}

	// Warn if InsecureSkipVerify is enabled
	if cfg.InsecureSkipVerify {
		glog.Warningf("LDAP provider '%s' has InsecureSkipVerify enabled. Do not use in production.", p.name)
	}

	// Set default attributes
	if cfg.Attributes.Email == "" {
		cfg.Attributes.Email = "mail"
	}
	if cfg.Attributes.DisplayName == "" {
		cfg.Attributes.DisplayName = "cn"
	}
	if cfg.Attributes.Groups == "" {
		cfg.Attributes.Groups = "memberOf"
	}
	if cfg.Attributes.UID == "" {
		cfg.Attributes.UID = "uid"
	}
	if cfg.GroupBaseDN == "" {
		cfg.GroupBaseDN = cfg.BaseDN
	}
	if cfg.ConnectionTimeout == 0 {
		cfg.ConnectionTimeout = 10 * time.Second
	}

	p.config = cfg

	// Initialize connection pool (default size: 10 connections)
	poolSize := 10
	if cfg.PoolSize > 0 {
		poolSize = cfg.PoolSize
	}
	p.pool = &connectionPool{
		conns: make(chan *ldap.Conn, poolSize),
		size:  poolSize,
	}

	p.initialized = true

	glog.V(1).Infof("LDAP provider '%s' initialized: server=%s, baseDN=%s",
		p.name, cfg.Server, cfg.BaseDN)

	return nil
}

// getConnection gets a connection from the pool or creates a new one
func (p *LDAPProvider) getConnection() (*ldap.Conn, error) {
	// Try to get a connection from the pool (non-blocking)
	select {
	case conn := <-p.pool.conns:
		// Test if connection is still alive
		if conn != nil && conn.IsClosing() {
			conn.Close()
			// Connection is dead, create a new one
			return p.createConnection()
		}
		return conn, nil
	default:
		// Pool is empty, create a new connection
		return p.createConnection()
	}
}

// returnConnection returns a connection to the pool
func (p *LDAPProvider) returnConnection(conn *ldap.Conn) {
	if conn == nil || conn.IsClosing() {
		if conn != nil {
			conn.Close()
		}
		return
	}

	// Check if pool is closed before attempting to send
	if atomic.LoadUint32(&p.pool.closed) == 1 {
		conn.Close()
		return
	}

	// Try to return to pool (non-blocking)
	select {
	case p.pool.conns <- conn:
		// Successfully returned to pool
	default:
		// Pool is full, close the connection
		conn.Close()
	}
}

// createConnection establishes a new connection to the LDAP server
func (p *LDAPProvider) createConnection() (*ldap.Conn, error) {
	var conn *ldap.Conn
	var err error

	// Create dialer with timeout
	dialer := &net.Dialer{Timeout: p.config.ConnectionTimeout}

	// Parse server URL
	if strings.HasPrefix(p.config.Server, "ldaps://") {
		// LDAPS connection
		tlsConfig := &tls.Config{
			InsecureSkipVerify: p.config.InsecureSkipVerify,
			MinVersion:         tls.VersionTLS12,
		}
		conn, err = ldap.DialURL(p.config.Server, ldap.DialWithDialer(dialer), ldap.DialWithTLSConfig(tlsConfig))
	} else {
		// LDAP connection
		conn, err = ldap.DialURL(p.config.Server, ldap.DialWithDialer(dialer))
		if err == nil && p.config.UseTLS {
			// StartTLS
			tlsConfig := &tls.Config{
				InsecureSkipVerify: p.config.InsecureSkipVerify,
				MinVersion:         tls.VersionTLS12,
			}
			if err = conn.StartTLS(tlsConfig); err != nil {
				conn.Close()
				return nil, fmt.Errorf("failed to start TLS: %w", err)
			}
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to LDAP server: %w", err)
	}

	return conn, nil
}

// Close closes all connections in the pool
func (p *LDAPProvider) Close() error {
	if p.pool == nil {
		return nil
	}

	// Atomically mark pool as closed to prevent new connections being returned
	if !atomic.CompareAndSwapUint32(&p.pool.closed, 0, 1) {
		// Already closed
		return nil
	}

	p.pool.mu.Lock()
	defer p.pool.mu.Unlock()

	// Now safe to close the channel since closed flag prevents new sends
	close(p.pool.conns)
	for conn := range p.pool.conns {
		if conn != nil {
			conn.Close()
		}
	}
	return nil
}

// Authenticate authenticates a user with username:password credentials
func (p *LDAPProvider) Authenticate(ctx context.Context, credentials string) (*providers.ExternalIdentity, error) {
	p.mu.RLock()
	if !p.initialized {
		p.mu.RUnlock()
		return nil, fmt.Errorf("LDAP provider not initialized")
	}
	config := p.config
	p.mu.RUnlock()

	// Parse credentials (username:password format)
	parts := strings.SplitN(credentials, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid credentials format (expected username:password)")
	}
	username, password := parts[0], parts[1]

	if username == "" || password == "" {
		return nil, fmt.Errorf("username and password are required")
	}

	// Get connection from pool
	conn, err := p.getConnection()
	if err != nil {
		return nil, err
	}
	// Note: defer returnConnection moved to after rebinding to service account

	// First, bind with service account to search for user
	if config.BindDN != "" {
		err = conn.Bind(config.BindDN, config.BindPassword)
		if err != nil {
			glog.V(2).Infof("LDAP service bind failed: %v", err)
			conn.Close() // Close on error, don't return to pool
			return nil, fmt.Errorf("LDAP service bind failed: %w", err)
		}
	}

	// Search for the user
	userFilter := fmt.Sprintf(config.UserFilter, ldap.EscapeFilter(username))
	searchRequest := ldap.NewSearchRequest(
		config.BaseDN,
		ldap.ScopeWholeSubtree,
		ldap.NeverDerefAliases,
		1, // Size limit
		int(config.ConnectionTimeout.Seconds()),
		false,
		userFilter,
		[]string{"dn", config.Attributes.Email, config.Attributes.DisplayName, config.Attributes.UID, config.Attributes.Groups},
		nil,
	)

	result, err := conn.Search(searchRequest)
	if err != nil {
		glog.V(2).Infof("LDAP user search failed: %v", err)
		conn.Close() // Close on error
		return nil, fmt.Errorf("LDAP user search failed: %w", err)
	}

	if len(result.Entries) == 0 {
		conn.Close() // Close on error
		return nil, fmt.Errorf("user not found")
	}
	if len(result.Entries) > 1 {
		conn.Close() // Close on error
		return nil, fmt.Errorf("multiple users found")
	}

	userEntry := result.Entries[0]
	userDN := userEntry.DN

	// Bind as the user to verify password
	err = conn.Bind(userDN, password)
	if err != nil {
		glog.V(2).Infof("LDAP user bind failed for %s: %v", username, err)
		conn.Close() // Close on error, don't return to pool
		return nil, fmt.Errorf("authentication failed: invalid credentials")
	}

	// Rebind to service account before returning connection to pool
	// This prevents pool corruption from authenticated user binds
	if config.BindDN != "" {
		if err = conn.Bind(config.BindDN, config.BindPassword); err != nil {
			glog.V(2).Infof("LDAP rebind to service account failed: %v", err)
			conn.Close() // Close on error, don't return to pool
			return nil, fmt.Errorf("LDAP service account rebind failed after successful user authentication (check bindDN %q and its credentials): %w", config.BindDN, err)
		}
	}
	// Now safe to defer return to pool with clean service account binding
	defer p.returnConnection(conn)

	// Build identity from LDAP attributes
	identity := &providers.ExternalIdentity{
		UserID:      username,
		Email:       userEntry.GetAttributeValue(config.Attributes.Email),
		DisplayName: userEntry.GetAttributeValue(config.Attributes.DisplayName),
		Groups:      userEntry.GetAttributeValues(config.Attributes.Groups),
		Provider:    p.name,
		Attributes: map[string]string{
			"dn":  userDN,
			"uid": userEntry.GetAttributeValue(config.Attributes.UID),
		},
	}

	// If no groups from memberOf, try group search
	if len(identity.Groups) == 0 && config.GroupFilter != "" {
		groups, err := p.searchUserGroups(conn, userDN, config)
		if err != nil {
			glog.V(2).Infof("Group search failed for %s: %v", username, err)
		} else {
			identity.Groups = groups
		}
	}

	glog.V(2).Infof("LDAP authentication successful for user: %s, groups: %v", username, identity.Groups)
	return identity, nil
}

// searchUserGroups searches for groups the user belongs to
func (p *LDAPProvider) searchUserGroups(conn *ldap.Conn, userDN string, config *LDAPConfig) ([]string, error) {
	groupFilter := fmt.Sprintf(config.GroupFilter, ldap.EscapeFilter(userDN))
	searchRequest := ldap.NewSearchRequest(
		config.GroupBaseDN,
		ldap.ScopeWholeSubtree,
		ldap.NeverDerefAliases,
		0,
		int(config.ConnectionTimeout.Seconds()),
		false,
		groupFilter,
		[]string{"cn", "dn"},
		nil,
	)

	result, err := conn.Search(searchRequest)
	if err != nil {
		return nil, err
	}

	var groups []string
	for _, entry := range result.Entries {
		cn := entry.GetAttributeValue("cn")
		if cn != "" {
			groups = append(groups, cn)
		}
	}

	return groups, nil
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
	// Note: defer returnConnection moved to after bind

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
		int(config.ConnectionTimeout.Seconds()),
		false,
		userFilter,
		[]string{"dn", config.Attributes.Email, config.Attributes.DisplayName, config.Attributes.UID, config.Attributes.Groups},
		nil,
	)

	result, err := conn.Search(searchRequest)
	if err != nil {
		return nil, fmt.Errorf("LDAP user search failed: %w", err)
	}

	if len(result.Entries) == 0 {
		return nil, fmt.Errorf("user not found")
	}
	if len(result.Entries) > 1 {
		return nil, fmt.Errorf("multiple users found")
	}

	userEntry := result.Entries[0]
	identity := &providers.ExternalIdentity{
		UserID:      userID,
		Email:       userEntry.GetAttributeValue(config.Attributes.Email),
		DisplayName: userEntry.GetAttributeValue(config.Attributes.DisplayName),
		Groups:      userEntry.GetAttributeValues(config.Attributes.Groups),
		Provider:    p.name,
		Attributes: map[string]string{
			"dn":  userEntry.DN,
			"uid": userEntry.GetAttributeValue(config.Attributes.UID),
		},
	}

	// If no groups from memberOf, try group search
	if len(identity.Groups) == 0 && config.GroupFilter != "" {
		groups, err := p.searchUserGroups(conn, userEntry.DN, config)
		if err != nil {
			glog.V(2).Infof("Group search failed for %s: %v", userID, err)
		} else {
			identity.Groups = groups
		}
	}

	return identity, nil
}

// ValidateToken validates credentials (username:password format) and returns claims
func (p *LDAPProvider) ValidateToken(ctx context.Context, token string) (*providers.TokenClaims, error) {
	identity, err := p.Authenticate(ctx, token)
	if err != nil {
		return nil, err
	}

	p.mu.RLock()
	config := p.config
	p.mu.RUnlock()

	// If audience is configured, validate it (consistent with OIDC approach)
	audience := p.name
	if config.Audience != "" {
		audience = config.Audience
	}

	// Populate standard TokenClaims fields for interface compliance
	now := time.Now()
	ttl := 1 * time.Hour // Default TTL for LDAP tokens

	return &providers.TokenClaims{
		Subject:   identity.UserID,
		Issuer:    p.name,
		Audience:  audience,
		IssuedAt:  now,
		ExpiresAt: now.Add(ttl),
		Claims: map[string]interface{}{
			"email":    identity.Email,
			"name":     identity.DisplayName,
			"groups":   identity.Groups,
			"dn":       identity.Attributes["dn"],
			"provider": p.name,
		},
	}, nil
}

// IsInitialized returns whether the provider is initialized
func (p *LDAPProvider) IsInitialized() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.initialized
}
