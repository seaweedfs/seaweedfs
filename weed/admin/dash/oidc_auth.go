package dash

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/sessions"
	iamoidc "github.com/seaweedfs/seaweedfs/weed/iam/oidc"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"golang.org/x/oauth2"
)

const (
	oidcSessionStateKey    = "oidc_state"
	oidcSessionNonceKey    = "oidc_nonce"
	oidcSessionIssuedAtKey = "oidc_issued_at_unix"
	oidcStateTTL           = 10 * time.Minute
)

type OIDCRoleMappingRuleConfig struct {
	Claim string `mapstructure:"claim"`
	Value string `mapstructure:"value"`
	Role  string `mapstructure:"role"`
}

type OIDCRoleMappingConfig struct {
	DefaultRole string                      `mapstructure:"default_role"`
	Rules       []OIDCRoleMappingRuleConfig `mapstructure:"rules"`
}

type OIDCAuthConfig struct {
	Enabled               bool                  `mapstructure:"enabled"`
	Issuer                string                `mapstructure:"issuer"`
	ClientID              string                `mapstructure:"client_id"`
	ClientSecret          string                `mapstructure:"client_secret"`
	RedirectURL           string                `mapstructure:"redirect_url"`
	Scopes                []string              `mapstructure:"scopes"`
	JWKSURI               string                `mapstructure:"jwks_uri"`
	TLSCACert             string                `mapstructure:"tls_ca_cert"`
	TLSInsecureSkipVerify bool                  `mapstructure:"tls_insecure_skip_verify"`
	RoleMapping           OIDCRoleMappingConfig `mapstructure:"role_mapping"`
}

type oidcDiscoveryDocument struct {
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	JWKSURI               string `json:"jwks_uri"`
}

type OIDCLoginResult struct {
	Username        string
	Role            string
	TokenExpiration *time.Time
}

type OIDCAuthService struct {
	config      OIDCAuthConfig
	roleMapping *providers.RoleMapping
	httpClient  *http.Client
	oauthConfig *oauth2.Config
	validator   *iamoidc.OIDCProvider
}

func NewOIDCAuthService(config OIDCAuthConfig) (*OIDCAuthService, error) {
	if !config.Enabled {
		return nil, nil
	}

	normalized := normalizeOIDCAuthConfig(config)
	if err := normalized.Validate(); err != nil {
		return nil, err
	}

	httpClient, err := createOIDCHTTPClient(normalized)
	if err != nil {
		return nil, err
	}

	discovery, err := fetchOIDCDiscoveryDocument(httpClient, normalized.Issuer)
	if err != nil {
		return nil, err
	}

	jwksURI := normalized.JWKSURI
	if jwksURI == "" {
		jwksURI = discovery.JWKSURI
	}

	roleMapping := normalized.toRoleMapping()

	validator := iamoidc.NewOIDCProvider("admin-ui-oidc")
	if err := validator.Initialize(&iamoidc.OIDCConfig{
		Issuer:                normalized.Issuer,
		ClientID:              normalized.ClientID,
		ClientSecret:          normalized.ClientSecret,
		JWKSUri:               jwksURI,
		Scopes:                normalized.EffectiveScopes(),
		RoleMapping:           roleMapping,
		TLSCACert:             normalized.TLSCACert,
		TLSInsecureSkipVerify: normalized.TLSInsecureSkipVerify,
	}); err != nil {
		return nil, fmt.Errorf("initialize OIDC token validator: %w", err)
	}

	return &OIDCAuthService{
		config:      normalized,
		roleMapping: roleMapping,
		httpClient:  httpClient,
		oauthConfig: &oauth2.Config{
			ClientID:     normalized.ClientID,
			ClientSecret: normalized.ClientSecret,
			RedirectURL:  normalized.RedirectURL,
			Scopes:       normalized.EffectiveScopes(),
			Endpoint: oauth2.Endpoint{
				AuthURL:  discovery.AuthorizationEndpoint,
				TokenURL: discovery.TokenEndpoint,
			},
		},
		validator: validator,
	}, nil
}

func (c OIDCAuthConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.Issuer == "" {
		return fmt.Errorf("admin.oidc.issuer is required when OIDC is enabled")
	}
	if c.ClientID == "" {
		return fmt.Errorf("admin.oidc.client_id is required when OIDC is enabled")
	}
	if c.ClientSecret == "" {
		return fmt.Errorf("admin.oidc.client_secret is required when OIDC is enabled")
	}
	if c.RedirectURL == "" {
		return fmt.Errorf("admin.oidc.redirect_url is required when OIDC is enabled")
	}

	redirectURL, err := url.Parse(c.RedirectURL)
	if err != nil {
		return fmt.Errorf("admin.oidc.redirect_url is invalid: %w", err)
	}
	if !redirectURL.IsAbs() {
		return fmt.Errorf("admin.oidc.redirect_url must be absolute")
	}

	if c.TLSCACert != "" && !filepath.IsAbs(c.TLSCACert) {
		return fmt.Errorf("admin.oidc.tls_ca_cert must be an absolute path")
	}

	if len(c.RoleMapping.Rules) == 0 && c.RoleMapping.DefaultRole == "" {
		return fmt.Errorf("admin.oidc.role_mapping must include at least one rule or default_role")
	}

	if c.RoleMapping.DefaultRole != "" && !isSupportedAdminRole(c.RoleMapping.DefaultRole) {
		return fmt.Errorf("admin.oidc.role_mapping.default_role must be one of: admin, readonly")
	}

	for i, rule := range c.RoleMapping.Rules {
		if strings.TrimSpace(rule.Claim) == "" {
			return fmt.Errorf("admin.oidc.role_mapping.rules[%d].claim is required", i)
		}
		if strings.TrimSpace(rule.Value) == "" {
			return fmt.Errorf("admin.oidc.role_mapping.rules[%d].value is required", i)
		}
		if !isSupportedAdminRole(rule.Role) {
			return fmt.Errorf("admin.oidc.role_mapping.rules[%d].role must be one of: admin, readonly", i)
		}
	}

	return nil
}

func (c OIDCAuthConfig) EffectiveScopes() []string {
	if len(c.Scopes) == 0 {
		return []string{"openid", "profile", "email"}
	}

	scopes := make([]string, 0, len(c.Scopes)+1)
	seen := make(map[string]struct{}, len(c.Scopes)+1)

	for _, scope := range c.Scopes {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			continue
		}
		if _, exists := seen[scope]; exists {
			continue
		}
		seen[scope] = struct{}{}
		scopes = append(scopes, scope)
	}

	if _, exists := seen["openid"]; !exists {
		scopes = append(scopes, "openid")
	}

	return scopes
}

func (c OIDCAuthConfig) toRoleMapping() *providers.RoleMapping {
	roleMapping := &providers.RoleMapping{
		DefaultRole: normalizeAdminRole(c.RoleMapping.DefaultRole),
	}

	for _, rule := range c.RoleMapping.Rules {
		roleMapping.Rules = append(roleMapping.Rules, providers.MappingRule{
			Claim: strings.TrimSpace(rule.Claim),
			Value: strings.TrimSpace(rule.Value),
			Role:  normalizeAdminRole(rule.Role),
		})
	}

	return roleMapping
}

func (s *OIDCAuthService) BeginLogin(session *sessions.Session, r *http.Request, w http.ResponseWriter) (string, error) {
	if s == nil {
		return "", fmt.Errorf("OIDC auth is not configured")
	}
	if session == nil {
		return "", fmt.Errorf("session is nil")
	}

	state, err := generateAuthFlowSecret()
	if err != nil {
		return "", fmt.Errorf("generate OIDC state: %w", err)
	}
	nonce, err := generateAuthFlowSecret()
	if err != nil {
		return "", fmt.Errorf("generate OIDC nonce: %w", err)
	}

	session.Values[oidcSessionStateKey] = state
	session.Values[oidcSessionNonceKey] = nonce
	session.Values[oidcSessionIssuedAtKey] = time.Now().Unix()
	if err := session.Save(r, w); err != nil {
		return "", fmt.Errorf("save OIDC login session state: %w", err)
	}

	return s.oauthConfig.AuthCodeURL(
		state,
		oauth2.AccessTypeOnline,
		oauth2.SetAuthURLParam("nonce", nonce),
	), nil
}

func (s *OIDCAuthService) CompleteLogin(session *sessions.Session, r *http.Request, w http.ResponseWriter) (*OIDCLoginResult, error) {
	if s == nil {
		return nil, fmt.Errorf("OIDC auth is not configured")
	}
	if session == nil {
		return nil, fmt.Errorf("session is nil")
	}

	if oidcError := strings.TrimSpace(r.URL.Query().Get("error")); oidcError != "" {
		description := strings.TrimSpace(r.URL.Query().Get("error_description"))
		if description != "" {
			return nil, fmt.Errorf("OIDC authorization failed: %s (%s)", oidcError, description)
		}
		return nil, fmt.Errorf("OIDC authorization failed: %s", oidcError)
	}

	state := strings.TrimSpace(r.URL.Query().Get("state"))
	code := strings.TrimSpace(r.URL.Query().Get("code"))
	if state == "" || code == "" {
		return nil, fmt.Errorf("missing OIDC callback state or code")
	}

	expectedState, _ := session.Values[oidcSessionStateKey].(string)
	expectedNonce, _ := session.Values[oidcSessionNonceKey].(string)
	issuedAtUnix, ok := sessionValueToInt64(session.Values[oidcSessionIssuedAtKey])
	if !ok {
		return nil, fmt.Errorf("missing OIDC login session state")
	}

	delete(session.Values, oidcSessionStateKey)
	delete(session.Values, oidcSessionNonceKey)
	delete(session.Values, oidcSessionIssuedAtKey)
	if err := session.Save(r, w); err != nil {
		return nil, fmt.Errorf("clear OIDC login session state: %w", err)
	}

	if expectedState == "" || expectedNonce == "" {
		return nil, fmt.Errorf("missing OIDC login session state")
	}
	if subtle.ConstantTimeCompare([]byte(state), []byte(expectedState)) != 1 {
		return nil, fmt.Errorf("invalid OIDC callback state")
	}
	if time.Since(time.Unix(issuedAtUnix, 0)) > oidcStateTTL {
		return nil, fmt.Errorf("OIDC callback has expired; please sign in again")
	}

	ctx := context.WithValue(r.Context(), oauth2.HTTPClient, s.httpClient)
	token, err := s.oauthConfig.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("exchange OIDC code for token: %w", err)
	}

	idToken, err := extractIDToken(token)
	if err != nil {
		return nil, err
	}

	claims, err := s.validator.ValidateToken(ctx, idToken)
	if err != nil {
		return nil, fmt.Errorf("validate OIDC ID token: %w", err)
	}

	nonce, ok := claims.GetClaimString("nonce")
	if !ok || subtle.ConstantTimeCompare([]byte(nonce), []byte(expectedNonce)) != 1 {
		return nil, fmt.Errorf("invalid OIDC token nonce")
	}

	mappedRoles := mapClaimsToRoles(claims, s.roleMapping)
	role, err := resolveAdminRole(mappedRoles)
	if err != nil {
		return nil, err
	}

	username := preferredOIDCUsername(claims)
	if username == "" {
		return nil, fmt.Errorf("OIDC token is missing a usable username claim")
	}

	result := &OIDCLoginResult{
		Username: username,
		Role:     role,
	}
	if !claims.ExpiresAt.IsZero() {
		expiresAt := claims.ExpiresAt
		result.TokenExpiration = &expiresAt
	}
	return result, nil
}

func createOIDCHTTPClient(config OIDCAuthConfig) (*http.Client, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.TLSInsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}

	if config.TLSCACert != "" {
		caCertBytes, err := os.ReadFile(config.TLSCACert)
		if err != nil {
			return nil, fmt.Errorf("read OIDC CA certificate: %w", err)
		}
		rootCAs, _ := x509.SystemCertPool()
		if rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}
		if !rootCAs.AppendCertsFromPEM(caCertBytes) {
			return nil, fmt.Errorf("append OIDC CA certificate from %s", config.TLSCACert)
		}
		tlsConfig.RootCAs = rootCAs
	}

	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}, nil
}

func fetchOIDCDiscoveryDocument(httpClient *http.Client, issuer string) (*oidcDiscoveryDocument, error) {
	discoveryURL := strings.TrimSuffix(issuer, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequest(http.MethodGet, discoveryURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build OIDC discovery request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch OIDC discovery document: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OIDC discovery returned status %d", resp.StatusCode)
	}

	var discovery oidcDiscoveryDocument
	if err := json.NewDecoder(resp.Body).Decode(&discovery); err != nil {
		return nil, fmt.Errorf("decode OIDC discovery document: %w", err)
	}

	if strings.TrimSpace(discovery.AuthorizationEndpoint) == "" {
		return nil, fmt.Errorf("OIDC discovery document is missing authorization_endpoint")
	}
	if strings.TrimSpace(discovery.TokenEndpoint) == "" {
		return nil, fmt.Errorf("OIDC discovery document is missing token_endpoint")
	}

	return &discovery, nil
}

func extractIDToken(token *oauth2.Token) (string, error) {
	if token == nil {
		return "", fmt.Errorf("OIDC token exchange returned no token")
	}

	rawIDToken := token.Extra("id_token")
	switch value := rawIDToken.(type) {
	case string:
		if strings.TrimSpace(value) == "" {
			return "", fmt.Errorf("OIDC token exchange returned an empty id_token")
		}
		return value, nil
	default:
		return "", fmt.Errorf("OIDC token exchange did not include id_token")
	}
}

func mapClaimsToRoles(claims *providers.TokenClaims, mapping *providers.RoleMapping) []string {
	if claims == nil || mapping == nil {
		return nil
	}

	roles := make([]string, 0, len(mapping.Rules)+1)
	seen := make(map[string]struct{}, len(mapping.Rules)+1)
	for _, rule := range mapping.Rules {
		if rule.Matches(claims) {
			role := normalizeAdminRole(rule.Role)
			if role == "" {
				continue
			}
			if _, exists := seen[role]; exists {
				continue
			}
			seen[role] = struct{}{}
			roles = append(roles, role)
		}
	}

	if len(roles) == 0 {
		defaultRole := normalizeAdminRole(mapping.DefaultRole)
		if defaultRole != "" {
			roles = append(roles, defaultRole)
		}
	}

	return roles
}

func resolveAdminRole(roles []string) (string, error) {
	hasReadonly := false
	for _, role := range roles {
		role = normalizeAdminRole(role)
		if role == "admin" {
			return "admin", nil
		}
		if role == "readonly" {
			hasReadonly = true
		}
	}
	if hasReadonly {
		return "readonly", nil
	}
	return "", fmt.Errorf("OIDC user does not map to an allowed admin role")
}

func preferredOIDCUsername(claims *providers.TokenClaims) string {
	if claims == nil {
		return ""
	}

	claimCandidates := []string{"preferred_username", "email", "name", "sub"}
	for _, key := range claimCandidates {
		if value, exists := claims.GetClaimString(key); exists && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}

	if strings.TrimSpace(claims.Subject) != "" {
		return strings.TrimSpace(claims.Subject)
	}
	return ""
}

func generateAuthFlowSecret() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func sessionValueToInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

func normalizeOIDCAuthConfig(config OIDCAuthConfig) OIDCAuthConfig {
	config.Issuer = strings.TrimSpace(config.Issuer)
	config.ClientID = strings.TrimSpace(config.ClientID)
	config.ClientSecret = strings.TrimSpace(config.ClientSecret)
	config.RedirectURL = strings.TrimSpace(config.RedirectURL)
	config.JWKSURI = strings.TrimSpace(config.JWKSURI)
	config.TLSCACert = strings.TrimSpace(config.TLSCACert)
	config.RoleMapping.DefaultRole = normalizeAdminRole(config.RoleMapping.DefaultRole)
	for i := range config.RoleMapping.Rules {
		config.RoleMapping.Rules[i].Claim = strings.TrimSpace(config.RoleMapping.Rules[i].Claim)
		config.RoleMapping.Rules[i].Value = strings.TrimSpace(config.RoleMapping.Rules[i].Value)
		config.RoleMapping.Rules[i].Role = normalizeAdminRole(config.RoleMapping.Rules[i].Role)
	}
	return config
}

func isSupportedAdminRole(role string) bool {
	switch normalizeAdminRole(role) {
	case "admin", "readonly":
		return true
	default:
		return false
	}
}

func normalizeAdminRole(role string) string {
	return strings.ToLower(strings.TrimSpace(role))
}
