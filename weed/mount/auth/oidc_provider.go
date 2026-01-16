package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type OIDCConfig struct {
	IssuerURL        string
	ClientID         string
	ClientSecretFile string
	Scopes           []string
	HTTPClient       *http.Client
}

type OIDCTokenProvider struct {
	tokenSource oauth2.TokenSource
}

type oidcDiscovery struct {
	TokenEndpoint string `json:"token_endpoint"`
}

func NewOIDCTokenProvider(ctx context.Context, cfg OIDCConfig) (*OIDCTokenProvider, error) {
	if cfg.IssuerURL == "" {
		return nil, fmt.Errorf("oidc: issuer URL is required")
	}
	if cfg.ClientID == "" {
		return nil, fmt.Errorf("oidc: client ID is required")
	}
	if cfg.ClientSecretFile == "" {
		return nil, fmt.Errorf("oidc: client secret file is required")
	}
	secretBytes, err := os.ReadFile(cfg.ClientSecretFile)
	if err != nil {
		return nil, fmt.Errorf("oidc: failed to read client secret file: %w", err)
	}
	clientSecret := strings.TrimSpace(string(secretBytes))
	if clientSecret == "" {
		return nil, fmt.Errorf("oidc: client secret file is empty")
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	glog.V(1).Infof("oidc: discovering token endpoint from %s", cfg.IssuerURL)
	tokenEndpoint, err := discoverTokenEndpoint(ctx, httpClient, cfg.IssuerURL)
	if err != nil {
		glog.Errorf("oidc: discovery failed: %v", err)
		return nil, err
	}
	glog.V(1).Infof("oidc: discovered token endpoint: %s", tokenEndpoint)
	ccConfig := clientcredentials.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: clientSecret,
		TokenURL:     tokenEndpoint,
		Scopes:       cfg.Scopes,
	}
	ctxWithClient := context.WithValue(ctx, oauth2.HTTPClient, httpClient)
	tokenSource := ccConfig.TokenSource(ctxWithClient)
	return &OIDCTokenProvider{tokenSource: tokenSource}, nil
}

func discoverTokenEndpoint(ctx context.Context, client *http.Client, issuerURL string) (string, error) {
	discoveryURL := strings.TrimSuffix(issuerURL, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL, nil)
	if err != nil {
		return "", fmt.Errorf("oidc: failed to create discovery request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("oidc: discovery request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("oidc: discovery failed with status %d", resp.StatusCode)
	}
	var discovery oidcDiscovery
	if err := json.NewDecoder(resp.Body).Decode(&discovery); err != nil {
		return "", fmt.Errorf("oidc: failed to parse discovery response: %w", err)
	}
	if discovery.TokenEndpoint == "" {
		return "", fmt.Errorf("oidc: token_endpoint not found in discovery response")
	}
	return discovery.TokenEndpoint, nil
}

func (p *OIDCTokenProvider) GetToken(ctx context.Context) (string, error) {
	token, err := p.tokenSource.Token()
	if err != nil {
		glog.Errorf("oidc: failed to get token: %v", err)
		return "", fmt.Errorf("oidc: failed to get token: %w", err)
	}
	glog.V(2).Infof("oidc: obtained token (expires: %v)", token.Expiry)
	return token.AccessToken, nil
}
