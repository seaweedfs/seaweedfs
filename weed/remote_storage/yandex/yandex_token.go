package yandex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
)

const (
	// yandexAuthURL and yandexTokenURL are the Yandex ID OAuth endpoints. They
	// are constants rather than configuration: a remote configuration written to
	// /etc/remote may come from an untrusted caller, and a token endpoint it
	// named would carry the client secret and the refresh token to that host.
	yandexAuthURL  = "https://oauth.yandex.ru/authorize"
	yandexTokenURL = "https://oauth.yandex.ru/token"

	// RedirectURI is the debug redirect that shows the authorization code for
	// the operator to copy. An OAuth app registered for the code flow with this
	// redirect URI works without a browser on the server.
	RedirectURI = "https://oauth.yandex.ru/verification_code"

	// authTokenPrefix selects the OAuth token scheme the Disk API expects:
	// "Authorization: OAuth <token>". The API answers "Bearer" tokens as well,
	// but the documented form is OAuth.
	authTokenPrefix = "OAuth "
)

// tokenRequestTimeout bounds one token request (a refresh or a code
// exchange). Token holds the source's lock while it refreshes, so a token
// endpoint that stops answering must not block every client of the
// configuration. It is a variable so a test can shorten it.
var tokenRequestTimeout = 30 * time.Second

// tokenHTTPClient is the client a token request goes through: the given one
// (or the default) with a per-request timeout. The timeout lives on the
// client, not on a context, because the oauth2 token source keeps its context
// for every later refresh.
func tokenHTTPClient(httpClient *http.Client) *http.Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	bounded := *httpClient
	if bounded.Timeout == 0 || bounded.Timeout > tokenRequestTimeout {
		bounded.Timeout = tokenRequestTimeout
	}
	return &bounded
}

// yandexOAuthEndpoint is the token exchange endpoint. AuthStyleInParams sends
// the client id and secret in the request body, the documented form; the
// default auto-detection would probe the Basic form first and cost an extra
// round trip on the first refresh.
var yandexOAuthEndpoint = oauth2.Endpoint{
	AuthURL:   yandexAuthURL,
	TokenURL:  yandexTokenURL,
	AuthStyle: oauth2.AuthStyleInParams,
}

// parseToken reads the yandex_token field: an OAuth token JSON document
// ({"access_token","refresh_token","token_type","expiry"}, the same shape
// rclone stores) or a bare access token. A bare token cannot be refreshed.
func parseToken(raw string) (*oauth2.Token, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("yandex token is empty")
	}
	if strings.HasPrefix(raw, "{") {
		token := &oauth2.Token{}
		if err := json.Unmarshal([]byte(raw), token); err != nil {
			return nil, fmt.Errorf("parse yandex token json: %w", err)
		}
		if token.AccessToken == "" && token.RefreshToken == "" {
			return nil, fmt.Errorf("yandex token json carries neither access_token nor refresh_token")
		}
		return token, nil
	}
	return &oauth2.Token{AccessToken: raw, TokenType: "bearer"}, nil
}

// MarshalToken renders a token in the form parseToken reads back, for
// remote.configure to store after an OAuth code exchange.
func MarshalToken(token *oauth2.Token) (string, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return "", fmt.Errorf("encode yandex token: %w", err)
	}
	return string(data), nil
}

// AuthCodeURL is the page an operator opens to authorize an OAuth app with the
// code flow. The app must be registered with the same redirect URI.
func AuthCodeURL(clientID string) string {
	conf := &oauth2.Config{
		ClientID:    clientID,
		RedirectURL: RedirectURI,
		Endpoint:    yandexOAuthEndpoint,
	}
	return conf.AuthCodeURL("seaweedfs")
}

// ExchangeCode trades an authorization code for an OAuth token and returns it
// in the stored JSON form. httpClient, when set, carries the token exchange.
func ExchangeCode(ctx context.Context, clientID, clientSecret, code string, httpClient *http.Client, tokenURL string) (string, error) {
	if clientID == "" || clientSecret == "" {
		return "", fmt.Errorf("a yandex code exchange needs both -yandex.client_id and -yandex.client_secret")
	}
	if code == "" {
		return "", fmt.Errorf("no yandex authorization code given")
	}
	if tokenURL == "" {
		tokenURL = yandexTokenURL
	}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, tokenHTTPClient(httpClient))
	conf := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  RedirectURI,
		Endpoint:     oauth2.Endpoint{TokenURL: tokenURL, AuthStyle: oauth2.AuthStyleInParams},
	}
	token, err := conf.Exchange(ctx, code)
	if err != nil {
		return "", fmt.Errorf("exchange yandex authorization code: %w", err)
	}
	if token.RefreshToken == "" {
		glog.Warningf("yandex returned no refresh token: the configuration has to be reissued when the access token expires")
	}
	return MarshalToken(token)
}

// tokenSource caches the access token and refreshes it when it expires or when
// a request reports it stale. A token without a refresh token is static.
type tokenSource struct {
	mu   sync.Mutex
	conf *oauth2.Config // nil when the token cannot be refreshed
	tok  *oauth2.Token
	src  oauth2.TokenSource
	ctx  context.Context
}

func newTokenSource(conf *remote_pb.RemoteConf, httpClient *http.Client, tokenURL string) (*tokenSource, error) {
	token, err := parseToken(conf.GetYandexToken())
	if err != nil {
		return nil, err
	}
	ts := &tokenSource{tok: token}
	if token.RefreshToken == "" || conf.GetYandexClientId() == "" || conf.GetYandexClientSecret() == "" {
		return ts, nil
	}
	if tokenURL == "" {
		tokenURL = yandexTokenURL
	}
	ts.ctx = context.WithValue(context.Background(), oauth2.HTTPClient, tokenHTTPClient(httpClient))
	ts.conf = &oauth2.Config{
		ClientID:     conf.GetYandexClientId(),
		ClientSecret: conf.GetYandexClientSecret(),
		Endpoint:     oauth2.Endpoint{TokenURL: tokenURL, AuthStyle: oauth2.AuthStyleInParams},
	}
	ts.src = ts.conf.TokenSource(ts.ctx, token)
	return ts, nil
}

// Token returns a usable access token. The token is refreshed only when it is
// known to have expired: a refresh token is rotated by Yandex on every
// refresh, and a refresh at build time would let one process rotate the token
// another process still holds.
func (ts *tokenSource) Token() (string, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.src == nil {
		if ts.tok.AccessToken == "" {
			return "", fmt.Errorf("yandex token has no access token")
		}
		return ts.tok.AccessToken, nil
	}
	token, err := ts.src.Token()
	if err != nil {
		return "", refreshError(err)
	}
	ts.tok = token
	return token.AccessToken, nil
}

// Invalidate marks the rejected access token stale so the next Token call
// refreshes it. The API answered 401: the token is rejected whatever its
// stored expiry says. Requests that were rejected with a token another
// request has already replaced refresh nothing, so concurrent 401s cost one
// refresh.
func (ts *tokenSource) Invalidate(rejected string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.src == nil || ts.tok.AccessToken != rejected {
		return
	}
	ts.src = ts.conf.TokenSource(ts.ctx, &oauth2.Token{
		RefreshToken: ts.tok.RefreshToken,
		Expiry:       time.Now().Add(-time.Minute),
	})
}

// refreshError explains a failed refresh in the terms the operator acts on: a
// rotated or revoked refresh token needs a new one from the OAuth flow.
func refreshError(err error) error {
	var retrieveErr *oauth2.RetrieveError
	if errors.As(err, &retrieveErr) && strings.Contains(string(retrieveErr.Body), "invalid_grant") {
		return fmt.Errorf("yandex refresh token is expired or revoked; issue a new token with remote.configure -yandex.code=<code> or -yandex.token=<token>: %w", err)
	}
	return fmt.Errorf("refresh yandex token: %w", err)
}

// tokenSources caches one token source per configuration within the process,
// so the plain and the guarded client of a configuration share it and never
// refresh the same refresh token twice.
var (
	tokenSourcesMu sync.Mutex
	tokenSources   = map[string]*tokenSource{}
)

func sharedTokenSource(conf *remote_pb.RemoteConf, httpClient *http.Client, tokenURL string) (*tokenSource, error) {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		conf.GetName(), conf.GetYandexToken(), conf.GetYandexClientId(), conf.GetYandexClientSecret(),
	}, "\x00")))
	key := hex.EncodeToString(sum[:16])

	tokenSourcesMu.Lock()
	defer tokenSourcesMu.Unlock()
	if ts, found := tokenSources[key]; found {
		return ts, nil
	}
	ts, err := newTokenSource(conf, httpClient, tokenURL)
	if err != nil {
		return nil, err
	}
	tokenSources[key] = ts
	return ts, nil
}

// ResetTokenSources drops the cached token sources. For tests.
func ResetTokenSources() {
	tokenSourcesMu.Lock()
	defer tokenSourcesMu.Unlock()
	tokenSources = map[string]*tokenSource{}
}
